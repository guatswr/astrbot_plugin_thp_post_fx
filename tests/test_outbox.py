"""SQLite outbox: idempotency, rate limit, capacity, atomic claim, retries, receipts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from services import outbox as ob
from services.delivery import BackoffSchedule

TTL = 600
CAPACITY = 1000


def payload(
    *,
    qq_id: str = "100000001",
    group_id: str = "123456789",
    message_id: str = "40001",
    content: str = "正文 🎉",
) -> dict[str, Any]:
    return {
        "event_id": "thp-demo-2026",
        "bot_id": "100000002",
        "group_id": group_id,
        "source_message_id": message_id,
        "qq_id": qq_id,
        "nickname": "样例昵称",
        "group_card": "样例群名片",
        "content": content,
        "source_timestamp": "2026-10-03T10:00:00Z",
    }


def key_for(data: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        data["event_id"],
        data["bot_id"],
        data["group_id"],
        data["source_message_id"],
    )


@pytest.fixture
async def store(tmp_path: Path) -> ob.Outbox:
    database = ob.Outbox(tmp_path / "outbox.db")
    await database.open()
    yield database
    await database.close()


async def prepare(
    store: ob.Outbox,
    data: dict[str, Any],
    *,
    clock,
    user_interval_seconds: int = 0,
    group_per_minute: int = 0,
    capacity: int = CAPACITY,
    origin: str = "http://203.0.113.10:18080",
    platform_id: str = "thp-bot-1",
    umo: str = "thp-bot-1:group:123456789",
) -> ob.PrepareResult:
    return await store.prepare_event(
        key=key_for(data),
        payload=data,
        origin_base_url=origin,
        platform_id=platform_id,
        unified_msg_origin=umo,
        ttl_seconds=TTL,
        capacity=capacity,
        user_interval_seconds=user_interval_seconds,
        group_per_minute=group_per_minute,
        now=clock(),
    )


# --- idempotency ------------------------------------------------------------


async def test_same_event_is_created_only_once(store: ob.Outbox, clock) -> None:
    data = payload()
    first = await prepare(store, data, clock=clock)
    second = await prepare(store, data, clock=clock)
    assert first.created and first.row is not None
    assert not second.created and second.duplicate
    assert second.row is not None
    assert second.row.key == first.row.key
    assert await store.count_active() == 1


async def test_same_body_with_a_different_message_id_is_a_new_submission(
    store: ob.Outbox, clock
) -> None:
    first = await prepare(store, payload(message_id="40001"), clock=clock)
    second = await prepare(store, payload(message_id="40002"), clock=clock)
    assert first.created
    assert second.created
    assert await store.count_active() == 2


async def test_duplicate_event_does_not_consume_rate_limit(store: ob.Outbox, clock) -> None:
    data = payload()
    first = await prepare(store, data, clock=clock, user_interval_seconds=10, group_per_minute=60)
    assert first.created
    duplicate = await prepare(
        store, data, clock=clock, user_interval_seconds=10, group_per_minute=60
    )
    assert duplicate.duplicate
    # A *different* message from the same user is now rate limited.
    blocked = await prepare(
        store,
        payload(message_id="40002"),
        clock=clock,
        user_interval_seconds=10,
        group_per_minute=60,
    )
    assert blocked.blocked == "rate_limited"


async def test_stored_payload_is_frozen(store: ob.Outbox, clock) -> None:
    data = payload()
    result = await prepare(store, data, clock=clock)
    assert result.row is not None
    assert result.row.payload == data
    assert result.row.payload_json == json.dumps(data, ensure_ascii=False, sort_keys=True)


# --- rate limiting ----------------------------------------------------------


async def test_per_user_interval_is_per_group(store: ob.Outbox, clock) -> None:
    data = payload(group_id="123456789", qq_id="100000001")
    assert (await prepare(store, data, clock=clock, user_interval_seconds=10)).created
    same_group = await prepare(
        store,
        payload(group_id="123456789", qq_id="100000001", message_id="40002"),
        clock=clock,
        user_interval_seconds=10,
    )
    assert same_group.blocked == "rate_limited"
    assert same_group.rate is not None
    assert same_group.rate.scope == "per_user"
    other_group = await prepare(
        store,
        payload(group_id="987654321", qq_id="100000001", message_id="40003"),
        clock=clock,
        user_interval_seconds=10,
    )
    assert other_group.created


async def test_per_user_interval_expires(store: ob.Outbox, clock) -> None:
    assert (await prepare(store, payload(), clock=clock, user_interval_seconds=10)).created
    clock.advance(10.1)
    assert (
        await prepare(store, payload(message_id="40002"), clock=clock, user_interval_seconds=10)
    ).created


async def test_group_per_minute_limit(store: ob.Outbox, clock) -> None:
    for index in range(3):
        result = await prepare(
            store,
            payload(qq_id=f"10000000{index}", message_id=f"4000{index}"),
            clock=clock,
            group_per_minute=3,
        )
        assert result.created, result
    blocked = await prepare(
        store,
        payload(qq_id="100000099", message_id="40099"),
        clock=clock,
        group_per_minute=3,
    )
    assert blocked.blocked == "rate_limited"
    assert blocked.rate is not None
    assert blocked.rate.scope == "per_group"
    assert blocked.rate.retry_after > 0
    clock.advance(61)
    assert (
        await prepare(
            store,
            payload(qq_id="100000099", message_id="40099"),
            clock=clock,
            group_per_minute=3,
        )
    ).created


async def test_disabled_limits_allow_everything(store: ob.Outbox, clock) -> None:
    for index in range(5):
        result = await prepare(
            store,
            payload(qq_id="100000001", message_id=f"4000{index}"),
            clock=clock,
            user_interval_seconds=0,
            group_per_minute=0,
        )
        assert result.created


# --- capacity ---------------------------------------------------------------


async def test_queue_full_rejects_new_rows_without_overwriting(store: ob.Outbox, clock) -> None:
    for index in range(2):
        assert (
            await prepare(store, payload(message_id=f"4000{index}"), clock=clock, capacity=2)
        ).created
    full = await prepare(store, payload(message_id="40099"), clock=clock, capacity=2)
    assert full.blocked == "queue_full"
    assert await store.count_active() == 2
    first = await store.get_by_key("thp-demo-2026", "100000002", "123456789", "40000")
    assert first is not None
    assert first.state == ob.PENDING


async def test_terminal_rows_do_not_consume_capacity(store: ob.Outbox, clock) -> None:
    assert (await prepare(store, payload(message_id="40000"), clock=clock, capacity=1)).created
    await store.mark_succeeded(
        ("thp-demo-2026", "100000002", "123456789", "40000"),
        submission_id=1,
        duplicate=False,
        now=clock(),
    )
    assert (await prepare(store, payload(message_id="40001"), clock=clock, capacity=1)).created


# --- claiming ---------------------------------------------------------------


async def claim(store: ob.Outbox, clock, **overrides: Any):
    params: dict[str, Any] = {
        "origin_base_url": "http://203.0.113.10:18080",
        "event_id": "thp-demo-2026",
        "allowed_group_ids": ("123456789",),
        "max_attempts": 20,
        "now": clock(),
    }
    params.update(overrides)
    return await store.claim_next(**params)


async def test_claim_marks_inflight_and_counts_the_attempt(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    claimed = await claim(store, clock)
    assert claimed is not None
    assert claimed.state == ob.INFLIGHT
    assert claimed.attempt_count == 1
    # A second worker must not be able to claim the same row.
    assert await claim(store, clock) is None


async def test_claim_respects_next_attempt_at(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    claimed = await claim(store, clock)
    assert claimed is not None
    await store.mark_retry(
        claimed.key,
        reason="timeout",
        error_code="timeout",
        diagnostic="simulated",
        attempt_count=1,
        max_attempts=20,
        deadline_at=claimed.deadline_at,
        schedule=BackoffSchedule(),
        now=clock(),
        jitter=lambda: 0.5,
    )
    assert await claim(store, clock) is None
    clock.advance(10)
    again = await claim(store, clock)
    assert again is not None
    assert again.attempt_count == 2


async def test_claim_never_picks_a_held_row(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    await store.set_held(key_for(payload()), reason=ob.HELD_TARGET_CHANGED, now=clock())
    assert await claim(store, clock) is None


# --- terminal outcomes ------------------------------------------------------


async def test_mark_succeeded_sets_id_and_queues_the_success_receipt(
    store: ob.Outbox, clock
) -> None:
    await prepare(store, payload(), clock=clock)
    claimed = await claim(store, clock)
    assert claimed is not None
    changed = await store.mark_succeeded(
        claimed.key,
        submission_id=321,
        duplicate=False,
        now=clock(),
        receipt_state=ob.RECEIPT_PENDING_SUCCESS,
        receipt_next_at=clock(),
    )
    assert changed
    row = await store.get_by_key(*claimed.key)
    assert row is not None
    assert row.state == ob.SUCCEEDED
    assert row.submission_id == 321
    assert row.receipt_state == ob.RECEIPT_PENDING_SUCCESS
    assert row.is_terminal


async def test_mark_succeeded_is_idempotent(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    assert await store.mark_succeeded(key, submission_id=1, duplicate=False, now=clock())
    assert not await store.mark_succeeded(key, submission_id=2, duplicate=False, now=clock())
    row = await store.get_by_key(*key)
    assert row is not None
    assert row.submission_id == 1


async def test_mark_retry_schedules_then_expires(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    claimed = await claim(store, clock)
    assert claimed is not None
    schedule = BackoffSchedule()
    outcome = await store.mark_retry(
        claimed.key,
        reason="timeout",
        error_code="timeout",
        diagnostic="simulated timeout",
        attempt_count=1,
        max_attempts=20,
        deadline_at=claimed.deadline_at,
        schedule=schedule,
        now=clock(),
        jitter=lambda: 0.5,
    )
    assert outcome == "scheduled"
    row = await store.get_by_key(*claimed.key)
    assert row is not None
    assert row.state == ob.PENDING
    assert row.next_attempt_at is not None
    assert row.next_attempt_at > clock()

    clock.advance(TTL + 1)
    outcome = await store.mark_retry(
        claimed.key,
        reason="timeout",
        error_code="timeout",
        diagnostic="simulated timeout",
        attempt_count=2,
        max_attempts=20,
        deadline_at=claimed.deadline_at,
        schedule=schedule,
        now=clock(),
        jitter=lambda: 0.5,
    )
    assert outcome == ob.REASON_TTL_EXPIRED
    row = await store.get_by_key(*claimed.key)
    assert row is not None
    assert row.state == ob.FAILED
    assert row.receipt_state == ob.RECEIPT_PENDING_STORED


async def test_mark_retry_stops_at_max_attempts(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    outcome = await store.mark_retry(
        key,
        reason="timeout",
        error_code="timeout",
        diagnostic="simulated",
        attempt_count=20,
        max_attempts=20,
        deadline_at=clock() + TTL,
        schedule=BackoffSchedule(),
        now=clock(),
        jitter=lambda: 0.5,
    )
    assert outcome == ob.REASON_MAX_ATTEMPTS
    row = await store.get_by_key(*key)
    assert row is not None and row.state == ob.FAILED


async def test_retry_after_past_the_deadline_terminates_instead_of_bypassing(
    store: ob.Outbox, clock
) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    outcome = await store.mark_retry(
        key,
        reason="rate_limited",
        error_code="rate_limited",
        diagnostic="server asked us to wait",
        attempt_count=1,
        max_attempts=20,
        deadline_at=clock() + 30,
        schedule=BackoffSchedule(),
        now=clock(),
        retry_after=120,
        jitter=lambda: 0.5,
    )
    assert outcome == ob.REASON_FINAL_RETRY_PAST_DEADLINE
    row = await store.get_by_key(*key)
    assert row is not None
    assert row.state == ob.FAILED
    assert "超过投递期限" in row.last_error


async def test_retry_after_within_the_deadline_is_honoured(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    outcome = await store.mark_retry(
        key,
        reason="rate_limited",
        error_code="rate_limited",
        diagnostic="rate limited",
        attempt_count=1,
        max_attempts=20,
        deadline_at=clock() + 300,
        schedule=BackoffSchedule(),
        now=clock(),
        retry_after=42,
        jitter=lambda: 0.5,
    )
    assert outcome == "scheduled"
    row = await store.get_by_key(*key)
    assert row is not None and row.next_attempt_at is not None
    assert row.next_attempt_at - clock() >= 42


async def test_overdue_rows_are_reported_for_expiry(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    assert await store.due_for_expiry(now=clock()) == []
    clock.advance(TTL + 1)
    due = await store.due_for_expiry(now=clock())
    assert len(due) == 1


# --- crash recovery ---------------------------------------------------------


async def test_stale_inflight_is_requeued_on_recovery(tmp_path: Path, clock, monkeypatch) -> None:
    # Pin the module clock to the fake one so lease arithmetic is deterministic.
    monkeypatch.setattr(ob, "_now", lambda: clock.now)
    database = ob.Outbox(tmp_path / "outbox.db")
    await database.open()
    await prepare(database, payload(), clock=clock)
    claimed = await claim(database, clock)
    assert claimed is not None
    # Simulate a crash: the process disappears without releasing the claim, so
    # only the lease expiry can rescue the row.
    clock.advance(120)
    await database.close()

    reopened = ob.Outbox(tmp_path / "outbox.db")
    await reopened.open()
    row = await reopened.get_by_key(*claimed.key)
    assert row is not None
    assert row.state == ob.PENDING
    assert row.last_error_code == ob.REASON_STALE_LEASE
    # The attempt counter is preserved so a crash loop cannot retry forever.
    assert row.attempt_count == 1
    await reopened.close()


async def test_unexpired_inflight_is_left_alone_on_recovery(
    tmp_path: Path, clock, monkeypatch
) -> None:
    monkeypatch.setattr(ob, "_now", lambda: clock.now)
    database = ob.Outbox(tmp_path / "outbox.db")
    await database.open()
    await prepare(database, payload(), clock=clock)
    claimed = await claim(database, clock, lease_seconds=600)
    assert claimed is not None
    await database.close()

    reopened = ob.Outbox(tmp_path / "outbox.db")
    await reopened.open()
    row = await reopened.get_by_key(*claimed.key)
    assert row is not None
    # The lease is still valid; the normal stale-lease sweeper takes over later.
    assert row.state == ob.INFLIGHT
    await reopened.close()


async def test_recover_stale_inflight_only_touches_expired_leases(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    claimed = await claim(store, clock, lease_seconds=600)
    assert claimed is not None
    assert await store.recover_stale_inflight(now=clock()) == 0
    row = await store.get_by_key(*claimed.key)
    assert row is not None and row.state == ob.INFLIGHT
    assert await store.recover_stale_inflight(now=clock() + 601) == 1


async def test_release_claim_restores_pending(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    claimed = await claim(store, clock)
    assert claimed is not None
    await store.release_claim(
        claimed.key,
        error_code="cancelled",
        diagnostic="shutdown",
        now=clock(),
        consumed_attempt=False,
    )
    row = await store.get_by_key(*claimed.key)
    assert row is not None
    assert row.state == ob.PENDING
    assert row.attempt_count == 0


# --- target / activity changes ---------------------------------------------


async def test_changed_origin_or_event_holds_old_rows(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(message_id="40001"), clock=clock)
    await prepare(
        store,
        payload(message_id="40002"),
        clock=clock,
        origin="http://198.51.100.9:18080",
    )
    claimed = await claim(store, clock)
    assert claimed is not None
    assert claimed.source_message_id == "40001"
    # The other row belongs to another backend and must be held, not delivered.
    other = await store.get_by_key("thp-demo-2026", "100000002", "123456789", "40002")
    assert other is not None
    assert other.state == ob.HELD
    assert other.held_reason == ob.HELD_TARGET_CHANGED
    assert other.next_attempt_at is None
    assert await claim(store, clock) is None


async def test_removed_group_is_held(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(group_id="123456789", message_id="40001"), clock=clock)
    await prepare(store, payload(group_id="987654321", message_id="40002"), clock=clock)
    claimed = await claim(store, clock, allowed_group_ids=("123456789",))
    assert claimed is not None
    removed = await store.get_by_key("thp-demo-2026", "100000002", "987654321", "40002")
    assert removed is not None
    assert removed.state == ob.HELD
    assert removed.held_reason == ob.HELD_GROUP_REMOVED


async def test_hold_is_not_undone_by_a_reverted_configuration(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(message_id="40001"), clock=clock)
    await claim(store, clock, origin_base_url="http://other.invalid:18080", event_id="other-event")
    row = await store.get_by_key("thp-demo-2026", "100000002", "123456789", "40001")
    assert row is not None and row.state == ob.HELD
    # Back to the original configuration: the held row stays parked.
    assert await claim(store, clock) is None


async def test_release_hold_requires_an_explicit_decision(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    await store.set_held(key, reason=ob.HELD_TARGET_CHANGED, now=clock())
    assert await store.release_hold(key, now=clock()) is False
    assert await store.release_hold(key, now=clock(), extend_ttl_seconds=TTL) is True
    row = await store.get_by_key(*key)
    assert row is not None
    assert row.state == ob.PENDING
    assert row.attempt_count == 0
    assert row.deadline_at > clock()


async def test_reset_for_retry_only_affects_failed_rows(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    assert await store.reset_for_retry(key, ttl_seconds=TTL, now=clock()) is False
    await store.mark_terminal_failure(
        key,
        reason=ob.REASON_REJECTED,
        error_code=ob.REASON_REJECTED,
        diagnostic="rejected",
        now=clock(),
    )
    assert await store.reset_for_retry(key, ttl_seconds=TTL, now=clock()) is True
    row = await store.get_by_key(*key)
    assert row is not None and row.state == ob.PENDING


# --- receipts ---------------------------------------------------------------


async def test_mark_deferred_only_applies_before_a_decision(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    assert await store.mark_deferred(key, now=clock(), delay_seconds=5) is True
    row = await store.get_by_key(*key)
    assert row is not None
    assert row.receipt_state == ob.RECEIPT_PENDING_STORED
    assert row.receipt_next_at == clock() + 5
    # A second deferral must not extend the notice or overwrite a decision.
    assert await store.mark_deferred(key, now=clock(), delay_seconds=99) is False


async def test_mark_deferred_loses_to_a_concurrent_success(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    await store.mark_succeeded(
        key,
        submission_id=9,
        duplicate=False,
        now=clock(),
        receipt_state=ob.RECEIPT_PENDING_SUCCESS,
        receipt_next_at=clock(),
    )
    assert await store.mark_deferred(key, now=clock(), delay_seconds=5) is False
    row = await store.get_by_key(*key)
    assert row is not None
    assert row.receipt_state == ob.RECEIPT_PENDING_SUCCESS


async def test_claim_receipt_is_exclusive(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    await store.mark_succeeded(
        key,
        submission_id=9,
        duplicate=False,
        now=clock(),
        receipt_state=ob.RECEIPT_PENDING_SUCCESS,
        receipt_next_at=clock(),
    )
    first = await store.claim_receipt(key, now=clock())
    assert first is not None
    assert first.receipt_state == ob.RECEIPT_SENDING
    assert await store.claim_receipt(key, now=clock()) is None
    # Once the send lease expires the receipt becomes claimable again.
    again = await store.claim_receipt(key, now=clock() + 61)
    assert again is not None


async def test_receipt_candidates_respects_schedule(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    await store.mark_deferred(key, now=clock(), delay_seconds=30)
    assert await store.receipt_candidates(now=clock()) == []
    due = await store.receipt_candidates(now=clock() + 31)
    assert len(due) == 1
    assert due[0].key == key


async def test_set_receipt_state_tracks_attempts_and_errors(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    await store.set_receipt_state(
        key,
        state=ob.RECEIPT_PENDING_STORED,
        now=clock(),
        next_attempt_at=clock() + 5,
        error="platform send failed",
    )
    row = await store.get_by_key(*key)
    assert row is not None
    assert row.receipt_attempts == 1
    assert row.last_receipt_error == "platform send failed"


# --- flags and diagnostics --------------------------------------------------


async def test_credentials_flag_round_trips(store: ob.Outbox) -> None:
    assert await store.get_flag("credentials_blocked") is None
    await store.set_flag("credentials_blocked", "unauthorized")
    assert await store.get_flag("credentials_blocked") == "unauthorized"


async def test_stats_counts_states(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(message_id="40001"), clock=clock)
    await prepare(store, payload(message_id="40002"), clock=clock)
    await store.mark_succeeded(
        ("thp-demo-2026", "100000002", "123456789", "40001"),
        submission_id=1,
        duplicate=False,
        now=clock(),
    )
    stats = await store.stats()
    assert stats.by_state.get(ob.SUCCEEDED) == 1
    assert stats.by_state.get(ob.PENDING) == 1
    assert stats.active == 1
    assert stats.pending_receipts == 1


async def test_recent_returns_newest_first(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(message_id="40001"), clock=clock)
    clock.advance(1)
    await prepare(store, payload(message_id="40002"), clock=clock)
    rows = await store.recent(limit=10)
    assert [row.source_message_id for row in rows] == ["40002", "40001"]


async def test_stored_error_text_is_bounded(store: ob.Outbox, clock) -> None:
    await prepare(store, payload(), clock=clock)
    key = key_for(payload())
    await store.mark_terminal_failure(
        key,
        reason="x",
        error_code="y",
        diagnostic="Z" * 5000,
        now=clock(),
    )
    row = await store.get_by_key(*key)
    assert row is not None
    assert len(row.last_error) <= 501


async def test_database_file_lives_where_it_is_asked(tmp_path: Path) -> None:
    database = ob.Outbox(tmp_path / "nested" / "dir" / "outbox.db")
    await database.open()
    assert (tmp_path / "nested" / "dir" / "outbox.db").exists()
    await database.close()


async def test_double_close_is_safe(store: ob.Outbox) -> None:
    await store.close()
    await store.close()


def test_default_db_path_uses_the_plugin_data_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("THP_POST_PLUGIN_DATA_DIR", str(tmp_path))
    # Without astrbot importable the env override is used.
    path = ob.default_db_path("astrbot_plugin_thp_post")
    assert path.name == "outbox.db"
    assert "plugin_data" in str(path)
