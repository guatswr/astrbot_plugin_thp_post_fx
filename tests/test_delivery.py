"""Delivery worker: retry policy, classification, receipts, recovery, shutdown."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from services import outbox as ob
from services import receipts
from services.backend_client import BackendClient
from services.delivery import (
    STATUS_REJECTED,
    STATUS_SUCCEEDED,
    STATUS_UNCONFIRMED,
    BackoffSchedule,
    DeliveryWorker,
)
from services.settings import load_settings
from tests.conftest import FakeClock, FakeMessageChain, FakePlain

TOKEN = "unit-test-token-0123456789abcdef"
ORIGIN = "http://203.0.113.10:18080"
GROUP = "123456789"

CONFIG: dict[str, Any] = {
    "enabled": True,
    "backend_url": ORIGIN,
    "ingest_token": TOKEN,
    "event_id": "thp-demo-2026",
    "allowed_group_ids": [GROUP],
    "initial_reply_wait_seconds": 2.0,
    "defer_notice_delay_seconds": 0.0,
}


def payload(message_id: str = "40001", content: str = "正文 🎉") -> dict[str, Any]:
    return {
        "event_id": "thp-demo-2026",
        "bot_id": "100000002",
        "group_id": GROUP,
        "source_message_id": message_id,
        "qq_id": "100000001",
        "nickname": "样例昵称",
        "group_card": "样例群名片",
        "content": content,
        "source_timestamp": "2026-10-03T10:00:00Z",
    }


def key_of(data: dict[str, Any]) -> tuple[str, str, str, str]:
    return (data["event_id"], data["bot_id"], data["group_id"], data["source_message_id"])


def success_response(submission_id: int = 321, duplicate: bool = False) -> httpx.Response:
    return httpx.Response(
        201 if not duplicate else 200,
        json={
            "duplicate": duplicate,
            "submission": {
                "id": submission_id,
                "event_id": "thp-demo-2026",
                "qq_id": "100000001",
                "display_name": "样例群名片",
                "nickname": "样例昵称",
                "group_card": "样例群名片",
                "avatar_url": "/api/v1/avatars/100000001",
                "content": "正文 🎉",
                "source_timestamp": "2026-10-03T10:00:00.000Z",
                "created_at": "2026-09-29T18:16:44.318Z",
                "updated_at": "2026-09-29T18:16:44.318Z",
                "is_read": False,
                "is_hidden": False,
                "selected": False,
            },
        },
    )


def error_response(
    status: int, code: str, *, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(
        status,
        json={"error": {"code": code, "message": code, "retryable": status >= 500}},
        headers=headers or {},
    )


class Recorder:
    """Captures proactive sends and lets the receipt path fail on demand."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []
        self.failures_left = 0
        self.failure_exception: Exception = RuntimeError("bot muted (simulated)")

    async def send(self, session: str, chain: Any) -> bool:
        if self.failures_left > 0:
            self.failures_left -= 1
            raise self.failure_exception
        self.sent.append((session, chain.text))
        return True

    @property
    def texts(self) -> list[str]:
        return [text for _, text in self.sent]


class Harness:
    """Wire an outbox + worker against a scripted mock transport.

    The clock is the test's adjustable wall clock plus real monotonic elapsed
    time. Tests may advance deadlines explicitly; all asyncio sleeps stay real
    and are never patched globally.
    """

    def __init__(
        self,
        tmp_path: Path,
        handler: Callable[[httpx.Request], httpx.Response],
        *,
        clock: FakeClock | None = None,
        config: dict[str, Any] | None = None,
        schedule: BackoffSchedule | None = None,
        receipt_schedule: BackoffSchedule | None = None,
    ) -> None:
        test_clock = clock or FakeClock()
        started = asyncio.get_running_loop().time()
        self.now = lambda: test_clock() + asyncio.get_running_loop().time() - started
        merged = dict(CONFIG)
        merged.update(config or {})
        self.settings = load_settings(merged)
        self.store = ob.Outbox(tmp_path / "outbox.db")
        self.recorder = Recorder()
        self._handler = handler
        self._schedule = schedule
        self._receipt_schedule = receipt_schedule
        self.worker: DeliveryWorker | None = None
        self.client: BackendClient | None = None

    async def __aenter__(self) -> Harness:
        await self.store.open()
        self.client = BackendClient(
            self.settings,
            transport=httpx.MockTransport(self._handler),
        )
        self.worker = DeliveryWorker(
            outbox_store=self.store,
            client=self.client,
            settings=self.settings,
            send_message=self.recorder.send,
            plain_chain=lambda text: FakeMessageChain([FakePlain(text)]),
            clock=self.now,
            schedule=self._schedule,
            receipt_schedule=self._receipt_schedule,
            jitter=lambda: 0.5,
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self.worker is not None:
            await self.worker.stop()
        if self.client is not None:
            await self.client.aclose()
        await self.store.close()

    async def enqueue(self, data: dict[str, Any] | None = None) -> ob.DeliveryRow:
        data = data or payload()
        result = await self.store.prepare_event(
            key=key_of(data),
            payload=data,
            origin_base_url=self.settings.backend_base_url,
            platform_id="thp-bot-1",
            unified_msg_origin=f"thp-bot-1:group:{data['group_id']}",
            ttl_seconds=self.settings.delivery_ttl_seconds,
            capacity=self.settings.queue_capacity,
            user_interval_seconds=0,
            group_per_minute=0,
            now=self.now(),
        )
        assert result.row is not None
        return result.row

    async def start(self) -> None:
        assert self.worker is not None
        await self.worker.start()

    async def stop(self) -> None:
        await self.worker.stop()


async def wait_for(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    """Poll ``predicate`` while yielding to the event loop."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


# --- backoff schedule -------------------------------------------------------


def test_backoff_schedule_matches_the_documented_defaults() -> None:
    schedule = BackoffSchedule()
    midpoint = lambda: 0.5  # noqa: E731 - terse on purpose
    assert schedule.delay_for(1, jitter=midpoint) == pytest.approx(2.0)
    assert schedule.delay_for(2, jitter=midpoint) == pytest.approx(5.0)
    assert schedule.delay_for(3, jitter=midpoint) == pytest.approx(15.0)
    assert schedule.delay_for(4, jitter=midpoint) == pytest.approx(30.0)
    assert schedule.delay_for(5, jitter=midpoint) == pytest.approx(60.0)
    assert schedule.delay_for(9, jitter=midpoint) == pytest.approx(60.0)
    assert schedule.delay_for(50, jitter=midpoint) == pytest.approx(60.0)


def test_backoff_jitter_stays_within_bounds_and_positive() -> None:
    schedule = BackoffSchedule()
    assert schedule.delay_for(1, jitter=lambda: 0.0) == pytest.approx(1.6)
    assert schedule.delay_for(1, jitter=lambda: 0.999999) == pytest.approx(2.4, rel=1e-3)
    assert schedule.delay_for(1, jitter=lambda: -5) > 0


# --- happy paths ------------------------------------------------------------


async def test_success_sends_the_backend_number_once(tmp_path: Path) -> None:
    async with Harness(tmp_path, lambda request: success_response(321)) as harness:
        await harness.enqueue()
        await harness.start()
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
        await asyncio.sleep(0.05)
    assert harness.recorder.texts == [receipts.success_text(321)]
    assert harness.recorder.sent[0][0] == f"thp-bot-1:group:{GROUP}"


async def test_duplicate_200_response_counts_as_success(tmp_path: Path) -> None:
    async with Harness(tmp_path, lambda request: success_response(77, duplicate=True)) as harness:
        await harness.enqueue()
        await harness.start()
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
    assert harness.recorder.texts == [receipts.success_text(77)]


async def test_submit_and_wait_returns_the_submission_id(tmp_path: Path) -> None:
    async with Harness(tmp_path, lambda request: success_response(555)) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=3.0)
    assert outcome.status == STATUS_SUCCEEDED
    assert outcome.submission_id == 555
    assert harness.recorder.texts == [receipts.success_text(555)]


async def test_only_one_success_notice_when_handler_and_worker_race(tmp_path: Path) -> None:
    async with Harness(tmp_path, lambda request: success_response(9)) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=3.0)
        # Force the handler-side fast path as well; the receipt claim must make
        # the second attempt a no-op.
        await harness.worker.send_receipt_now(row.key)
        await asyncio.sleep(0.05)
    assert outcome.status == STATUS_SUCCEEDED
    assert harness.recorder.texts == [receipts.success_text(9)]


# --- retries ----------------------------------------------------------------


async def test_transient_5xx_is_retried_with_the_original_payload(tmp_path: Path) -> None:
    bodies: list[bytes] = []
    responses = [
        error_response(500, "internal_error"),
        error_response(503, "db_unavailable"),
        success_response(4242),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return responses[min(len(bodies) - 1, len(responses) - 1)]

    schedule = BackoffSchedule(delays=(0.5, 0.5, 0.5), maximum=0.5)
    async with Harness(tmp_path, handler, schedule=schedule) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=5.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
    assert outcome.status == STATUS_SUCCEEDED
    assert outcome.submission_id == 4242
    assert len(bodies) == 3
    assert bodies[0] == bodies[1] == bodies[2]
    assert harness.recorder.texts == [receipts.success_text(4242)]


async def test_timeout_is_retried(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ReadTimeout("simulated", request=request)
        return success_response(11)

    schedule = BackoffSchedule(delays=(0.5,), maximum=0.5)
    async with Harness(tmp_path, handler, schedule=schedule) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=5.0)
    assert calls["n"] == 2
    assert outcome.status == STATUS_SUCCEEDED


async def test_429_retry_after_is_honoured(tmp_path: Path) -> None:
    calls: list[float] = []
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(clock())
        if len(calls) == 1:
            return error_response(429, "rate_limited", headers={"Retry-After": "45"})
        return success_response(12)

    async with Harness(tmp_path, handler, clock=clock) as harness:
        await harness.enqueue()
        await harness.start()
        await wait_for(lambda: len(calls) >= 1)
        row = await harness.store.get_by_key("thp-demo-2026", "100000002", GROUP, "40001")
        assert row is not None
        assert row.next_attempt_at is not None
        assert row.next_attempt_at - clock() >= 45
        # Nothing may be sent before the server-allowed time.
        assert len(calls) == 1
        clock.advance(46)
        harness.worker.notify_new_work()
        await wait_for(lambda: len(calls) >= 2)
    assert harness.recorder.texts == [receipts.success_text(12)]


async def test_429_past_the_deadline_terminates(tmp_path: Path) -> None:
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        return error_response(429, "rate_limited", headers={"Retry-After": "99999"})

    async with Harness(tmp_path, handler, clock=clock) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=3.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
        stored = await harness.store.get_by_key(*row.key)
    assert outcome.status == STATUS_UNCONFIRMED
    assert stored is not None
    assert stored.state == ob.FAILED
    # ``last_error_code`` keeps the backend's own code; the plugin's local
    # classification lives in ``failure_reason``.
    assert stored.failure_reason == ob.REASON_FINAL_RETRY_PAST_DEADLINE
    assert stored.last_error_code == "rate_limited"
    assert harness.recorder.texts == [receipts.uncertain_text()]


async def test_max_attempts_terminates_with_an_honest_notice(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return error_response(500, "internal_error")

    schedule = BackoffSchedule(delays=(0.5,), maximum=0.5)
    async with Harness(
        tmp_path,
        handler,
        schedule=schedule,
        config={"max_attempts": 3},
    ) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=5.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
        stored = await harness.store.get_by_key(*row.key)
    assert stored is not None
    assert stored.attempt_count == 3
    assert stored.state == ob.FAILED
    assert stored.failure_reason == ob.REASON_MAX_ATTEMPTS
    assert stored.last_error_code == "internal_error"
    assert outcome.status == STATUS_UNCONFIRMED
    assert harness.recorder.texts == [receipts.uncertain_text()]


async def test_deadline_expiry_terminates(tmp_path: Path) -> None:
    clock = FakeClock()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return error_response(500, "internal_error")

    async with Harness(tmp_path, handler, clock=clock) as harness:
        await harness.enqueue()
        await harness.start()
        await wait_for(lambda: calls["n"] >= 1)
        # Move past the whole TTL so the next scheduling pass expires the row.
        clock.advance(700)
        harness.worker.notify_new_work()
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
        stored = await harness.store.recent(limit=1)
    assert stored[0].state == ob.FAILED
    assert stored[0].last_error_code in (
        ob.REASON_TTL_EXPIRED,
        ob.REASON_FINAL_RETRY_PAST_DEADLINE,
    )
    assert harness.recorder.texts == [receipts.uncertain_text()]


# --- terminal rejections ----------------------------------------------------


@pytest.mark.parametrize(
    "status,code",
    [(400, "bad_request"), (413, "payload_too_large"), (422, "content_invalid")],
)
async def test_4xx_rejections_are_not_retried(tmp_path: Path, status: int, code: str) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return error_response(status, code)

    async with Harness(tmp_path, handler) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=3.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
    assert calls["n"] == 1
    assert outcome.status == STATUS_REJECTED
    assert harness.recorder.texts == [receipts.rejected_text()]


async def test_idempotency_conflict_terminates(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return error_response(409, "idempotency_conflict")

    async with Harness(tmp_path, handler) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=3.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
    assert calls["n"] == 1
    assert outcome.status == STATUS_REJECTED


async def test_group_not_allowed_is_a_per_submission_rejection(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return error_response(403, "group_not_allowed")

    async with Harness(tmp_path, handler) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=3.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
    assert outcome.status == STATUS_REJECTED
    assert not harness.worker._credentials_blocked


async def test_unauthorized_pauses_all_delivery(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return error_response(401, "unauthorized")

    async with Harness(tmp_path, handler) as harness:
        await harness.enqueue(payload("40001"))
        await harness.enqueue(payload("40002"))
        await harness.start()
        await wait_for(lambda: harness.worker._credentials_blocked)
        await asyncio.sleep(0.05)
        # The second row is never attempted while the token is known-bad.
        assert calls["n"] == 1
        assert await harness.store.get_flag("credentials_blocked") == "unauthorized"
        first = await harness.store.get_by_key("thp-demo-2026", "100000002", GROUP, "40001")
        second = await harness.store.get_by_key("thp-demo-2026", "100000002", GROUP, "40002")
    assert first is not None and second is not None
    assert first.state == ob.FAILED
    assert first.failure_reason == ob.REASON_CREDENTIALS
    assert first.last_error_code == "unauthorized"
    assert second.state == ob.PENDING


async def test_credentials_block_survives_a_restart(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return error_response(401, "unauthorized")

    async with Harness(tmp_path, handler) as harness:
        await harness.enqueue()
        await harness.start()
        await wait_for(lambda: harness.worker._credentials_blocked)
        await harness.worker.stop()

        # A fresh worker on the same database must not keep hammering the API.
        calls = {"n": 0}

        def counting(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return error_response(401, "unauthorized")

        harness._handler = counting
        harness.client = BackendClient(harness.settings, transport=httpx.MockTransport(counting))
        harness.worker = DeliveryWorker(
            outbox_store=harness.store,
            client=harness.client,
            settings=harness.settings,
            send_message=harness.recorder.send,
            plain_chain=lambda text: FakeMessageChain([FakePlain(text)]),
            clock=harness.now,
            jitter=lambda: 0.5,
        )
        await harness.worker.start()
        await asyncio.sleep(0.1)
    assert calls["n"] == 0
    assert harness.worker._credentials_blocked is False


async def test_expected_backend_failure_reports_the_address_problem(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "http://elsewhere.invalid/collect"})

    async with Harness(tmp_path, handler) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=3.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
        stored = await harness.store.get_by_key(*row.key)
    assert outcome.status == STATUS_REJECTED
    assert stored is not None
    assert stored.failure_reason == ob.REASON_MISCONFIGURED
    assert stored.last_error_code == "unexpected_redirect"
    assert harness.recorder.texts == [receipts.rejected_text()]


async def test_html_success_response_is_not_a_false_success(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            200, content=b"<html>proxy error</html>", headers={"Content-Type": "text/html"}
        )

    schedule = BackoffSchedule(delays=(0.5,), maximum=0.5)
    async with Harness(
        tmp_path,
        handler,
        schedule=schedule,
        config={"max_attempts": 2},
    ) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=5.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
    assert calls["n"] == 2
    assert outcome.status == STATUS_UNCONFIRMED
    assert harness.recorder.texts == [receipts.uncertain_text()]


# --- receipts ---------------------------------------------------------------


async def test_stored_notice_is_sent_before_the_first_retry(tmp_path: Path) -> None:
    clock = FakeClock()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return error_response(500, "internal_error")

    schedule = BackoffSchedule(delays=(30.0,), maximum=30.0)
    async with Harness(tmp_path, handler, clock=clock, schedule=schedule) as harness:
        row = await harness.enqueue()
        await harness.start()
        outcome = await harness.worker.submit_and_wait(row, timeout_seconds=1.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
    assert outcome.status == STATUS_UNCONFIRMED
    assert harness.recorder.texts == [receipts.STORED]


async def test_stored_notice_is_superseded_by_success(tmp_path: Path) -> None:
    # The deferral window is long, the retry is fast: success must win and the
    # "暂存" notice must never be sent.
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        return success_response(1001)

    async with Harness(
        tmp_path,
        handler,
        clock=clock,
        config={"defer_notice_delay_seconds": 60.0},
    ) as harness:
        row = await harness.enqueue()
        # Simulate "the handler timed out, then the worker succeeded".
        assert await harness.store.mark_deferred(row.key, now=clock(), delay_seconds=60) is True
        await harness.start()
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
        stored = await harness.store.get_by_key(*row.key)
    assert harness.recorder.texts == [receipts.success_text(1001)]
    assert stored is not None
    assert stored.receipt_state == ob.RECEIPT_SENT_SUCCESS


async def test_no_stored_notice_after_a_success(tmp_path: Path) -> None:
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        return success_response(7)

    async with Harness(tmp_path, handler, clock=clock) as harness:
        row = await harness.enqueue()
        await harness.start()
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
        # A late deferral request (e.g. a slow handler) must be refused.
        assert await harness.store.mark_deferred(row.key, now=clock(), delay_seconds=0) is False
        harness.worker.notify_new_work()
        await asyncio.sleep(0.1)
    assert harness.recorder.texts == [receipts.success_text(7)]


async def test_receipt_failure_does_not_reupload(tmp_path: Path) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return success_response(8)

    async with Harness(tmp_path, handler) as harness:
        harness.recorder.failures_left = 10
        row = await harness.enqueue()
        await harness.start()
        await wait_for(lambda: calls["n"] >= 1)
        await asyncio.sleep(0.15)
        stored = await harness.store.get_by_key(*row.key)
    assert calls["n"] == 1  # exactly one upload, no retry because of the receipt
    assert stored is not None
    assert stored.state == ob.SUCCEEDED
    assert stored.receipt_state in (ob.RECEIPT_SENDING, ob.RECEIPT_PENDING_SUCCESS)
    assert stored.receipt_attempts >= 1
    assert harness.recorder.sent == []


async def test_receipt_retry_gives_up_after_the_bound(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response(9)

    receipt_schedule = BackoffSchedule(delays=(0.5, 0.5, 0.5, 0.5, 0.5), maximum=0.5)
    async with Harness(tmp_path, handler, receipt_schedule=receipt_schedule) as harness:
        harness.recorder.failures_left = 100
        row = await harness.enqueue()
        await harness.start()
        deadline = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            stored = await harness.store.get_by_key(*row.key)
            if stored is not None and stored.receipt_state == ob.RECEIPT_ABANDONED:
                break
            await asyncio.sleep(0.02)
        stored = await harness.store.get_by_key(*row.key)
    assert stored is not None
    assert stored.receipt_state == ob.RECEIPT_ABANDONED
    assert stored.receipt_attempts >= 5


async def test_receipt_uses_the_original_bot_and_group(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response(3)

    async with Harness(tmp_path, handler) as harness:
        data = payload()
        result = await harness.store.prepare_event(
            key=key_of(data),
            payload=data,
            origin_base_url=harness.settings.backend_base_url,
            platform_id="bot-A",
            unified_msg_origin="bot-A:group:987654321",
            ttl_seconds=600,
            capacity=100,
            user_interval_seconds=0,
            group_per_minute=0,
            now=harness.now(),
        )
        assert result.row is not None
        await harness.start()
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
    # Even though the receipt text is about group 123456789, the routing uses the
    # session captured with the submission.
    assert harness.recorder.sent[0][0] == "bot-A:group:987654321"


async def test_held_row_never_gets_a_late_notice(tmp_path: Path) -> None:
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        return error_response(500, "internal_error")

    schedule = BackoffSchedule(delays=(30.0,), maximum=30.0)
    async with Harness(tmp_path, handler, clock=clock, schedule=schedule) as harness:
        row = await harness.enqueue()
        await harness.start()
        await harness.worker.submit_and_wait(row, timeout_seconds=1.0)
        await wait_for(lambda: len(harness.recorder.sent) >= 1)
        # Now the target changes: the row is parked and must stop owing a notice.
        await harness.store.set_held(row.key, reason=ob.HELD_TARGET_CHANGED, now=clock())
        clock.advance(1)
        harness.worker.notify_new_work()
        await asyncio.sleep(0.1)
        stored = await harness.store.get_by_key(*row.key)
    assert len(harness.recorder.sent) == 1  # the earlier stored notice only
    assert stored is not None
    assert stored.state == ob.HELD


# --- claiming / concurrency -------------------------------------------------


async def test_new_submission_and_retry_share_the_single_worker(tmp_path: Path) -> None:
    """A retry of an old row and a brand-new row are serialised by one worker."""
    order: list[str] = []
    clock = FakeClock()
    attempts: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        message_id = body["source_message_id"]
        attempts[message_id] = attempts.get(message_id, 0) + 1
        order.append(message_id)
        if message_id == "40001" and attempts[message_id] == 1:
            return error_response(500, "internal_error")
        return success_response(100 + len(order))

    schedule = BackoffSchedule(delays=(0.5,), maximum=0.5)
    async with Harness(tmp_path, handler, clock=clock, schedule=schedule) as harness:
        await harness.enqueue(payload("40001"))
        await harness.start()
        await wait_for(lambda: "40001" in order)
        await harness.enqueue(payload("40002"))
        harness.worker.notify_new_work()
        await wait_for(lambda: len(order) >= 3, timeout=5)
        await asyncio.sleep(0.05)
        rows = {row.source_message_id: row for row in await harness.store.recent(limit=5)}
    assert order == ["40001", "40002", "40001"]
    assert rows["40001"].state == ob.SUCCEEDED
    assert rows["40002"].state == ob.SUCCEEDED
    # Exactly one success notice per row, no duplicates from the re-entrant path.
    assert sorted(harness.recorder.texts) == sorted(
        [receipts.success_text(102), receipts.success_text(103)],
    )


async def test_worker_does_not_double_claim_a_row(tmp_path: Path) -> None:
    calls: list[str] = []
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        calls.append(body["source_message_id"])
        return success_response(len(calls))

    async with Harness(tmp_path, handler, clock=clock) as harness:
        row = await harness.enqueue()
        await harness.start()
        await wait_for(lambda: len(calls) >= 1)
        await asyncio.sleep(0.15)
        stored = await harness.store.get_by_key(*row.key)
    assert calls == ["40001"]
    assert stored is not None and stored.attempt_count == 1


async def test_worker_skips_rows_of_another_target(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return success_response(1)

    async with Harness(tmp_path, handler) as harness:
        data = payload()
        await harness.store.prepare_event(
            key=key_of(data),
            payload=data,
            origin_base_url="http://198.51.100.7:18080",
            platform_id="thp-bot-1",
            unified_msg_origin="thp-bot-1:group:123456789",
            ttl_seconds=600,
            capacity=100,
            user_interval_seconds=0,
            group_per_minute=0,
            now=harness.now(),
        )
        await harness.start()
        await asyncio.sleep(0.15)
        stored = await harness.store.get_by_key(*key_of(data))
    assert stored is not None
    assert stored.state == ob.HELD
    assert harness.recorder.sent == []


# --- lifecycle --------------------------------------------------------------


async def test_start_is_idempotent(tmp_path: Path) -> None:
    async with Harness(tmp_path, lambda request: success_response(1)) as harness:
        await harness.start()
        first = harness.worker._task
        await harness.start()
        assert harness.worker._task is first
        assert harness.worker.is_running


async def test_stop_leaves_no_background_task(tmp_path: Path) -> None:
    async with Harness(tmp_path, lambda request: success_response(1)) as harness:
        await harness.enqueue()
        await harness.start()
        await harness.stop()
        assert not harness.worker.is_running
        assert harness.worker._task is None
        # A second stop must be harmless (reload/terminate double call).
        await harness.stop()
        # Nothing pending in the event loop from this worker.
        assert all(
            task.get_name() != "thp-post-delivery"
            for task in asyncio.all_tasks()
            if not task.done()
        )


async def test_stop_releases_a_claim_that_was_in_flight(tmp_path: Path) -> None:
    started = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        started.set()
        return error_response(500, "internal_error")

    async with Harness(tmp_path, handler) as harness:
        row = await harness.enqueue()
        await harness.start()
        await wait_for(lambda: started.is_set())
        await harness.worker.stop()
        stored = await harness.store.get_by_key(*row.key)
    assert stored is not None
    # Either it already recorded the failed attempt, or the claim was released;
    # both keep the row deliverable and never leave it stuck inflight.
    assert stored.state in (ob.PENDING, ob.FAILED)
    assert stored.state != ob.INFLIGHT


async def test_reload_recovers_a_crash_left_inflight_row(
    tmp_path: Path, clock, monkeypatch
) -> None:
    # The startup lease sweep deliberately uses wall-clock time (a lease is only
    # meaningful in real elapsed time), so pin it for a deterministic test.
    monkeypatch.setattr("services.delivery.time.time", lambda: clock.now)

    def handler(request: httpx.Request) -> httpx.Response:
        return success_response(66)

    async with Harness(tmp_path, handler, clock=clock) as harness:
        await harness.enqueue()
        # Simulate a crash: claim the row and never release it.
        claimed = await harness.store.claim_next(
            origin_base_url=harness.settings.backend_base_url,
            event_id=harness.settings.event_id,
            allowed_group_ids=harness.settings.allowed_group_ids,
            max_attempts=harness.settings.max_attempts,
            now=harness.now(),
        )
        assert claimed is not None
        assert claimed.state == ob.INFLIGHT
        clock.advance(120)
        await harness.start()
        await wait_for(lambda: len(harness.recorder.sent) >= 1, timeout=5)
    assert harness.recorder.texts == [receipts.success_text(66)]
