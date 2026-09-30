"""Behavioural regressions found while taking over the incomplete plugin."""

import asyncio

import httpx
import pytest

from services import outbox as ob
from services.backend_client import RETRY
from services.settings import load_settings, parse_origin
from tests.test_backend_client import _success_body, make_client
from tests.test_delivery import Harness, error_response, payload, success_response, wait_for


def test_ipv6_origin_keeps_brackets():
    assert parse_origin("http://[::1]:18080/").base_url == "http://[::1]:18080"


def test_malformed_ipv6_disables_instead_of_crashing():
    config = {
        "enabled": True,
        "backend_url": "http://[broken",
        "ingest_token": "test-token",
        "event_id": "test-event",
        "allowed_group_ids": ["123456789"],
    }
    settings = load_settings(config)
    assert not settings.can_submit and settings.problems


@pytest.mark.parametrize(
    "field,value",
    [("created_at", None), ("updated_at", "no date"), ("created_at", "2026-01-01T12:00:00")],
)
async def test_invalid_success_time_is_uncertain(field, value):
    body = _success_body()
    body["submission"][field] = value
    client = make_client(lambda _: httpx.Response(201, json=body))
    try:
        assert (await client.submit(payload())).kind == RETRY
    finally:
        await client.aclose()


async def test_terminal_failure_stays_terminal(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return error_response(422, "content_invalid")

    async with Harness(tmp_path, handler) as h:
        row = await h.enqueue()
        await h.start()
        await wait_for(lambda: bool(h.recorder.sent))
        for _ in range(3):
            await h.worker._tick()
        current = await h.store.find(row.key)
        assert current.state == ob.FAILED
        assert len(calls) == 1
        assert len(h.recorder.sent) == 1


async def test_expired_submission_never_touches_network(tmp_path):
    calls = []
    async with Harness(tmp_path, lambda r: calls.append(r) or success_response()) as h:
        row = await h.enqueue()
        h.worker._clock = lambda: row.deadline_at + 1
        await h.worker._tick()
        assert not calls
        assert (await h.store.find(row.key)).state == ob.FAILED


async def test_empty_groups_park_pending(tmp_path):
    async with Harness(tmp_path, lambda _: success_response()) as h:
        row = await h.enqueue()
        assert (
            await h.store.claim_next(
                origin_base_url=h.settings.backend_base_url,
                event_id=h.settings.event_id,
                allowed_group_ids=(),
                max_attempts=20,
                now=h.now(),
            )
            is None
        )
        assert (await h.store.find(row.key)).state == ob.HELD


async def test_success_receipt_is_not_sent_to_removed_group(tmp_path):
    async with Harness(
        tmp_path, lambda _: success_response(), config={"allowed_group_ids": ["987654321"]}
    ) as h:
        data = payload()
        result = await h.store.prepare_event(
            key=(data["event_id"], data["bot_id"], data["group_id"], data["source_message_id"]),
            payload=data,
            origin_base_url=h.settings.backend_base_url,
            platform_id="old-bot",
            unified_msg_origin="old-bot:GroupMessage:123456789",
            ttl_seconds=600,
            capacity=1000,
            user_interval_seconds=0,
            group_per_minute=0,
            now=h.now(),
        )
        await h.store.mark_succeeded(result.row.key, submission_id=12, duplicate=False, now=h.now())
        await h.worker.send_receipt_now(result.row.key)
        assert not h.recorder.sent
        assert (await h.store.find(result.row.key)).receipt_state == ob.RECEIPT_ABANDONED


async def test_stored_receipt_cannot_overwrite_success_during_slow_send(tmp_path):
    async with Harness(tmp_path, lambda _: success_response(55)) as h:
        row = await h.enqueue()
        await h.store.mark_deferred(row.key, now=h.now(), delay_seconds=0)
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow_send(session, chain):
            if "暂存" in chain.text:
                entered.set()
                await release.wait()
            return await h.recorder.send(session, chain)

        h.worker._send_message = slow_send
        sender = asyncio.create_task(h.worker.send_receipt_now(row.key))
        await asyncio.wait_for(entered.wait(), 2)
        await h.start()
        await asyncio.sleep(0.05)
        release.set()
        await sender
        await wait_for(lambda: len(h.recorder.sent) == 2)
        assert "暂存" in h.recorder.texts[0]
        assert "#55" in h.recorder.texts[1]
        assert (await h.store.find(row.key)).receipt_state == ob.RECEIPT_SENT_SUCCESS


async def test_local_same_key_changed_content_is_refused(tmp_path):
    async with Harness(tmp_path, lambda _: success_response()) as h:
        row = await h.enqueue()
        data = payload(content="different")
        result = await h.store.prepare_event(
            key=row.key,
            payload=data,
            origin_base_url=h.settings.backend_base_url,
            platform_id=row.platform_id,
            unified_msg_origin=row.unified_msg_origin,
            ttl_seconds=600,
            capacity=1000,
            user_interval_seconds=0,
            group_per_minute=0,
            now=h.now(),
        )
        assert result.blocked == "idempotency_conflict"
        assert (await h.store.find(row.key)).content != "different"
