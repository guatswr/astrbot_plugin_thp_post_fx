"""Plugin client and durable worker against the actual FastAPI/SQLite backend."""

import sys
from pathlib import Path

import httpx
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("alembic")
sys.path.append(str(Path(__file__).resolve().parents[2] / "thp_post_backend"))

from app.core.config import Settings as BackendSettings
from app.main import create_app

from services.backend_client import CREDENTIALS, REJECTED, BackendClient
from services.delivery import BackoffSchedule, DeliveryWorker
from services.outbox import Outbox
from services.settings import load_settings
from tests.test_delivery import payload, wait_for

pytestmark = pytest.mark.integration
TOKEN, HOST, SCREEN = "i" * 40, "h" * 40, "d" * 40


@pytest.fixture
async def backend(tmp_path):
    settings = BackendSettings(
        event_id="thp-demo-2026",
        allowed_group_ids=("123456789",),
        ingest_token=TOKEN,
        host_token=HOST,
        display_token=SCREEN,
        data_dir=tmp_path / "backend",
        rate_limit_user_seconds=0,
        rate_limit_group_per_minute=1000,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        yield app


def plugin_settings(**overrides):
    return load_settings(
        dict(
            enabled=True,
            backend_url="http://backend.test",
            ingest_token=TOKEN,
            event_id="thp-demo-2026",
            allowed_group_ids=["123456789"],
            **overrides,
        )
    )


async def test_real_contract_create_duplicate_conflict_and_screen_standby(backend):
    client = BackendClient(plugin_settings(), transport=httpx.ASGITransport(app=backend))
    data = payload(content="第一行  两空格\n第二行 🎉")
    try:
        first = await client.submit(data)
        duplicate = await client.submit(data)
        conflict = await client.submit(dict(data, content="changed"))
        assert first.succeeded and not first.receipt.duplicate
        assert duplicate.succeeded and duplicate.receipt.duplicate
        assert first.receipt.submission_id == duplicate.receipt.submission_id
        assert conflict.kind == REJECTED and conflict.error_code == "idempotency_conflict"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=backend), base_url="http://backend.test"
        ) as reader:
            listing = await reader.get(
                "/api/v1/host/submissions", headers={"Authorization": f"Bearer {HOST}"}
            )
            assert len(listing.json()["items"]) == 1
            assert listing.json()["items"][0]["content"] == data["content"]
            screen = await reader.get(
                "/api/v1/screen/snapshot", headers={"Authorization": f"Bearer {SCREEN}"}
            )
            assert screen.json()["mode"] == "standby"
            assert screen.json()["submissions"] == []
            assert screen.json()["focused_submission"] is None
    finally:
        await client.aclose()


async def test_real_backend_authorization_and_group_activity_errors(backend):
    client = BackendClient(plugin_settings(), transport=httpx.ASGITransport(app=backend))
    try:
        for token in (HOST, SCREEN, "wrong-token"):
            assert (await client.submit(payload(), ingest_token=token)).kind == CREDENTIALS
        for data, code in (
            (dict(payload(), group_id="987654321"), "group_not_allowed"),
            (dict(payload(), event_id="another-event"), "event_mismatch"),
        ):
            result = await client.submit(data)
            assert result.kind == REJECTED and result.error_code == code
    finally:
        await client.aclose()


async def test_lost_response_retry_has_one_real_backend_record(backend, tmp_path):
    transport = httpx.ASGITransport(app=backend)
    requests = []

    class LoseFirstResponse(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            requests.append(request.content)
            response = await transport.handle_async_request(request)
            if len(requests) == 1:
                await response.aclose()
                raise httpx.ReadTimeout("response lost after commit", request=request)
            return response

    store = Outbox(tmp_path / "outbox.db")
    await store.open()
    settings = plugin_settings()
    client = BackendClient(settings, transport=LoseFirstResponse())
    notices = []

    async def send(session, text):
        notices.append((session, text))
        return True

    worker = DeliveryWorker(
        outbox_store=store,
        client=client,
        settings=settings,
        send_message=send,
        plain_chain=lambda text: text,
        schedule=BackoffSchedule(delays=(0.5,), maximum=0.5),
        jitter=lambda: 0.5,
    )
    data = payload()
    row = await store.prepare_event(
        key=(data["event_id"], data["bot_id"], data["group_id"], data["source_message_id"]),
        payload=data,
        origin_base_url=settings.backend_base_url,
        platform_id="original-bot",
        unified_msg_origin="original-bot:GroupMessage:123456789",
        ttl_seconds=600,
        capacity=1000,
        user_interval_seconds=0,
        group_per_minute=0,
    )
    try:
        await worker.start()
        await wait_for(lambda: bool(notices))
        assert len(requests) == 2 and requests[0] == requests[1]
        assert len(notices) == 1 and "投稿成功" in notices[0][1]
        assert (await store.find(row.row.key)).duplicate is True
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=backend), base_url="http://backend.test"
        ) as reader:
            result = await reader.get(
                "/api/v1/host/submissions", headers={"Authorization": f"Bearer {HOST}"}
            )
            assert len(result.json()["items"]) == 1
    finally:
        await worker.stop()
        await client.aclose()
        await store.close()
