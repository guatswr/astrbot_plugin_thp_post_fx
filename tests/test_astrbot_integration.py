"""Official AstrBot classes/registrar/pipeline, with network transport replaced.

Requires a separately installed AstrBot 4.28.2. This is not a live NapCat test.
"""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

pytest.importorskip("astrbot.api.star")
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from astrbot.api.message_components import At, Face, Plain
from astrbot.api.star import Context
from astrbot.core.pipeline.context import PipelineContext
from astrbot.core.pipeline.context_utils import call_handler
from astrbot.core.pipeline.waking_check.stage import WakingCheckStage
from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
from astrbot.core.platform.message_type import MessageType
from astrbot.core.platform.platform_metadata import PlatformMetadata
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
from astrbot.core.star.session_plugin_manager import SessionPluginManager
from astrbot.core.star.star import star_map
from astrbot.core.star.star_handler import star_handlers_registry
from astrbot.core.star.star_manager import PluginManager
from astrbot_plugin_thp_post import main as entry
from astrbot_plugin_thp_post.services.backend_client import BackendClient

from tests.test_backend_integration import TOKEN
from tests.test_backend_integration import backend as backend
from tests.test_delivery import CONFIG, success_response

pytestmark = pytest.mark.integration


def make_event(
    segments=None, *, text="/投稿 第一行  两空格\n第二行 🎉", group="123456789", sender="100000001"
):
    msg = AstrBotMessage()
    msg.type = MessageType.GROUP_MESSAGE
    msg.self_id, msg.group_id, msg.message_id = "100000002", group, "-40001"
    msg.sender = MessageMember(sender, "卡片")
    msg.message = segments if segments is not None else [Plain(text)]
    msg.message_str = text
    msg.raw_message = {
        "self_id": 100000002,
        "group_id": int(group),
        "message_id": -40001,
        "user_id": int(sender),
        "time": 1790000000,
        "sender": {"user_id": int(sender), "nickname": "昵称", "card": "卡片"},
    }
    return AiocqhttpMessageEvent(
        text, msg, PlatformMetadata("aiocqhttp", "test", "original-bot"), group, None
    )


@pytest.fixture
async def plugin(tmp_path, monkeypatch):
    monkeypatch.setattr(
        entry.outbox_module, "default_db_path", lambda _: tmp_path / "data" / "outbox.db"
    )
    sent, requests = [], []

    class Platform:
        def meta(self):
            return PlatformMetadata("aiocqhttp", "test", "original-bot")

        async def send_by_session(self, session, chain):
            sent.append((str(session), chain.get_plain_text()))

    context = Context.__new__(Context)
    context.platform_manager = SimpleNamespace(platform_insts=[Platform()])
    context.get_config = lambda **_: {"provider_ltm_settings": {}}

    def transport(request):
        requests.append(request)
        return success_response(321)

    monkeypatch.setattr(
        entry,
        "BackendClient",
        lambda settings: BackendClient(settings, transport=httpx.MockTransport(transport)),
    )
    p = entry.ThpPostPlugin(context, dict(CONFIG, initial_reply_wait_seconds=1.0))
    md = star_map[entry.__name__]
    md.name, md.activated, md.star_cls = "astrbot_plugin_thp_post", True, p
    await p.initialize()
    assert p._initialized
    yield p, sent, requests
    await p.terminate()


async def consume(plugin, event):
    results = []
    async for _ in call_handler(event, plugin.on_group_message):
        if event.get_result() and event.get_result().get_plain_text():
            results.append(event.get_result().get_plain_text())
    return results


def test_official_metadata_and_single_registration():
    root = Path(__file__).resolve().parents[1]
    md = PluginManager._load_plugin_metadata(str(root))
    assert md.name == "astrbot_plugin_thp_post"
    schema = PluginManager._load_plugin_config_schema(str(root / "_conf_schema.json"))
    assert schema["ingest_token"]["secret"] is True
    handlers = star_handlers_registry.get_handlers_by_module_name(entry.__name__)
    assert len(handlers) == 1
    assert len(handlers[0].event_filters) == 3
    custom = next(f for f in handlers[0].event_filters if isinstance(f, entry.CommandFilter))
    assert custom.raise_error is False


async def test_official_waking_stage_accepts_without_at(plugin, monkeypatch):
    p, sent, requests = plugin
    config = {
        "platform_settings": {},
        "admins_id": [],
        "wake_prefix": ["/"],
        "plugin_set": ["astrbot_plugin_thp_post"],
    }
    stage = WakingCheckStage()
    await stage.initialize(PipelineContext(config, None, "test", None))
    stage._umo_auto_name_recorder.schedule = lambda _: None
    monkeypatch.setattr(
        SessionPluginManager,
        "filter_handlers_by_session",
        AsyncMock(side_effect=lambda e, handlers: handlers),
    )
    event = make_event()
    await stage.process(event)
    assert event.is_wake
    assert len(event.get_extra("activated_handlers")) == 1
    assert await consume(p, event) == []
    assert event.is_stopped()
    assert len(requests) == 1
    assert len(sent) == 1
    assert sent[0] == ("original-bot:GroupMessage:123456789", "投稿成功，编号 #321，已送达主持端。")
    import json

    body = json.loads(requests[0].content)
    assert body["content"] == "第一行  两空格\n第二行 🎉"
    assert body["nickname"] == "昵称" and body["group_card"] == "卡片"
    await consume(p, make_event())
    assert len(requests) == 1 and len(sent) == 1


@pytest.mark.parametrize("text", ["/投稿", "/投稿 " + "字" * 501])
async def test_validation_reply_stops_pipeline(plugin, text):
    p, _, requests = plugin
    event = make_event(text=text)
    assert len(await consume(p, event)) == 1
    assert event.is_stopped()
    assert not requests


async def test_at_prefix_and_native_face_are_rejected(plugin):
    p, _, requests = plugin
    event = make_event(segments=[At(qq="100000002"), Plain("/投稿 文字"), Face(id=1)])
    assert await consume(p, event) == ["目前仅支持文字，请重新发送 /投稿 内容"]
    assert event.is_stopped() and not requests


@pytest.mark.parametrize(
    "group,sender,text",
    [
        ("987654321", "100000001", "/投稿 内容"),
        ("123456789", "100000002", "/投稿 内容"),
        ("123456789", "100000001", "/其他命令"),
    ],
)
async def test_unrelated_events_decline_filter(plugin, group, sender, text):
    p, _, requests = plugin
    event = make_event(group=group, sender=sender, text=text)
    assert not p._is_our_command(event)
    assert await consume(p, event) == []
    assert not event.is_stopped() and not requests


async def test_unwritable_queue_reports_failure(plugin, monkeypatch):
    p, _, requests = plugin
    monkeypatch.setattr(p._outbox, "prepare_event", AsyncMock(side_effect=OSError("disk full")))
    event = make_event()
    assert await consume(p, event) == ["本次投稿未能暂存，请稍后重新投稿。"]
    assert event.is_stopped() and not requests


async def test_duplicate_instance_cannot_start_worker(plugin):
    p, _, _ = plugin
    other = entry.ThpPostPlugin(p.context, CONFIG)
    await other.initialize()
    try:
        assert not other._initialized
        assert other._worker is None
    finally:
        await other.terminate()
        entry.CommandFilter.plugin_ref = p


async def test_initialize_and_terminate_are_idempotent(plugin):
    p, _, _ = plugin
    original = p._worker
    await p.initialize()
    assert p._worker is original
    await p.terminate()
    await p.terminate()
    assert not original.is_running
    assert not [t for t in asyncio.all_tasks() if t.get_name() == "thp-post-delivery"]


@pytest.mark.parametrize("extra", ["mface", "unsupported_future_segment", "markdown"])
async def test_official_adapter_dropped_or_converted_segments_are_still_rejected(plugin, extra):
    from aiocqhttp import Event
    from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_platform_adapter import AiocqhttpAdapter

    p, _, requests = plugin
    raw = dict(
        make_event().message_obj.raw_message,
        post_type="message",
        message_type="group",
        sub_type="normal",
        message=[
            {"type": "text", "data": {"text": "/投稿 文字"}},
            {"type": extra, "data": {"content": "converted markdown"}},
        ],
    )
    adapter = AiocqhttpAdapter.__new__(AiocqhttpAdapter)
    message = await adapter._convert_handle_message_event(Event.from_payload(raw))
    event = AiocqhttpMessageEvent(
        message.message_str,
        message,
        PlatformMetadata("aiocqhttp", "test", "original-bot"),
        "123456789",
        None,
    )
    assert await consume(p, event) == ["目前仅支持文字，请重新发送 /投稿 内容"]
    assert not requests


async def test_official_event_through_worker_to_real_backend(plugin, backend):
    from dataclasses import replace

    p, sent, _ = plugin
    await p._worker.stop()
    await p._client.aclose()
    p._client = BackendClient(
        replace(p.settings, ingest_token=TOKEN), transport=httpx.ASGITransport(app=backend)
    )
    p._worker._client = p._client
    await p._worker.start()
    event = make_event()
    assert await consume(p, event) == []
    assert event.is_stopped()
    assert sent == [("original-bot:GroupMessage:123456789", "投稿成功，编号 #1，已送达主持端。")]
    row = (await p._outbox.recent())[0]
    assert row.submission_id == 1 and row.state == "succeeded"


async def test_empty_allowlist_on_reload_holds_old_queue(plugin):
    from astrbot_plugin_thp_post.services.outbox import Outbox

    p, _, _ = plugin
    await p._worker.stop()
    event = make_event()
    identity, _ = entry.identity_service.try_extract_identity(event.message_obj)
    result = await p._prepare_event(
        event, identity, entry.build_payload(identity, "pending", p.settings.event_id)
    )
    path = p._outbox.db_path
    await p.terminate()
    other = entry.ThpPostPlugin(p.context, dict(CONFIG, allowed_group_ids=[]))
    await other.initialize()
    assert not other._initialized
    store = Outbox(path)
    await store.open()
    try:
        assert (await store.find(result.row.key)).state == "held"
    finally:
        await store.close()
        await other.terminate()
