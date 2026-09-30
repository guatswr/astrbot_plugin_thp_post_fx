"""THP 群投稿插件 —— AstrBot 入口。

只注册 **一个** 消息入口
-------------------------
本插件使用一个事件监听器加自定义过滤器，而不是 ``@filter.command``：

* ``CommandFilter`` 会把连续空白折叠成单个空格并按空格切分参数
  （AstrBot v4.28.2 ``astrbot/core/star/filter/command.py`` L199-211），多行正文
  会被破坏；
* 同一个方法上叠加 ``@filter.command`` 与 ``@filter.event_message_type`` 时，
  只有第一个装饰器负责创建共享的 ``StarHandlerMetadata``，容易漏掉过滤器。

因此这里自行解析 ``message_obj.message`` 消息段，只保留一个入口，既不会双投，
也不会消费其他插件的命令（过滤器只匹配 ``/投稿`` 这一个 token）。

如何保证不触发 LLM
------------------
自定义过滤器匹配时，``WakingCheckStage`` 会把 ``is_wake`` 置 True，但
``is_at_or_wake_command`` 保持 False（``astrbot/core/pipeline/waking_check/stage.py``
L232-233）。``ProcessStage`` 只在 ``is_at_or_wake_command`` 为真时才调用 LLM
（``process_stage/stage.py`` L56-60），所以普通群聊与 ``/投稿`` 都不会触发 LLM。
若用户以 ``/`` 唤醒前缀或 ``@机器人`` 的方式触发，``is_at_or_wake_command`` 为真，
此时处理器会调用 ``event.stop_event()`` 阻断后续（包括 LLM）。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from astrbot.api import logger as astrbot_logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star
from filelock import FileLock

from .services import delivery as delivery_service
from .services import identity as identity_service
from .services import outbox as outbox_module
from .services import receipts
from .services.backend_client import BackendClient
from .services.outbox import Outbox
from .services.parser import (
    ATTACHMENT_ONLY,
    EMPTY,
    NOT_COMMAND,
    OK,
    TOO_LONG,
    parse_submission,
)
from .services.settings import Settings, load_settings

PLUGIN_VERSION = "1.0.0"


def _configure_logger(settings: Settings, name: str) -> logging.Logger:
    """Attach AstrBot's log sink to this module's logger.

    The plugin's ``self.logger`` is the per-plugin AstrBot logger; the service
    modules use plain ``logging.getLogger(__name__)`` so they stay importable
    without AstrBot. Routing them through the plugin logger keeps a single,
    filterable prefix in the WebUI log view.
    """
    plugin_logger = astrbot_logger
    level = getattr(logging, settings.log_level, logging.INFO)
    for module in (
        __name__,
        f"{__package__}.services.delivery",
        f"{__package__}.services.outbox",
        f"{__package__}.services.backend_client",
        f"{__package__}.services.identity",
    ):
        module_logger = logging.getLogger(module)
        module_logger.setLevel(level)
        if plugin_logger is not None and plugin_logger not in module_logger.handlers:
            # ``logger`` from astrbot.api is a Logger instance; reuse its
            # handlers so plugin output lands in the same place.
            for handler in getattr(plugin_logger, "handlers", []):
                if handler not in module_logger.handlers:
                    module_logger.addHandler(handler)
            if module_logger.handlers:
                module_logger.propagate = False
    return logging.getLogger(name)


_log = logging.getLogger(__name__)


def build_payload(
    identity: identity_service.EventIdentity,
    content: str,
    event_id: str,
) -> dict[str, Any]:
    """Build the frozen ingest request body exactly once.

    The result is persisted verbatim in the outbox, so a retry can never pick up
    a changed nickname, activity id or body — that is the whole point of
    freezing it here rather than at request time.

    Args:
        identity: Normalised event identity.
        content: Parsed submission text (already trimmed, 1-500 code points).
        event_id: Configured activity id.

    Returns:
        A contract-shaped request body (``docs/API.md`` §3).
    """
    return {
        "event_id": event_id,
        "bot_id": identity.bot_id,
        "group_id": identity.group_id,
        "source_message_id": identity.source_message_id,
        "qq_id": identity.qq_id,
        "nickname": identity.nickname,
        "group_card": identity.group_card,
        "content": content,
        "source_timestamp": identity.source_timestamp,
    }


def submission_segments(message_obj):
    # OneBot's original array retains mface/unknown/file segments that AstrBot
    # may discard or convert to text. Never reinterpret literal CQ syntax.
    raw = getattr(message_obj, "raw_message", None)
    segments = raw.get("message") if isinstance(raw, dict) else getattr(raw, "message", None)
    return segments if isinstance(segments, list) else getattr(message_obj, "message", None)


class CommandFilter(filter.CustomFilter):
    """Activate the submission handler only for the configured command token.

    AstrBot's ``register_custom_filter`` instantiates the class with
    ``raise_error`` as its only argument (see
    ``astrbot/core/star/register/star_handler.py`` L155-156), so the filter
    cannot receive the plugin instance directly. It reads it from
    :attr:`CommandFilter.plugin_ref`, which :class:`ThpPostPlugin` sets in
    ``__init__``; until then the filter simply declines and the plugin is inert.

    Only the "is this our command?" question is decided here. Everything else
    (length, attachments, identity, allow-list) is handled inside the handler so
    the plugin can answer with a helpful message instead of staying silent.
    ``raise_error=False`` keeps a filter-time exception from being reported to
    the group.
    """

    plugin_ref: ThpPostPlugin | None = None

    def __init__(self, raise_error: bool = True, **_: Any) -> None:
        super().__init__(raise_error=raise_error)

    def filter(self, event: AstrMessageEvent, cfg: Any) -> bool:  # noqa: ARG002
        plugin = CommandFilter.plugin_ref
        if plugin is None or not plugin._initialized:
            return False
        return plugin._is_our_command(event)


class ThpPostPlugin(Star):
    """AstrBot Star plugin: QQ group submissions -> THP backend."""

    def __init__(self, context: Context, config: Any = None) -> None:
        super().__init__(context)
        self.config = config if config is not None else {}
        self.settings: Settings = load_settings(self.config)
        self._outbox: Outbox | None = None
        self._client: BackendClient | None = None
        self._worker: delivery_service.DeliveryWorker | None = None
        self._initialized = False
        self._init_failed_reason = ""
        self._process_lock: FileLock | None = None
        # Only one instance is alive at a time (AstrBot terminates the old
        # instance before instantiating a reloaded plugin), so a single slot is
        # enough for the filter to reach the current instance.
        if CommandFilter.plugin_ref is None or not CommandFilter.plugin_ref._initialized:
            CommandFilter.plugin_ref = self

    # -- lifecycle ----------------------------------------------------------

    async def initialize(self) -> None:
        """Create storage, HTTP client and the delivery worker.

        Deliberately does not touch the network: a cloud backend that is down
        (or unreachable from the container) must never delay AstrBot startup.
        """
        if self._initialized:
            return
        self.settings = load_settings(self.config)
        _configure_logger(self.settings, "thp_post")
        for problem in self.settings.problems:
            _log.warning("[thp_post] 配置问题：%s", problem)

        if not self.settings.enabled:
            _log.info("[thp_post] 插件未启用（enabled=false），不会接收投稿")
            return
        if not self.settings.can_submit:
            # Park old groups even when the empty allow-list prevents a worker
            # from starting. Reverting configuration must not resume them.
            if (
                self.settings.origin
                and self.settings.event_id
                and not self.settings.allowed_group_ids
            ):
                db_path = outbox_module.default_db_path("astrbot_plugin_thp_post")
                if db_path.exists():
                    try:
                        with FileLock(str(db_path) + ".lock", timeout=0):
                            store = Outbox(db_path)
                            await store.open()
                            try:
                                await store.claim_next(
                                    origin_base_url=self.settings.backend_base_url,
                                    event_id=self.settings.event_id,
                                    allowed_group_ids=(),
                                    max_attempts=self.settings.max_attempts,
                                )
                            finally:
                                await store.close()
                    except Exception as exc:
                        _log.error("[thp_post] 无法暂停旧群队列：%s", type(exc).__name__)
            self._init_failed_reason = "配置不完整，投稿能力已禁用：" + "；".join(
                self.settings.problems
            )
            _log.error("[thp_post] %s", self._init_failed_reason)
            return

        try:
            db_path = outbox_module.default_db_path("astrbot_plugin_thp_post")
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._process_lock = FileLock(str(db_path) + ".lock")
            self._process_lock.acquire(timeout=0)
            store = Outbox(db_path)
            await store.open()
        except Exception as exc:
            if self._process_lock is not None:
                self._process_lock.release()
                self._process_lock = None
            self._init_failed_reason = f"无法打开投稿队列数据库：{type(exc).__name__}: {exc}"
            _log.error(
                "[thp_post] %s（投稿能力已禁用；请检查数据目录权限与磁盘空间）",
                self._init_failed_reason,
            )
            return
        self._outbox = store

        try:
            self._client = BackendClient(self.settings)
        except Exception as exc:
            self._init_failed_reason = f"无法创建后端 HTTP 客户端：{type(exc).__name__}: {exc}"
            _log.error("[thp_post] %s", self._init_failed_reason)
            await self._outbox.close()
            self._outbox = None
            self._process_lock.release()
            self._process_lock = None
            return

        self._worker = delivery_service.DeliveryWorker(
            outbox_store=self._outbox,
            client=self._client,
            settings=self.settings,
            send_message=self._send_plain,
            plain_chain=self._plain_chain,
            clock=_monotonic_like_wall_clock,
        )
        try:
            await self._worker.start()
            stats = await self._outbox.stats()
        except Exception as exc:
            _log.error("[thp_post] 调度器初始化失败：%s", type(exc).__name__)
            await self.terminate()
            return
        self._initialized = True
        CommandFilter.plugin_ref = self
        _log.info(
            "[thp_post] 已就绪：目标=%s 活动=%s 允许群=%s 队列=%s/%d",
            self.settings.backend_base_url,
            self.settings.event_id,
            ",".join(self.settings.allowed_group_ids),
            stats.active,
            self.settings.queue_capacity,
        )

    async def terminate(self) -> None:
        """Stop accepting submissions and release every resource.

        Order matters: stop the worker first (so no new claim is taken), then
        close HTTP, then the database. Both ``aclose`` calls are idempotent, so a
        second terminate after a reload cannot leak or double-free.
        """
        self._initialized = False
        worker, self._worker = self._worker, None
        client, self._client = self._client, None
        store, self._outbox = self._outbox, None
        if worker is not None:
            try:
                await worker.stop()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - shutdown path
                _log.warning("[thp_post] 停止调度器时出现异常：%s", type(exc).__name__)
        if client is not None:
            await client.aclose()
        if store is not None:
            await store.close()
        if self._process_lock is not None:
            self._process_lock.release()
            self._process_lock = None
        if CommandFilter.plugin_ref is self:
            CommandFilter.plugin_ref = None
        _log.info("[thp_post] 插件已卸载，资源已释放")

    # -- helpers ------------------------------------------------------------

    async def _send_plain(self, unified_msg_origin: str, message_chain: MessageChain) -> bool:
        """Send plain text to a stored AstrBot session.

        Args:
            unified_msg_origin: ``platform_id:message_type:session_id`` captured
                when the submission was enqueued.
            message_chain: Message chain constructed by the delivery worker.

        Returns:
            True when a matching platform instance accepted the send.
        """
        if not unified_msg_origin:
            _log.warning("[thp_post] 缺少会话标识，无法发送群回执")
            return False
        try:
            return bool(await self.context.send_message(unified_msg_origin, message_chain))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - receipts must never break delivery
            _log.warning(
                "[thp_post] 群回执发送失败（投稿数据不受影响）：%s",
                type(exc).__name__,
            )
            return False

    @staticmethod
    def _plain_chain(text: str) -> MessageChain:
        return MessageChain(chain=[Plain(text)])

    # -- message entry point ------------------------------------------------

    def _is_our_command(self, event: AstrMessageEvent) -> bool:
        """Does this event look like ``/投稿 ...``?

        See :class:`CommandFilter` for why only this narrow question is answered
        here.
        """
        settings = self.settings
        message_obj = getattr(event, "message_obj", None)
        if not settings.can_submit:
            return False
        if not settings.is_group_allowed(str(event.get_group_id() or "")):
            return False
        if str(event.get_sender_id() or "") == str(getattr(message_obj, "self_id", "")):
            return False
        try:
            result = parse_submission(
                submission_segments(message_obj),
                command_token=settings.command_token,
                self_id=str(getattr(message_obj, "self_id", "") or ""),
            )
        except Exception:  # pragma: no cover - parser is defensive already
            return False
        return result.code != NOT_COMMAND

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.custom_filter(CommandFilter, False)
    async def on_group_message(self, event: AstrMessageEvent):
        """Handle one inbound group message (the single submission entry point)."""
        try:
            async for result in self._handle(event):
                # call_handler replaces event._result with each yielded result.
                # Preserve the stop flag on validation/storage error replies.
                result.stop_event()
                yield result
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never break the whole pipeline
            _log.exception("[thp_post] 处理群消息时出现未预期异常：%s", type(exc).__name__)

    async def _handle(self, event: AstrMessageEvent):
        settings = self.settings
        message_obj = getattr(event, "message_obj", None)

        # 1. Bot's own messages are never submissions.
        try:
            self_id = str(getattr(message_obj, "self_id", "") or "")
            sender_id = event.get_sender_id()
        except Exception:  # pragma: no cover - defensive
            self_id, sender_id = "", ""
        if self_id and sender_id and self_id == sender_id:
            return

        if not settings.enabled or not settings.can_submit:
            # Misconfigured or disabled: stay silent in the group (the operator
            # sees the reason in the AstrBot log), and let other handlers run.
            if settings.enabled and not self._initialized:
                _log.debug("[thp_post] 配置不完整，忽略群消息")
            return

        # 2. Group allow-list (empty list = disabled; never reply there).
        try:
            group_id = str(event.get_group_id() or "")
        except Exception:  # pragma: no cover - defensive
            group_id = ""
        if not settings.is_group_allowed(group_id):
            return

        # 3. Parse command + body from message segments.
        parsed = parse_submission(
            submission_segments(message_obj),
            command_token=settings.command_token,
            self_id=self_id,
        )
        if parsed.code == NOT_COMMAND:
            return

        if parsed.code == ATTACHMENT_ONLY:
            yield event.plain_result(receipts.ATTACHMENT_UNSUPPORTED)
            event.stop_event()
            return
        if parsed.code == EMPTY:
            yield event.plain_result(receipts.USAGE)
            event.stop_event()
            return
        if parsed.code == TOO_LONG:
            yield event.plain_result(receipts.TOO_LONG)
            event.stop_event()
            return
        if parsed.code != OK:  # pragma: no cover - exhaustive above
            return

        # From here on the message is a legitimate submission attempt, so the
        # plugin owns the reply and stops further propagation (no LLM answer).
        event.stop_event()

        identity, diagnostic = identity_service.try_extract_identity(message_obj)
        if identity is None:
            _log.warning(
                "[thp_post] 事件身份缺失，未上传：reason=%s field=%s bot=%s group=%s qq=%s",
                diagnostic.reason if diagnostic else "?",
                diagnostic.field_name if diagnostic else "?",
                diagnostic.bot_id if diagnostic else "",
                diagnostic.group_id if diagnostic else "",
                diagnostic.qq_id if diagnostic else "",
            )
            yield event.plain_result(receipts.IDENTITY_INVALID)
            return

        assert self._outbox is not None and self._worker is not None

        payload = build_payload(identity, parsed.content, settings.event_id)
        try:
            prepared = await self._prepare_event(event, identity, payload)
        except Exception as exc:
            _log.error("[thp_post] 投稿未能写入本地队列：%s", type(exc).__name__)
            yield event.plain_result(receipts.STORAGE_FAILED)
            return

        if prepared.blocked == "rate_limited":
            yield event.plain_result(receipts.RATE_LIMITED)
            return
        if prepared.blocked == "queue_full":
            yield event.plain_result(receipts.QUEUE_FULL)
            return
        if prepared.blocked == "idempotency_conflict":
            yield event.plain_result(receipts.rejected_text())
            return
        if prepared.duplicate:
            return
        if prepared.row is not None:
            await self._worker.submit_and_wait(
                prepared.row,
                timeout_seconds=settings.initial_reply_wait_seconds,
            )

    async def _prepare_event(self, event, identity, payload):
        settings = self.settings
        assert self._outbox is not None
        return await self._outbox.prepare_event(
            key=(
                settings.event_id,
                identity.bot_id,
                identity.group_id,
                identity.source_message_id,
            ),
            payload=payload,
            origin_base_url=settings.backend_base_url,
            platform_id=identity_service.platform_id_of(event),
            unified_msg_origin=identity_service.unified_msg_origin_of(event),
            ttl_seconds=settings.delivery_ttl_seconds,
            capacity=settings.queue_capacity,
            user_interval_seconds=settings.user_interval_seconds,
            group_per_minute=settings.group_per_minute,
        )


def _monotonic_like_wall_clock() -> float:
    """Clock used by the delivery layer.

    Wall-clock time is intentional: the delivery deadline must survive a
    restart, and ``time.time()`` is what the outbox stores.
    """
    import time

    return time.time()


__all__ = ["CommandFilter", "ThpPostPlugin", "build_payload"]
