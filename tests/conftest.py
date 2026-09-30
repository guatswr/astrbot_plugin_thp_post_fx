"""Shared pytest fixtures.

The plugin is imported as the top-level package ``astrbot_plugin_thp_post`` so
the tests exercise exactly the modules AstrBot will import (``from .services …
import`` inside ``main.py``), without needing AstrBot to be installed.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import pytest

PLUGIN_ROOT = Path(__file__).resolve().parent.parent
if str(PLUGIN_ROOT) not in sys.path:
    sys.path.insert(0, str(PLUGIN_ROOT))

FIXTURES = Path(__file__).resolve().parent / "fixtures"
REPO_ROOT = PLUGIN_ROOT.parent
CONTRACT_EXAMPLES = REPO_ROOT / "contracts" / "examples"


@pytest.fixture(scope="session")
def plugin_root() -> Path:
    return PLUGIN_ROOT


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


def load_fixture(name: str) -> Any:
    """Load a JSON fixture from ``tests/fixtures``."""
    path = FIXTURES / name
    return json.loads(path.read_text(encoding="utf-8"))


def load_contract_example(name: str) -> Any | None:
    """Load a JSON example from the shared ``contracts/examples`` directory.

    Returns ``None`` when the backend project has not produced it, so the plugin
    test suite never becomes a hard dependency of the backend's progress.
    """
    path = CONTRACT_EXAMPLES / name
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def contract_loader():
    return load_contract_example


class FakeClock:
    """Deterministic wall clock for deadline/backoff assertions."""

    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += float(seconds)
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


class FakeJitter:
    """Deterministic jitter source: always returns the midpoint (factor 1.0)."""

    def __init__(self, value: float = 0.5) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


@pytest.fixture
def jitter() -> FakeJitter:
    return FakeJitter()


class FakeAstrBotMessage:
    """Minimal stand-in for ``AstrBotMessage`` (fields verified against 4.28.2)."""

    def __init__(
        self,
        *,
        message: list[Any] | None = None,
        message_str: str = "",
        self_id: str = "100000002",
        group_id: str = "123456789",
        message_id: str = "40001",
        sender_user_id: str = "100000001",
        sender_nickname: str | None = "观众昵称",
        raw_sender: dict[str, Any] | None = None,
        raw_time: int | None = 1_790_000_000,
        message_type: str = "group",
        raw_group_id: str | None = None,
    ) -> None:
        self.type = "group" if message_type == "group" else "friend"
        self.self_id = self_id
        self.group_id = group_id
        self.message_id = message_id
        self.message = message or []
        self.message_str = message_str
        self.timestamp = 1_790_000_100
        sender = {"user_id": sender_user_id}
        if sender_nickname is not None:
            sender["nickname"] = sender_nickname
        if raw_sender:
            sender.update(raw_sender)
        raw: dict[str, Any] = {
            "post_type": "message",
            "message_type": message_type,
            "self_id": int(self_id),
            "message_id": int(message_id),
            "group_id": int(raw_group_id or group_id),
            "user_id": int(sender_user_id),
            "sender": sender,
        }
        if raw_time is not None:
            raw["time"] = raw_time
        self.raw_message = raw

        class _Sender:
            user_id = sender_user_id
            nickname = sender.get("card") or sender_nickname

        self.sender = _Sender()


class FakeEvent:
    """Minimal stand-in for ``AstrMessageEvent`` used by the message handler."""

    def __init__(
        self,
        message_obj: FakeAstrBotMessage,
        *,
        platform_id: str = "thp-bot-1",
        message_type: str = "group",
    ) -> None:
        self.message_obj = message_obj
        self._platform_id = platform_id
        self._stopped = False
        self._results: list[Any] = []
        self.unified_msg_origin = f"{platform_id}:{message_type}:{message_obj.group_id}"

    def get_sender_id(self) -> str:
        return self.message_obj.sender.user_id

    def get_group_id(self) -> str:
        return self.message_obj.group_id

    def get_self_id(self) -> str:
        return self.message_obj.self_id

    def get_platform_id(self) -> str:
        return self._platform_id

    def plain_result(self, text: str) -> str:
        return text

    def stop_event(self) -> None:
        self._stopped = True

    def is_stopped(self) -> bool:
        return self._stopped

    @property
    def replies(self) -> list[str]:
        return [item for item in self._results if isinstance(item, str)]


class FakePlain:
    def __init__(self, text: str) -> None:
        self.type = "Plain"
        self.text = text


class FakeMessageChain:
    """Stand-in for ``astrbot.api.event.MessageChain``."""

    def __init__(self, chain: list[Any] | None = None, **_: Any) -> None:
        self.chain = chain or []

    @property
    def text(self) -> str:
        return "".join(getattr(part, "text", "") for part in self.chain)


class FakeContext:
    """Captures proactive sends so receipt behaviour can be asserted."""

    def __init__(self, *, send_ok: bool = True) -> None:
        self.sent: list[tuple[str, str]] = []
        self.send_ok = send_ok
        self.fail_until = 0

    async def send_message(self, session: str, message_chain: Any) -> bool:
        if self.fail_until > 0:
            self.fail_until -= 1
            raise RuntimeError("platform send failed (simulated)")
        text = getattr(message_chain, "text", "")
        self.sent.append((session, text))
        return self.send_ok


async def collect_replies(event: FakeEvent, handler_async_gen) -> list[str]:
    """Drive an async-generator handler and collect the yielded reply strings."""
    replies: list[str] = []
    async for item in handler_async_gen:
        if isinstance(item, str):
            replies.append(item)
    return replies


@pytest.fixture
def fake_message_factory():
    def factory(**kwargs: Any) -> FakeAstrBotMessage:
        return FakeAstrBotMessage(**kwargs)

    return factory


@pytest.fixture
def fake_event_factory():
    def factory(message_obj: FakeAstrBotMessage | None = None, **kwargs: Any) -> FakeEvent:
        return FakeEvent(message_obj or FakeAstrBotMessage(), **kwargs)

    return factory


@pytest.fixture
def anyio_backend() -> str:  # pragma: no cover - safety for anyio-style tests
    return "asyncio"


# Keep a reference so linters do not flag the import as unused.
_ = asyncio
