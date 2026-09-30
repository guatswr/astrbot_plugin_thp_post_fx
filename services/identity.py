"""Event identity extraction.

Reads the fields the backend contract needs **only** from the trusted inbound
event. Nothing is guessed from the message body: a QQ number that merely looks
like a QQ number inside the submitted text is content, never identity, and a
quoted/forwarded message's original author is never used as the submitter.

Facts verified against AstrBot v4.28.2
--------------------------------------
* ``AstrBotMessage.self_id`` is ``str(event.self_id)``
  (``aiocqhttp_platform_adapter.py`` line 210) — the connected bot's account.
* ``AstrBotMessage.group_id`` proxies ``Group.group_id`` and is ``""`` for
  private chat (``astrbot_message.py`` lines 74-92).
* ``AstrBotMessage.message_id`` is ``str(event.message_id)`` (line 228);
  OneBot may report negative ids, so it stays a string end to end.
* ``AstrBotMessage.sender`` is ``MessageMember(user_id, card or nickname)``
  (lines 211-214) — i.e. **the card is copied into ``nickname``**, which is why
  this module prefers ``raw_message["sender"]`` when reading the two fields
  separately.
* ``abm.timestamp = int(time.time())`` (line 421) is the *receive* time, not the
  OneBot ``time`` field, so the original event time is taken from
  ``raw_message["time"]`` when present.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .settings import GROUP_ID_RE, MESSAGE_ID_RE, QQ_ID_RE


class IdentityError(ValueError):
    """Raised when a required identity field is missing or unusable."""

    def __init__(self, field_name: str, reason: str) -> None:
        super().__init__(f"{field_name}: {reason}")
        self.field_name = field_name
        self.reason = reason


@dataclass(frozen=True)
class EventIdentity:
    """Normalised identity of one inbound group message."""

    bot_id: str
    group_id: str
    source_message_id: str
    qq_id: str
    nickname: str | None
    group_card: str | None
    source_timestamp: str | None
    """UTC ISO 8601 with a ``Z`` suffix, or ``None`` when the event carries no
    usable time (the backend accepts ``null`` and only uses it for display)."""


def _dig(source: object, key: str) -> object:
    """Read ``key`` from a mapping or an object, never raising."""
    if source is None:
        return None
    if isinstance(source, dict):
        return source.get(key)
    return getattr(source, key, None)


def _as_plain_str(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else str(value)
    if isinstance(value, str):
        return value.strip()
    return ""


def _clean_display_name(value: object, *, limit: int = 100) -> str | None:
    """Trim and bound a nickname / group card.

    Returns ``None`` for "absent", matching the contract (``nickname`` and
    ``group_card`` are ``string|null``). Control characters are dropped rather
    than rejected: a missing or weird display name must never block a
    submission that has a valid QQ number.
    """
    text = _as_plain_str(value)
    if not text:
        return None
    cleaned = "".join(ch for ch in text if ch == "\t" or ch >= " ")
    cleaned = cleaned.strip()
    if not cleaned:
        return None
    return cleaned[:limit]


def _coerce_epoch(value: object) -> int | None:
    """Interpret a OneBot ``time`` field (seconds since epoch)."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def epoch_to_utc_iso_z(epoch_seconds: int, *, milliseconds: bool = False) -> str:
    """Format seconds-since-epoch as UTC ISO 8601 with a trailing ``Z``.

    The shape matches the backend's ``to_iso_z`` helper
    (``app/core/timeutil.py``) so the value round-trips unchanged.
    """
    moment = datetime.fromtimestamp(epoch_seconds, tz=UTC)
    if milliseconds:
        return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class IdentityDiagnostic:
    """A sanitised description of a rejected event (safe to log)."""

    reason: str
    field_name: str
    bot_id: str
    group_id: str
    qq_id: str

    def as_log_fields(self) -> dict[str, str]:
        return {
            "reason": self.reason,
            "field": self.field_name,
            "bot_id": self.bot_id,
            "group_id": self.group_id,
            "qq_id": self.qq_id,
        }


def extract_identity(message_obj: object) -> EventIdentity:
    """Build an :class:`EventIdentity` from an ``AstrBotMessage``.

    Args:
        message_obj: The event's ``message_obj`` (``AstrBotMessage``).

    Returns:
        The normalised identity.

    Raises:
        IdentityError: If a required field (bot id, group id, message id,
            sender QQ number) is missing or malformed, or if the raw OneBot
            event contradicts the unified message object.
    """
    raw = _dig(message_obj, "raw_message")
    sender = _dig(message_obj, "sender")
    raw_sender = _dig(raw, "sender")

    bot_id = _as_plain_str(_dig(message_obj, "self_id"))
    raw_bot_id = _as_plain_str(_dig(raw, "self_id"))
    if bot_id and raw_bot_id and bot_id != raw_bot_id:
        # Two independent identity sources disagree: refuse instead of picking.
        raise IdentityError("bot_id", "raw_event_mismatch")
    bot_id = bot_id or raw_bot_id

    group_id = _as_plain_str(_dig(message_obj, "group_id"))
    raw_group_id = _as_plain_str(_dig(raw, "group_id"))
    if group_id and raw_group_id and group_id != raw_group_id:
        raise IdentityError("group_id", "raw_event_mismatch")
    group_id = group_id or raw_group_id

    source_message_id = _as_plain_str(_dig(message_obj, "message_id"))
    raw_message_id = _as_plain_str(_dig(raw, "message_id"))
    if source_message_id and raw_message_id and source_message_id != raw_message_id:
        raise IdentityError("source_message_id", "raw_event_mismatch")
    source_message_id = source_message_id or raw_message_id

    qq_id = _as_plain_str(_dig(sender, "user_id"))
    raw_qq_id = _as_plain_str(_dig(raw_sender, "user_id"))
    if qq_id and raw_qq_id and qq_id != raw_qq_id:
        # e.g. a quote/reply adapter path that rebuilds the sender: never guess.
        raise IdentityError("qq_id", "raw_event_mismatch")
    qq_id = qq_id or raw_qq_id

    if not bot_id:
        raise IdentityError("bot_id", "missing")
    if not QQ_ID_RE.match(bot_id):
        raise IdentityError("bot_id", "not_a_qq_number")
    if not group_id:
        raise IdentityError("group_id", "missing")
    if not GROUP_ID_RE.match(group_id):
        raise IdentityError("group_id", "not_a_group_number")
    if not source_message_id:
        raise IdentityError("source_message_id", "missing")
    if not MESSAGE_ID_RE.match(source_message_id):
        raise IdentityError("source_message_id", "not_a_message_id")
    if not qq_id:
        raise IdentityError("qq_id", "missing")
    if not QQ_ID_RE.match(qq_id):
        raise IdentityError("qq_id", "not_a_qq_number")
    raw_user_id = _as_plain_str(_dig(raw, "user_id"))
    if raw_user_id and qq_id != raw_user_id:
        raise IdentityError("qq_id", "raw_event_mismatch")

    # Display names: prefer the raw OneBot sender because the unified
    # ``MessageMember.nickname`` already collapsed ``card or nickname``.
    nickname = _clean_display_name(_dig(raw_sender, "nickname"))
    group_card = _clean_display_name(_dig(raw_sender, "card"))
    if nickname is None and group_card is None:
        # Fall back to whatever the unified message carries (still never fatal).
        fallback = _clean_display_name(_dig(sender, "nickname"))
        if fallback:
            nickname = fallback

    try:
        epoch = _coerce_epoch(_dig(raw, "time"))
        source_timestamp = epoch_to_utc_iso_z(epoch) if epoch is not None else None
    except (ValueError, OverflowError, OSError):
        source_timestamp = None

    return EventIdentity(
        bot_id=bot_id,
        group_id=group_id,
        source_message_id=source_message_id,
        qq_id=qq_id,
        nickname=nickname,
        group_card=group_card,
        source_timestamp=source_timestamp,
    )


def try_extract_identity(
    message_obj: object,
) -> tuple[EventIdentity | None, IdentityDiagnostic | None]:
    """Non-raising wrapper around :func:`extract_identity` for the message path."""
    raw = _dig(message_obj, "raw_message")
    sender = _dig(message_obj, "sender")
    raw_sender = _dig(raw, "sender")
    try:
        return extract_identity(message_obj), None
    except IdentityError as exc:
        return (
            None,
            IdentityDiagnostic(
                reason=exc.reason,
                field_name=exc.field_name,
                bot_id=_as_plain_str(_dig(message_obj, "self_id"))
                or _as_plain_str(_dig(raw, "self_id")),
                group_id=_as_plain_str(_dig(message_obj, "group_id"))
                or _as_plain_str(_dig(raw, "group_id")),
                qq_id=_as_plain_str(_dig(sender, "user_id"))
                or _as_plain_str(_dig(raw_sender, "user_id")),
            ),
        )


def unified_msg_origin_of(event: object) -> str:
    """Return the event's ``unified_msg_origin``.

    The stored value is what AstrBot's ``Context.send_message`` needs later
    (``platform_id:message_type:session_id``), so receipts never depend on a
    live event object.
    """
    value = getattr(event, "unified_msg_origin", None)
    if isinstance(value, str) and value:
        return value
    # Fall back to reconstructing it from ``session``.
    session = getattr(event, "session", None)
    if session is not None:
        try:
            return str(session)
        except Exception:  # pragma: no cover - defensive
            return ""
    return ""


def platform_id_of(event: object) -> str:
    """Return the AstrBot platform instance id (needed to route receipts)."""
    getter = getattr(event, "get_platform_id", None)
    if callable(getter):
        try:
            value = getter()
            if isinstance(value, str):
                return value
        except Exception:  # pragma: no cover - defensive
            pass
    meta = getattr(event, "platform_meta", None)
    meta_id = getattr(meta, "id", None)
    return meta_id if isinstance(meta_id, str) else ""


def raw_message_of(message_obj: object) -> Any:
    """Expose the raw adapter payload (used only for diagnostics/counters)."""
    return _dig(message_obj, "raw_message")


__all__ = [
    "EventIdentity",
    "IdentityDiagnostic",
    "IdentityError",
    "epoch_to_utc_iso_z",
    "extract_identity",
    "platform_id_of",
    "raw_message_of",
    "try_extract_identity",
    "unified_msg_origin_of",
]
