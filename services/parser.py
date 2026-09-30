"""Command boundary detection and message-segment parsing.

Why this module exists
----------------------
AstrBot's built-in ``@filter.command`` handler is *not* usable for ``/投稿``:

* ``CommandFilter.filter`` collapses every run of whitespace
  (``re.sub(r"\\s+", " ", ...)``) and then splits the remainder on ``" "``,
  which destroys multi-line submissions (AstrBot v4.28.2
  ``astrbot/core/star/filter/command.py`` lines 199-211);
* only the *first* ``@filter.command`` handler that creates the shared
  ``StarHandlerMetadata`` wins, so stacking a bare command plus a listener on one
  method is fragile.

So the plugin registers exactly **one** message listener and parses the raw
message chain itself. Segment-level inspection is mandatory: the OneBot v11
adapter (``aiocqhttp_platform_adapter.py`` lines 243-419) already drops some
non-text segments (``mface``), converts others (``reply`` -> ``Reply`` with the
quoted text embedded) and appends spaces around mentions, so the flattened
``event.message_str`` cannot tell "the user quoted an image" from "the user
typed text".

The module is intentionally import-free of AstrBot: it accepts anything
iterable that yields objects with a ``type`` attribute (or a ``{"type", "data"}``
mapping) so it can be unit tested standalone.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .settings import CONTENT_MAX_CODEPOINTS

# --- result codes -----------------------------------------------------------

NOT_COMMAND = "not_command"
"""The message is not a submission command; stay completely silent."""

EMPTY = "empty"
"""The command was used but carries no text: reply with usage."""

ATTACHMENT_ONLY = "attachment_only"
"""Non-text segments are mixed in: reply that only text is supported."""

TOO_LONG = "too_long"
"""Text exceeds the code-point limit."""

OK = "ok"
"""A submittable text payload was extracted."""


@dataclass(frozen=True)
class Segment:
    """Normalised view of one message segment."""

    kind: str
    """Normalised kind, e.g. ``text``/``at``/``image``/``reply``/``face``."""
    text: str | None = None
    at_qq: str | None = None


@dataclass(frozen=True)
class ParseResult:
    """Outcome of parsing one inbound message."""

    code: str
    content: str = ""
    """Extracted submission text (only meaningful when ``code == OK``)."""
    length: int = 0
    """Code-point length of ``content`` after trimming."""
    reason: str = ""
    """Short machine-readable reason for diagnostics (never sent to the group)."""

    @property
    def is_command(self) -> bool:
        return self.code != NOT_COMMAND


# Non-text kinds that mean "an attachment / a non-plain structure was involved".
_ATTACHMENT_KINDS = frozenset(
    {
        "image",
        "record",
        "video",
        "file",
        "face",
        "mface",
        "reply",
        "forward",
        "node",
        "nodes",
        "json",
        "share",
        "music",
        "contact",
        "location",
        "poke",
        "rps",
        "dice",
        "shake",
        "unknown",
        "markdown",
    },
)

_TEXT_KINDS = frozenset({"text", "plain"})
_AT_KINDS = frozenset({"at"})
_AT_ALL_KINDS = frozenset({"atall", "at_all"})

_KNOWN_TEXT_ATTRS = ("text", "content", "message_str")


def _kind_of(raw: object) -> str:
    """Best-effort normalisation of a segment's type marker.

    Handles both AstrBot ``BaseMessageComponent`` subclasses (``type`` is a
    ``ComponentType`` string enum) and raw OneBot ``{"type": ..., "data": ...}``
    dicts. Unknown shapes degrade to ``unknown`` instead of raising, so an
    unfamiliar adapter message can never crash the message pipeline.
    """
    kind: Any = None
    if isinstance(raw, dict):
        kind = raw.get("type")
    else:
        kind = getattr(raw, "type", None)
    if kind is None:
        return "unknown"
    name = getattr(kind, "value", kind)
    text = str(name).strip().lower()
    if text.startswith("componenttype."):  # pragma: no cover - defensive
        text = text.split(".", 1)[1]
    return text or "unknown"


def _text_of(raw: object) -> str:
    """Extract plain text from a text segment."""
    if isinstance(raw, dict):
        data = raw.get("data")
        if isinstance(data, dict):
            for key in _KNOWN_TEXT_ATTRS:
                value = data.get(key)
                if isinstance(value, str):
                    return value
        for key in _KNOWN_TEXT_ATTRS:
            value = raw.get(key)
            if isinstance(value, str):
                return value
        return ""
    for key in _KNOWN_TEXT_ATTRS:
        value = getattr(raw, key, None)
        if isinstance(value, str):
            return value
    return ""


def _at_qq_of(raw: object) -> str:
    if isinstance(raw, dict):
        data = raw.get("data")
        qq = data.get("qq") if isinstance(data, dict) else raw.get("qq")
    else:
        qq = getattr(raw, "qq", None)
    if qq is None:
        return ""
    return str(qq)


def normalise_segments(raw_segments: object) -> list[Segment]:
    """Convert an arbitrary message chain into :class:`Segment` objects."""
    segments: list[Segment] = []
    if raw_segments is None:
        return segments
    try:
        iterator = list(raw_segments)  # type: ignore[arg-type]
    except TypeError:
        return segments
    for raw in iterator:
        kind = _kind_of(raw)
        if kind in _TEXT_KINDS:
            text = _text_of(raw)
            if segments and segments[-1].kind == "text":
                segments[-1] = Segment(kind="text", text=segments[-1].text + text)
            else:
                segments.append(Segment(kind="text", text=text))
        elif kind in _AT_KINDS or kind in _AT_ALL_KINDS:
            segments.append(Segment(kind="at", at_qq=_at_qq_of(raw)))
        else:
            segments.append(Segment(kind=kind))
    return segments


def is_wake_mention(segment: Segment, self_id: str) -> bool:
    """True when the segment is an @-mention used only to wake the bot.

    ``@全体成员`` intentionally does **not** count: a submission that starts
    with an at-all is about other people, not the bot.
    """
    return segment.kind == "at" and bool(self_id) and segment.at_qq == self_id


def _is_all_whitespace(value: str) -> bool:
    return not value.strip()


def normalise_line_endings(text: str) -> str:
    """Convert CRLF/CR to LF.

    Must match the backend exactly: ``app/services/validation.py``
    ``normalize_content`` does ``replace("\\r\\n", "\\n").replace("\\r", "\\n")``
    before trimming. Doing it on both sides keeps the payload that is persisted
    in the outbox byte-identical to what the backend stores, and keeps the local
    idempotency comparison stable if a client ever resends with different line
    endings.
    """
    return text.replace("\r\n", "\n").replace("\r", "\n")


@dataclass
class _Walker:
    command_token: str
    self_id: str
    parts: list[str] = field(default_factory=list)
    command_seen: bool = False
    attachment_before_or_inside: bool = False

    def result(self) -> ParseResult:
        if not self.command_seen:
            return ParseResult(NOT_COMMAND, reason="command_not_found")
        raw = normalise_line_endings("".join(self.parts))
        content = raw.strip()
        if self.attachment_before_or_inside:
            return ParseResult(ATTACHMENT_ONLY, reason="non_text_segment")
        if not content:
            return ParseResult(EMPTY, reason="empty_after_command")
        length = len(content)
        if length > CONTENT_MAX_CODEPOINTS:
            return ParseResult(
                TOO_LONG,
                content=content,
                length=length,
                reason="over_code_point_limit",
            )
        return ParseResult(OK, content=content, length=length)

    def feed(self, segment: Segment) -> bool:
        """Consume one segment. Returns False once nothing more is accepted."""
        if segment.kind == "text":
            text = segment.text or ""
            if not self.command_seen:
                if _is_all_whitespace(text):
                    return True
                stripped = text.lstrip()
                if not stripped.startswith(self.command_token):
                    return False
                rest = stripped[len(self.command_token) :]
                # ``/投稿箱`` / ``/投稿你好`` are different commands: the token must
                # be followed by whitespace/newline or end of the segment.
                if rest and not rest[0].isspace():
                    return False
                self.command_seen = True
                self.parts.append(rest)
                return True
            self.parts.append(text)
            return True

        if segment.kind == "at":
            if not self.command_seen:
                # Allow exactly the "wake the bot with @" form; any other mention
                # before the command means this is not our command.
                if is_wake_mention(segment, self.self_id):
                    return True
                return False
            # Inside the body any mention (bot or others) is unsupported.
            self.attachment_before_or_inside = True
            return True

        # Any other non-text kind.
        if not self.command_seen:
            if segment.kind in ("empty", "spacer"):
                return True
            # A quote/file can precede the command. Scan the following text to
            # give the unsupported-attachment reply, without reading its author.
            self.attachment_before_or_inside = True
            return True
        self.attachment_before_or_inside = True
        return True


def parse_submission(
    segments: object,
    *,
    command_token: str = "/投稿",
    self_id: str = "",
) -> ParseResult:
    """Parse a normalised-or-raw message chain into a submission result.

    Args:
        segments: Iterable of message segments (AstrBot components or OneBot
            segment dicts).
        command_token: Command token including the leading slash.
        self_id: The bot's own account id, used to recognise the wake @.

    Returns:
        A :class:`ParseResult`. Only :data:`OK` results carry submittable text.
    """
    provided = _as_list(segments)
    if provided and all(isinstance(item, Segment) for item in provided):
        chain: list[Segment] = provided  # type: ignore[assignment]
    else:
        chain = normalise_segments(provided)

    walker = _Walker(command_token=command_token, self_id=str(self_id))
    for segment in chain:
        # Keep scanning to the very end: a trailing attachment or mention must
        # invalidate an otherwise acceptable body.
        if not walker.feed(segment):
            return ParseResult(NOT_COMMAND, reason="not_this_command")
    return walker.result()


def _as_list(segments: object) -> list[object]:
    if segments is None:
        return []
    try:
        return list(segments)  # type: ignore[arg-type]
    except TypeError:
        return []


__all__ = [
    "ATTACHMENT_ONLY",
    "EMPTY",
    "NOT_COMMAND",
    "OK",
    "TOO_LONG",
    "ParseResult",
    "Segment",
    "is_wake_mention",
    "normalise_line_endings",
    "normalise_segments",
    "parse_submission",
]
