"""Plugin configuration parsing and validation.

AstrBot hands the plugin an ``AstrBotConfig`` (a ``dict`` subclass built from
``_conf_schema.json``). Everything in here is deliberately framework-free so it
can be unit tested without AstrBot installed.

Design rules (see ``docs/PLUGIN_HANDOFF.md`` §configuration):

* a missing or malformed value disables the *submission* capability instead of
  crashing the whole bot — the reason is reported through ``Settings.problems``
  and logged once at plugin start;
* ``backend_url`` is a site **origin** (scheme + host + optional port). Paths,
  query strings, fragments and embedded credentials are rejected instead of
  being silently normalised, so a wrong value like
  ``http://host:18080/host`` fails loudly rather than posting to
  ``/host/api/v1/ingest/submissions``;
* tokens are never rendered into log messages or exception text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

EVENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

QQ_ID_RE = re.compile(r"^[0-9]{5,15}$")
GROUP_ID_RE = QQ_ID_RE
# OneBot message ids are integers; NapCat may report them as negative strings.
MESSAGE_ID_RE = re.compile(r"^-?[0-9]{1,20}$")

CONTENT_MIN_CODEPOINTS = 1
CONTENT_MAX_CODEPOINTS = 500

DEFAULT_COMMAND_NAME = "投稿"

_ALLOWED_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


class ConfigProblem(ValueError):
    """Raised for a single invalid configuration value."""


@dataclass(frozen=True)
class Origin:
    """A validated backend site origin (no path, query, fragment, credentials)."""

    scheme: str
    host: str
    port: int | None = None

    @property
    def base_url(self) -> str:
        """Origin as a string without a trailing slash, e.g. ``http://a:18080``."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        if self.port is None:
            return f"{self.scheme}://{host}"
        return f"{self.scheme}://{host}:{self.port}"

    @property
    def is_https(self) -> bool:
        return self.scheme == "https"

    def submission_url(self) -> str:
        """Absolute URL of the ingest endpoint.

        Built by concatenation on purpose: ``urljoin`` would drop a
        user-supplied path segment, and string-formatting with slashes risks
        producing ``//api/v1`` when a value contains a trailing slash.
        """
        return f"{self.base_url}/api/v1/ingest/submissions"

    def same_target(self, other: object) -> bool:
        return isinstance(other, Origin) and self.base_url == other.base_url


def parse_origin(raw: str) -> Origin:
    """Validate a user supplied ``backend_url`` value.

    Args:
        raw: Raw configuration value.

    Returns:
        The parsed origin.

    Raises:
        ConfigProblem: If the value is empty, is not an http/https origin, or
            carries a path/query/fragment/credentials.
    """
    value = (raw or "").strip()
    if not value:
        raise ConfigProblem(
            "backend_url 未填写。正确格式：http://服务器地址:18080（只填网站源地址）",
        )
    if value != value.strip():
        value = value.strip()
    try:
        parts = urlsplit(value)
    except ValueError as exc:
        raise ConfigProblem("backend_url 不是合法网站源地址，请检查主机名和 IPv6 方括号") from exc
    if parts.scheme not in ("http", "https"):
        raise ConfigProblem(
            f"backend_url 必须以 http:// 或 https:// 开头，当前为 {parts.scheme or '(空)'!r}。"
            "正确格式：http://服务器地址:18080",
        )
    if "@" in parts.netloc:
        raise ConfigProblem("backend_url 不能包含用户名或密码，请只填写网站源地址")
    if parts.query or parts.fragment:
        raise ConfigProblem("backend_url 不能包含 ?query 或 #fragment，请只填写网站源地址")
    if parts.path not in ("", "/"):
        raise ConfigProblem(
            f"backend_url 不能包含路径（当前为 {parts.path!r}）。"
            "请去掉 /host、/screen、/api/v1 等后缀，只保留 http(s)://主机[:端口]",
        )
    host = parts.hostname
    if not host:
        raise ConfigProblem("backend_url 缺少主机名，正确格式：http://服务器地址:18080")
    try:
        port = parts.port
    except ValueError as exc:  # pragma: no cover - urlsplit raises for bad ports
        raise ConfigProblem("backend_url 的端口不是合法数字") from exc
    if port is not None and not (1 <= port <= 65535):
        raise ConfigProblem("backend_url 的端口必须在 1–65535 之间")
    if not re.fullmatch(r"[A-Za-z0-9._\-\[\]:]+", host):
        raise ConfigProblem("backend_url 的主机名包含不支持的字符")
    return Origin(scheme=parts.scheme, host=host, port=port)


def _coerce_bool(raw: object, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        if lowered in ("true", "yes", "1", "on"):
            return True
        if lowered in ("false", "no", "0", "off"):
            return False
    return default


def _coerce_float(
    raw: object,
    default: float,
    *,
    minimum: float,
    maximum: float,
    name: str,
    problems: list[str],
) -> float:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return default
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        problems.append(f"{name} 不是合法数字，已使用默认值 {default}")
        return default
    if not (minimum <= value <= maximum):
        problems.append(f"{name} 必须在 {minimum}–{maximum} 之间，已使用默认值 {default}")
        return default
    return value


def _coerce_int(
    raw: object,
    default: int,
    *,
    minimum: int,
    maximum: int,
    name: str,
    problems: list[str],
) -> int:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        problems.append(f"{name} 不是合法整数，已使用默认值 {default}")
        return default
    if not (minimum <= value <= maximum):
        problems.append(f"{name} 必须在 {minimum}–{maximum} 之间，已使用默认值 {default}")
        return default
    return value


def _coerce_str_list(raw: object, name: str, problems: list[str]) -> tuple[str, ...]:
    """Accept a list (or comma/space separated string) of group ids."""
    items: list[str] = []
    if raw is None:
        return ()
    if isinstance(raw, str):
        items = [part for part in re.split(r"[,;\s]+", raw.strip()) if part]
    elif isinstance(raw, (list, tuple, set)):
        for entry in raw:
            if entry is None:
                continue
            for part in re.split(r"[,;\s]+", str(entry).strip()):
                if part:
                    items.append(part)
    else:
        problems.append(f"{name} 必须是字符串列表，已忽略非法值")
        return ()
    seen: list[str] = []
    for item in items:
        if not GROUP_ID_RE.match(item):
            problems.append(f"{name} 中的 {item!r} 不是 5–15 位 QQ 群号，已忽略")
            continue
        if item not in seen:
            seen.append(item)
    return tuple(seen)


@dataclass(frozen=True)
class Settings:
    """Validated plugin settings."""

    enabled: bool = False
    origin: Origin | None = None
    ingest_token: str = field(default="", repr=False)
    event_id: str = ""
    allowed_group_ids: tuple[str, ...] = ()
    tls_verify: bool = True
    use_env_proxy: bool = False
    connect_timeout_seconds: float = 3.0
    request_timeout_seconds: float = 10.0
    max_attempts: int = 20
    delivery_ttl_seconds: int = 600
    queue_capacity: int = 1000
    user_interval_seconds: int = 10
    group_per_minute: int = 60
    initial_reply_wait_seconds: float = 8.0
    defer_notice_delay_seconds: float = 5.0
    log_level: str = "INFO"
    command_name: str = DEFAULT_COMMAND_NAME
    problems: tuple[str, ...] = ()

    @property
    def command_token(self) -> str:
        """Command token including the leading slash, e.g. ``/投稿``."""
        return f"/{self.command_name}"

    @property
    def backend_base_url(self) -> str:
        return self.origin.base_url if self.origin else ""

    @property
    def can_submit(self) -> bool:
        """True when the plugin is fully usable and may accept submissions."""
        return (
            self.enabled
            and self.origin is not None
            and bool(self.ingest_token)
            and bool(self.event_id)
            and bool(self.allowed_group_ids)
        )

    def is_group_allowed(self, group_id: str) -> bool:
        return bool(group_id) and group_id in self.allowed_group_ids


def load_settings(config: object) -> Settings:
    """Build :class:`Settings` from an AstrBot plugin config mapping.

    Args:
        config: Mapping-like plugin config (``AstrBotConfig`` or a plain dict).

    Returns:
        Parsed settings. Invalid values never raise: they are recorded in
        ``Settings.problems`` and the capability is disabled instead.
    """
    mapping = config if hasattr(config, "get") else {}
    problems: list[str] = []

    def get(key: str, default: object = None) -> object:
        try:
            value = mapping.get(key, default)  # type: ignore[union-attr]
        except Exception:  # pragma: no cover - defensive against odd mappings
            return default
        return default if value is None else value

    enabled = _coerce_bool(get("enabled", False), False)

    origin: Origin | None = None
    raw_origin = str(get("backend_url", "") or "")
    if raw_origin.strip():
        try:
            origin = parse_origin(raw_origin)
        except ConfigProblem as exc:
            problems.append(str(exc))
    elif enabled:
        problems.append("backend_url 未填写，投稿能力已禁用")

    ingest_token = str(get("ingest_token", "") or "").strip()
    if not ingest_token and enabled:
        problems.append("ingest_token 未填写，投稿能力已禁用")
    if ingest_token != ingest_token.strip("\r\n"):
        problems.append("ingest_token 含首尾换行符，已自动去除，请检查粘贴内容")

    event_id = str(get("event_id", "") or "").strip()
    if event_id and not EVENT_ID_RE.match(event_id):
        problems.append(
            "event_id 不合法（应为字母或数字开头、只含字母数字点下划线连字符、≤64 字符），已忽略",
        )
        event_id = ""
    elif not event_id and enabled:
        problems.append("event_id 未填写，投稿能力已禁用")

    allowed_group_ids = _coerce_str_list(
        get("allowed_group_ids", []),
        "allowed_group_ids",
        problems,
    )
    if enabled and not allowed_group_ids:
        problems.append("allowed_group_ids 为空，不会接收任何群的投稿")

    command_name = str(get("command_name", DEFAULT_COMMAND_NAME) or "").strip()
    if not command_name or " " in command_name or command_name.startswith("/"):
        problems.append(
            f"command_name 不合法（不能为空、不能含空格、不要带斜杠），已使用默认值 {DEFAULT_COMMAND_NAME}",
        )
        command_name = DEFAULT_COMMAND_NAME

    log_level = str(get("log_level", "INFO") or "INFO").strip().upper()
    if log_level not in _ALLOWED_LOG_LEVELS:
        problems.append(f"log_level 不在 {_ALLOWED_LOG_LEVELS} 中，已使用 INFO")
        log_level = "INFO"

    settings = Settings(
        enabled=enabled,
        origin=origin,
        ingest_token=ingest_token,
        event_id=event_id,
        allowed_group_ids=allowed_group_ids,
        tls_verify=_coerce_bool(get("tls_verify", True), True),
        use_env_proxy=_coerce_bool(get("use_env_proxy", False), False),
        connect_timeout_seconds=_coerce_float(
            get("connect_timeout_seconds"),
            3.0,
            minimum=0.5,
            maximum=30.0,
            name="connect_timeout_seconds",
            problems=problems,
        ),
        request_timeout_seconds=_coerce_float(
            get("request_timeout_seconds"),
            10.0,
            minimum=1.0,
            maximum=120.0,
            name="request_timeout_seconds",
            problems=problems,
        ),
        max_attempts=_coerce_int(
            get("max_attempts"),
            20,
            minimum=1,
            maximum=100,
            name="max_attempts",
            problems=problems,
        ),
        delivery_ttl_seconds=_coerce_int(
            get("delivery_ttl_seconds"),
            600,
            minimum=30,
            maximum=86400,
            name="delivery_ttl_seconds",
            problems=problems,
        ),
        queue_capacity=_coerce_int(
            get("queue_capacity"),
            1000,
            minimum=10,
            maximum=100000,
            name="queue_capacity",
            problems=problems,
        ),
        user_interval_seconds=_coerce_int(
            get("user_interval_seconds"),
            10,
            minimum=0,
            maximum=600,
            name="user_interval_seconds",
            problems=problems,
        ),
        group_per_minute=_coerce_int(
            get("group_per_minute"),
            60,
            minimum=0,
            maximum=100000,
            name="group_per_minute",
            problems=problems,
        ),
        initial_reply_wait_seconds=_coerce_float(
            get("initial_reply_wait_seconds"),
            8.0,
            minimum=1.0,
            maximum=25.0,
            name="initial_reply_wait_seconds",
            problems=problems,
        ),
        defer_notice_delay_seconds=_coerce_float(
            get("defer_notice_delay_seconds"),
            5.0,
            minimum=0.0,
            maximum=20.0,
            name="defer_notice_delay_seconds",
            problems=problems,
        ),
        log_level=log_level,
        command_name=command_name,
        problems=tuple(problems),
    )
    return settings


__all__ = [
    "CONTENT_MAX_CODEPOINTS",
    "CONTENT_MIN_CODEPOINTS",
    "DEFAULT_COMMAND_NAME",
    "EVENT_ID_RE",
    "GROUP_ID_RE",
    "MESSAGE_ID_RE",
    "QQ_ID_RE",
    "ConfigProblem",
    "Origin",
    "Settings",
    "load_settings",
    "parse_origin",
]
