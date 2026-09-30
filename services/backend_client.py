"""HTTP client for ``POST {backend_url}/api/v1/ingest/submissions``.

Contract source of truth: ``contracts/openapi.json``, ``docs/API.md`` §3 and
``thp_post_backend/app/api/routes_ingest.py`` (read at implementation time).

Key behaviours enforced here
----------------------------
* only **200** and **201** count as success, and only when the body has the
  contract shape (``duplicate`` boolean + ``submission.id`` integer + the
  timestamps). A 200 that returns an HTML error page from a proxy is *not*
  success — it is classified ``unsupported`` and retried within the delivery
  deadline, with a redacted diagnostic for the operator;
* redirects are never followed (``follow_redirects=False``) so the
  ``Authorization`` header and submission body can never be replayed to another
  origin; a 3xx is reported as a backend/proxy misconfiguration;
* the ingest token is only ever sent in the ``Authorization`` header of the
  configured origin — never in a URL, query string or log line;
* environment proxies are ignored unless ``use_env_proxy`` is explicitly set,
  because a container-inherited ``HTTP_PROXY`` pointing nowhere is a common
  cause of "connection reset" in Chinese hosting environments.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from .settings import Origin, Settings

logger = logging.getLogger(__name__)

SUBMISSION_PATH = "/api/v1/ingest/submissions"

# --- outcome kinds ----------------------------------------------------------

OK = "ok"
"""Backend confirmed persistence (200 or 201) with a valid contract body."""

RETRY = "retry"
"""Transient failure: resend the *same* payload, honouring ``retry_after``."""

REJECTED = "rejected"
"""The backend refused this submission; retrying cannot help."""

CREDENTIALS = "credentials"
"""Global credential problem (401/403 role/token); pause all network delivery."""

MISCONFIGURED = "misconfigured"
"""Address/proxy problem (3xx, wrong path); retrying the same URL cannot help."""


@dataclass(frozen=True)
class SubmissionReceipt:
    """Parsed success body."""

    submission_id: int
    duplicate: bool
    created_at: str | None = None
    source_timestamp: str | None = None


@dataclass(frozen=True)
class DeliveryAttempt:
    """Result of one HTTP attempt."""

    kind: str
    receipt: SubmissionReceipt | None = None
    status_code: int | None = None
    error_code: str = ""
    """Stable ``error.code`` from the backend envelope, or a local marker."""
    error_message: str = ""
    """Backend message (already bounded/redacted), for logs only."""
    request_id: str = ""
    retry_after: float | None = None
    """Seconds to wait before the next attempt (429 ``Retry-After`` or 503)."""
    diagnostic: str = ""
    """Redacted technical detail for the operator log."""
    elapsed_ms: int = 0

    @property
    def succeeded(self) -> bool:
        return self.kind == OK and self.receipt is not None

    def log_fields(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "status": self.status_code,
            "error_code": self.error_code,
            "request_id": self.request_id,
            "elapsed_ms": self.elapsed_ms,
            "diagnostic": self.diagnostic,
        }


def parse_retry_after(value: str | None, *, now: float | None = None) -> float | None:
    """Parse an HTTP ``Retry-After`` header.

    Accepts both the delta-seconds and the HTTP-date forms (RFC 9110). Returns
    ``None`` when the header is missing or unusable, in which case the caller
    falls back to the regular backoff schedule.

    Args:
        value: Raw header value.
        now: Reference timestamp (defaults to ``time.time()``).

    Returns:
        Non-negative seconds to wait, or ``None``.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        pass
    else:
        if seconds != seconds or seconds in (float("inf"), float("-inf")):  # NaN/inf
            return None
        return max(0.0, seconds)
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    reference = time.time() if now is None else now
    return max(0.0, moment.timestamp() - reference)


def _bounded_message(value: object, limit: int = 200) -> str:
    text = str(value or "").strip().replace("\n", " ").replace("\r", " ")
    if len(text) > limit:
        return text[:limit] + "…"
    return text


class BackendClient:
    """Async HTTP client bound to one configured backend origin."""

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Any = None,
    ) -> None:
        """
        Args:
            settings: Validated plugin settings; ``settings.origin`` and
                ``settings.ingest_token`` must be usable.
            transport: Optional httpx transport override (tests inject a
                ``httpx.MockTransport`` or an ``ASGITransport``).
            clock: Optional monotonic-clock callable (tests use a fake clock).
        """
        if settings.origin is None:
            raise ValueError("BackendClient requires a validated backend_url")
        self._settings = settings
        self._origin: Origin = settings.origin
        self._clock = clock or time.monotonic
        timeout = httpx.Timeout(
            settings.request_timeout_seconds,
            connect=settings.connect_timeout_seconds,
        )
        self._client = httpx.AsyncClient(
            transport=transport,
            timeout=timeout,
            verify=settings.tls_verify,
            follow_redirects=False,
            trust_env=settings.use_env_proxy,
            headers={
                "User-Agent": "astrbot-plugin-thp-post/1.0 (+onebot-v11)",
                "Accept": "application/json",
            },
        )

    # -- lifecycle ----------------------------------------------------------

    async def aclose(self) -> None:
        """Close the underlying connection pool (idempotent)."""
        try:
            await self._client.aclose()
        except Exception:  # pragma: no cover - closed twice
            pass

    @property
    def origin(self) -> Origin:
        return self._origin

    @property
    def submission_url(self) -> str:
        return self._origin.submission_url()

    def matches(self, origin_base_url: str) -> bool:
        """True when ``origin_base_url`` equals this client's target."""
        return self._origin.base_url == origin_base_url

    # -- request ------------------------------------------------------------

    async def submit(
        self,
        payload: dict[str, Any],
        *,
        ingest_token: str | None = None,
    ) -> DeliveryAttempt:
        """Send one submission payload.

        The payload is sent verbatim; callers must pass the payload persisted in
        the outbox so retries reuse the identical body (including the original
        ``source_message_id`` and ``source_timestamp``).

        Args:
            payload: Contract-shaped request body.
            ingest_token: Override for the token (defaults to the configured
                one). Used by tests only.

        Returns:
            A :class:`DeliveryAttempt`. This method never raises for HTTP or
            transport errors — failures are classified instead.
        """
        token = self._settings.ingest_token if ingest_token is None else ingest_token
        started = self._clock()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        try:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            return DeliveryAttempt(
                kind=REJECTED,
                error_code="payload_not_serialisable",
                diagnostic=type(exc).__name__,
            )

        try:
            response = await asyncio.wait_for(
                self._client.post(self.submission_url, content=body, headers=headers),
                timeout=self._settings.request_timeout_seconds,
            )
        except TimeoutError as exc:
            return self._transport_failure("timeout", exc, started)
        except httpx.TimeoutException as exc:
            return self._transport_failure("timeout", exc, started)
        except httpx.ConnectError as exc:
            return self._transport_failure("connect_error", exc, started)
        except httpx.TooManyRedirects as exc:  # pragma: no cover - follow off
            return self._transport_failure("too_many_redirects", exc, started)
        except httpx.TransportError as exc:
            return self._transport_failure("transport_error", exc, started)
        except Exception as exc:  # pragma: no cover - defensive
            return self._transport_failure("unexpected_error", exc, started)

        elapsed_ms = int((self._clock() - started) * 1000)
        return self._classify(response, elapsed_ms)

    def _transport_failure(
        self, marker: str, exc: BaseException, started: float
    ) -> DeliveryAttempt:
        elapsed_ms = int((self._clock() - started) * 1000)
        # Exception text can echo the URL; it never contains the token because
        # the token only lives in a header.
        return DeliveryAttempt(
            kind=RETRY,
            error_code=marker,
            diagnostic=_bounded_message(f"{type(exc).__name__}: {exc}", 200),
            elapsed_ms=elapsed_ms,
        )

    def _classify(self, response: httpx.Response, elapsed_ms: int) -> DeliveryAttempt:
        status = response.status_code
        request_id = response.headers.get("X-Request-Id", "") or ""

        if 300 <= status < 400:
            location = response.headers.get("Location", "")
            return DeliveryAttempt(
                kind=MISCONFIGURED,
                status_code=status,
                error_code="unexpected_redirect",
                diagnostic=(
                    f"HTTP {status} 重定向到 {_bounded_message(location, 120)!r}；"
                    "插件不跟随重定向，请检查 backend_url 与反向代理配置"
                ),
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )

        body_text = ""
        try:
            body_text = response.text
        except Exception:  # pragma: no cover - defensive
            body_text = ""
        parsed: Any = None
        if body_text:
            try:
                parsed = json.loads(body_text)
            except (ValueError, TypeError):
                parsed = None

        if status in (200, 201):
            return self._classify_success(status, parsed, body_text, request_id, elapsed_ms)

        if status == 401 or status == 403:
            code = self._error_code(parsed)
            if code in ("unauthorized", "forbidden_role", ""):
                return DeliveryAttempt(
                    kind=CREDENTIALS,
                    status_code=status,
                    error_code=code or "unauthorized",
                    error_message=_bounded_message(self._error_message(parsed)),
                    diagnostic=(
                        "投稿令牌无效或角色不符：请核对插件 ingest_token 与后端 INGEST_TOKEN "
                        "是否为同一个值（不要填 HOST_TOKEN / DISPLAY_TOKEN）"
                    ),
                    request_id=request_id,
                    elapsed_ms=elapsed_ms,
                )
            return DeliveryAttempt(
                kind=REJECTED,
                status_code=status,
                error_code=code,
                error_message=_bounded_message(self._error_message(parsed)),
                diagnostic="后端拒绝该投稿（群/活动配置问题）",
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )

        if status == 409:
            code = self._error_code(parsed)
            return DeliveryAttempt(
                kind=REJECTED,
                status_code=status,
                error_code=code or "conflict",
                error_message=_bounded_message(self._error_message(parsed)),
                diagnostic="幂等冲突：同一来源消息此前已入库且内容不同，按契约终止重试",
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )

        if status == 429:
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is None:
                retry_after = self._retry_after_from_body(parsed)
            return DeliveryAttempt(
                kind=RETRY,
                status_code=status,
                error_code=self._error_code(parsed) or "rate_limited",
                error_message=_bounded_message(self._error_message(parsed)),
                retry_after=retry_after,
                diagnostic=(
                    "触发后端限流"
                    if retry_after is not None
                    else "触发后端限流（Retry-After 缺失或不可解析，按常规退避）"
                ),
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )

        if status in (413, 422, 400):
            code = self._error_code(parsed)
            return DeliveryAttempt(
                kind=REJECTED,
                status_code=status,
                error_code=code or f"http_{status}",
                error_message=_bounded_message(self._error_message(parsed)),
                diagnostic="请求体被后端判定为非法，重试无意义",
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )

        if status >= 500:
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            return DeliveryAttempt(
                kind=RETRY,
                status_code=status,
                error_code=self._error_code(parsed) or f"http_{status}",
                error_message=_bounded_message(self._error_message(parsed)),
                retry_after=retry_after,
                diagnostic="后端暂时不可用，将按原 payload 重试",
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )

        # 404 / 405 / anything else: address or version mismatch. Retrying the
        # identical URL will not help, so this terminates the delivery.
        return DeliveryAttempt(
            kind=MISCONFIGURED,
            status_code=status,
            error_code=self._error_code(parsed) or f"http_{status}",
            error_message=_bounded_message(self._error_message(parsed)),
            diagnostic=(
                f"HTTP {status}：请检查 backend_url 是否为网站源地址"
                "（不要带 /host、/screen 或 /api 路径），以及反向代理是否保留了 /api 前缀"
            ),
            request_id=request_id,
            elapsed_ms=elapsed_ms,
        )

    def _classify_success(
        self,
        status: int,
        parsed: Any,
        body_text: str,
        request_id: str,
        elapsed_ms: int,
    ) -> DeliveryAttempt:
        if not isinstance(parsed, dict):
            return DeliveryAttempt(
                kind=RETRY,
                status_code=status,
                error_code="non_json_success_body",
                diagnostic=(
                    "HTTP 200/201 但响应体不是 JSON（很可能是反向代理返回了 HTML），"
                    "按结果不确定有限重试；响应正文不写入日志"
                ),
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )
        duplicate = parsed.get("duplicate")
        submission = parsed.get("submission")
        if not isinstance(duplicate, bool) or not isinstance(submission, dict):
            return DeliveryAttempt(
                kind=RETRY,
                status_code=status,
                error_code="unexpected_success_body",
                diagnostic="HTTP 200/201 但响应体缺少 duplicate/submission 字段，按结果不确定有限重试",
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )
        submission_id = submission.get("id")
        if isinstance(submission_id, bool) or not isinstance(submission_id, int):
            return DeliveryAttempt(
                kind=RETRY,
                status_code=status,
                error_code="missing_submission_id",
                diagnostic="HTTP 200/201 但未返回整数 submission.id，不能当作成功，按结果不确定有限重试",
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )
        if submission_id < 1:
            return DeliveryAttempt(
                kind=RETRY,
                status_code=status,
                error_code="invalid_submission_id",
                diagnostic="HTTP 200/201 但 submission.id 不是正整数，按结果不确定有限重试",
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )
        created_at = submission.get("created_at")
        source_timestamp = submission.get("source_timestamp")
        try:
            for value in (created_at, submission.get("updated_at")):
                if (
                    not isinstance(value, str)
                    or datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is None
                ):
                    raise ValueError("missing timezone")
            if source_timestamp is not None:
                if (
                    not isinstance(source_timestamp, str)
                    or datetime.fromisoformat(source_timestamp.replace("Z", "+00:00")).tzinfo
                    is None
                ):
                    raise ValueError("invalid source timestamp")
        except (TypeError, ValueError):
            return DeliveryAttempt(
                kind=RETRY,
                status_code=status,
                error_code="invalid_success_timestamp",
                diagnostic="成功响应缺少合法的带时区时间，按结果不确定有限重试",
                request_id=request_id,
                elapsed_ms=elapsed_ms,
            )
        return DeliveryAttempt(
            kind=OK,
            status_code=status,
            error_code="",
            request_id=request_id,
            elapsed_ms=elapsed_ms,
            receipt=SubmissionReceipt(
                submission_id=submission_id,
                duplicate=duplicate,
                created_at=created_at if isinstance(created_at, str) else None,
                source_timestamp=source_timestamp if isinstance(source_timestamp, str) else None,
            ),
        )

    @staticmethod
    def _error_code(parsed: Any) -> str:
        if isinstance(parsed, dict):
            error = parsed.get("error")
            if isinstance(error, dict):
                code = error.get("code")
                if isinstance(code, str):
                    return code.strip()
        return ""

    @staticmethod
    def _error_message(parsed: Any) -> str:
        if isinstance(parsed, dict):
            error = parsed.get("error")
            if isinstance(error, dict):
                message = error.get("message")
                if isinstance(message, str):
                    return message
        return ""

    @staticmethod
    def _retry_after_from_body(parsed: Any) -> float | None:
        """Fall back to the contract's ``details.retry_after_seconds``."""
        if isinstance(parsed, dict):
            error = parsed.get("error")
            if isinstance(error, dict):
                details = error.get("details")
                if isinstance(details, dict):
                    value = details.get("retry_after_seconds")
                    if isinstance(value, bool):
                        return None
                    if isinstance(value, (int, float)) and value >= 0:
                        return float(value)
        return None


__all__ = [
    "CREDENTIALS",
    "MISCONFIGURED",
    "OK",
    "REJECTED",
    "RETRY",
    "SUBMISSION_PATH",
    "BackendClient",
    "DeliveryAttempt",
    "SubmissionReceipt",
    "parse_retry_after",
]
