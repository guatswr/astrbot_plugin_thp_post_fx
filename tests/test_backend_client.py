"""HTTP client: success validation, error classification, retry hints, transport."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import httpx
import pytest

from services.backend_client import (
    CREDENTIALS,
    MISCONFIGURED,
    OK,
    REJECTED,
    RETRY,
    BackendClient,
    parse_retry_after,
)
from services.settings import load_settings

TOKEN = "unit-test-token-0123456789abcdef"
ORIGIN = "http://203.0.113.10:18080"

CONFIG: dict[str, Any] = {
    "enabled": True,
    "backend_url": ORIGIN,
    "ingest_token": TOKEN,
    "event_id": "thp-demo-2026",
    "allowed_group_ids": ["123456789"],
}

PAYLOAD: dict[str, Any] = {
    "event_id": "thp-demo-2026",
    "bot_id": "100000002",
    "group_id": "123456789",
    "source_message_id": "40001",
    "qq_id": "100000001",
    "nickname": "样例昵称",
    "group_card": "样例群名片",
    "content": "正文 🎉",
    "source_timestamp": "2026-10-03T10:00:00Z",
}


def _success_body(submission_id: int = 321, duplicate: bool = False) -> dict[str, Any]:
    return {
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
    }


def _error_body(code: str, retryable: bool = False, details: dict | None = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": f"{code} message", "retryable": retryable}
    if details:
        error["details"] = details
    return {"error": error, "request_id": "req-123"}


def make_client(handler, **config_overrides: Any) -> BackendClient:
    config = dict(CONFIG)
    config.update(config_overrides)
    settings = load_settings(config)
    return BackendClient(settings, transport=httpx.MockTransport(handler))


def json_response(
    status: int,
    body: Any,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(
        status,
        json=body,
        headers=headers or {},
    )


# --- success ----------------------------------------------------------------


async def test_201_created_is_success() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("Authorization")
        seen["content_type"] = request.headers.get("Content-Type")
        seen["body"] = json.loads(request.content.decode("utf-8"))
        return json_response(201, _success_body())

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()

    assert attempt.kind == OK
    assert attempt.receipt is not None
    assert attempt.receipt.submission_id == 321
    assert attempt.receipt.duplicate is False
    assert attempt.status_code == 201
    assert seen["url"] == f"{ORIGIN}/api/v1/ingest/submissions"
    assert seen["auth"] == f"Bearer {TOKEN}"
    assert seen["content_type"] == "application/json"
    assert seen["body"] == PAYLOAD


async def test_200_duplicate_is_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(200, _success_body(submission_id=77, duplicate=True))

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == OK
    assert attempt.receipt is not None
    assert attempt.receipt.duplicate is True
    assert attempt.receipt.submission_id == 77


async def test_payload_is_sent_byte_identical_on_retry() -> None:
    bodies: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return json_response(500, _error_body("internal_error", retryable=True))

    client = make_client(handler)
    try:
        await client.submit(PAYLOAD)
        await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert bodies[0] == bodies[1]


@pytest.mark.parametrize(
    "body",
    [
        {"duplicate": False},
        {"submission": {"id": 1}},
        {"duplicate": "yes", "submission": {"id": 1}},
        {"duplicate": False, "submission": {}},
        {"duplicate": False, "submission": {"id": "1"}},
        {"duplicate": False, "submission": {"id": 0}},
        {"duplicate": False, "submission": {"id": True}},
        {"duplicate": False, "submission": None},
    ],
)
async def test_200_with_wrong_shape_is_not_a_false_success(body: dict) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(200, body)

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == RETRY
    assert attempt.receipt is None


async def test_200_html_from_proxy_is_not_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"<html><body>502 Bad Gateway</body></html>",
            headers={"Content-Type": "text/html"},
        )

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == RETRY
    assert attempt.error_code == "non_json_success_body"
    # The diagnostic carries a bounded, single-line excerpt for the operator log;
    # the group-facing path never sees it (see services/receipts.py).
    assert "HTML" in attempt.diagnostic
    assert "<html>" not in attempt.diagnostic
    assert len(attempt.diagnostic) < 300


# --- error classification ---------------------------------------------------


@pytest.mark.parametrize(
    "status,code,retryable,expected",
    [
        (400, "bad_request", False, REJECTED),
        (413, "payload_too_large", False, REJECTED),
        (422, "content_invalid", False, REJECTED),
        (409, "idempotency_conflict", False, REJECTED),
        (500, "internal_error", False, RETRY),
        (503, "service_not_ready", True, RETRY),
        (500, "", False, RETRY),
    ],
)
async def test_status_classification(
    status: int, code: str, retryable: bool, expected: str
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(status, _error_body(code, retryable))

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == expected
    assert attempt.status_code == status


async def test_401_is_a_global_credential_problem() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(401, _error_body("unauthorized"))

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == CREDENTIALS
    assert "ingest_token" in attempt.diagnostic
    assert TOKEN not in attempt.diagnostic


async def test_403_forbidden_role_is_a_global_credential_problem() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(403, _error_body("forbidden_role"))

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == CREDENTIALS


@pytest.mark.parametrize("code", ["group_not_allowed", "event_mismatch"])
async def test_403_config_refusal_is_a_per_submission_rejection(code: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(403, _error_body(code))

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == REJECTED
    assert attempt.error_code == code


async def test_404_is_reported_as_a_misconfigured_address() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(404, _error_body("not_found"))

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == MISCONFIGURED
    assert "backend_url" in attempt.diagnostic


async def test_redirect_is_never_followed() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://evil.invalid/collect"})

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == MISCONFIGURED
    assert attempt.status_code == 302
    assert calls == [f"{ORIGIN}/api/v1/ingest/submissions"]
    assert "evil.invalid" in attempt.diagnostic


async def test_429_reads_retry_after_seconds() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(
            429,
            _error_body("rate_limited", retryable=True, details={"scope": "per_user"}),
            headers={"Retry-After": "12"},
        )

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == RETRY
    assert attempt.retry_after == 12.0


async def test_429_reads_retry_after_http_date() -> None:
    # Build the header relative to the real clock: a hard-coded date would turn
    # into "in the past" as soon as the wall clock moves past it.
    future = datetime.now(UTC) + timedelta(seconds=30)
    header = format_datetime(future, usegmt=True)

    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(
            429,
            _error_body("rate_limited", retryable=True),
            headers={"Retry-After": header},
        )

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == RETRY
    assert attempt.retry_after is not None
    assert 0 < attempt.retry_after <= 31


async def test_429_with_invalid_retry_after_falls_back_to_backoff() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(
            429,
            _error_body("rate_limited", retryable=True),
            headers={"Retry-After": "soon-ish"},
        )

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == RETRY
    assert attempt.retry_after is None
    assert "常规退避" in attempt.diagnostic


async def test_429_uses_details_when_header_is_missing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(
            429,
            _error_body(
                "rate_limited",
                retryable=True,
                details={"scope": "per_group", "retry_after_seconds": 7},
            ),
        )

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.retry_after == 7.0


@pytest.mark.parametrize(
    "value,expected",
    [
        ("10", 10.0),
        (" 3 ", 3.0),
        ("0", 0.0),
        ("-5", 0.0),
        ("abc", None),
        ("", None),
        (None, None),
        ("Infinity", None),
    ],
)
def test_parse_retry_after_numeric_forms(value, expected) -> None:
    assert parse_retry_after(value) == expected


def test_parse_retry_after_http_date() -> None:
    future = datetime.now(UTC) + timedelta(seconds=45)
    seconds = parse_retry_after(format_datetime(future, usegmt=True))
    assert seconds is not None
    assert 40 <= seconds <= 46


def test_parse_retry_after_past_date_is_zero() -> None:
    past = datetime.now(UTC) - timedelta(seconds=60)
    assert parse_retry_after(format_datetime(past, usegmt=True)) == 0.0


# --- transport failures -----------------------------------------------------


async def test_timeout_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("simulated timeout", request=request)

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == RETRY
    assert attempt.error_code == "timeout"


async def test_connect_error_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == RETRY
    assert attempt.error_code == "connect_error"


async def test_unexpected_exception_is_retryable_and_marked() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("boom")

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == RETRY
    assert attempt.error_code == "unexpected_error"
    assert "RuntimeError" in attempt.diagnostic


# --- configuration of the client itself -------------------------------------


def test_client_is_bound_to_the_configured_origin() -> None:
    client = make_client(lambda request: httpx.Response(200))
    assert client.submission_url == f"{ORIGIN}/api/v1/ingest/submissions"
    assert client.matches(ORIGIN)
    assert not client.matches("http://other.invalid:18080")
    assert client.origin.base_url == ORIGIN


async def test_token_never_appears_in_the_url_or_diagnostics() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["query"] = str(request.url.query)
        return json_response(401, _error_body("unauthorized"))

    client = make_client(handler)
    try:
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert TOKEN not in captured["url"]
    assert captured["query"] in ("", "b''")
    assert TOKEN not in json.dumps(attempt.log_fields(), ensure_ascii=False)


async def test_env_proxy_is_ignored_by_default(monkeypatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://unreachable.invalid:3128")
    monkeypatch.setenv("HTTPS_PROXY", "http://unreachable.invalid:3128")

    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(201, _success_body())

    client = make_client(handler)
    try:
        # trust_env=False means the (broken) environment proxy is not consulted.
        assert client._client.trust_env is False
        attempt = await client.submit(PAYLOAD)
    finally:
        await client.aclose()
    assert attempt.kind == OK


async def test_env_proxy_can_be_opted_into() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return json_response(201, _success_body())

    client = make_client(handler, use_env_proxy=True)
    try:
        assert client._client.trust_env is True
    finally:
        await client.aclose()


def test_tls_verify_is_scoped_to_this_client() -> None:
    """``tls_verify=false`` must not touch any global SSL state."""
    import ssl

    default_context = ssl.create_default_context().verify_mode
    client = make_client(lambda request: httpx.Response(200), tls_verify=False)
    assert client._settings.tls_verify is False
    # The process-wide default context is unchanged.
    assert ssl.create_default_context().verify_mode == default_context == ssl.CERT_REQUIRED


async def test_follow_redirects_is_disabled_on_the_client() -> None:
    client = make_client(lambda request: httpx.Response(200))
    try:
        assert client._client.follow_redirects is False
    finally:
        await client.aclose()


def test_client_requires_a_validated_origin() -> None:
    settings = load_settings({})
    with pytest.raises(ValueError):
        BackendClient(settings)
