"""Configuration parsing and validation tests."""

from __future__ import annotations

import pytest

from services.settings import (
    ConfigProblem,
    Settings,
    load_settings,
    parse_origin,
)

BASE_CONFIG = {
    "enabled": True,
    "backend_url": "http://203.0.113.10:18080",
    "ingest_token": "unit-test-token-0123456789abcdef",
    "event_id": "thp-demo-2026",
    "allowed_group_ids": ["123456789"],
}


def test_parse_origin_accepts_http_https_and_ports() -> None:
    origin = parse_origin("http://203.0.113.10:18080")
    assert origin.scheme == "http"
    assert origin.port == 18080
    assert origin.base_url == "http://203.0.113.10:18080"
    assert origin.submission_url() == "http://203.0.113.10:18080/api/v1/ingest/submissions"

    https_origin = parse_origin("https://events.example.invalid")
    assert https_origin.is_https
    assert https_origin.port is None
    assert (
        https_origin.submission_url() == "https://events.example.invalid/api/v1/ingest/submissions"
    )


def test_parse_origin_strips_single_trailing_slash() -> None:
    assert parse_origin("http://example.invalid:18080/").base_url == "http://example.invalid:18080"


@pytest.mark.parametrize(
    "value",
    [
        "http://example.invalid:18080/host",
        "http://example.invalid:18080/api/v1",
        "http://example.invalid:18080?x=1",
        "http://example.invalid:18080#frag",
        "http://user:pass@example.invalid:18080",
        "ftp://example.invalid",
        "example.invalid:18080",
        "",
        "   ",
    ],
)
def test_parse_origin_rejects_bad_values(value: str) -> None:
    with pytest.raises(ConfigProblem):
        parse_origin(value)


def test_parse_origin_error_message_mentions_correct_format() -> None:
    with pytest.raises(ConfigProblem) as excinfo:
        parse_origin("http://example.invalid:18080/screen")
    message = str(excinfo.value)
    assert "/screen" in message
    assert "http(s)://" in message


def test_submission_url_never_doubles_the_api_prefix() -> None:
    origin = parse_origin("http://example.invalid:18080/")
    assert origin.submission_url().count("/api/v1") == 1


def test_load_settings_happy_path() -> None:
    settings = load_settings(BASE_CONFIG)
    assert settings.can_submit
    assert settings.problems == ()
    assert settings.origin is not None
    assert settings.command_token == "/投稿"
    assert settings.max_attempts == 20
    assert settings.delivery_ttl_seconds == 600
    assert settings.queue_capacity == 1000
    assert settings.user_interval_seconds == 10
    assert settings.group_per_minute == 60
    assert settings.connect_timeout_seconds == 3.0
    assert settings.request_timeout_seconds == 10.0
    assert settings.tls_verify is True
    assert settings.use_env_proxy is False
    assert settings.is_group_allowed("123456789")
    assert not settings.is_group_allowed("987654321")


def test_disabled_by_default_and_no_problems_recorded() -> None:
    settings = load_settings({})
    assert settings.enabled is False
    assert settings.can_submit is False
    # Nothing is reported until the operator opts in.
    assert settings.problems == ()


@pytest.mark.parametrize(
    "patch,expected_fragment",
    [
        ({"backend_url": ""}, "backend_url"),
        ({"backend_url": "http://x.invalid:18080/host"}, "路径"),
        ({"ingest_token": ""}, "ingest_token"),
        ({"event_id": ""}, "event_id"),
        ({"event_id": "bad id!"}, "event_id"),
    ],
)
def test_incomplete_config_disables_submission_with_reason(
    patch: dict, expected_fragment: str
) -> None:
    config = dict(BASE_CONFIG)
    config.update(patch)
    settings = load_settings(config)
    assert settings.can_submit is False
    assert any(expected_fragment in problem for problem in settings.problems), settings.problems


def test_empty_allowed_groups_disables_reception_with_reason() -> None:
    config = dict(BASE_CONFIG)
    config["allowed_group_ids"] = []
    settings = load_settings(config)
    assert settings.allowed_group_ids == ()
    assert settings.can_submit is False
    assert any("allowed_group_ids" in problem for problem in settings.problems)


def test_group_list_accepts_strings_and_filters_invalid_entries() -> None:
    config = dict(BASE_CONFIG)
    config["allowed_group_ids"] = "123456789, 987654321 bad,1234"
    settings = load_settings(config)
    assert settings.allowed_group_ids == ("123456789", "987654321")
    assert any("bad" in problem or "1234" in problem for problem in settings.problems)


def test_numeric_config_values_are_clamped_with_a_problem() -> None:
    config = dict(BASE_CONFIG)
    config.update(
        {
            "max_attempts": 999,
            "delivery_ttl_seconds": "abc",
            "queue_capacity": 5,
            "user_interval_seconds": -1,
            "group_per_minute": 10**9,
            "connect_timeout_seconds": 99,
        },
    )
    settings = load_settings(config)
    assert settings.max_attempts == 20
    assert settings.delivery_ttl_seconds == 600
    assert settings.queue_capacity == 1000
    assert settings.user_interval_seconds == 10
    assert settings.group_per_minute == 60
    assert settings.connect_timeout_seconds == 3.0
    assert len(settings.problems) == 6


def test_bool_coercion_accepts_strings() -> None:
    config = dict(BASE_CONFIG)
    config.update({"enabled": "true", "tls_verify": "false", "use_env_proxy": "yes"})
    settings = load_settings(config)
    assert settings.enabled is True
    assert settings.tls_verify is False
    assert settings.use_env_proxy is True


@pytest.mark.parametrize(
    "value,expected",
    [("投稿", "投稿"), ("/投稿", "投稿"), ("投 稿", "投稿"), ("", "投稿")],
)
def test_command_name_falls_back_to_default(value: str, expected: str) -> None:
    config = dict(BASE_CONFIG)
    config["command_name"] = value
    settings = load_settings(config)
    assert settings.command_name == expected


def test_log_level_is_validated() -> None:
    config = dict(BASE_CONFIG)
    config["log_level"] = "TRACE"
    settings = load_settings(config)
    assert settings.log_level == "INFO"
    assert any("log_level" in problem for problem in settings.problems)


def test_token_is_not_exposed_in_repr() -> None:
    settings = load_settings(BASE_CONFIG)
    assert "unit-test-token" not in repr(settings)


def test_settings_is_frozen() -> None:
    from dataclasses import FrozenInstanceError

    settings = Settings()
    with pytest.raises(FrozenInstanceError):
        settings.enabled = True  # type: ignore[misc]
