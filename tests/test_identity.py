"""Identity extraction: QQ id / group / bot / message id / display names / time."""

from __future__ import annotations

import pytest

from services.identity import (
    EventIdentity,
    IdentityError,
    epoch_to_utc_iso_z,
    extract_identity,
    platform_id_of,
    try_extract_identity,
    unified_msg_origin_of,
)
from tests.conftest import FakeAstrBotMessage, FakeEvent


def test_happy_path_reads_card_and_nickname_separately() -> None:
    message = FakeAstrBotMessage(
        raw_sender={"user_id": 100000001, "nickname": "昵称", "card": "群名片"},
    )
    identity = extract_identity(message)
    assert identity == EventIdentity(
        bot_id="100000002",
        group_id="123456789",
        source_message_id="40001",
        qq_id="100000001",
        nickname="昵称",
        group_card="群名片",
        source_timestamp=epoch_to_utc_iso_z(1_790_000_000),
    )


def test_group_card_missing_becomes_null_but_submission_still_works() -> None:
    message = FakeAstrBotMessage(raw_sender={"user_id": 100000001, "nickname": "只有昵称"})
    identity = extract_identity(message)
    assert identity.group_card is None
    assert identity.nickname == "只有昵称"


def test_both_names_missing_is_not_fatal() -> None:
    message = FakeAstrBotMessage(sender_nickname=None, raw_sender={"user_id": 100000001})
    identity = extract_identity(message)
    assert identity.nickname is None
    assert identity.group_card is None
    assert identity.qq_id == "100000001"


def test_empty_card_string_is_treated_as_absent() -> None:
    message = FakeAstrBotMessage(
        raw_sender={"user_id": 100000001, "nickname": "昵称", "card": "  "}
    )
    identity = extract_identity(message)
    assert identity.group_card is None


def test_negative_message_id_is_kept_as_a_string() -> None:
    message = FakeAstrBotMessage(message_id="-1234567890")
    message.raw_message["message_id"] = -1234567890
    identity = extract_identity(message)
    assert identity.source_message_id == "-1234567890"


def test_display_name_is_bounded_and_stripped_of_control_characters() -> None:
    message = FakeAstrBotMessage(
        raw_sender={"user_id": 100000001, "nickname": "bad\x00name\x07", "card": "x" * 200},
    )
    identity = extract_identity(message)
    assert identity.nickname == "badname"
    assert identity.group_card is not None
    assert len(identity.group_card) == 100


def test_missing_group_id_is_rejected() -> None:
    message = FakeAstrBotMessage()
    message.group_id = ""
    message.raw_message.pop("group_id")
    with pytest.raises(IdentityError) as excinfo:
        extract_identity(message)
    assert excinfo.value.field_name == "group_id"


def test_missing_message_id_is_rejected_instead_of_inventing_one() -> None:
    message = FakeAstrBotMessage()
    message.message_id = ""
    message.raw_message.pop("message_id")
    with pytest.raises(IdentityError) as excinfo:
        extract_identity(message)
    assert excinfo.value.field_name == "source_message_id"


def test_missing_sender_is_rejected() -> None:
    message = FakeAstrBotMessage()
    message.sender.user_id = ""
    message.raw_message["sender"] = {"nickname": "匿名"}
    message.raw_message["user_id"] = 0
    with pytest.raises(IdentityError) as excinfo:
        extract_identity(message)
    assert excinfo.value.field_name == "qq_id"


def test_non_numeric_qq_id_is_rejected() -> None:
    message = FakeAstrBotMessage(sender_user_id="100000001")
    message.raw_message["sender"]["user_id"] = "not-a-number"
    message.sender.user_id = "not-a-number"
    with pytest.raises(IdentityError) as excinfo:
        extract_identity(message)
    assert excinfo.value.reason == "not_a_qq_number"


def test_raw_event_and_unified_message_disagreement_is_rejected() -> None:
    message = FakeAstrBotMessage()
    # A reply/forward adapter path could rebuild the sender: refuse, never pick.
    message.raw_message["sender"]["user_id"] = 200000002
    with pytest.raises(IdentityError) as excinfo:
        extract_identity(message)
    assert excinfo.value.reason == "raw_event_mismatch"
    assert excinfo.value.field_name == "qq_id"


def test_group_id_mismatch_is_rejected() -> None:
    message = FakeAstrBotMessage()
    message.raw_message["group_id"] = 999999999
    with pytest.raises(IdentityError) as excinfo:
        extract_identity(message)
    assert excinfo.value.field_name == "group_id"


def test_message_id_mismatch_is_rejected() -> None:
    message = FakeAstrBotMessage()
    message.raw_message["message_id"] = 40002
    with pytest.raises(IdentityError) as excinfo:
        extract_identity(message)
    assert excinfo.value.field_name == "source_message_id"


def test_missing_event_time_gives_null_timestamp() -> None:
    message = FakeAstrBotMessage(raw_time=None)
    identity = extract_identity(message)
    assert identity.source_timestamp is None


def test_string_epoch_time_is_accepted() -> None:
    message = FakeAstrBotMessage()
    message.raw_message["time"] = "1790000000"
    assert extract_identity(message).source_timestamp == epoch_to_utc_iso_z(1_790_000_000)


def test_boolean_epoch_time_is_ignored() -> None:
    message = FakeAstrBotMessage()
    message.raw_message["time"] = True
    assert extract_identity(message).source_timestamp is None


def test_try_extract_returns_a_sanitised_diagnostic() -> None:
    message = FakeAstrBotMessage()
    message.message_id = ""
    message.raw_message.pop("message_id")
    identity, diagnostic = try_extract_identity(message)
    assert identity is None
    assert diagnostic is not None
    assert diagnostic.field_name == "source_message_id"
    assert diagnostic.as_log_fields()["group_id"] == "123456789"


def test_epoch_formatting_is_utc_iso_z() -> None:
    assert epoch_to_utc_iso_z(0) == "1970-01-01T00:00:00Z"
    assert epoch_to_utc_iso_z(1_790_000_000).endswith("Z")
    assert epoch_to_utc_iso_z(1_790_000_000, milliseconds=True).endswith(".000Z")


def test_unified_msg_origin_and_platform_id_helpers() -> None:
    message = FakeAstrBotMessage()
    event = FakeEvent(message, platform_id="thp-bot-1")
    assert unified_msg_origin_of(event) == "thp-bot-1:group:123456789"
    assert platform_id_of(event) == "thp-bot-1"
