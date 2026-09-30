"""Message segment parsing: command boundary, attachments, length in code points."""

from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import pytest

from services.parser import (
    ATTACHMENT_ONLY,
    EMPTY,
    NOT_COMMAND,
    OK,
    TOO_LONG,
    normalise_line_endings,
    normalise_segments,
    parse_submission,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
SELF_ID = "100000002"


def _segments(name: str) -> list[dict]:
    data = json.loads((FIXTURES / "onebot_segments.json").read_text(encoding="utf-8"))
    return data["cases"][name]


def _text(value: str) -> list[dict]:
    return [{"type": "text", "data": {"text": value}}]


# --- the rule table from the task specification -----------------------------


def test_single_line_text() -> None:
    result = parse_submission(_text("/投稿 今天很开心"), self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "今天很开心"
    assert result.length == 5


def test_multiline_keeps_inner_newlines_and_spaces() -> None:
    result = parse_submission(
        _text("/投稿 第一行 🎉\n第二行  保留空格\n第三行"),
        self_id=SELF_ID,
    )
    assert result.code == OK
    assert result.content == "第一行 🎉\n第二行  保留空格\n第三行"
    assert result.content.count("\n") == 2
    assert "  " in result.content


def test_newline_right_after_the_command_is_a_separator() -> None:
    result = parse_submission(_text("/投稿\n第一行 🎉\n第二行"), self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "第一行 🎉\n第二行"
    assert result.content.count("\n") == 1


def test_internal_blank_lines_are_preserved() -> None:
    result = parse_submission(_text("/投稿 第一段\n\n第二段"), self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "第一段\n\n第二段"


def test_crlf_and_cr_are_normalised_like_the_backend() -> None:
    # Canonicalised to LF exactly like the backend's normalize_content, so the
    # payload persisted in the outbox is byte-identical to what is stored.
    result = parse_submission(_text("/投稿 a\r\nb\rc"), self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "a\nb\nc"


def test_no_unicode_nfkc_normalisation() -> None:
    raw = "ＡＢＣ ① ㍿"
    result = parse_submission(_text(f"/投稿 {raw}"), self_id=SELF_ID)
    assert result.code == OK
    assert result.content == raw
    assert result.content != unicodedata.normalize("NFKC", raw)


def test_leading_command_whitespace_and_trailing_body_whitespace_are_stripped() -> None:
    result = parse_submission(_text("   \n  /投稿   正文有首尾空白   "), self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "正文有首尾空白"


def test_command_only_replies_with_usage() -> None:
    assert parse_submission(_text("/投稿"), self_id=SELF_ID).code == EMPTY
    assert parse_submission(_text("/投稿   "), self_id=SELF_ID).code == EMPTY
    assert parse_submission(_text("/投稿\n"), self_id=SELF_ID).code == EMPTY


@pytest.mark.parametrize("value", ["/投稿箱", "/投稿你好", "/投稿箱 内容"])
def test_similar_commands_are_not_ours(value: str) -> None:
    assert parse_submission(_text(value), self_id=SELF_ID).code == NOT_COMMAND


def test_command_inside_a_sentence_is_not_ours() -> None:
    assert parse_submission(_text("我想/投稿 内容"), self_id=SELF_ID).code == NOT_COMMAND
    assert parse_submission(_text("请看 /投稿 内容"), self_id=SELF_ID).code == NOT_COMMAND


def test_wake_at_of_the_bot_is_removed() -> None:
    segments = [
        {"type": "at", "data": {"qq": SELF_ID}},
        {"type": "text", "data": {"text": " /投稿 唤醒形式"}},
    ]
    result = parse_submission(segments, self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "唤醒形式"


def test_at_of_another_user_before_the_command_is_not_ours() -> None:
    assert parse_submission(_segments("at_other_prefix"), self_id=SELF_ID).code == NOT_COMMAND


def test_at_all_is_not_a_wake_mention() -> None:
    assert parse_submission(_segments("at_all_prefix"), self_id=SELF_ID).code == NOT_COMMAND


def test_other_user_at_in_body_is_rejected() -> None:
    segments = [
        {"type": "text", "data": {"text": "/投稿 想说给"}},
        {"type": "at", "data": {"qq": "100000009"}},
        {"type": "text", "data": {"text": "听"}},
    ]
    assert parse_submission(segments, self_id=SELF_ID).code == ATTACHMENT_ONLY


def test_bot_at_in_body_is_also_rejected() -> None:
    segments = [
        {"type": "text", "data": {"text": "/投稿 再说一次"}},
        {"type": "at", "data": {"qq": SELF_ID}},
    ]
    assert parse_submission(segments, self_id=SELF_ID).code == ATTACHMENT_ONLY


# --- attachments ------------------------------------------------------------


@pytest.mark.parametrize("name", ["voice_only", "file_only", "video_only"])
def test_attachment_only_messages_are_never_our_command(name: str) -> None:
    assert parse_submission(_segments(name), self_id=SELF_ID).code == NOT_COMMAND


@pytest.mark.parametrize("name", ["command_then_voice", "unknown_segment_after_command"])
def test_attachment_after_the_command_is_reported(name: str) -> None:
    result = parse_submission(_segments(name), self_id=SELF_ID)
    assert result.code == ATTACHMENT_ONLY
    assert result.content == ""


def test_image_after_text_is_not_silently_dropped() -> None:
    segments = [
        {"type": "text", "data": {"text": "/投稿 带图的文字"}},
        {"type": "image", "data": {"file": "sample.jpg"}},
    ]
    result = parse_submission(segments, self_id=SELF_ID)
    assert result.code == ATTACHMENT_ONLY
    assert result.content == ""


def test_face_after_text_is_not_silently_dropped() -> None:
    segments = [
        {"type": "text", "data": {"text": "/投稿 表情"}},
        {"type": "face", "data": {"id": "14"}},
    ]
    assert parse_submission(segments, self_id=SELF_ID).code == ATTACHMENT_ONLY


def test_reply_before_text_is_not_a_submission() -> None:
    # A quote means "this is about another message": refuse rather than quietly
    # uploading only the trailing text.
    segments = [
        {"type": "reply", "data": {"id": "39999"}},
        {"type": "text", "data": {"text": "/投稿 引用后投稿"}},
    ]
    assert parse_submission(segments, self_id=SELF_ID).code == ATTACHMENT_ONLY


def test_forward_after_text_is_rejected() -> None:
    segments = [
        {"type": "text", "data": {"text": "/投稿 转发内容"}},
        {"type": "forward", "data": {"id": "forward-1"}},
    ]
    assert parse_submission(segments, self_id=SELF_ID).code == ATTACHMENT_ONLY


def test_image_only_message_is_not_our_command() -> None:
    segments = [{"type": "image", "data": {"file": "sample.jpg"}}]
    assert parse_submission(segments, self_id=SELF_ID).code == NOT_COMMAND


def test_literal_cq_syntax_is_plain_text_not_an_attachment() -> None:
    result = parse_submission(
        _text("/投稿 字面量 [CQ:at,qq=100000009] 不是真附件"),
        self_id=SELF_ID,
    )
    assert result.code == OK
    assert "[CQ:at,qq=100000009]" in result.content


def test_literal_cq_image_syntax_is_plain_text() -> None:
    result = parse_submission(_text("/投稿 字面量 [CQ:image,file=x.jpg] 文本"), self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "字面量 [CQ:image,file=x.jpg] 文本"


# --- length is measured in Unicode code points ------------------------------


def test_exactly_500_code_points_is_accepted() -> None:
    body = "字" * 500
    result = parse_submission(_text(f"/投稿 {body}"), self_id=SELF_ID)
    assert result.code == OK
    assert result.length == 500


def test_501_code_points_is_rejected() -> None:
    body = "字" * 501
    result = parse_submission(_text(f"/投稿 {body}"), self_id=SELF_ID)
    assert result.code == TOO_LONG
    assert result.length == 501


def test_emoji_are_counted_as_code_points_not_utf16_units() -> None:
    # One emoji is 1 code point, 2 UTF-16 units and 4 UTF-8 bytes.
    body = "🎉" * 500
    result = parse_submission(_text(f"/投稿 {body}"), self_id=SELF_ID)
    assert result.code == OK
    assert result.length == 500
    assert len(body) == 500
    assert len(body.encode("utf-8")) == 2000


def test_combining_characters_count_as_code_points() -> None:
    # "e" + U+0301 is 2 code points but a single grapheme.
    body = "e\u0301" * 250
    result = parse_submission(_text(f"/投稿 {body}"), self_id=SELF_ID)
    assert result.code == OK
    assert result.length == 500


def test_multiple_text_segments_are_joined_without_separators() -> None:
    segments = [
        {"type": "text", "data": {"text": "/投稿 前半"}},
        {"type": "text", "data": {"text": "后半"}},
    ]
    result = parse_submission(segments, self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "前半后半"


# --- robustness -------------------------------------------------------------


def test_missing_message_chain_is_not_a_command() -> None:
    assert parse_submission(None, self_id=SELF_ID).code == NOT_COMMAND
    assert parse_submission([], self_id=SELF_ID).code == NOT_COMMAND


def test_normalise_segments_handles_astrbot_style_components() -> None:
    class Plain:
        type = "Plain"
        text = "hello"

    class At:
        type = "At"
        qq = 100000002

    class Weird:
        type = "SomethingNew"

    segments = normalise_segments([Plain(), At(), Weird()])
    assert [segment.kind for segment in segments] == ["text", "at", "somethingnew"]
    assert segments[0].text == "hello"
    assert segments[1].at_qq == "100000002"


def test_parse_accepts_astrbot_style_components_directly() -> None:
    class Plain:
        type = "Plain"
        text = "/投稿 组件形式"

    result = parse_submission([Plain()], self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "组件形式"


def test_unknown_segment_before_the_command_does_not_match() -> None:
    segments = [
        {"type": "json", "data": {"data": "{}"}},
        {"type": "text", "data": {"text": "/投稿 卡片之后"}},
    ]
    assert parse_submission(segments, self_id=SELF_ID).code == ATTACHMENT_ONLY


def test_custom_command_token_is_honoured() -> None:
    result = parse_submission(_text("/tougao hi"), command_token="/tougao", self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "hi"
    assert (
        parse_submission(_text("/投稿 hi"), command_token="/tougao", self_id=SELF_ID).code
        == NOT_COMMAND
    )


def test_whitespace_only_leading_segment_before_command() -> None:
    segments = [
        {"type": "text", "data": {"text": "  \n "}},
        {"type": "text", "data": {"text": "/投稿 正文"}},
    ]
    result = parse_submission(segments, self_id=SELF_ID)
    assert result.code == OK
    assert result.content == "正文"


def test_normalise_line_endings_helper() -> None:
    assert normalise_line_endings("a\r\nb\rc\nd") == "a\nb\nc\nd"
