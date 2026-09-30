"""Group receipt wording and delivery.

Honesty rules (from the task specification and AGENT.md §4.3):

* a successful submission only means "the backend stored it and the host console
  can see it" — never "已上屏 / 已展示 / 主持已读";
* the "stored, will retry" notice may only be sent *after* the payload has been
  committed to SQLite;
* failures distinguish "definitely refused" from "still unknown", and never
  claim the submission was not stored when the outcome is genuinely unknown;
* group-visible text never contains the server address, token, stack trace, raw
  response body, or information about other groups/users.

The module is deliberately sync-only (pure string building) so it can be tested
without AstrBot; the plugin layer turns the strings into an AstrBot
``MessageChain``.
"""

from __future__ import annotations

USAGE = "用法：/投稿 你的文字（去掉首尾空白后 1–500 字，可多行）"
ATTACHMENT_UNSUPPORTED = "目前仅支持文字，请重新发送 /投稿 内容"
TOO_LONG = "投稿内容超过 500 字，请精简后重新发送"
RATE_LIMITED = "投稿太快了，请稍后再试"
QUEUE_FULL = "投稿队列已满，请稍后重新投稿"
DISABLED = "投稿功能当前未启用，请联系活动管理员"
IDENTITY_INVALID = "未能识别你的投稿身份，请稍后重新发送"
STORAGE_FAILED = "本次投稿未能暂存，请稍后重新投稿。"


def success_text(submission_id: int) -> str:
    """Receipt for a confirmed (new or deduplicated) submission."""
    return f"投稿成功，编号 #{int(submission_id)}，已送达主持端。"


STORED = "网络暂时异常，投稿已暂存，将自动重试。"


def rejected_text() -> str:
    """Backend refused this submission on purpose (no retry can help)."""
    return "本次投稿未被接受，请联系活动管理员核对活动设置后重新投稿。"


def uncertain_text() -> str:
    """Deadline/attempts exhausted without a confirmed backend answer.

    Deliberately does not claim the submission was not stored: a lost response
    after a successful commit is indistinguishable from a failure.
    """
    return "本次投稿未确认成功，请稍后重新投稿。"


__all__ = [
    "ATTACHMENT_UNSUPPORTED",
    "DISABLED",
    "IDENTITY_INVALID",
    "QUEUE_FULL",
    "RATE_LIMITED",
    "STORED",
    "TOO_LONG",
    "USAGE",
    "rejected_text",
    "success_text",
    "uncertain_text",
]
