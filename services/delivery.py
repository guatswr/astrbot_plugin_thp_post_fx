"""Delivery scheduling: one worker, bounded retries, crash recovery, receipts.

Responsibilities
----------------
* a single :class:`DeliveryWorker` per plugin instance — never one background
  loop per message. The message handler only persists the payload and then waits
  a bounded time for this worker to settle it;
* claim-then-act: the row is claimed (state -> ``inflight``, attempt counter
  incremented) inside a short transaction; HTTP happens with no database lock
  held; the result is written back in another short transaction;
* retries always resend the *stored* payload to the *stored* target and never
  regenerate a message id or a timestamp;
* terminal outcomes are persisted before any group notice is attempted, so a
  failed receipt can never roll back or re-upload a successful submission;
* receipts are sent through AstrBot's proactive-message API using the stored
  ``unified_msg_origin``, which means a restart does not need the original
  ``AstrMessageEvent`` object.

Retry policy (defaults, all configurable)
-----------------------------------------
attempt  1  2  3   4   5   6+      -> delay 2s 5s 15s 30s 60s 60s (with ±20 %
                                       jitter), capped by the delivery deadline;
attempts are capped at ``max_attempts`` (20, first attempt included) and the
whole submission expires ``delivery_ttl_seconds`` (600 s) after it was first
enqueued. Whichever limit is reached first terminates the delivery.

``Retry-After`` on 429 is honoured when it still fits inside the deadline; when
the server asks us to wait past the deadline the submission is failed instead of
bypassing the server's rate limit. The same rule applies to the attempt ceiling.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from . import backend_client, outbox, receipts
from .backend_client import BackendClient, DeliveryAttempt
from .outbox import DeliveryRow, Outbox
from .settings import Settings

logger = logging.getLogger(__name__)

BACKOFF_SCHEDULE: tuple[float, ...] = (2.0, 5.0, 15.0, 30.0, 60.0)
MAX_BACKOFF_SECONDS = 60.0

RECEIPT_BACKOFF_SCHEDULE: tuple[float, ...] = (5.0, 15.0, 60.0, 300.0, 900.0)
RECEIPT_MAX_ATTEMPTS = 5
RECEIPT_TTL_SECONDS = 3600.0

# A claim is exclusive for this long; a longer HTTP attempt than this is not
# possible (request_timeout_seconds <= 120) but the lease is re-checked anyway.
CLAIM_LEASE_SECONDS = 90.0

# Worker wake-ups: signal-driven for new work, periodic for receipts/expiry.
WORKER_IDLE_INTERVAL_SECONDS = 0.5
WORKER_FAST_INTERVAL_SECONDS = 0.01
WORKER_RECEIPTS_PER_TICK = 5

#: How often ``submit_and_wait`` re-reads the row while waiting for a decision.
WAIT_POLL_INTERVAL_SECONDS = 0.05

# Outcome vocabulary shared with ``main.py`` (never shown to the group verbatim).
STATUS_SUCCEEDED = "succeeded"
STATUS_REJECTED = "rejected"
STATUS_CREDENTIALS = "credentials"
STATUS_UNCONFIRMED = "unconfirmed"
STATUS_NOT_ATTEMPTED = "not_attempted"


class SendMessageFn(Protocol):
    """Signature of ``Context.send_message`` (and of the test double)."""

    async def __call__(self, session: str, message_chain: Any) -> bool: ...


class PlainChainFactory(Protocol):
    """Builds a platform message chain from plain text."""

    def __call__(self, text: str) -> Any: ...


@dataclass(frozen=True)
class BackoffSchedule:
    """Delay schedule for retries."""

    delays: tuple[float, ...] = BACKOFF_SCHEDULE
    maximum: float = MAX_BACKOFF_SECONDS

    def delay_for(self, attempt_count: int, *, jitter: Callable[[], float] | None = None) -> float:
        """Delay before the retry that follows ``attempt_count`` attempts.

        Args:
            attempt_count: Attempts already performed (>=1).
            jitter: Optional ``random()``-like callable in ``[0, 1)``; tests pass
                a deterministic function, production uses ``random.random``.

        Returns:
            Seconds to wait (always positive, capped at ``maximum``).
        """
        index = max(0, int(attempt_count) - 1)
        base = self.delays[index] if index < len(self.delays) else self.delays[-1]
        base = min(base, self.maximum)
        random_value = (jitter or random.random)()
        # ±20 % jitter keeps a thundering herd from re-synchronising.
        factor = 0.8 + 0.4 * max(0.0, min(random_value, 0.999999))
        return max(0.5, base * factor)


@dataclass(frozen=True)
class IngestOutcome:
    """What the message handler should tell the group (and whether to stay quiet)."""

    status: str
    submission_id: int | None = None
    reason: str = ""

    @property
    def should_reply(self) -> bool:
        return self.status != STATUS_NOT_ATTEMPTED


class DeliveryWorker:
    """Owns the retry loop and all outbound HTTP for this plugin instance."""

    def __init__(
        self,
        *,
        outbox_store: Outbox,
        client: BackendClient,
        settings: Settings,
        send_message: SendMessageFn,
        plain_chain: PlainChainFactory,
        clock: Callable[[], float] = time.time,
        schedule: BackoffSchedule | None = None,
        receipt_schedule: BackoffSchedule | None = None,
        jitter: Callable[[], float] | None = None,
    ) -> None:
        self._outbox = outbox_store
        self._client = client
        self._settings = settings
        self._send_message = send_message
        self._plain_chain = plain_chain
        self._clock = clock
        self._schedule = schedule or BackoffSchedule()
        self._receipt_schedule = receipt_schedule or BackoffSchedule(
            delays=RECEIPT_BACKOFF_SCHEDULE,
            maximum=RECEIPT_BACKOFF_SCHEDULE[-1],
        )
        self._jitter = jitter

        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._credentials_blocked = False
        # Serialise receipt sends with delivery outcome writes. An in-flight
        # stored notice must finish before a success notice can replace it.
        self._receipt_lock = asyncio.Lock()

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        """Start the worker task (no-op when already running)."""
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        # Recover this process's predecessors before the first claim. The lease
        # check uses wall-clock time on purpose: a lease is only meaningful
        # relative to real elapsed time, and a restart is exactly the moment when
        # any previous process is definitely gone.
        recovered = await self._outbox.recover_stale_inflight(now=self._clock(), force=True)
        if recovered:
            logger.info("[thp_post] 恢复上次中断的 %d 条发送中投稿", recovered)
        # A deliberate plugin reload resumes remaining pending rows with the
        # current credential; previously failed rows remain terminal.
        await self._outbox.set_flag("credentials_blocked", "")
        self._credentials_blocked = False
        self._task = asyncio.create_task(self._run(), name="thp-post-delivery")
        logger.info("[thp_post] 投递调度器已启动")

    async def stop(self, *, drain_timeout: float = 5.0) -> None:
        """Stop the worker, giving an in-flight attempt a bounded grace period.

        The current attempt is never abandoned mid-request without releasing its
        claim: ``CancelledError`` propagates (it is never swallowed) and the
        ``finally`` block puts the row back with its attempt counter restored.
        """
        self._stopping = True
        self._wake.set()
        task, self._task = self._task, None
        if task is None:
            return
        try:
            await asyncio.wait_for(task, timeout=drain_timeout)
        except TimeoutError:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 - shutdown path
                pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("[thp_post] 投递调度器退出时出现异常：%s", type(exc).__name__)
        logger.info("[thp_post] 投递调度器已停止")

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def notify_new_work(self) -> None:
        """Wake the worker because a row was just enqueued."""
        self._wake.set()

    # -- public API used by main.py ----------------------------------------

    async def submit_and_wait(
        self,
        row: DeliveryRow,
        *,
        timeout_seconds: float,
    ) -> IngestOutcome:
        """Wait a bounded time for ``row`` to reach a confirmed state.

        This is the only place a group notice is produced from the message path.
        Nothing here blocks the delivery worker: the worker runs in its own task
        and this coroutine only re-reads *persisted* state through short
        transactions, so the inbound ``AstrMessageEvent`` is never handed to the
        scheduler and no long-lived object leaks out of the handler. Re-reading
        the database (instead of sharing in-memory signals) also means a
        restart, a second worker or a crashed tick can never desynchronise the
        result.

        Args:
            row: Freshly persisted row.
            timeout_seconds: Maximum time to block the message handler.

        Returns:
            An :class:`IngestOutcome` describing what to tell the group. The
            "stored, will retry" notice is scheduled here — before returning —
            through a compare-and-set, so a concurrent success can no longer
            produce a second, contradictory notice.
        """
        key = row.key
        self.notify_new_work()

        deadline = asyncio.get_running_loop().time() + max(0.0, timeout_seconds)
        current = row
        while asyncio.get_running_loop().time() < deadline:
            if self._state_of(current):
                break
            await asyncio.sleep(WAIT_POLL_INTERVAL_SECONDS)
            refreshed = await self._outbox.get_by_key(*key)
            if refreshed is not None:
                current = refreshed

        state = self._state_of(current)
        if state == STATUS_SUCCEEDED:
            # Fast path: claim and send the success notice right here so the
            # group gets the number immediately (the worker may also have sent
            # it already — the claim makes that a no-op).
            await self.send_receipt_now(key)
            return IngestOutcome(status=STATUS_SUCCEEDED, submission_id=current.submission_id)
        if state == STATUS_REJECTED:
            await self.send_receipt_now(key)
            return IngestOutcome(status=STATUS_REJECTED, reason="rejected")
        if state == STATUS_CREDENTIALS:
            await self.send_receipt_now(key)
            return IngestOutcome(status=STATUS_CREDENTIALS, reason="credentials")

        # Still unconfirmed: schedule the persisted deferral notice. If the row
        # reached a decided receipt state in the meantime this returns False and
        # that notice wins.
        scheduled = await self._outbox.mark_deferred(
            key,
            delay_seconds=self._settings.defer_notice_delay_seconds,
            now=self._clock(),
        )
        if not scheduled:
            latest = await self._outbox.get_by_key(*key)
            if latest is not None and latest.state == outbox.SUCCEEDED:
                await self.send_receipt_now(key)
                return IngestOutcome(
                    status=STATUS_SUCCEEDED,
                    submission_id=latest.submission_id,
                )
        self.notify_new_work()
        await self.send_receipt_now(key)
        return IngestOutcome(status=STATUS_UNCONFIRMED, reason="pending")

    @staticmethod
    def _state_of(row: DeliveryRow | None) -> str:
        if row is None:
            return STATUS_NOT_ATTEMPTED
        if row.state == outbox.SUCCEEDED:
            return STATUS_SUCCEEDED
        if row.state == outbox.FAILED:
            reason = row.failure_reason_label
            if reason == outbox.REASON_CREDENTIALS:
                return STATUS_CREDENTIALS
            if reason in (outbox.REASON_REJECTED, outbox.REASON_MISCONFIGURED):
                return STATUS_REJECTED
            return STATUS_UNCONFIRMED
        if row.state == outbox.HELD:
            return STATUS_UNCONFIRMED
        return ""  # not decided yet

    # -- worker loop --------------------------------------------------------

    async def _run(self) -> None:
        try:
            while not self._stopping:
                did_work = False
                try:
                    did_work = await self._tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pragma: no cover - defensive
                    logger.exception("[thp_post] 调度循环出现未预期异常：%s", type(exc).__name__)
                if self._stopping:
                    break
                if did_work:
                    # Yield to the event loop between batches so a long retry
                    # burst cannot starve message handling.
                    await asyncio.sleep(WORKER_FAST_INTERVAL_SECONDS)
                    continue
                self._wake.clear()
                try:
                    await asyncio.wait_for(
                        self._wake.wait(),
                        timeout=WORKER_IDLE_INTERVAL_SECONDS,
                    )
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        finally:
            # Nothing to release beyond the claim, which is released in
            # ``_handle_row``'s finally block.
            self._wake.clear()

    async def _tick(self) -> bool:
        """Run one scheduling pass. Returns True when work was available."""
        did_work = False
        async with self._receipt_lock:
            await self._expire_overdue()
        if not self._credentials_blocked:
            claimed = await self._outbox.claim_next(
                origin_base_url=self._settings.backend_base_url,
                event_id=self._settings.event_id,
                allowed_group_ids=self._settings.allowed_group_ids,
                max_attempts=self._settings.max_attempts,
                now=self._clock(),
                lease_seconds=CLAIM_LEASE_SECONDS,
            )
            if claimed is not None:
                did_work = True
                try:
                    await self._handle_row(claimed)
                finally:
                    # The claim is always resolved, even if the attempt raised:
                    # a row must never be left inflight by an unexpected error.
                    await self._release_claim_if_still_held(claimed.key)
        sent = await self._dispatch_receipts()
        did_work = did_work or sent
        return did_work

    async def _release_claim_if_still_held(self, key: tuple[str, str, str, str]) -> None:
        """Safety net: put an inflight row back after an unexpected failure.

        The lease is intentionally *not* used as a sweep here (that happens at
        startup, where the process is known to be fresh): a lease-based sweep
        running on every pass would fight with a just-made claim. This explicit
        release covers the "the attempt raised before recording anything" case,
        which is the only way a row could otherwise stay stuck.
        """
        row = await self._outbox.get_by_key(*key)
        if row is None or row.state != outbox.INFLIGHT:
            return
        logger.warning(
            "[thp_post] 发送尝试异常结束，已把该稿放回队列：key=%s code=%s",
            _key_label(row),
            row.last_error_code or "-",
        )
        await self._outbox.release_claim(
            key,
            error_code="attempt_aborted",
            diagnostic="发送尝试异常结束，未确认结果，已放回队列等待重试",
            now=self._now_for_ledger(row),
        )

    def _now_for_ledger(self, row: DeliveryRow) -> float:
        """Keep ledger timestamps monotonic even with a coarse clock."""
        return max(self._clock(), row.updated_at)

    async def _expire_overdue(self) -> None:
        """Fail rows whose deadline passed while they were waiting."""
        now = self._clock()
        rows = await self._outbox.due_for_expiry(now=now)
        for row in rows:
            changed = await self._outbox.mark_terminal_failure(
                row.key,
                reason=outbox.REASON_TTL_EXPIRED,
                error_code=outbox.REASON_TTL_EXPIRED,
                diagnostic="投稿在投递期限内未能确认成功",
                now=now,
                receipt_state=outbox.RECEIPT_PENDING_STORED,
                receipt_next_at=now,
            )
            if changed:
                logger.warning(
                    "[thp_post] 投稿到期未确认：key=%s attempts=%d",
                    _key_label(row),
                    row.attempt_count,
                )
                self._wake.set()

    async def _handle_row(self, row: DeliveryRow) -> None:
        """Perform one HTTP attempt for a claimed row and persist the outcome."""
        key = row.key
        attempt = row.attempt_count or 1
        payload = row.payload
        if not payload:
            await self._outbox.mark_terminal_failure(
                key,
                reason=outbox.REASON_REJECTED,
                error_code="payload_missing",
                diagnostic="队列中的 payload 无法解析（数据库损坏？），已终止该稿",
                now=self._clock(),
                receipt_state=outbox.RECEIPT_PENDING_STORED,
                receipt_next_at=self._clock(),
            )
            self._wake.set()
            return

        cancelled = False
        try:
            attempt_result: DeliveryAttempt = await self._client.submit(payload)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if cancelled:
                # Release the claim without consuming an attempt. ``shield`` keeps
                # this bookkeeping alive while the surrounding task is being
                # cancelled, so a shutdown cannot lose the claim.
                await asyncio.shield(
                    self._release_cancelled_claim(row),
                )

        logger.debug("[thp_post] 投递结果 key=%s %s", _key_label(row), attempt_result.log_fields())

        async with self._receipt_lock:
            if attempt_result.kind == backend_client.OK and attempt_result.receipt is not None:
                await self._on_success(row, attempt_result)
            elif attempt_result.kind == backend_client.CREDENTIALS:
                await self._on_credentials_problem(row, attempt_result)
            elif attempt_result.kind == backend_client.REJECTED:
                await self._on_rejected(row, attempt_result)
            elif attempt_result.kind == backend_client.MISCONFIGURED:
                await self._on_misconfigured(row, attempt_result)
            else:
                await self._on_retryable(row, attempt_result, attempt)

    async def _release_cancelled_claim(self, row: DeliveryRow) -> None:
        """Put back a claimed row whose HTTP attempt was cancelled mid-flight."""
        await self._outbox.release_claim(
            row.key,
            error_code="cancelled",
            diagnostic="插件停止时中止了本次发送，未确认结果，已放回队列",
            now=self._now_for_ledger(row),
        )
        self._wake.set()

    async def _on_success(self, row: DeliveryRow, attempt_result: DeliveryAttempt) -> None:
        receipt = attempt_result.receipt
        assert receipt is not None
        changed = await self._outbox.mark_succeeded(
            row.key,
            submission_id=receipt.submission_id,
            duplicate=receipt.duplicate,
            now=self._clock(),
            receipt_state=outbox.RECEIPT_PENDING_SUCCESS,
            receipt_next_at=self._clock(),
        )
        if changed:
            logger.info(
                "[thp_post] 投稿已入库：编号=%s duplicate=%s key=%s 用时=%dms",
                receipt.submission_id,
                receipt.duplicate,
                _key_label(row),
                attempt_result.elapsed_ms,
            )
        self._wake.set()
        # The receipt itself is sent by whoever holds the receipt claim: the
        # message handler's fast path, or (on timeout / after a failed send) the
        # next scheduling pass. Never from inside the HTTP path, so a failed
        # receipt cannot invalidate the stored success.

    async def _on_credentials_problem(
        self, row: DeliveryRow, attempt_result: DeliveryAttempt
    ) -> None:
        # Distinguish a global credential problem from a per-group/activity
        # refusal. The backend uses stable codes: unauthorized / forbidden_role
        # are global; event_mismatch / group_not_allowed are per submission
        # (``BackendClient`` already routed those to REJECTED).
        await self._outbox.mark_terminal_failure(
            row.key,
            reason=outbox.REASON_CREDENTIALS,
            error_code=attempt_result.error_code or outbox.REASON_CREDENTIALS,
            diagnostic=attempt_result.diagnostic,
            now=self._clock(),
            receipt_state=outbox.RECEIPT_PENDING_STORED,
            receipt_next_at=self._clock(),
        )
        await self._outbox.set_flag("credentials_blocked", attempt_result.error_code)
        self._credentials_blocked = True
        logger.error(
            "[thp_post] 投稿令牌被后端拒绝（%s），已暂停全部网络投递；"
            "请核对插件配置 ingest_token 与后端 INGEST_TOKEN 是否一致，"
            "修正后在 WebUI 重载插件。request_id=%s",
            attempt_result.error_code,
            attempt_result.request_id or "-",
        )
        self._wake.set()

    async def _on_rejected(self, row: DeliveryRow, attempt_result: DeliveryAttempt) -> None:
        await self._outbox.mark_terminal_failure(
            row.key,
            reason=outbox.REASON_REJECTED,
            error_code=attempt_result.error_code or outbox.REASON_REJECTED,
            diagnostic=attempt_result.diagnostic,
            now=self._clock(),
            receipt_state=outbox.RECEIPT_PENDING_STORED,
            receipt_next_at=self._clock(),
        )
        logger.warning(
            "[thp_post] 投稿被后端拒绝：code=%s status=%s key=%s request_id=%s",
            attempt_result.error_code,
            attempt_result.status_code,
            _key_label(row),
            attempt_result.request_id or "-",
        )
        self._wake.set()

    async def _on_misconfigured(self, row: DeliveryRow, attempt_result: DeliveryAttempt) -> None:
        await self._outbox.mark_terminal_failure(
            row.key,
            reason=outbox.REASON_MISCONFIGURED,
            error_code=attempt_result.error_code or outbox.REASON_MISCONFIGURED,
            diagnostic=attempt_result.diagnostic,
            now=self._clock(),
            receipt_state=outbox.RECEIPT_PENDING_STORED,
            receipt_next_at=self._clock(),
        )
        logger.error(
            "[thp_post] 后端地址/代理配置错误：status=%s code=%s key=%s；%s",
            attempt_result.status_code,
            attempt_result.error_code,
            _key_label(row),
            attempt_result.diagnostic,
        )
        self._wake.set()

    async def _on_retryable(
        self,
        row: DeliveryRow,
        attempt_result: DeliveryAttempt,
        attempt: int,
    ) -> None:
        outcome = await self._outbox.mark_retry(
            row.key,
            reason=attempt_result.error_code or "transient",
            error_code=attempt_result.error_code,
            diagnostic=attempt_result.diagnostic or attempt_result.error_message,
            attempt_count=attempt,
            max_attempts=self._settings.max_attempts,
            deadline_at=row.deadline_at,
            schedule=self._schedule,
            now=self._clock(),
            retry_after=attempt_result.retry_after,
            jitter=self._jitter,
        )
        if outcome == "scheduled":
            current = await self._outbox.get_by_key(*row.key)
            logger.info(
                "[thp_post] 投稿将重试：key=%s 第 %d 次尝试失败（%s），下次 %s",
                _key_label(row),
                attempt,
                attempt_result.error_code or attempt_result.status_code,
                _format_eta(current.next_attempt_at if current else None, self._clock()),
            )
        else:
            logger.warning(
                "[thp_post] 投稿终止：key=%s 原因=%s code=%s attempts=%d",
                _key_label(row),
                outcome,
                attempt_result.error_code,
                attempt,
            )
            self._wake.set()

    # -- receipts -----------------------------------------------------------

    async def _dispatch_receipts(self) -> bool:
        sent_any = False
        rows = await self._outbox.receipt_candidates(now=self._clock())
        for row in rows[:WORKER_RECEIPTS_PER_TICK]:
            if await self._send_one_receipt(row.key):
                sent_any = True
        return sent_any

    async def _send_one_receipt(self, key: tuple[str, str, str, str]) -> bool:
        async with self._receipt_lock:
            return await self._send_one_receipt_locked(key)

    async def _send_one_receipt_locked(self, key: tuple[str, str, str, str]) -> bool:
        """Send whatever receipt the row currently owes; returns True when sent.

        Ordering guarantee: a ``pending_stored`` notice is superseded by the
        success state (the success transaction rewrites the receipt state), so
        "暂存" can never follow "投稿成功". Concurrency guarantee: only the caller
        that wins :meth:`Outbox.claim_receipt` sends, so the handler fast path
        and the scheduling pass cannot both send the same notice.
        """
        claimed = await self._outbox.claim_receipt(key, now=self._clock())
        if claimed is None:
            return False
        row = claimed
        if row.receipt_state != outbox.RECEIPT_SENDING:  # pragma: no cover - CAS guard
            return False

        if (
            row.origin_base_url != self._settings.backend_base_url
            or row.event_id != self._settings.event_id
            or not self._settings.is_group_allowed(row.group_id)
        ):
            await self._outbox.set_receipt_state(
                key,
                state=outbox.RECEIPT_ABANDONED,
                now=self._clock(),
                error="target/event/group changed",
                increment_attempts=False,
            )
            return False
        if (
            row.receipt_attempts >= RECEIPT_MAX_ATTEMPTS
            or self._clock() >= row.enqueued_at + RECEIPT_TTL_SECONDS
        ):
            await self._outbox.set_receipt_state(
                key,
                state=outbox.RECEIPT_ABANDONED,
                now=self._clock(),
                error="receipt retries exhausted",
                increment_attempts=False,
            )
            return False

        if row.state == outbox.SUCCEEDED:
            final_state = outbox.RECEIPT_SENT_SUCCESS
        elif row.state == outbox.HELD:
            # Held rows must not produce late notices (e.g. after the group was
            # removed from the allow-list).
            await self._outbox.set_receipt_state(
                key,
                state=outbox.RECEIPT_ABANDONED,
                now=self._clock(),
                error="held",
                increment_attempts=False,
            )
            return False
        elif row.state == outbox.FAILED:
            final_state = outbox.RECEIPT_SENT_FAILED
        else:
            # Still pending/inflight: "stored and retrying".
            final_state = outbox.RECEIPT_SENT_STORED

        text = self._receipt_text(row, final_state)

        try:
            # The adapter must not keep the worker blocked forever when offline.
            delivered = await asyncio.wait_for(
                self._send_message(row.unified_msg_origin, self._plain_chain(text)),
                timeout=10.0,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - receipt failures must not escalate
            delivered = False
            error = type(exc).__name__
        else:
            error = "" if delivered else "平台会话未找到或发送被拒绝"

        if delivered:
            await self._outbox.set_receipt_state(
                key,
                state=final_state,
                now=self._clock(),
                error="",
            )
            logger.debug("[thp_post] 回执已发送：key=%s state=%s", _key_label(row), final_state)
            return True

        # Bounded receipt retry; the submission itself is untouched.
        attempts = row.receipt_attempts + 1
        if (
            attempts >= RECEIPT_MAX_ATTEMPTS
            or self._clock() >= row.enqueued_at + RECEIPT_TTL_SECONDS
        ):
            await self._outbox.set_receipt_state(
                key,
                state=outbox.RECEIPT_ABANDONED,
                now=self._clock(),
                error=error or "receipt retries exhausted",
                increment_attempts=True,
            )
            logger.warning(
                "[thp_post] 群回执最终发送失败（投稿数据不受影响）：key=%s 原因=%s",
                _key_label(row),
                error or "unknown",
            )
            return False
        delay = self._receipt_schedule.delay_for(attempts, jitter=self._jitter)
        await self._outbox.set_receipt_state(
            key,
            state=row.receipt_state,
            now=self._clock(),
            next_attempt_at=self._clock() + delay,
            error=error or "send failed",
        )
        logger.info(
            "[thp_post] 群回执发送失败，将重试：key=%s 第 %d 次 原因=%s",
            _key_label(row),
            attempts,
            error or "unknown",
        )
        return False

    @staticmethod
    def _receipt_text(row: DeliveryRow, final_state: str) -> str:
        if final_state == outbox.RECEIPT_SENT_SUCCESS:
            return receipts.success_text(row.submission_id or 0)
        if final_state == outbox.RECEIPT_SENT_STORED:
            return receipts.STORED
        if _is_uncertain(row):
            return receipts.uncertain_text()
        return receipts.rejected_text()

    async def send_receipt_now(self, key: tuple[str, str, str, str]) -> bool:
        """Public entry point used by ``main.py`` for the handler fast path.

        Args:
            key: Idempotency key of the row.

        Returns:
            ``True`` when this call actually delivered the notice.
        """
        return await self._send_one_receipt(key)


def _is_uncertain(row: DeliveryRow) -> bool:
    """True when the final state is inconclusive rather than a definite refusal.

    ``failed_rejected`` (the backend refused this submission) and
    ``failed_misconfigured`` (wrong address/redirect) both mean "definitely not
    stored", so the group is told the post was not accepted. Everything else —
    exhausted attempts, an expired deadline, a rejected token — leaves the real
    outcome unknown, and the wording must not claim the post was not stored.
    """
    return row.failure_reason_label in (
        outbox.REASON_CREDENTIALS,
        outbox.REASON_TTL_EXPIRED,
        outbox.REASON_MAX_ATTEMPTS,
        outbox.REASON_FINAL_RETRY_PAST_DEADLINE,
    )


def _key_label(row: DeliveryRow) -> str:
    return f"g{row.group_id}/u{row.qq_id}/m{row.source_message_id}"


def _format_eta(next_attempt_at: float | None, now: float) -> str:
    if next_attempt_at is None:
        return "-"
    delta = max(0.0, next_attempt_at - now)
    return f"{delta:.1f}s 后"


__all__ = [
    "BACKOFF_SCHEDULE",
    "CLAIM_LEASE_SECONDS",
    "MAX_BACKOFF_SECONDS",
    "RECEIPT_BACKOFF_SCHEDULE",
    "RECEIPT_MAX_ATTEMPTS",
    "STATUS_CREDENTIALS",
    "STATUS_NOT_ATTEMPTED",
    "STATUS_REJECTED",
    "STATUS_SUCCEEDED",
    "STATUS_UNCONFIRMED",
    "BackoffSchedule",
    "DeliveryWorker",
    "IngestOutcome",
]
