"""SQLite outbox: durable staging, local idempotency, rate limiting, capacity.

Storage location
----------------
``<AstrBot data>/plugin_data/<plugin name>/outbox.db`` — obtained through
``astrbot.core.utils.astrbot_path.get_astrbot_data_path()`` (documented in
AstrBot's ``docs/zh/dev/star/guides/storage.md``). The database therefore lives
in the mounted data volume, not in the plugin directory, so replacing plugin
code during an upgrade can never lose pending submissions.

Concurrency model
-----------------
One process, one connection, one :class:`asyncio.Lock` guarding every
transaction. AstrBot instantiates each plugin once and the delivery scheduler
runs a single worker, so this is both simpler and safer than a reader/writer
pool. Write operations use ``BEGIN IMMEDIATE`` so the lock is taken up front and
``busy_timeout`` (5 s) covers any external tool (the admin CLI) touching the
file. Network I/O never happens inside a transaction: rows are claimed, then the
lock is released, then HTTP is attempted, then the result is written back.

Payload immutability
--------------------
The JSON payload is generated exactly once, before the first attempt, and stored
verbatim. Retries always resend the stored bytes plus the stored
``event_id``/``origin_base_url``; the current configuration is never used to
rewrite an already-queued submission. The ingest token is deliberately *not*
stored per row (only in the plugin config).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)

# --- delivery states --------------------------------------------------------

PENDING = "pending"
INFLIGHT = "inflight"
SUCCEEDED = "succeeded"
FAILED = "failed"
HELD = "held"

# Terminal-for-retry reasons attached to ``failed`` rows.
REASON_MAX_ATTEMPTS = "max_attempts_reached"
REASON_TTL_EXPIRED = "deadline_expired"
REASON_FINAL_RETRY_PAST_DEADLINE = "retry_window_exhausted"
REASON_REJECTED = "rejected_by_backend"
REASON_CREDENTIALS = "credentials_rejected"
REASON_MISCONFIGURED = "backend_address_misconfigured"
REASON_STALE_LEASE = "lease_expired_during_shutdown"

# Held reasons.
HELD_TARGET_CHANGED = "target_changed"
HELD_EVENT_CHANGED = "event_changed"
HELD_GROUP_REMOVED = "group_removed"

# Receipt states.
RECEIPT_NONE = "none"
RECEIPT_PENDING_SUCCESS = "pending_success"
RECEIPT_PENDING_STORED = "pending_stored"
RECEIPT_SENDING = "sending"
"""A sender has claimed this receipt; ``receipt_next_at`` is the resend lease."""
RECEIPT_SENT_SUCCESS = "sent_success"
RECEIPT_SENT_STORED = "sent_stored"
RECEIPT_SENT_FAILED = "sent_failed"
RECEIPT_ABANDONED = "abandoned"

#: States that are allowed to (re)send a receipt.
RECEIPT_CLAIMABLE = (RECEIPT_PENDING_SUCCESS, RECEIPT_PENDING_STORED, RECEIPT_SENDING)

_ACTIVE_STATES = (PENDING, INFLIGHT)

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS deliveries (
    event_id            TEXT    NOT NULL,
    bot_id              TEXT    NOT NULL,
    group_id            TEXT    NOT NULL,
    source_message_id   TEXT    NOT NULL,
    qq_id               TEXT    NOT NULL,
    nickname            TEXT,
    group_card          TEXT,
    content             TEXT    NOT NULL,
    payload_json        TEXT    NOT NULL,
    origin_base_url     TEXT    NOT NULL,
    platform_id         TEXT    NOT NULL,
    unified_msg_origin  TEXT    NOT NULL,
    state               TEXT    NOT NULL,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    next_attempt_at     REAL,
    lease_expires_at    REAL,
    enqueued_at         REAL    NOT NULL,
    deadline_at         REAL    NOT NULL,
    last_error          TEXT    NOT NULL DEFAULT '',
    last_error_code     TEXT    NOT NULL DEFAULT '',
    failure_reason      TEXT    NOT NULL DEFAULT '',
    last_attempt_at     REAL,
    succeeded_at        REAL,
    submission_id       INTEGER,
    duplicate           INTEGER,
    receipt_state       TEXT    NOT NULL DEFAULT 'none',
    receipt_attempts    INTEGER NOT NULL DEFAULT 0,
    receipt_next_at     REAL,
    last_receipt_error  TEXT    NOT NULL DEFAULT '',
    held_reason         TEXT    NOT NULL DEFAULT '',
    created_at          REAL    NOT NULL,
    updated_at          REAL    NOT NULL,
    PRIMARY KEY (event_id, bot_id, group_id, source_message_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_state_next
    ON deliveries (state, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_event_state
    ON deliveries (event_id, state);
CREATE INDEX IF NOT EXISTS idx_deliveries_receipt
    ON deliveries (receipt_state, receipt_next_at);

CREATE TABLE IF NOT EXISTS plugin_state (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  REAL NOT NULL
);
"""

_SHORT_TEXT_LIMIT = 500

#: Delays below this are treated as "due now" by :meth:`Outbox.claim_next`.
#: Production delays are whole seconds (2 s at the very least); this only makes
#: sub-100 ms test schedules deterministic, where jitter could otherwise leave a
#: row a fraction of a millisecond in the future and therefore never due.
_MIN_SCHEDULE_RESOLUTION_SECONDS = 0.05


def _now() -> float:
    return time.time()


def _clip(value: object, limit: int = _SHORT_TEXT_LIMIT) -> str:
    text = str(value or "").replace("\x00", "")
    if len(text) > limit:
        return text[:limit] + "…"
    return text


@dataclass
class DeliveryRow:
    """One outbox row, decoded."""

    event_id: str
    bot_id: str
    group_id: str
    source_message_id: str
    qq_id: str
    nickname: str | None
    group_card: str | None
    content: str
    payload_json: str
    origin_base_url: str
    platform_id: str
    unified_msg_origin: str
    state: str
    attempt_count: int
    next_attempt_at: float | None
    lease_expires_at: float | None
    enqueued_at: float
    deadline_at: float
    last_error: str = ""
    last_error_code: str = ""
    """Stable backend ``error.code`` (or a local marker) kept for diagnostics."""
    failure_reason: str = ""
    """Local classification of the terminal state (:data:`REASON_*`)."""

    @property
    def failure_reason_label(self) -> str:
        return self.failure_reason or self.last_error_code

    last_attempt_at: float | None = None
    succeeded_at: float | None = None
    submission_id: int | None = None
    duplicate: bool | None = None
    receipt_state: str = RECEIPT_NONE
    receipt_attempts: int = 0
    receipt_next_at: float | None = None
    last_receipt_error: str = ""
    held_reason: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.event_id, self.bot_id, self.group_id, self.source_message_id)

    @property
    def payload(self) -> dict[str, Any]:
        try:
            data = json.loads(self.payload_json)
        except (TypeError, ValueError):  # pragma: no cover - stored by us
            return {}
        return data if isinstance(data, dict) else {}

    @property
    def is_terminal(self) -> bool:
        return self.state in (SUCCEEDED, FAILED, HELD)

    def seconds_left(self, now: float | None = None) -> float:
        return self.deadline_at - (_now() if now is None else now)


@dataclass(frozen=True)
class EnqueueOutcome:
    """Result of :meth:`Outbox.enqueue`."""

    created: bool
    row: DeliveryRow | None = None
    # When the row could not be created: a stable reason for the caller.
    blocker: str = ""
    capacity: int = 0

    @property
    def key(self) -> tuple[str, str, str, str] | None:
        return self.row.key if self.row is not None else None


@dataclass(frozen=True)
class RateDecision:
    """Result of a local rate-limit probe."""

    allowed: bool
    retry_after: float = 0.0
    scope: str = ""

    @property
    def scope_label(self) -> str:
        return {
            "per_user": "每人",
            "per_group": "每群",
        }.get(self.scope, self.scope)


@dataclass(frozen=True)
class PrepareResult:
    """Result of :meth:`Outbox.prepare_event` — idempotency + rate limit + capacity."""

    created: bool = False
    duplicate: bool = False
    blocked: str = ""
    """``rate_limited`` / ``queue_full`` when nothing was written."""
    rate: RateDecision | None = None
    row: DeliveryRow | None = None
    row_key: tuple[str, str, str, str] | None = None


@dataclass
class OutboxStats:
    """Counts per delivery state (for logs and the admin CLI)."""

    by_state: dict[str, int] = field(default_factory=dict)
    pending_receipts: int = 0
    oldest_active_age: float | None = None

    @property
    def active(self) -> int:
        return self.by_state.get(PENDING, 0) + self.by_state.get(INFLIGHT, 0)


class Outbox:
    """Async SQLite outbox bound to one database file."""

    def __init__(self, db_path: Path | str, *, busy_timeout_ms: int = 5000) -> None:
        self.db_path = Path(db_path)
        self._busy_timeout_ms = busy_timeout_ms
        self._db: aiosqlite.Connection | None = None
        self._lock = None  # set in open(); asyncio.Lock must be created in-loop
        self._user_slots: dict[tuple[str, str, str], float] = {}

    # -- lifecycle ----------------------------------------------------------

    async def open(self) -> None:
        """Create the database file (and its parent directory) and the schema."""
        import asyncio

        if self._db is not None:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        db = await aiosqlite.connect(str(self.db_path))
        db.row_factory = aiosqlite.Row
        try:
            await db.execute(f"PRAGMA busy_timeout={int(self._busy_timeout_ms)}")
            await db.execute("PRAGMA foreign_keys=ON")
            for statement in SCHEMA.strip().split(";"):
                if statement.strip():
                    await db.execute(statement)
            columns = {
                row["name"]
                for row in await (await db.execute("PRAGMA table_info(deliveries)")).fetchall()
            }
            if "failure_reason" not in columns:
                await db.execute(
                    "ALTER TABLE deliveries ADD COLUMN failure_reason TEXT NOT NULL DEFAULT ''"
                )
            await self._recover_stale_inflight_locked(db)
            await db.commit()
        except BaseException:
            await db.close()
            raise
        # Recover rows left inflight by a crash/restart *before* any scheduling:
        # the lease is void once the process is gone.
        self._db = db

    async def close(self) -> None:
        """Close the connection (idempotent)."""
        db, self._db = self._db, None
        if db is not None:
            try:
                await db.close()
            except Exception:  # pragma: no cover - already closed
                pass

    @property
    def is_open(self) -> bool:
        return self._db is not None

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Outbox is not open")
        return self._db

    @asynccontextmanager
    async def _write(self) -> AsyncIterator[aiosqlite.Connection]:
        """Serialise and wrap a write transaction."""
        assert self._lock is not None
        async with self._lock:
            db = self.connection
            await db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    @asynccontextmanager
    async def _read(self) -> AsyncIterator[aiosqlite.Connection]:
        assert self._lock is not None
        async with self._lock:
            yield self.connection

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _row(record: Any) -> DeliveryRow:
        return DeliveryRow(
            event_id=record["event_id"],
            bot_id=record["bot_id"],
            group_id=record["group_id"],
            source_message_id=record["source_message_id"],
            qq_id=record["qq_id"],
            nickname=record["nickname"],
            group_card=record["group_card"],
            content=record["content"],
            payload_json=record["payload_json"],
            origin_base_url=record["origin_base_url"],
            platform_id=record["platform_id"],
            unified_msg_origin=record["unified_msg_origin"],
            state=record["state"],
            attempt_count=int(record["attempt_count"]),
            next_attempt_at=record["next_attempt_at"],
            lease_expires_at=record["lease_expires_at"],
            enqueued_at=float(record["enqueued_at"]),
            deadline_at=float(record["deadline_at"]),
            last_error=record["last_error"] or "",
            last_error_code=record["last_error_code"] or "",
            failure_reason=record["failure_reason"] or "",
            last_attempt_at=record["last_attempt_at"],
            succeeded_at=record["succeeded_at"],
            submission_id=record["submission_id"],
            duplicate=None if record["duplicate"] is None else bool(record["duplicate"]),
            receipt_state=record["receipt_state"] or RECEIPT_NONE,
            receipt_attempts=int(record["receipt_attempts"]),
            receipt_next_at=record["receipt_next_at"],
            last_receipt_error=record["last_receipt_error"] or "",
            held_reason=record["held_reason"] or "",
            created_at=float(record["created_at"]),
            updated_at=float(record["updated_at"]),
        )

    # -- reads --------------------------------------------------------------

    async def find(self, key: Iterable[str]) -> DeliveryRow | None:
        """Fetch one row by its idempotency key."""
        event_id, bot_id, group_id, source_message_id = list(key)
        async with self._read() as db:
            cursor = await db.execute(
                "SELECT * FROM deliveries WHERE event_id=? AND bot_id=? AND group_id=? "
                "AND source_message_id=?",
                (event_id, bot_id, group_id, source_message_id),
            )
            record = await cursor.fetchone()
            await cursor.close()
        return self._row(record) if record is not None else None

    async def get_by_key(
        self,
        event_id: str,
        bot_id: str,
        group_id: str,
        source_message_id: str,
    ) -> DeliveryRow | None:
        return await self.find((event_id, bot_id, group_id, source_message_id))

    async def count_active(self) -> int:
        """Number of rows in pending/inflight (the "active queue" size)."""
        async with self._read() as db:
            cursor = await db.execute(
                f"SELECT COUNT(*) AS n FROM deliveries WHERE state IN ({_placeholders(_ACTIVE_STATES)})",
                _ACTIVE_STATES,
            )
            row = await cursor.fetchone()
            await cursor.close()
        return int(row["n"]) if row is not None else 0

    async def stats(self) -> OutboxStats:
        async with self._read() as db:
            cursor = await db.execute("SELECT state, COUNT(*) AS n FROM deliveries GROUP BY state")
            rows = await cursor.fetchall()
            await cursor.close()
            cursor = await db.execute(
                "SELECT COUNT(*) AS n FROM deliveries WHERE receipt_state IN (?, ?)",
                (RECEIPT_PENDING_SUCCESS, RECEIPT_PENDING_STORED),
            )
            pending_receipts = await cursor.fetchone()
            await cursor.close()
            cursor = await db.execute(
                f"SELECT MIN(enqueued_at) AS m FROM deliveries WHERE state IN ({_placeholders(_ACTIVE_STATES)})",
                _ACTIVE_STATES,
            )
            oldest = await cursor.fetchone()
            await cursor.close()
        by_state = {str(row["state"]): int(row["n"]) for row in rows}
        oldest_at = oldest["m"] if oldest is not None else None
        return OutboxStats(
            by_state=by_state,
            pending_receipts=int(pending_receipts["n"]) if pending_receipts else 0,
            oldest_active_age=(_now() - float(oldest_at)) if oldest_at else None,
        )

    async def recent(self, limit: int = 20) -> list[DeliveryRow]:
        """Most recently created rows (admin CLI / diagnostics)."""
        capped = max(1, min(int(limit), 200))
        async with self._read() as db:
            cursor = await db.execute(
                "SELECT * FROM deliveries ORDER BY created_at DESC LIMIT ?",
                (capped,),
            )
            rows = await cursor.fetchall()
            await cursor.close()
        return [self._row(record) for record in rows]

    async def due_for_expiry(
        self, *, now: float | None = None, limit: int = 50
    ) -> list[DeliveryRow]:
        """Rows still in pending/failed whose fixed deadline has already passed."""
        moment = _now() if now is None else now
        async with self._read() as db:
            cursor = await db.execute(
                "SELECT * FROM deliveries WHERE state='pending' "
                "AND deadline_at <= ? ORDER BY deadline_at ASC LIMIT ?",
                (moment, max(1, min(int(limit), 200))),
            )
            rows = await cursor.fetchall()
            await cursor.close()
        return [self._row(record) for record in rows]

    # -- writes -------------------------------------------------------------

    async def prepare_event(
        self,
        *,
        key: tuple[str, str, str, str],
        payload: dict[str, Any],
        origin_base_url: str,
        platform_id: str,
        unified_msg_origin: str,
        ttl_seconds: int,
        capacity: int,
        user_interval_seconds: int,
        group_per_minute: int,
        now: float | None = None,
    ) -> PrepareResult:
        """Idempotency check, local rate limit and capacity check in one step.

        Order matches AGENT.md §4.3: an already-known event is answered *before*
        the rate limit is consulted, so a replay/retry neither consumes quota nor
        produces a second group reply.

        Args:
            key: ``(event_id, bot_id, group_id, source_message_id)``.
            payload: Frozen request body; stored verbatim.
            origin_base_url: Target origin of this submission (immutable).
            platform_id: AstrBot platform instance id used for receipts.
            unified_msg_origin: AstrBot session string used for receipts.
            ttl_seconds: Delivery deadline measured from first enqueue.
            capacity: Maximum number of pending+inflight rows.
            user_interval_seconds: Per (group, user) minimum interval; 0 disables.
            group_per_minute: Per group rolling-60s limit; 0 disables.
            now: Clock override for tests.

        Returns:
            A :class:`PrepareResult` describing what happened.
        """
        moment = _now() if now is None else now
        event_id, bot_id, group_id, source_message_id = key
        self._prune_user_slots(moment)
        async with self._write() as db:
            cursor = await db.execute(
                "SELECT * FROM deliveries WHERE event_id=? AND bot_id=? AND group_id=? "
                "AND source_message_id=?",
                (event_id, bot_id, group_id, source_message_id),
            )
            existing = await cursor.fetchone()
            await cursor.close()
            if existing is not None:
                row = self._row(existing)
                if any(
                    row.payload.get(field) != payload.get(field) for field in ("qq_id", "content")
                ):
                    return PrepareResult(blocked="idempotency_conflict", row=row)
                return PrepareResult(duplicate=True, row=row)

            decision = await self._check_rate_limit_locked(
                db,
                group_id=group_id,
                qq_id=str(payload.get("qq_id") or ""),
                user_interval_seconds=user_interval_seconds,
                group_per_minute=group_per_minute,
                now=moment,
            )
            if not decision.allowed:
                return PrepareResult(blocked="rate_limited", rate=decision)

            cursor = await db.execute(
                f"SELECT COUNT(*) AS n FROM deliveries WHERE state IN ({_placeholders(_ACTIVE_STATES)})",
                _ACTIVE_STATES,
            )
            active = await cursor.fetchone()
            await cursor.close()
            active_count = int(active["n"]) if active is not None else 0
            if active_count >= capacity:
                return PrepareResult(blocked="queue_full")

            payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            created_at = moment
            await db.execute(
                """
                INSERT INTO deliveries (
                    event_id, bot_id, group_id, source_message_id,
                    qq_id, nickname, group_card, content, payload_json,
                    origin_base_url, platform_id, unified_msg_origin,
                    state, attempt_count, next_attempt_at, lease_expires_at,
                    enqueued_at, deadline_at,
                    last_error, last_error_code,
                    receipt_state, receipt_attempts, receipt_next_at, last_receipt_error,
                    held_reason, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_id,
                    bot_id,
                    group_id,
                    source_message_id,
                    str(payload.get("qq_id") or ""),
                    payload.get("nickname"),
                    payload.get("group_card"),
                    str(payload.get("content") or ""),
                    payload_json,
                    origin_base_url,
                    platform_id,
                    unified_msg_origin,
                    PENDING,
                    0,
                    moment,
                    None,
                    moment,
                    moment + float(ttl_seconds),
                    "",
                    "",
                    RECEIPT_NONE,
                    0,
                    None,
                    "",
                    "",
                    created_at,
                    created_at,
                ),
            )
            self._remember_user_slot(group_id, str(payload.get("qq_id") or ""), moment)
        row = await self.get_by_key(event_id, bot_id, group_id, source_message_id)
        return PrepareResult(created=True, row=row, row_key=key, rate=decision)

    async def _check_rate_limit_locked(
        self,
        db: aiosqlite.Connection,
        *,
        group_id: str,
        qq_id: str,
        user_interval_seconds: int,
        group_per_minute: int,
        now: float,
    ) -> RateDecision:
        """Local rate limit.

        Scope is deliberately explicit: ``per_user`` is *within one group* for
        one QQ number, ``per_group`` is one group's rolling 60-second window.
        Only genuinely accepted new submissions are counted; retries and
        duplicate events never reach this check.
        """
        if user_interval_seconds > 0:
            last = self._user_slots.get((group_id, qq_id, ""))
            if last is not None and now - last < user_interval_seconds:
                return RateDecision(
                    allowed=False,
                    retry_after=user_interval_seconds - (now - last),
                    scope="per_user",
                )
        if group_per_minute > 0:
            window_start = now - 60.0
            cursor = await db.execute(
                "SELECT COUNT(*) AS n, MIN(enqueued_at) AS oldest FROM deliveries "
                "WHERE group_id=? AND enqueued_at > ?",
                (group_id, window_start),
            )
            record = await cursor.fetchone()
            await cursor.close()
            count = int(record["n"]) if record is not None else 0
            if count >= group_per_minute:
                oldest = record["oldest"] if record is not None else None
                retry_after = 60.0
                if oldest is not None:
                    retry_after = max(1.0, float(oldest) + 60.0 - now)
                return RateDecision(allowed=False, retry_after=retry_after, scope="per_group")
        return RateDecision(allowed=True)

    def _prune_user_slots(self, now: float) -> None:
        for key in [k for k, ts in self._user_slots.items() if now - ts > 3600]:
            self._user_slots.pop(key, None)

    def _remember_user_slot(self, group_id: str, qq_id: str, moment: float) -> None:
        self._user_slots[(group_id, qq_id, "")] = moment

    async def claim_next(
        self,
        *,
        origin_base_url: str,
        event_id: str,
        allowed_group_ids: Iterable[str],
        max_attempts: int,
        now: float | None = None,
        lease_seconds: float = 60.0,
    ) -> DeliveryRow | None:
        """Atomically claim the oldest due row for the current target.

        Rows whose stored target no longer matches the configuration are marked
        ``held`` instead of being claimed: a pending submission must never be
        silently delivered to a new backend or a new activity.

        Args:
            origin_base_url: Current configured origin.
            event_id: Current configured event id.
            allowed_group_ids: Current group allow-list.
            max_attempts: Attempt ceiling (first attempt included).
            now: Clock override.
            lease_seconds: How long the claim stays exclusive.

        Returns:
            The claimed row, or ``None`` when nothing is due.
        """
        moment = _now() if now is None else now
        groups = tuple(allowed_group_ids)
        async with self._write() as db:
            await self._hold_mismatched_locked(
                db,
                origin_base_url=origin_base_url,
                event_id=event_id,
                allowed_group_ids=groups,
            )
            for condition, reason in (
                ("deadline_at<=?", REASON_TTL_EXPIRED),
                ("attempt_count>=?", REASON_MAX_ATTEMPTS),
            ):
                threshold = moment if reason == REASON_TTL_EXPIRED else max_attempts
                await db.execute(
                    "UPDATE deliveries SET state=?, failure_reason=?, next_attempt_at=NULL, "
                    "lease_expires_at=NULL, receipt_state=?, receipt_next_at=?, updated_at=? "
                    f"WHERE state='pending' AND {condition}",
                    (FAILED, reason, RECEIPT_PENDING_STORED, moment, moment, threshold),
                )
            constraints = ["state='pending'"]
            params: list[Any] = []
            constraints.append("(next_attempt_at IS NULL OR next_attempt_at <= ?)")
            params.append(moment)
            constraints.append("origin_base_url = ?")
            params.append(origin_base_url)
            constraints.append("event_id = ?")
            params.append(event_id)
            if groups:
                constraints.append(f"group_id IN ({_placeholders(groups)})")
                params.extend(groups)
            else:
                constraints.append("0")
            constraints.append("attempt_count < ?")
            params.append(int(max_attempts))
            sql = (
                "SELECT * FROM deliveries WHERE "
                + " AND ".join(constraints)
                + " ORDER BY next_attempt_at ASC, enqueued_at ASC LIMIT 1"
            )
            cursor = await db.execute(sql, tuple(params))
            record = await cursor.fetchone()
            await cursor.close()
            if record is None:
                return None
            key = (
                record["event_id"],
                record["bot_id"],
                record["group_id"],
                record["source_message_id"],
            )
            await db.execute(
                "UPDATE deliveries SET state=?, attempt_count=attempt_count+1, "
                "lease_expires_at=?, last_attempt_at=?, next_attempt_at=NULL, updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                "AND state='pending'",
                (INFLIGHT, moment + lease_seconds, moment, moment, *key),
            )
        return await self.get_by_key(*key)

    async def _hold_mismatched_locked(
        self,
        db: aiosqlite.Connection,
        *,
        origin_base_url: str,
        event_id: str,
        allowed_group_ids: tuple[str, ...],
    ) -> None:
        """Park rows that belong to another activity/target/group set.

        ``next_attempt_at`` is nulled so the hold survives restarts and cannot be
        un-done by ordinary scheduling. An administrator must decide what happens
        to them (see ``scripts/thp_outbox_admin.py`` and PLUGIN_HANDOFF.md).
        """
        await db.execute(
            f"UPDATE deliveries SET state=?, held_reason=?, next_attempt_at=NULL, "
            f"lease_expires_at=NULL, updated_at=? "
            f"WHERE state IN ({_placeholders(_ACTIVE_STATES)}) "
            f"AND (origin_base_url<>? OR event_id<>?)",
            (
                HELD,
                HELD_TARGET_CHANGED,
                _now(),
                *_ACTIVE_STATES,
                origin_base_url,
                event_id,
            ),
        )
        if allowed_group_ids:
            await db.execute(
                f"UPDATE deliveries SET state=?, held_reason=?, next_attempt_at=NULL, "
                f"lease_expires_at=NULL, updated_at=? "
                f"WHERE state IN ({_placeholders(_ACTIVE_STATES)}) "
                f"AND group_id NOT IN ({_placeholders(allowed_group_ids)})",
                (
                    HELD,
                    HELD_GROUP_REMOVED,
                    _now(),
                    *_ACTIVE_STATES,
                    *allowed_group_ids,
                ),
            )
        else:
            await db.execute(
                "UPDATE deliveries SET state=?, held_reason=?, next_attempt_at=NULL, "
                "lease_expires_at=NULL, updated_at=? WHERE state IN ('pending','inflight')",
                (HELD, HELD_GROUP_REMOVED, _now()),
            )

    async def mark_succeeded(
        self,
        key: tuple[str, str, str, str],
        *,
        submission_id: int,
        duplicate: bool,
        now: float | None = None,
        receipt_state: str = RECEIPT_PENDING_SUCCESS,
        receipt_next_at: float | None = None,
    ) -> bool:
        """Persist a confirmed backend result and enqueue the success receipt.

        Returns:
            ``True`` when this call performed the transition (i.e. the row was
            still non-terminal). A concurrent writer that already succeeded the
            row makes this return ``False``.
        """
        moment = _now() if now is None else now
        async with self._write() as db:
            cursor = await db.execute(
                "UPDATE deliveries SET state=?, submission_id=?, duplicate=?, "
                "succeeded_at=?, lease_expires_at=NULL, next_attempt_at=NULL, "
                "last_error='', last_error_code='', "
                "receipt_state=?, receipt_next_at=?, updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                "AND state IN ('pending','inflight')",
                (
                    SUCCEEDED,
                    int(submission_id),
                    1 if duplicate else 0,
                    moment,
                    receipt_state,
                    receipt_next_at,
                    moment,
                    *key,
                ),
            )
            changed = cursor.rowcount or 0
            await cursor.close()
        return changed > 0

    async def mark_retry(
        self,
        key: tuple[str, str, str, str],
        *,
        reason: str,
        error_code: str,
        diagnostic: str = "",
        attempt_count: int,
        max_attempts: int,
        deadline_at: float,
        schedule: Any,
        now: float | None = None,
        retry_after: float | None = None,
        jitter: Any = None,
    ) -> str:
        """Persist a failed attempt and decide whether another one is scheduled.

        Args:
            key: Idempotency key of the row.
            reason: Human-readable cause for logs/feedback.
            error_code: Stable code (backend ``error.code`` or local marker).
            diagnostic: Redacted technical detail.
            attempt_count: Attempts already used (including this one).
            max_attempts: Hard ceiling.
            deadline_at: Fixed delivery deadline.
            schedule: Backoff schedule object (``services.delivery.BackoffSchedule``).
            now: Clock override.
            retry_after: Server-mandated wait (429), honoured when it fits.
            jitter: Optional jitter callable for deterministic tests.

        Returns:
            ``"scheduled"`` when a retry was written, otherwise a terminal
            reason constant (:data:`REASON_*`).
        """
        moment = _now() if now is None else now
        if attempt_count >= max_attempts:
            await self._finalise_failure(
                key,
                state=FAILED,
                reason=REASON_MAX_ATTEMPTS,
                error_code=error_code or REASON_MAX_ATTEMPTS,
                diagnostic=diagnostic or reason,
                now=moment,
                receipt_state=RECEIPT_PENDING_STORED,
                receipt_next_at=moment,
            )
            return REASON_MAX_ATTEMPTS
        if moment >= deadline_at:
            await self._finalise_failure(
                key,
                state=FAILED,
                reason=REASON_TTL_EXPIRED,
                error_code=error_code or REASON_TTL_EXPIRED,
                diagnostic=diagnostic or reason,
                now=moment,
                receipt_state=RECEIPT_PENDING_STORED,
                receipt_next_at=moment,
            )
            return REASON_TTL_EXPIRED

        delay = schedule.delay_for(attempt_count, jitter=jitter)
        if retry_after is not None:
            delay = max(delay, float(retry_after))
        next_at = moment + delay
        if next_at > deadline_at:
            await self._finalise_failure(
                key,
                state=FAILED,
                reason=REASON_FINAL_RETRY_PAST_DEADLINE,
                error_code=error_code or REASON_FINAL_RETRY_PAST_DEADLINE,
                diagnostic=(
                    (diagnostic or reason)
                    + f"；下一次允许重试时间 {next_at:.0f} 已超过投递期限 {deadline_at:.0f}"
                ),
                now=moment,
                receipt_state=RECEIPT_PENDING_STORED,
                receipt_next_at=moment,
            )
            return REASON_FINAL_RETRY_PAST_DEADLINE

        async with self._write() as db:
            cursor = await db.execute(
                "UPDATE deliveries SET state=?, next_attempt_at=?, lease_expires_at=NULL, "
                "last_error=?, last_error_code=?, updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                "AND state IN ('pending','inflight')",
                (
                    PENDING,
                    next_at,
                    _clip(diagnostic or reason),
                    _clip(error_code, 80),
                    moment,
                    *key,
                ),
            )
            changed = cursor.rowcount or 0
            await cursor.close()
        return "scheduled" if changed > 0 else "already_terminal"

    async def mark_terminal_failure(
        self,
        key: tuple[str, str, str, str],
        *,
        reason: str,
        error_code: str,
        diagnostic: str = "",
        now: float | None = None,
        receipt_state: str | None = None,
        receipt_next_at: float | None = None,
    ) -> bool:
        """Mark a row terminally failed (rejected / credentials / misconfigured)."""
        return await self._finalise_failure(
            key,
            state=FAILED,
            reason=reason,
            error_code=error_code,
            diagnostic=diagnostic,
            now=now,
            receipt_state=receipt_state,
            receipt_next_at=receipt_next_at,
        )

    async def _finalise_failure(
        self,
        key: tuple[str, str, str, str],
        *,
        state: str,
        reason: str,
        error_code: str,
        diagnostic: str,
        now: float | None = None,
        receipt_state: str | None = None,
        receipt_next_at: float | None = None,
    ) -> bool:
        """Write a terminal state.

        ``reason`` is the plugin's own classification (:data:`REASON_*`) and
        ``error_code`` is the backend's stable ``error.code`` (or a local marker
        when there was no usable response). Keeping them apart lets the receipt
        wording stay correct ("definitely refused" vs "still unknown") while the
        operator log keeps the backend's own code.
        """
        moment = _now() if now is None else now
        sets = [
            "state=?",
            "next_attempt_at=NULL",
            "lease_expires_at=NULL",
            "last_error=?",
            "last_error_code=?",
            "failure_reason=?",
            "updated_at=?",
        ]
        params: list[Any] = [
            state,
            _clip(diagnostic or reason),
            _clip(error_code or reason, 80),
            _clip(reason, 80),
            moment,
        ]
        if receipt_state is not None:
            sets.append("receipt_state=?")
            params.append(receipt_state)
            sets.append("receipt_next_at=?")
            params.append(receipt_next_at)
        params.extend(key)
        async with self._write() as db:
            cursor = await db.execute(
                f"UPDATE deliveries SET {', '.join(sets)} "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                "AND state IN ('pending','inflight')",
                tuple(params),
            )
            changed = cursor.rowcount or 0
            await cursor.close()
        return changed > 0

    async def set_held(
        self,
        key: tuple[str, str, str, str],
        *,
        reason: str,
        now: float | None = None,
    ) -> bool:
        """Manually park a row (admin action)."""
        moment = _now() if now is None else now
        async with self._write() as db:
            cursor = await db.execute(
                "UPDATE deliveries SET state=?, held_reason=?, next_attempt_at=NULL, "
                "lease_expires_at=NULL, updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=?",
                (HELD, reason, moment, *key),
            )
            changed = cursor.rowcount or 0
            await cursor.close()
        return changed > 0

    async def release_hold(
        self,
        key: tuple[str, str, str, str],
        *,
        now: float | None = None,
        extend_ttl_seconds: float | None = None,
    ) -> bool:
        """Return a held row to ``pending`` (explicit administrator action only).

        Args:
            key: Idempotency key.
            now: Clock override.
            extend_ttl_seconds: **Required** to move a row. The original deadline
                is intentionally left in the past, so resuming without granting a
                fresh delivery window would fail again immediately (and would
                bypass the "已到期稿件不会因为管理员操作意外复活" guarantee).
                Passing a value pushes ``deadline_at`` forward by that many
                seconds and resets ``enqueued_at``/``attempt_count``.

        Returns:
            ``True`` when a held row was released.
        """
        if extend_ttl_seconds is None:
            return False
        moment = _now() if now is None else now
        async with self._write() as db:
            cursor = await db.execute(
                "UPDATE deliveries SET state=?, held_reason='', next_attempt_at=?, "
                "attempt_count=0, deadline_at=?, enqueued_at=?, updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                "AND state=?",
                (
                    PENDING,
                    moment,
                    moment + float(extend_ttl_seconds),
                    moment,
                    moment,
                    *key,
                    HELD,
                ),
            )
            changed = cursor.rowcount or 0
            await cursor.close()
        return changed > 0

    async def reset_for_retry(
        self,
        key: tuple[str, str, str, str],
        *,
        ttl_seconds: int,
        now: float | None = None,
    ) -> bool:
        """Administrator action: give a terminally failed row a fresh TTL."""
        moment = _now() if now is None else now
        async with self._write() as db:
            cursor = await db.execute(
                "UPDATE deliveries SET state=?, attempt_count=0, next_attempt_at=?, "
                "lease_expires_at=NULL, deadline_at=?, enqueued_at=?, last_error='', "
                "last_error_code='', updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                "AND state=?",
                (
                    PENDING,
                    moment,
                    moment + float(ttl_seconds),
                    moment,
                    moment,
                    *key,
                    FAILED,
                ),
            )
            changed = cursor.rowcount or 0
            await cursor.close()
        return changed > 0

    async def release_claim(
        self,
        key: tuple[str, str, str, str],
        *,
        error_code: str,
        diagnostic: str,
        now: float | None = None,
        consumed_attempt: bool = True,
    ) -> None:
        """Put a claimed row back after a shutdown/cancel.

        Args:
            key: Idempotency key.
            error_code: Marker stored as ``last_error_code``.
            diagnostic: Human-readable explanation (redacted).
            now: Clock override.
            consumed_attempt: Keep the attempt counter as-is (default). The
                delivery deadline still bounds the number of retries, so a
                cancelled attempt can never extend a submission's life.
        """
        moment = _now() if now is None else now
        decrement = 0 if consumed_attempt else 1
        async with self._write() as db:
            await db.execute(
                "UPDATE deliveries SET state=?, lease_expires_at=NULL, "
                "attempt_count=CASE WHEN attempt_count>=? THEN attempt_count-? ELSE 0 END, "
                "next_attempt_at=?, last_error=?, last_error_code=?, updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                "AND state=?",
                (
                    PENDING,
                    decrement,
                    decrement,
                    moment,
                    _clip(diagnostic),
                    _clip(error_code, 80),
                    moment,
                    *key,
                    INFLIGHT,
                ),
            )

    async def recover_stale_inflight(self, *, now: float | None = None, force: bool = False) -> int:
        """Requeue rows whose lease expired (crash/restart recovery)."""
        async with self._write() as db:
            return await self._recover_stale_inflight_locked(db, now=now, force=force)

    async def _recover_stale_inflight_locked(
        self,
        db: aiosqlite.Connection,
        *,
        now: float | None = None,
        force: bool = False,
    ) -> int:
        moment = _now() if now is None else now
        cursor = await db.execute(
            "UPDATE deliveries SET state=?, lease_expires_at=NULL, next_attempt_at=?, "
            "last_error=?, last_error_code=?, updated_at=? "
            "WHERE state=? AND (? OR lease_expires_at IS NULL OR lease_expires_at<=?)",
            (
                PENDING,
                moment,
                "上次运行中断时该稿处于发送中，已重新入队",
                REASON_STALE_LEASE,
                moment,
                INFLIGHT,
                int(force),
                moment,
            ),
        )
        changed = cursor.rowcount or 0
        await cursor.close()
        return changed

    # -- receipts -----------------------------------------------------------

    async def set_receipt_state(
        self,
        key: tuple[str, str, str, str],
        *,
        state: str,
        now: float | None = None,
        next_attempt_at: float | None = None,
        error: str = "",
        increment_attempts: bool = True,
    ) -> None:
        moment = _now() if now is None else now
        async with self._write() as db:
            await db.execute(
                "UPDATE deliveries SET receipt_state=?, receipt_next_at=?, "
                "last_receipt_error=?, "
                "receipt_attempts=receipt_attempts + ?, updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=?",
                (
                    state,
                    next_attempt_at,
                    _clip(error),
                    1 if increment_attempts else 0,
                    moment,
                    *key,
                ),
            )

    async def claim_receipt(
        self,
        key: tuple[str, str, str, str],
        *,
        now: float | None = None,
        lease_seconds: float = 60.0,
    ) -> DeliveryRow | None:
        """Atomically claim the right to send this row's receipt.

        Exactly one caller (the message handler fast path or the scheduling
        worker) wins the compare-and-set, which is what keeps a single group
        notice from being sent twice when both paths race.

        Args:
            key: Idempotency key.
            now: Clock override.
            lease_seconds: How long the claim is exclusive. A claimed-but-unsent
                receipt (crash between send and write-back) becomes claimable
                again after the lease, giving at-most-once-in-practice delivery
                with a documented crash window.

        Returns:
            The claimed row, or ``None`` when the receipt is not claimable.
        """
        moment = _now() if now is None else now
        async with self._write() as db:
            cursor = await db.execute(
                f"UPDATE deliveries SET receipt_state=?, receipt_next_at=?, updated_at=? "
                f"WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                f"AND receipt_state IN ({_placeholders(RECEIPT_CLAIMABLE)}) "
                f"AND (receipt_next_at IS NULL OR receipt_next_at <= ?)",
                (
                    RECEIPT_SENDING,
                    moment + float(lease_seconds),
                    moment,
                    *key,
                    *RECEIPT_CLAIMABLE,
                    moment,
                ),
            )
            claimed = cursor.rowcount or 0
            await cursor.close()
        if not claimed:
            return None
        return await self.get_by_key(*key)

    async def mark_deferred(
        self,
        key: tuple[str, str, str, str],
        *,
        now: float | None = None,
        delay_seconds: float,
    ) -> bool:
        """Schedule the "stored, will retry" notice — only if still un-decided.

        Guarded on ``receipt_state = 'none'`` so it can neither overwrite an
        already-pending success notice nor postpone a failure notice that a
        terminal retry outcome already queued.

        Returns ``False`` when the row already reached a decided receipt state,
        in which case the authoritative notice was (or will be) sent instead:
        this is the persistent guard against "暂存" arriving after "投稿成功".
        """
        moment = _now() if now is None else now
        async with self._write() as db:
            cursor = await db.execute(
                "UPDATE deliveries SET receipt_state=?, receipt_next_at=?, updated_at=? "
                "WHERE event_id=? AND bot_id=? AND group_id=? AND source_message_id=? "
                "AND state IN ('pending','inflight') AND receipt_state=?",
                (
                    RECEIPT_PENDING_STORED,
                    moment + max(0.0, float(delay_seconds)),
                    moment,
                    *key,
                    RECEIPT_NONE,
                ),
            )
            changed = cursor.rowcount or 0
            await cursor.close()
        return changed > 0

    async def receipt_candidates(
        self,
        *,
        now: float | None = None,
        limit: int = 20,
    ) -> list[DeliveryRow]:
        """Rows that owe the group a receipt right now.

        Includes previously claimed rows whose send lease expired (the crash
        window between "message sent" and "state written back").
        """
        moment = _now() if now is None else now
        async with self._read() as db:
            cursor = await db.execute(
                f"SELECT * FROM deliveries WHERE receipt_state IN "
                f"({_placeholders(RECEIPT_CLAIMABLE)}) "
                f"AND (receipt_next_at IS NULL OR receipt_next_at <= ?) "
                f"ORDER BY updated_at ASC LIMIT ?",
                (*RECEIPT_CLAIMABLE, moment, max(1, min(limit, 100))),
            )
            rows = await cursor.fetchall()
            await cursor.close()
        return [self._row(record) for record in rows]

    # -- key/value (holds global flags such as a credential block) ----------

    async def set_flag(self, name: str, value: str, *, now: float | None = None) -> None:
        moment = _now() if now is None else now
        async with self._write() as db:
            await db.execute(
                "INSERT INTO plugin_state (key, value, updated_at) VALUES (?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (name, value, moment),
            )

    async def get_flag(self, name: str) -> str | None:
        async with self._read() as db:
            cursor = await db.execute("SELECT value FROM plugin_state WHERE key=?", (name,))
            record = await cursor.fetchone()
            await cursor.close()
        return str(record["value"]) if record is not None else None


def _placeholders(values: Iterable[Any]) -> str:
    return ",".join("?" for _ in values)


def default_db_path(plugin_name: str) -> Path:
    """Resolve ``<AstrBot data>/plugin_data/<plugin>/outbox.db``.

    Falls back to ``~/.astrbot`` only if AstrBot's path helper cannot be
    imported (standalone tests / CLI usage).

    Raises:
        RuntimeError: If neither AstrBot's helper nor a writable home directory
            is available, because silently writing the outbox into the plugin
            directory would lose data on upgrade.
    """
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path  # type: ignore

        base = Path(get_astrbot_data_path())
    except Exception:
        env_override = os.environ.get("THP_POST_PLUGIN_DATA_DIR", "").strip()
        if env_override:
            base = Path(env_override).expanduser()
        else:
            home = Path.home()
            if not home.exists():  # pragma: no cover - exotic environments
                raise RuntimeError(
                    "无法定位 AstrBot 插件数据目录：astrbot 不可导入且没有可用的用户目录。"
                    "请设置环境变量 THP_POST_PLUGIN_DATA_DIR 指向持久化目录。",
                ) from None
            base = home / ".astrbot" / "data"
    safe_name = plugin_name or "astrbot_plugin_thp_post"
    return base / "plugin_data" / safe_name / "outbox.db"


def sqlite_available() -> bool:
    return sqlite3.sqlite_version_info >= (3, 24, 0)


__all__ = [
    "FAILED",
    "HELD",
    "HELD_EVENT_CHANGED",
    "HELD_GROUP_REMOVED",
    "HELD_TARGET_CHANGED",
    "INFLIGHT",
    "PENDING",
    "RECEIPT_ABANDONED",
    "RECEIPT_NONE",
    "RECEIPT_PENDING_STORED",
    "RECEIPT_PENDING_SUCCESS",
    "RECEIPT_SENT_FAILED",
    "RECEIPT_SENT_STORED",
    "RECEIPT_SENT_SUCCESS",
    "REASON_CREDENTIALS",
    "REASON_FINAL_RETRY_PAST_DEADLINE",
    "REASON_MAX_ATTEMPTS",
    "REASON_MISCONFIGURED",
    "REASON_REJECTED",
    "REASON_STALE_LEASE",
    "REASON_TTL_EXPIRED",
    "SUCCEEDED",
    "DeliveryRow",
    "EnqueueOutcome",
    "Outbox",
    "OutboxStats",
    "PrepareResult",
    "RateDecision",
    "default_db_path",
    "sqlite_available",
]
