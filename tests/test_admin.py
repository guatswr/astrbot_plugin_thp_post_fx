"""Maintenance is read-only by default and cannot revive historical posts."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from filelock import FileLock

from services import outbox as ob
from tests.test_delivery import Harness, success_response

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "thp_outbox_admin.py"


def run(db, *args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--db", str(db), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=dict(os.environ, PYTHONIOENCODING="utf-8"),
        timeout=5,
    )


def release_args(row):
    return (
        "release-held",
        "--event-id",
        row.event_id,
        "--bot-id",
        row.bot_id,
        "--group-id",
        row.group_id,
        "--message-id",
        row.source_message_id,
        "--backend-url",
        row.origin_base_url,
    )


async def test_status_and_recent_do_not_print_body_or_change_row(tmp_path):
    async with Harness(tmp_path, lambda _: success_response()) as h:
        row = await h.enqueue()
        before = (await h.store.find(row.key)).payload_json
        result = run(h.store.db_path, "status")
        assert result.returncode == 0 and json.loads(result.stdout) == [
            {"state": "pending", "count": 1}
        ]
        result = run(h.store.db_path, "recent")
        assert result.returncode == 0 and row.content not in result.stdout
        assert (await h.store.find(row.key)).payload_json == before


async def test_release_held_preserves_original_deadline_and_payload(tmp_path):
    async with Harness(tmp_path, lambda _: success_response()) as h:
        row = await h.enqueue()
        await h.store.set_held(row.key, reason=ob.HELD_TARGET_CHANGED)
        result = run(h.store.db_path, *release_args(row))
        assert result.returncode == 0
        current = await h.store.find(row.key)
        assert current.state == ob.PENDING
        assert current.deadline_at == row.deadline_at and current.payload_json == row.payload_json


async def test_release_refuses_running_worker_and_expired_row(tmp_path):
    async with Harness(tmp_path, lambda _: success_response()) as h:
        row = await h.enqueue()
        await h.store.set_held(row.key, reason=ob.HELD_TARGET_CHANGED)
        with FileLock(str(h.store.db_path) + ".lock"):
            assert run(h.store.db_path, *release_args(row)).returncode != 0
        await h.store.connection.execute("UPDATE deliveries SET deadline_at=?", (time.time() - 10,))
        await h.store.connection.commit()
        assert run(h.store.db_path, *release_args(row)).returncode != 0
        assert (await h.store.find(row.key)).state == ob.HELD
