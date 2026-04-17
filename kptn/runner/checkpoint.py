from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import time

import duckdb

from kptn.change_detector.detector import is_stale
from kptn.runner.plan import (
    emit_backup_end,
    emit_backup_start,
    emit_checkpoint_select,
    emit_checkpoint_stale,
    emit_restore_end,
    emit_restore_start,
)

if TYPE_CHECKING:
    from kptn.graph.nodes import AnyNode

logger = logging.getLogger(__name__)


def get_db_path(conn: Any) -> Path | None:
    """Return the filesystem path for the active DuckDB database, or None if in-memory."""
    row = conn.execute(
        "SELECT path FROM duckdb_databases() WHERE database_name = current_database()"
    ).fetchone()
    if not row or not row[0]:
        return None
    return Path(row[0])


def checkpoint_path(db_path: Path, task_name: str) -> Path:
    """Return the backup path for a task checkpoint.

    Example: example.ddb + "load" → example.load.backup.ddb
    """
    return db_path.parent / f"{db_path.stem}.{task_name}.backup{db_path.suffix}"


def save_checkpoint(conn: Any, db_path: Path, task_name: str) -> None:
    """Flush the WAL, close *conn*, and copy *db_path* to a task checkpoint file.

    The connection is closed so the file lock is released before copying.
    The factory must detect the closed state and reopen on its next call —
    duckdb_checkpoint=True requires an idempotent factory (one that creates a
    fresh connection each call, or detects the closed state and reconnects).

    Shared-connection mode (state store and pipeline DB on the same connection)
    is not supported with duckdb_checkpoint — checkpoint safely requires sole
    ownership of the file.
    """
    conn.execute("CHECKPOINT")
    conn.close()
    dest = checkpoint_path(db_path, task_name)
    emit_backup_start(task_name, str(dest), timestamp=True)
    shutil.copyfile(db_path, dest)
    emit_backup_end(task_name, timestamp=True)


def restore_checkpoint(conn: Any, candidate: Path, db_path: Path) -> None:
    """Close *conn* and overwrite *db_path* with the checkpoint at *candidate*."""
    conn.close()
    emit_restore_start(str(candidate), timestamp=True)
    t0 = time.monotonic()
    shutil.copyfile(candidate, db_path)
    emit_restore_end(time.monotonic() - t0, timestamp=True)


class _BackupStore:
    """Read-only view of _kptn.task_state in a backup DuckDB file."""

    def __init__(self, conn: "duckdb.DuckDBPyConnection") -> None:
        self._conn = conn

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None:
        try:
            row = self._conn.execute(
                "SELECT output_hash FROM _kptn.task_state "
                "WHERE storage_key=? AND pipeline_name=? AND task_name=?",
                (storage_key, pipeline, task),
            ).fetchone()
            return row[0] if row else None
        except Exception:
            return None


def find_restore_candidate(
    ordered: list[AnyNode],
    storage_key: str,
    pipeline: str,
    db_path: Path,
) -> Path | None:
    """Walk *ordered* in topo order and return the furthest eligible checkpoint path.

    A checkpoint at node B (index i) is eligible when, based on the _kptn state
    inside the backup file, all tasks from ordered[0:i+1] (e.g. init through
    big_task inclusive) would be cache-hits. If any would re-run after restore,
    the restore is wasted so we skip that candidate.
    """
    best: Path | None = None
    for i, node in enumerate(ordered):
        spec = getattr(node, "spec", None)
        if spec is None or not getattr(spec, "duckdb_checkpoint", False):
            continue
        cp = checkpoint_path(db_path, node.name)
        if not cp.exists():
            continue
        try:
            conn = duckdb.connect(str(cp), read_only=True)
        except Exception:
            continue
        try:
            backup_store = _BackupStore(conn)
            stale_task: str | None = None
            for n in ordered[: i + 1]:
                if is_stale(n, backup_store, storage_key, pipeline)[0]:
                    stale_task = n.name
                    break
        finally:
            conn.close()
        if stale_task is None:
            emit_checkpoint_select(node.name, timestamp=True)
            best = cp
        else:
            emit_checkpoint_stale(node.name, stale_task, timestamp=True)
            cp.unlink(missing_ok=True)

    return best
