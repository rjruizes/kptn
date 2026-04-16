from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import time

from kptn.runner.plan import emit_backup_end, emit_backup_start, emit_restore_end, emit_restore_start

if TYPE_CHECKING:
    from kptn.graph.nodes import AnyNode
    from kptn.state_store.protocol import StateStoreBackend

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


def find_restore_candidate(
    ordered: list[AnyNode],
    state_store: StateStoreBackend,
    storage_key: str,
    pipeline: str,
    db_path: Path,
) -> Path | None:
    """Walk *ordered* in topo order and return the furthest eligible checkpoint path.

    A checkpoint is eligible when:
    - The node declares duckdb_checkpoint=True
    - The checkpoint file exists on disk
    - The task has no cached hash (meaning it will re-run, so restoring here is safe)
    """
    best: Path | None = None
    for node in ordered:
        spec = getattr(node, "spec", None)
        if spec is None or not getattr(spec, "duckdb_checkpoint", False):
            continue
        cp = checkpoint_path(db_path, node.name)
        if not cp.exists():
            continue
        if state_store.read_hash(storage_key, pipeline, node.name) is not None:
            continue
        best = cp
    return best
