"""Tests for DbClientDuckDB.

Covers:
- Full BaseDbClientTest contract (CRUD, binning, subtasks)
- kptn schema is stored inside the DuckDB file
- kptn state survives a checkpoint/restore cycle (the core fix)
"""

import shutil
from pathlib import Path

import pytest

pytest.importorskip("duckdb", reason="duckdb extra not installed")
import duckdb

from kptn.caching.client.DbClientDuckDB import DbClientDuckDB
from kptn.caching.models import TaskState
from tests.base_db_client_test import BaseDbClientTest


def _make_client(conn) -> DbClientDuckDB:
    client = DbClientDuckDB(
        table_name="tasks",
        storage_key="branch1",
        pipeline="test_pipeline",
    )
    client.wire_conn(conn)
    return client


class TestDuckDBClient(BaseDbClientTest):
    """Run the shared contract tests against DbClientDuckDB."""

    @pytest.fixture
    def db(self, tmp_path):
        db_path = str(tmp_path / "test.ddb")
        conn = duckdb.connect(db_path)
        client = _make_client(conn)
        yield client
        conn.close()


# ---------------------------------------------------------------------------
# Additional DuckDB-specific tests
# ---------------------------------------------------------------------------


def test_wire_conn_creates_kptn_schema(tmp_path):
    """wire_conn should create the kptn schema and tables inside the DuckDB file."""
    db_path = tmp_path / "test.ddb"
    conn = duckdb.connect(str(db_path))
    client = _make_client(conn)

    schemas = [r[0] for r in conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()]
    assert "kptn" in schemas

    tables = [
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'kptn'"
        ).fetchall()
    ]
    assert "tasks" in tables
    assert "taskdata_bins" in tables
    assert "subtask_bins" in tables
    conn.close()


def test_raises_before_wire_conn():
    """Accessing conn before wire_conn should raise RuntimeError."""
    client = DbClientDuckDB(storage_key="k", pipeline="p")
    with pytest.raises(RuntimeError, match="wire_conn"):
        _ = client.conn


def test_kptn_state_survives_checkpoint_restore(tmp_path):
    """Simulate the checkpoint/restore cycle.

    After restoring the DuckDB file from a backup, the kptn state
    (task completion records) should be present, so the pipeline can
    skip tasks that already ran.
    """
    db_path = tmp_path / "pipeline.ddb"
    backup_path = tmp_path / "pipeline.task_a.backup.ddb"

    # --- Run 1: task_a completes and is checkpointed ---
    conn = duckdb.connect(str(db_path))
    client = _make_client(conn)

    client.create_task("task_a", TaskState(start_time="2024-01-01T00:00:00"))
    client.set_task_ended("task_a", status="SUCCESS")

    # Simulate save_duckdb_checkpoint: flush then copy
    conn.execute("checkpoint")
    shutil.copy2(db_path, backup_path)
    conn.close()

    # --- Partial run 2 (process dies mid-task_b): restore checkpoint ---
    # Overwrite the db with a "partial" state to simulate corruption
    conn_partial = duckdb.connect(str(db_path))
    # task_a row is still there, but pretend task_b wrote garbage
    conn_partial.execute("DELETE FROM kptn.tasks WHERE task_id = 'task_a'")
    conn_partial.execute("checkpoint")
    conn_partial.close()

    # Restore from backup (what restore_duckdb_checkpoint does)
    shutil.copy2(backup_path, db_path)

    # --- After restore: kptn state should reflect completed task_a ---
    conn_restored = duckdb.connect(str(db_path))
    client_restored = _make_client(conn_restored)

    state = client_restored.get_task("task_a")
    assert state is not None, "task_a state should survive checkpoint restore"
    assert state.end_time is not None, "task_a should have an end_time after restore"

    conn_restored.close()


def test_multiple_pipelines_isolated(tmp_path):
    """Two pipelines sharing the same DuckDB file should not see each other's tasks."""
    conn = duckdb.connect(str(tmp_path / "shared.ddb"))

    client_a = DbClientDuckDB(storage_key="branch", pipeline="pipe_a")
    client_a.wire_conn(conn)

    client_b = DbClientDuckDB(storage_key="branch", pipeline="pipe_b")
    client_b._conn = conn  # share the already-initialised connection

    client_a.create_task("task1", TaskState(start_time="t"))
    client_b.create_task("task1", TaskState(start_time="t"))
    client_a.set_task_ended("task1", status="SUCCESS")

    state_a = client_a.get_task("task1")
    state_b = client_b.get_task("task1")

    assert state_a.end_time is not None
    assert state_b.end_time is None  # pipe_b task1 was not ended

    conn.close()


def test_storage_key_isolation(tmp_path):
    """Different storage keys (branches) are isolated even within the same pipeline."""
    conn = duckdb.connect(str(tmp_path / "db.ddb"))

    client_main = DbClientDuckDB(storage_key="main", pipeline="pipe")
    client_main.wire_conn(conn)

    client_dev = DbClientDuckDB(storage_key="dev", pipeline="pipe")
    client_dev._conn = conn

    client_main.create_task("task1", TaskState(start_time="t"))
    assert client_dev.get_task("task1") is None

    conn.close()
