"""DuckDB-specific tests for DuckDbBackend.

Tests AC-3 (schema), AC-5 (error wrapping), and DuckDB-specific behaviors.
"""

import pytest

pytest.importorskip("duckdb", reason="duckdb extra not installed")

import duckdb

from datetime import datetime
from unittest.mock import MagicMock

from kptn.exceptions import StateStoreError
from kptn.state_store.duckdb import DuckDbBackend
from kptn.state_store.protocol import StateStoreBackend


# ─── Fixtures ────────────────────────────────────────────────────────────── #


@pytest.fixture
def backend(tmp_path):
    return DuckDbBackend(path=str(tmp_path / "test.duckdb"))


# ─── AC-3: Schema is exactly as specified ────────────────────────────────── #


def test_schema_exact_columns(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    DuckDbBackend(path=db_path)
    conn = duckdb.connect(db_path)
    rows = conn.execute("PRAGMA table_info(task_state)").fetchall()
    conn.close()
    col_names = [r[1] for r in rows]
    col_types = [r[2] for r in rows]
    assert col_names == ["storage_key", "pipeline_name", "task_name", "output_hash", "status", "ran_at"]
    # DuckDB normalizes TEXT → VARCHAR internally; both are equivalent string types
    assert all(t in ("TEXT", "VARCHAR") for t in col_types)


def test_schema_no_extra_columns(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    DuckDbBackend(path=db_path)
    conn = duckdb.connect(db_path)
    rows = conn.execute("PRAGMA table_info(task_state)").fetchall()
    conn.close()
    assert len(rows) == 6


def test_schema_composite_pk(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    DuckDbBackend(path=db_path)
    conn = duckdb.connect(db_path)
    rows = conn.execute("PRAGMA table_info(task_state)").fetchall()
    conn.close()
    # pk index is column 5; non-zero means it's part of the PK
    pk_cols = {r[1] for r in rows if r[5] != 0}
    assert pk_cols == {"storage_key", "pipeline_name", "task_name"}


# ─── AC-5: DuckDB errors wrapped in StateStoreError ──────────────────────── #


def _make_erroring_backend(tmp_path):
    """Return a DuckDbBackend whose connection raises OperationalError on execute."""
    b = DuckDbBackend(path=str(tmp_path / "test.duckdb"))
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = duckdb.OperationalError("injected error")
    b._conn = mock_conn
    return b


def test_write_hash_duckdb_error_raises_state_store_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError):
        b.write_hash("sk", "pipe", "task", "hash")


def test_read_hash_duckdb_error_raises_state_store_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError):
        b.read_hash("sk", "pipe", "task")


def test_delete_duckdb_error_raises_state_store_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError):
        b.delete("sk", "pipe", "task")


def test_list_tasks_duckdb_error_raises_state_store_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError):
        b.list_tasks("sk", "pipe")


def test_state_store_error_has_cause(tmp_path):
    original = duckdb.OperationalError("disk full")
    b = DuckDbBackend(path=str(tmp_path / "test.duckdb"))
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = original
    b._conn = mock_conn
    with pytest.raises(StateStoreError) as exc_info:
        b.write_hash("sk", "pipe", "task", "hash")
    assert exc_info.value.__cause__ is original


def test_errors_are_not_bare_duckdb_errors(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError) as exc_info:
        b.write_hash("sk", "pipe", "task", "hash")
    assert isinstance(exc_info.value, StateStoreError)
    assert not isinstance(exc_info.value, duckdb.Error)


# ─── Directory auto-creation ─────────────────────────────────────────────── #


def test_creates_parent_directory_if_missing(tmp_path):
    nested = tmp_path / "deep" / "nested" / "kptn.duckdb"
    DuckDbBackend(path=str(nested))
    assert nested.exists()


# ─── ran_at and status fields ─────────────────────────────────────────────── #


def test_ran_at_is_set_on_write(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    b = DuckDbBackend(path=db_path)
    b.write_hash("sk", "pipe", "task", "hash")
    conn = duckdb.connect(db_path)
    row = conn.execute("SELECT ran_at FROM task_state").fetchone()
    conn.close()
    assert row is not None
    parsed = datetime.fromisoformat(row[0])
    assert isinstance(parsed, datetime)


def test_status_is_success_on_write(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    b = DuckDbBackend(path=db_path)
    b.write_hash("sk", "pipe", "task", "hash")
    conn = duckdb.connect(db_path)
    row = conn.execute("SELECT status FROM task_state").fetchone()
    conn.close()
    assert row[0] == "success"


# ─── Protocol conformance ─────────────────────────────────────────────────── #


def test_isinstance_protocol(backend):
    assert isinstance(backend, StateStoreBackend)


# ─── Factory integration ─────────────────────────────────────────────────── #


def test_factory_returns_duckdb_backend(tmp_path):
    from kptn.state_store import init_state_store

    class Settings:
        db = "duckdb"
        db_path = str(tmp_path / "factory_test.duckdb")

    result = init_state_store(Settings())
    assert isinstance(result, DuckDbBackend)
