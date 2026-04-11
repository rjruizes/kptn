"""SQLite-specific tests for SqliteBackend.

Tests AC-5 (schema) and AC-6 (error wrapping) plus SQLite-specific behaviors.
"""

import sqlite3
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from kptn.exceptions import StateStoreError
from kptn.state_store.sqlite import SqliteBackend


# ─── Fixtures ────────────────────────────────────────────────────────────── #


@pytest.fixture
def backend(tmp_path):
    return SqliteBackend(path=str(tmp_path / "test.db"))


# ─── AC-5: Schema is exactly as specified ────────────────────────────────── #


def test_schema_exact_columns(tmp_path):
    db_path = str(tmp_path / "test.db")
    SqliteBackend(path=db_path)
    conn = sqlite3.connect(db_path)
    rows = conn.execute("PRAGMA table_info(task_state)").fetchall()
    conn.close()
    # columns: (cid, name, type, notnull, default, pk)
    col_names = [r[1] for r in rows]
    col_types = [r[2] for r in rows]
    assert col_names == ["storage_key", "pipeline_name", "task_name", "output_hash", "status", "ran_at"]
    assert col_types == ["TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"]


def test_schema_not_null_constraints(tmp_path):
    db_path = str(tmp_path / "test.db")
    SqliteBackend(path=db_path)
    conn = sqlite3.connect(db_path)
    rows = conn.execute("PRAGMA table_info(task_state)").fetchall()
    conn.close()
    # notnull is index 3
    not_null_map = {r[1]: bool(r[3]) for r in rows}
    assert not_null_map["storage_key"] is True
    assert not_null_map["pipeline_name"] is True
    assert not_null_map["task_name"] is True
    assert not_null_map["output_hash"] is False
    assert not_null_map["status"] is False
    assert not_null_map["ran_at"] is False


def test_schema_composite_pk(tmp_path):
    db_path = str(tmp_path / "test.db")
    SqliteBackend(path=db_path)
    conn = sqlite3.connect(db_path)
    rows = conn.execute("PRAGMA table_info(task_state)").fetchall()
    conn.close()
    # pk index is column 5; non-zero means it's part of the PK, value = position in PK
    pk_cols = {r[1] for r in rows if r[5] != 0}
    assert pk_cols == {"storage_key", "pipeline_name", "task_name"}


def test_schema_no_extra_columns(tmp_path):
    db_path = str(tmp_path / "test.db")
    SqliteBackend(path=db_path)
    conn = sqlite3.connect(db_path)
    rows = conn.execute("PRAGMA table_info(task_state)").fetchall()
    conn.close()
    assert len(rows) == 6


# ─── AC-5: Directory auto-creation ───────────────────────────────────────── #


def test_creates_parent_directory_if_missing(tmp_path):
    nested = tmp_path / "deep" / "nested" / "kptn.db"
    SqliteBackend(path=str(nested))
    assert nested.exists()


def test_default_path_directory_created(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    SqliteBackend()
    assert (tmp_path / ".kptn" / "kptn.db").exists()


# ─── AC-6: SQLite errors wrapped in StateStoreError ──────────────────────── #


def _make_erroring_backend(tmp_path):
    """Return a SqliteBackend whose connection raises OperationalError on execute."""
    b = SqliteBackend(path=str(tmp_path / "test.db"))
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.execute.side_effect = sqlite3.OperationalError("injected error")
    b._conn = mock_conn
    return b


def test_write_hash_sqlite_error_raises_state_store_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError):
        b.write_hash("sk", "pipe", "task", "hash")


def test_write_hash_error_not_bare_sqlite_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError) as exc_info:
        b.write_hash("sk", "pipe", "task", "hash")
    assert isinstance(exc_info.value, StateStoreError)
    assert not isinstance(exc_info.value, sqlite3.Error)


def test_read_hash_sqlite_error_raises_state_store_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError):
        b.read_hash("sk", "pipe", "task")


def test_delete_sqlite_error_raises_state_store_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError):
        b.delete("sk", "pipe", "task")


def test_list_tasks_sqlite_error_raises_state_store_error(tmp_path):
    b = _make_erroring_backend(tmp_path)
    with pytest.raises(StateStoreError):
        b.list_tasks("sk", "pipe")


def test_state_store_error_has_cause(tmp_path):
    original = sqlite3.OperationalError("disk full")
    b = SqliteBackend(path=str(tmp_path / "test.db"))
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_conn.execute.side_effect = original
    b._conn = mock_conn
    with pytest.raises(StateStoreError) as exc_info:
        b.write_hash("sk", "pipe", "task", "hash")
    assert exc_info.value.__cause__ is original


# ─── ran_at and status fields ─────────────────────────────────────────────── #


def test_ran_at_is_set_on_write(tmp_path):
    db_path = str(tmp_path / "test.db")
    b = SqliteBackend(path=db_path)
    b.write_hash("sk", "pipe", "task", "hash")
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT ran_at FROM task_state").fetchone()
    conn.close()
    assert row is not None
    ran_at = row[0]
    # Should be a valid ISO 8601 string
    parsed = datetime.fromisoformat(ran_at)
    assert isinstance(parsed, datetime)


def test_status_is_success_on_write(tmp_path):
    db_path = str(tmp_path / "test.db")
    b = SqliteBackend(path=db_path)
    b.write_hash("sk", "pipe", "task", "hash")
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT status FROM task_state").fetchone()
    conn.close()
    assert row[0] == "success"


def test_write_hash_accepts_none_hash(backend):
    # output_hash allows SQL NULL — must not raise
    backend.write_hash("sk", "pipe", "task", None)
    result = backend.read_hash("sk", "pipe", "task")
    assert result is None


# ─── Public API surface ───────────────────────────────────────────────────── #


def test_init_state_store_importable():
    from kptn.state_store import init_state_store
    assert callable(init_state_store)


def test_init_state_store_returns_sqlite_by_default(tmp_path):
    from kptn.state_store import init_state_store

    class Settings:
        db = "sqlite"
        db_path = str(tmp_path / "test.db")

    backend = init_state_store(Settings())
    assert isinstance(backend, SqliteBackend)


def test_init_state_store_raises_for_unknown_backend():
    from kptn.state_store import init_state_store

    class Settings:
        db = "postgres"
        db_path = None

    with pytest.raises(ValueError, match="postgres"):
        init_state_store(Settings())
