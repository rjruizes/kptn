"""Tests for kptn.change_detector.hasher — determinism and correctness (AC 1–5)."""

import hashlib
import sqlite3

import pytest

from kptn.change_detector.hasher import (
    hash_file,
    hash_sqlite_table,
    hash_task_source,
)
from kptn.exceptions import HashError

duckdb = pytest.importorskip("duckdb")

from kptn.change_detector.hasher import hash_duckdb_table  # noqa: E402 — duckdb guarded


# ---------------------------------------------------------------------------
# SQLite tests
# ---------------------------------------------------------------------------


def _make_sqlite_db(path, rows: list[tuple]) -> str:
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE t (a TEXT, b INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    return f"{path}::t"


def test_hash_sqlite_table_determinism(tmp_path):
    rows = [("foo", 1), ("bar", 2)]
    uri = _make_sqlite_db(tmp_path / "db.sqlite", rows)
    h1 = hash_sqlite_table(uri)
    h2 = hash_sqlite_table(uri)
    assert h1 == h2
    assert isinstance(h1, str) and len(h1) == 32  # MD5 hex


def test_hash_sqlite_table_changes_when_row_added(tmp_path):
    uri = _make_sqlite_db(tmp_path / "db.sqlite", [("a", 1)])
    h1 = hash_sqlite_table(uri)
    conn = sqlite3.connect(str(tmp_path / "db.sqlite"))
    conn.execute("INSERT INTO t VALUES ('b', 2)")
    conn.commit()
    conn.close()
    h2 = hash_sqlite_table(uri)
    assert h1 != h2


def test_hash_sqlite_table_changes_when_value_modified(tmp_path):
    uri = _make_sqlite_db(tmp_path / "db.sqlite", [("original", 1)])
    h1 = hash_sqlite_table(uri)
    conn = sqlite3.connect(str(tmp_path / "db.sqlite"))
    conn.execute("UPDATE t SET a = 'changed' WHERE b = 1")
    conn.commit()
    conn.close()
    h2 = hash_sqlite_table(uri)
    assert h1 != h2


def test_hash_sqlite_table_empty_table_returns_stable_sentinel(tmp_path):
    db = tmp_path / "db.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (a TEXT)")
    conn.commit()
    conn.close()
    uri = f"{db}::t"
    h1 = hash_sqlite_table(uri)
    h2 = hash_sqlite_table(uri)
    expected = hashlib.md5(b"empty:").hexdigest()
    assert h1 == expected
    assert h1 == h2


def test_hash_sqlite_table_invalid_uri_raises_hash_error():
    with pytest.raises(HashError, match="missing '::' separator"):
        hash_sqlite_table("no-separator-here")


# ---------------------------------------------------------------------------
# hash_file tests
# ---------------------------------------------------------------------------


def test_hash_file_returns_sha256(tmp_path):
    f = tmp_path / "file.txt"
    f.write_bytes(b"hello world")
    h = hash_file(str(f))
    expected = hashlib.sha256(b"hello world").hexdigest()
    assert h == expected
    assert len(h) == 64
    assert h == h.lower()


def test_hash_file_deterministic(tmp_path):
    f = tmp_path / "file.txt"
    f.write_bytes(b"same content")
    h1 = hash_file(str(f))
    h2 = hash_file(str(f))
    assert h1 == h2


def test_hash_file_differs_when_content_changes(tmp_path):
    f = tmp_path / "file.txt"
    f.write_bytes(b"original")
    h1 = hash_file(str(f))
    f.write_bytes(b"mutated")
    h2 = hash_file(str(f))
    assert h1 != h2


def test_hash_file_nonexistent_raises_hash_error(tmp_path):
    with pytest.raises(HashError):
        hash_file(str(tmp_path / "does_not_exist.txt"))


# ---------------------------------------------------------------------------
# hash_task_source tests
# ---------------------------------------------------------------------------


def test_hash_task_source_returns_sha256():
    def my_fn(x):
        return x + 1

    h = hash_task_source(my_fn)
    assert len(h) == 64
    assert h == h.lower()


def test_hash_task_source_stable_across_comment_changes(monkeypatch):
    import inspect as inspect_mod
    import kptn.change_detector.hasher as hasher_mod

    monkeypatch.setattr(inspect_mod, "getsource", lambda _fn: "def my_func(x):\n    # this comment should be stripped\n    return x * 2\n")
    h1 = hash_task_source(lambda: None)

    monkeypatch.setattr(inspect_mod, "getsource", lambda _fn: "def my_func(x):\n    return x * 2\n")
    h2 = hash_task_source(lambda: None)

    assert h1 == h2


def test_hash_task_source_stable_across_whitespace_changes(monkeypatch):
    import inspect as inspect_mod

    monkeypatch.setattr(inspect_mod, "getsource", lambda _fn: "def my_func(x):\n    return x + 1\n")
    h1 = hash_task_source(lambda: None)

    monkeypatch.setattr(inspect_mod, "getsource", lambda _fn: "def my_func(x):\n\n\n    return x + 1\n")
    h2 = hash_task_source(lambda: None)

    assert h1 == h2


def test_hash_task_source_changes_when_logic_changes():
    def fn_add(x):
        return x + 1

    def fn_mul(x):
        return x * 2

    assert hash_task_source(fn_add) != hash_task_source(fn_mul)


# ---------------------------------------------------------------------------
# DuckDB tests (guarded by pytest.importorskip at module level)
# ---------------------------------------------------------------------------


def _make_duckdb(path, rows: list[tuple]) -> str:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE t (a VARCHAR, b INTEGER)")
    for row in rows:
        conn.execute("INSERT INTO t VALUES (?, ?)", row)
    conn.close()
    return f"{path}::t"


def test_hash_duckdb_table_determinism(tmp_path):
    uri = _make_duckdb(tmp_path / "db.duckdb", [("x", 1), ("y", 2)])
    h1 = hash_duckdb_table(uri)
    h2 = hash_duckdb_table(uri)
    assert h1 == h2
    assert isinstance(h1, str) and len(h1) == 32


def test_hash_duckdb_table_changes_when_row_added(tmp_path):
    uri = _make_duckdb(tmp_path / "db.duckdb", [("x", 1)])
    h1 = hash_duckdb_table(uri)
    conn = duckdb.connect(str(tmp_path / "db.duckdb"))
    conn.execute("INSERT INTO t VALUES ('z', 99)")
    conn.close()
    h2 = hash_duckdb_table(uri)
    assert h1 != h2


def test_hash_duckdb_table_empty_table_stable_sentinel(tmp_path):
    db = tmp_path / "db.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute("CREATE TABLE t (a VARCHAR)")
    conn.close()
    uri = f"{db}::t"
    h1 = hash_duckdb_table(uri)
    h2 = hash_duckdb_table(uri)
    expected = hashlib.md5(b"empty:").hexdigest()
    assert h1 == expected
    assert h1 == h2


def test_hash_duckdb_table_invalid_uri_raises_hash_error():
    with pytest.raises(HashError, match="missing '::' separator"):
        hash_duckdb_table("no-separator")
