"""Conformance suite for StateStoreBackend protocol.

Tests AC-1, AC-2, AC-3, AC-4 across all conforming backends.
Story 2.2 will add DuckDbBackend to the parametrized list.
"""

import pytest

from kptn.state_store.protocol import StateStoreBackend
from kptn.state_store.sqlite import SqliteBackend


# ─── Fixtures ────────────────────────────────────────────────────────────── #


@pytest.fixture
def sqlite_backend(tmp_path):
    return SqliteBackend(path=str(tmp_path / "test.db"))


@pytest.fixture(params=["sqlite", "duckdb"])
def backend(request, tmp_path, sqlite_backend):
    if request.param == "duckdb":
        pytest.importorskip("duckdb")
        from kptn.state_store.duckdb import DuckDbBackend
        return DuckDbBackend(path=str(tmp_path / "test.duckdb"))
    return sqlite_backend


# ─── AC-1: Protocol is runtime-checkable ─────────────────────────────────── #


def test_sqlite_backend_isinstance_protocol(sqlite_backend):
    assert isinstance(sqlite_backend, StateStoreBackend)


def test_protocol_is_runtime_checkable():
    # Verify the Protocol can be used with isinstance (would raise TypeError if not @runtime_checkable)
    class Conforming:
        def read_hash(self, storage_key, pipeline, task): ...
        def write_hash(self, storage_key, pipeline, task, hash): ...
        def delete(self, storage_key, pipeline, task): ...
        def list_tasks(self, storage_key, pipeline): ...

    assert isinstance(Conforming(), StateStoreBackend)


# ─── AC-2: write_hash upserts with correct composite PK ──────────────────── #


def test_write_hash_creates_row(backend):
    backend.write_hash("sk", "pipe", "taskA", "abc123")
    result = backend.read_hash("sk", "pipe", "taskA")
    assert result == "abc123"


def test_write_hash_upserts_not_duplicates(backend):
    backend.write_hash("sk", "pipe", "taskA", "first")
    backend.write_hash("sk", "pipe", "taskA", "second")
    result = backend.read_hash("sk", "pipe", "taskA")
    assert result == "second"


def test_write_hash_composite_pk_distinguishes_storage_key(backend):
    backend.write_hash("sk1", "pipe", "taskA", "hash1")
    backend.write_hash("sk2", "pipe", "taskA", "hash2")
    assert backend.read_hash("sk1", "pipe", "taskA") == "hash1"
    assert backend.read_hash("sk2", "pipe", "taskA") == "hash2"


def test_write_hash_composite_pk_distinguishes_pipeline(backend):
    backend.write_hash("sk", "pipeA", "taskX", "hashA")
    backend.write_hash("sk", "pipeB", "taskX", "hashB")
    assert backend.read_hash("sk", "pipeA", "taskX") == "hashA"
    assert backend.read_hash("sk", "pipeB", "taskX") == "hashB"


def test_write_hash_composite_pk_distinguishes_task(backend):
    backend.write_hash("sk", "pipe", "task1", "h1")
    backend.write_hash("sk", "pipe", "task2", "h2")
    assert backend.read_hash("sk", "pipe", "task1") == "h1"
    assert backend.read_hash("sk", "pipe", "task2") == "h2"


# ─── AC-3: read_hash returns stored hash ─────────────────────────────────── #


def test_read_hash_round_trip(backend):
    hash_val = "deadbeef" * 8  # 64-char hex string
    backend.write_hash("sk", "pipe", "task", hash_val)
    assert backend.read_hash("sk", "pipe", "task") == hash_val


# ─── AC-4: read_hash returns None for unknown task ───────────────────────── #


def test_read_hash_unknown_task_returns_none(backend):
    result = backend.read_hash("sk", "pipe", "nonexistent")
    assert result is None


def test_read_hash_returns_none_not_empty_string(backend):
    result = backend.read_hash("unknown_sk", "unknown_pipe", "unknown_task")
    assert result is None
    assert result != ""


# ─── delete ──────────────────────────────────────────────────────────────── #


def test_delete_removes_row(backend):
    backend.write_hash("sk", "pipe", "task", "hash")
    backend.delete("sk", "pipe", "task")
    assert backend.read_hash("sk", "pipe", "task") is None


def test_delete_nonexistent_row_does_not_raise(backend):
    backend.delete("sk", "pipe", "nonexistent")  # should not raise


def test_delete_only_removes_matching_row(backend):
    backend.write_hash("sk", "pipe", "taskA", "hashA")
    backend.write_hash("sk", "pipe", "taskB", "hashB")
    backend.delete("sk", "pipe", "taskA")
    assert backend.read_hash("sk", "pipe", "taskA") is None
    assert backend.read_hash("sk", "pipe", "taskB") == "hashB"


# ─── list_tasks ──────────────────────────────────────────────────────────── #


def test_list_tasks_returns_task_names(backend):
    backend.write_hash("sk", "pipe", "task1", "h1")
    backend.write_hash("sk", "pipe", "task2", "h2")
    tasks = backend.list_tasks("sk", "pipe")
    assert sorted(tasks) == ["task1", "task2"]


def test_list_tasks_empty_pipeline(backend):
    tasks = backend.list_tasks("sk", "empty_pipe")
    assert tasks == []


def test_list_tasks_scoped_to_storage_key_and_pipeline(backend):
    backend.write_hash("sk1", "pipe", "taskA", "h1")
    backend.write_hash("sk2", "pipe", "taskA", "h2")
    backend.write_hash("sk1", "other", "taskB", "h3")
    assert backend.list_tasks("sk1", "pipe") == ["taskA"]


# ─── Public API surface ───────────────────────────────────────────────────── #


def test_state_store_backend_importable_from_kptn_state_store():
    from kptn.state_store import StateStoreBackend as _SSB
    assert _SSB is StateStoreBackend


def test_sqlite_backend_importable_from_kptn_state_store():
    from kptn.state_store import SqliteBackend as _SB
    assert _SB is SqliteBackend


def test_state_store_all_exports():
    import kptn.state_store as ss
    assert "StateStoreBackend" in ss.__all__
    assert "SqliteBackend" in ss.__all__
    assert "init_state_store" in ss.__all__
