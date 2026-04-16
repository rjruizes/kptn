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
    rows = conn.execute(
        "SELECT column_name, data_type FROM duckdb_columns() WHERE schema_name='_kptn' AND table_name='task_state'"
    ).fetchall()
    conn.close()
    col_names = [r[0] for r in rows]
    col_types = [r[1] for r in rows]
    assert col_names == ["storage_key", "pipeline_name", "task_name", "output_hash", "status", "ran_at"]
    # DuckDB normalizes TEXT → VARCHAR internally; both are equivalent string types
    assert all(t in ("TEXT", "VARCHAR") for t in col_types)


def test_schema_no_extra_columns(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    DuckDbBackend(path=db_path)
    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT column_name FROM duckdb_columns() WHERE schema_name='_kptn' AND table_name='task_state'"
    ).fetchall()
    conn.close()
    assert len(rows) == 6


def test_schema_composite_pk(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    DuckDbBackend(path=db_path)
    conn = duckdb.connect(db_path)
    row = conn.execute(
        "SELECT constraint_column_names FROM duckdb_constraints() WHERE schema_name='_kptn' AND table_name='task_state' AND constraint_type='PRIMARY KEY'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert set(row[0]) == {"storage_key", "pipeline_name", "task_name"}


# ─── AC-5: DuckDB errors wrapped in StateStoreError ──────────────────────── #


def _make_erroring_backend(tmp_path):
    """Return a DuckDbBackend whose connection raises OperationalError on execute."""
    b = DuckDbBackend(path=str(tmp_path / "test.duckdb"))
    mock_conn_instance = MagicMock()
    mock_conn_instance.execute.side_effect = duckdb.OperationalError("injected error")
    b._conn = lambda: mock_conn_instance
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
    mock_conn_instance = MagicMock()
    mock_conn_instance.execute.side_effect = original
    b._conn = lambda: mock_conn_instance
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
    row = conn.execute("SELECT ran_at FROM _kptn.task_state").fetchone()
    conn.close()
    assert row is not None
    parsed = datetime.fromisoformat(row[0])
    assert isinstance(parsed, datetime)


def test_status_is_success_on_write(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    b = DuckDbBackend(path=db_path)
    b.write_hash("sk", "pipe", "task", "hash")
    conn = duckdb.connect(db_path)
    row = conn.execute("SELECT status FROM _kptn.task_state").fetchone()
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


# ─── DuckDbBackend factory mode ──────────────────────────────────────────── #


def test_backend_factory_mode_reads_and_writes(tmp_path):
    """DuckDbBackend(factory=...) can read and write hashes."""
    db_path = str(tmp_path / "shared.duckdb")
    conn = duckdb.connect(db_path)

    def get_engine():
        return conn

    b = DuckDbBackend(factory=get_engine)
    b.write_hash("sk", "pipe", "t1", "abc123")
    result = b.read_hash("sk", "pipe", "t1")
    assert result == "abc123"
    conn.close()


def test_backend_factory_mode_calls_factory_each_time(tmp_path):
    """Factory is called on every operation — simulates connection being replaced mid-pipeline."""
    db_path = str(tmp_path / "shared.duckdb")
    # Start with connection A
    conn_a = duckdb.connect(db_path)
    connections = [conn_a]

    call_count = 0

    def get_engine():
        nonlocal call_count
        call_count += 1
        return connections[-1]

    b = DuckDbBackend(factory=get_engine)
    b.write_hash("sk", "pipe", "t1", "hash1")
    # Simulate task replacing connection: close A, open B on same file
    conn_a.close()
    conn_b = duckdb.connect(db_path)
    connections.append(conn_b)

    result = b.read_hash("sk", "pipe", "t1")
    assert result == "hash1"
    assert call_count >= 2  # factory was called at least for write + read
    conn_b.close()


def test_backend_factory_mode_close_delegates_to_factory():
    """DuckDbBackend.close() calls factory().close()."""
    mock_conn = MagicMock()
    b = DuckDbBackend(factory=lambda: mock_conn)
    b.close()
    mock_conn.close.assert_called_once()


def test_init_state_store_with_duckdb_factory(tmp_path):
    """init_state_store(duckdb_factory=...) returns a DuckDbBackend in factory mode."""
    from kptn.state_store.factory import init_state_store

    conn = duckdb.connect(str(tmp_path / "shared.duckdb"))

    b = init_state_store(duckdb_factory=lambda: conn)
    assert isinstance(b, DuckDbBackend)
    b.write_hash("sk", "pipe", "t1", "xyz")
    assert b.read_hash("sk", "pipe", "t1") == "xyz"
    conn.close()


def test_init_state_store_factory_takes_precedence_over_settings(tmp_path):
    """duckdb_factory overrides settings.db when both are provided."""
    from kptn.state_store.factory import init_state_store

    conn = duckdb.connect(str(tmp_path / "factory.duckdb"))

    class Settings:
        db = "sqlite"  # would normally use sqlite
        db_path = str(tmp_path / "settings.db")

    b = init_state_store(Settings(), duckdb_factory=lambda: conn)
    assert isinstance(b, DuckDbBackend)
    conn.close()


# ─── keep_db_open behaviour ──────────────────────────────────────────────── #


def test_keep_db_open_true_returns_connection(tmp_path):
    """pipeline.run(keep_db_open=True) returns the live duckdb connection."""
    from kptn.graph.decorators import TaskSpec
    from kptn.graph.nodes import TaskNode, ConfigNode
    from kptn.graph.graph import Graph
    from kptn.graph.pipeline import Pipeline
    from kptn.runner.executor import execute
    from kptn.profiles.resolved import ResolvedGraph
    from tests.fakes import FakeStateStore

    mock_conn = MagicMock()

    def get_engine():
        return mock_conn

    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "my_task"
    task_node = TaskNode(fn=task_fn, spec=TaskSpec(outputs=[]), name="my_task")
    config_node = ConfigNode(spec={"duckdb": get_engine})

    graph = Graph(
        nodes=[config_node, task_node],
        edges=[(config_node, task_node)],
    )
    resolved = ResolvedGraph(graph=graph, pipeline="test", storage_key="sk")
    state_store = FakeStateStore()

    result = execute(
        resolved, state_store,
        duckdb_factory=get_engine,
        keep_db_open=True,
    )
    assert result is mock_conn


def test_keep_db_open_false_closes_connection(tmp_path):
    """pipeline.run(keep_db_open=False) closes the duckdb connection."""
    from kptn.graph.decorators import TaskSpec
    from kptn.graph.nodes import TaskNode, ConfigNode
    from kptn.graph.graph import Graph
    from kptn.profiles.resolved import ResolvedGraph
    from kptn.runner.executor import execute
    from tests.fakes import FakeStateStore

    mock_conn = MagicMock()

    def get_engine():
        return mock_conn

    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "my_task"
    task_node = TaskNode(fn=task_fn, spec=TaskSpec(outputs=[]), name="my_task")
    config_node = ConfigNode(spec={"duckdb": get_engine})

    graph = Graph(
        nodes=[config_node, task_node],
        edges=[(config_node, task_node)],
    )
    resolved = ResolvedGraph(graph=graph, pipeline="test", storage_key="sk")
    state_store = FakeStateStore()

    result = execute(
        resolved, state_store,
        duckdb_factory=get_engine,
        keep_db_open=False,
    )
    assert result is None
    mock_conn.close.assert_called_once()
