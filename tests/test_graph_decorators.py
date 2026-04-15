import pytest
from kptn.graph.decorators import TaskSpec, task
from kptn.graph.nodes import TaskNode


def test_task_is_callable() -> None:
    """AC-1: decorated function remains callable."""

    @task(outputs=["duckdb://schema.table"])
    def my_fn() -> int:
        return 42

    assert callable(my_fn)


def test_task_kptn_attribute_attached() -> None:
    """AC-2: __kptn__ is a TaskSpec."""

    @task(outputs=["duckdb://schema.table"])
    def my_fn() -> None:
        pass

    assert hasattr(my_fn, "__kptn__")
    assert isinstance(my_fn.__kptn__, TaskSpec)


def test_task_spec_outputs() -> None:
    """TaskSpec.outputs holds the declared list."""

    @task(outputs=["duckdb://s.t", "duckdb://s.u"])
    def my_fn() -> None:
        pass

    assert my_fn.__kptn__.outputs == ["duckdb://s.t", "duckdb://s.u"]


def test_task_spec_optional_default_none() -> None:
    """optional defaults to None."""

    @task(outputs=["duckdb://s.t"])
    def my_fn() -> None:
        pass

    assert my_fn.__kptn__.optional is None


def test_task_spec_compute_default_none() -> None:
    """compute defaults to None."""

    @task(outputs=["duckdb://s.t"])
    def my_fn() -> None:
        pass

    assert my_fn.__kptn__.compute is None


def test_task_is_not_task_node() -> None:
    """AC-2: isinstance(fn, TaskNode) is False."""

    @task(outputs=["duckdb://s.t"])
    def my_fn() -> None:
        pass

    assert not isinstance(my_fn, TaskNode)


def test_task_return_value_unchanged() -> None:
    """AC-1: calling fn returns original return value."""

    @task(outputs=["duckdb://s.t"])
    def my_fn() -> int:
        return 99

    assert my_fn() == 99


def test_task_spec_optional_and_compute_set() -> None:
    """TaskSpec stores optional and compute when provided."""

    @task(outputs=["duckdb://s.t"], optional="skip", compute="large")
    def my_fn() -> None:
        pass

    assert my_fn.__kptn__.optional == "skip"
    assert my_fn.__kptn__.compute == "large"


# ─── _SqlTaskHandle.__call__ ──────────────────────────────────────────────────

import duckdb as _duckdb
from pathlib import Path as _Path
from kptn.graph.decorators import sql_task
from kptn.exceptions import TaskError


def test_sql_task_handle_call_executes_sql(tmp_path, monkeypatch) -> None:
    """Happy path: calling a handle with duckdb=conn executes the SQL file."""
    sql_file = tmp_path / "create_t.sql"
    sql_file.write_text("CREATE TABLE t (x INT); INSERT INTO t VALUES (1)")

    handle = sql_task(str(sql_file), outputs=[])
    conn = _duckdb.connect()
    monkeypatch.chdir(tmp_path)
    handle(duckdb=conn)

    result = conn.execute("SELECT x FROM t").fetchall()
    assert result == [(1,)]
    conn.close()


def test_sql_task_handle_call_raises_without_duckdb_kwarg(tmp_path) -> None:
    """Missing duckdb= kwarg raises TypeError with a helpful message."""
    sql_file = tmp_path / "noop.sql"
    sql_file.write_text("SELECT 1")
    handle = sql_task(str(sql_file), outputs=[])

    with pytest.raises(TypeError, match="duckdb="):
        handle()


def test_sql_task_handle_call_rejects_no_cache_kwarg(tmp_path) -> None:
    """Passing no_cache= explicitly raises TypeError."""
    sql_file = tmp_path / "noop.sql"
    sql_file.write_text("SELECT 1")
    handle = sql_task(str(sql_file), outputs=[])
    conn = _duckdb.connect()

    with pytest.raises(TypeError, match="no_cache"):
        handle(duckdb=conn, no_cache=True)

    conn.close()


def test_sql_task_handle_call_propagates_task_error(tmp_path, monkeypatch) -> None:
    """Bad SQL causes TaskError to propagate out of __call__."""
    sql_file = tmp_path / "bad.sql"
    sql_file.write_text("THIS IS NOT SQL")
    handle = sql_task(str(sql_file), outputs=[])
    conn = _duckdb.connect()
    monkeypatch.chdir(tmp_path)
    try:
        with pytest.raises(TaskError):
            handle(duckdb=conn)
    finally:
        conn.close()


def test_sql_task_handle_call_raises_with_duckdb_none(tmp_path) -> None:
    """duckdb=None raises TypeError with a message about None (not 'missing kwarg')."""
    sql_file = tmp_path / "noop.sql"
    sql_file.write_text("SELECT 1")
    handle = sql_task(str(sql_file), outputs=[])

    with pytest.raises(TypeError, match=r"duckdb=None"):
        handle(duckdb=None)


def test_sql_task_handle_call_rejects_unknown_kwargs(tmp_path) -> None:
    """Unknown kwargs beyond duckdb= raise TypeError immediately."""
    sql_file = tmp_path / "noop.sql"
    sql_file.write_text("SELECT 1")
    handle = sql_task(str(sql_file), outputs=[])
    conn = _duckdb.connect()

    with pytest.raises(TypeError, match="unexpected keyword"):
        handle(duckdb=conn, typo_kwarg=True)

    conn.close()


def test_sql_task_handle_call_executes_sql(tmp_path, monkeypatch) -> None:
    """Happy path: calling a handle with duckdb=conn executes the SQL file."""
    sql_file = tmp_path / "create_t.sql"
    sql_file.write_text("CREATE TABLE t (x INT); INSERT INTO t VALUES (1)")

    handle = sql_task(str(sql_file), outputs=[])
    conn = _duckdb.connect()
    monkeypatch.chdir(tmp_path)
    handle(duckdb=conn)

    result = conn.execute("SELECT x FROM t").fetchall()
    assert result == [(1,)]
    conn.close()


def test_sql_task_handle_call_raises_without_duckdb_kwarg(tmp_path) -> None:
    """Missing duckdb= kwarg raises TypeError with a helpful message."""
    sql_file = tmp_path / "noop.sql"
    sql_file.write_text("SELECT 1")
    handle = sql_task(str(sql_file), outputs=[])

    with pytest.raises(TypeError, match="duckdb="):
        handle()


def test_sql_task_handle_call_rejects_no_cache_kwarg(tmp_path) -> None:
    """Passing no_cache= explicitly raises TypeError."""
    sql_file = tmp_path / "noop.sql"
    sql_file.write_text("SELECT 1")
    handle = sql_task(str(sql_file), outputs=[])
    conn = _duckdb.connect()

    with pytest.raises(TypeError, match="no_cache"):
        handle(duckdb=conn, no_cache=True)

    conn.close()


def test_sql_task_handle_call_propagates_task_error(tmp_path, monkeypatch) -> None:
    """Bad SQL causes TaskError to propagate out of __call__."""
    sql_file = tmp_path / "bad.sql"
    sql_file.write_text("THIS IS NOT SQL")
    handle = sql_task(str(sql_file), outputs=[])
    conn = _duckdb.connect()
    monkeypatch.chdir(tmp_path)
    try:
        with pytest.raises(TaskError):
            handle(duckdb=conn)
    finally:
        conn.close()
