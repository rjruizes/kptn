from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

import kptn
from kptn.graph.decorators import TaskSpec
from kptn.graph.graph import Graph
from kptn.graph.nodes import SourceNode, TaskNode
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.events import EventEmitter, EventKind
from kptn.runner.executor import execute
from kptn.sources.views import apply_search_path, register_views, with_search_path
from kptn_server.service import _apply_bronze_search_path
from tests.fakes import FakeStateStore


def _parquet(root: Path, source: str, table: str, value: int) -> None:
    p = root / "_bronze" / source / f"{table}.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY (SELECT '{value}' AS v) TO '{p}' (FORMAT parquet)")


def _search_path(conn) -> str:
    return conn.execute("SELECT current_setting('file_search_path')").fetchone()[0]


def test_apply_search_path_appends_once(tmp_path: Path) -> None:
    conn = duckdb.connect()
    conn.execute(f"SET file_search_path = '{tmp_path / 'other'}'")
    apply_search_path(conn, tmp_path / "_bronze")
    apply_search_path(conn, tmp_path / "_bronze")
    assert _search_path(conn).split(",") == [str(tmp_path / "other"), str((tmp_path / "_bronze").resolve())]


def test_with_search_path_wraps_every_connection(tmp_path: Path) -> None:
    factory = with_search_path(duckdb.connect, tmp_path / "_bronze")
    assert str((tmp_path / "_bronze").resolve()) in _search_path(factory())
    assert str((tmp_path / "_bronze").resolve()) in _search_path(factory())


def test_register_views_uses_relative_paths_that_survive_a_move(tmp_path: Path) -> None:
    root = tmp_path / "branch_a"
    _parquet(root, "sales", "orders", 1)
    _parquet(root, "survey", "codes", 2)
    db = root / "pipeline.db"
    conn = duckdb.connect(str(db))
    apply_search_path(conn, root / "_bronze")
    created = register_views(conn, root / "_bronze", "sales")
    assert created == ['raw_sales."orders"']
    assert register_views(conn, root / "_bronze", "absent") == []
    conn.close()

    moved = tmp_path / "branch_b"
    shutil.move(root, moved)
    conn = duckdb.connect(str(moved / "pipeline.db"))
    apply_search_path(conn, moved / "_bronze")
    assert conn.sql('SELECT v FROM raw_sales."orders"').fetchall() == [("1",)]


def _views(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return conn.sql(
        "SELECT schema_name, view_name FROM duckdb_views() WHERE NOT internal ORDER BY ALL"
    ).fetchall()


def test_register_views_drops_views_of_tables_gone_since_any_run(tmp_path: Path) -> None:
    """Cleanup reads the schema, not what this run deleted: a table whose
    parquet went away in a run that failed, or had no connection, still
    loses its view."""
    _parquet(tmp_path, "sales", "orders", 1)
    _parquet(tmp_path, "sales", "returns", 2)
    conn = duckdb.connect()
    apply_search_path(conn, tmp_path / "_bronze")
    register_views(conn, tmp_path / "_bronze", "sales")
    (tmp_path / "_bronze" / "sales" / "returns.parquet").unlink()
    assert register_views(conn, tmp_path / "_bronze", "sales") == ['raw_sales."orders"']
    assert _views(conn) == [("raw_sales", "orders")]


def test_a_source_with_no_tables_left_has_no_views(tmp_path: Path) -> None:
    _parquet(tmp_path, "sales", "orders", 1)
    conn = duckdb.connect()
    apply_search_path(conn, tmp_path / "_bronze")
    register_views(conn, tmp_path / "_bronze", "sales")
    shutil.rmtree(tmp_path / "_bronze" / "sales")
    assert register_views(conn, tmp_path / "_bronze", "sales") == []
    assert _views(conn) == []


def test_sources_whose_names_overlap_keep_their_own_views(tmp_path: Path) -> None:
    """lab's results_blood and lab_results's blood once both mapped to
    raw.lab_results_blood; in their own schemas they never meet."""
    _parquet(tmp_path, "lab", "results_blood", 1)
    _parquet(tmp_path, "lab_results", "blood", 2)
    conn = duckdb.connect()
    apply_search_path(conn, tmp_path / "_bronze")
    register_views(conn, tmp_path / "_bronze", "lab")
    register_views(conn, tmp_path / "_bronze", "lab_results")
    shutil.rmtree(tmp_path / "_bronze" / "lab")
    register_views(conn, tmp_path / "_bronze", "lab")
    assert _views(conn) == [("raw_lab_results", "blood")]
    assert conn.sql('SELECT v FROM raw_lab_results."blood"').fetchall() == [("2",)]


def test_cleanup_leaves_other_objects_in_the_schema(tmp_path: Path) -> None:
    _parquet(tmp_path, "sales", "orders", 1)
    conn = duckdb.connect()
    apply_search_path(conn, tmp_path / "_bronze")
    register_views(conn, tmp_path / "_bronze", "sales")
    conn.execute("CREATE TABLE raw_sales.notes AS SELECT 1 AS n")
    register_views(conn, tmp_path / "_bronze", "sales")
    assert conn.sql("SELECT n FROM raw_sales.notes").fetchall() == [(1,)]


def _source_then_consumer(tmp_path: Path) -> tuple[ResolvedGraph, MagicMock]:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("v\n1\n")
    handle = kptn.source("sales", files=str(tmp_path / "in"))
    source = SourceNode(spec=handle.__kptn__, name=handle.__name__)
    fn = MagicMock(return_value=None)
    fn.__name__ = "consumer"
    consumer = TaskNode(fn=fn, spec=TaskSpec(outputs=[]), name="consumer")
    graph = Graph(nodes=[source, consumer], edges=[(source, consumer)])
    return ResolvedGraph(graph=graph, pipeline="default", storage_key="kptn"), fn


def _run(resolved: ResolvedGraph, store: FakeStateStore, root: Path, factory=None) -> list:
    events = []

    class Sink:
        def emit(self, event):
            events.append(event)

    with patch("kptn.runner.executor.is_stale", return_value=(False, "cached")):
        execute(
            resolved,
            store,
            cwd=root,
            duckdb_factory=factory,
            emitter=EventEmitter("run-1", "default", None, Sink()),
        )
    return events


def test_source_serves_its_tables_as_views_even_when_cached(tmp_path: Path) -> None:
    db = str(tmp_path / "pipeline.db")
    resolved, _ = _source_then_consumer(tmp_path)
    store = FakeStateStore()
    with pytest.warns(UserWarning):  # no check file
        _run(resolved, store, tmp_path, lambda: duckdb.connect(db))
    (tmp_path / "pipeline.db").unlink()  # as a schema rebuild would

    events = _run(resolved, store, tmp_path, lambda: duckdb.connect(db))
    skipped = [e.task_name for e in events if e.kind is EventKind.TASK_SKIPPED]
    assert {"sales[orders]", "consumer"} <= set(skipped)
    conn = duckdb.connect(db)
    apply_search_path(conn, tmp_path / "_bronze")
    assert conn.sql('SELECT v FROM raw_sales."orders"').fetchall() == [("1",)]


def test_source_without_a_duckdb_connection_builds_bronze_only(tmp_path: Path) -> None:
    resolved, fn = _source_then_consumer(tmp_path)
    with pytest.warns(UserWarning):  # no check file
        _run(resolved, FakeStateStore(), tmp_path)
    assert (tmp_path / "_bronze" / "sales" / "orders.parquet").exists()
    fn.assert_called_once()


def test_preview_connection_gets_search_path_when_bronze_exists(tmp_path: Path) -> None:
    conn = duckdb.connect()
    _apply_bronze_search_path(conn, tmp_path)
    assert _search_path(conn) == ""
    (tmp_path / "_bronze").mkdir()
    _apply_bronze_search_path(conn, tmp_path)
    assert str((tmp_path / "_bronze").resolve()) in _search_path(conn)
