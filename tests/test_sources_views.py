from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

import kptn
from kptn.exceptions import TaskError
from kptn.graph.decorators import TaskSpec
from kptn.graph.graph import Graph
from kptn.graph.nodes import SourceViewsNode, TaskNode
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
    created = register_views(conn, root / "_bronze", ("sales", "survey", "absent"))
    assert created == ['raw."sales_orders"', 'raw."survey_codes"']
    conn.close()

    moved = tmp_path / "branch_b"
    shutil.move(root, moved)
    conn = duckdb.connect(str(moved / "pipeline.db"))
    apply_search_path(conn, moved / "_bronze")
    assert conn.sql('SELECT v FROM raw."sales_orders"').fetchall() == [("1",)]


def _views_node() -> SourceViewsNode:
    a = kptn.source("sales", files="in")
    handle = kptn.source_views(kptn.Stage("raw_ingest", a))
    return SourceViewsNode(spec=handle.__kptn__, name=handle.__name__)


def test_executor_registers_views_and_does_not_dirty_downstream(tmp_path: Path) -> None:
    _parquet(tmp_path, "sales", "orders", 1)
    db = str(tmp_path / "pipeline.db")
    views = _views_node()
    fn = MagicMock(return_value=None)
    fn.__name__ = "consumer"
    consumer = TaskNode(fn=fn, spec=TaskSpec(outputs=[]), name="consumer")
    graph = Graph(nodes=[views, consumer], edges=[(views, consumer)])
    resolved = ResolvedGraph(graph=graph, pipeline="default", storage_key="kptn")
    sink_events = []

    class Sink:
        def emit(self, event):
            sink_events.append(event)

    from unittest.mock import patch

    with patch("kptn.runner.executor.is_stale", return_value=(False, "cached")):
        execute(
            resolved,
            FakeStateStore(),
            cwd=tmp_path,
            duckdb_factory=lambda: duckdb.connect(db),
            emitter=EventEmitter("run-1", "default", None, Sink()),
        )
    names_started = [e.task_name for e in sink_events if e.kind is EventKind.TASK_STARTED]
    names_skipped = [e.task_name for e in sink_events if e.kind is EventKind.TASK_SKIPPED]
    assert "raw_ingest_views" in names_started
    assert "consumer" in names_skipped
    conn = duckdb.connect(db)
    apply_search_path(conn, tmp_path / "_bronze")
    assert conn.sql('SELECT v FROM raw."sales_orders"').fetchall() == [("1",)]


def test_executor_views_need_a_duckdb_factory(tmp_path: Path) -> None:
    graph = Graph(nodes=[_views_node()], edges=[])
    resolved = ResolvedGraph(graph=graph, pipeline="default", storage_key="kptn")
    with pytest.raises(TaskError, match="kptn.config"):
        execute(resolved, FakeStateStore(), cwd=tmp_path)


def test_preview_connection_gets_search_path_when_bronze_exists(tmp_path: Path) -> None:
    conn = duckdb.connect()
    _apply_bronze_search_path(conn, tmp_path)
    assert _search_path(conn) == ""
    (tmp_path / "_bronze").mkdir()
    _apply_bronze_search_path(conn, tmp_path)
    assert str((tmp_path / "_bronze").resolve()) in _search_path(conn)
