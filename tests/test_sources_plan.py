from __future__ import annotations

from pathlib import Path

import pytest

import kptn
from kptn.graph.graph import Graph
from kptn.graph.nodes import ConfigNode, SourceNode, SourceViewsNode
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.events import EventEmitter
from kptn.runner.executor import execute
from kptn.runner.plan import PlanAction, build_plan, config_values_for_plan
from tests.fakes import FakeStateStore


class _Sink:
    def emit(self, event) -> None: ...


def _write(root: Path, rel: str, text: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def _resolved(*nodes) -> ResolvedGraph:
    return ResolvedGraph(graph=Graph(nodes=list(nodes), edges=[]), pipeline="default", storage_key="kptn")


def test_source_plans_per_table(tmp_path: Path) -> None:
    _write(tmp_path, "in/a.csv", "x\n1\n")
    _write(tmp_path, "in/b.csv", "y\n2\n")
    node = SourceNode(spec=kptn.source("s", files=str(tmp_path / "in")).__kptn__, name="s")
    store = FakeStateStore()
    entries = build_plan(_resolved(node), store, config_values={}, root=tmp_path)
    assert [(e.task_name, e.action) for e in entries] == [("s[a]", PlanAction.RUN), ("s[b]", PlanAction.RUN)]

    with pytest.warns(UserWarning):
        execute(_resolved(node), store, cwd=tmp_path, emitter=EventEmitter("r", "default", None, _Sink()))
    _write(tmp_path, "in/b.csv", "y\n33\n")
    entries = build_plan(_resolved(node), store, config_values={}, root=tmp_path)
    assert [(e.task_name, e.action) for e in entries] == [("s[a]", PlanAction.SKIP), ("s[b]", PlanAction.RUN)]


def test_config_derived_files_without_config_values(tmp_path: Path) -> None:
    node = SourceNode(spec=kptn.source("s", files=lambda config: config["in"]).__kptn__, name="s")
    [entry] = build_plan(_resolved(node), FakeStateStore(), root=tmp_path)
    assert (entry.task_name, entry.action, entry.reason) == ("s", PlanAction.RUN, "files depend on config")


def test_resolution_error_is_a_run_entry(tmp_path: Path) -> None:
    node = SourceNode(spec=kptn.source("s", files=str(tmp_path / "missing")).__kptn__, name="s")
    [entry] = build_plan(_resolved(node), FakeStateStore(), config_values={}, root=tmp_path)
    assert entry.action is PlanAction.RUN and "matched no files" in entry.reason


def test_views_always_run() -> None:
    a = kptn.source("a", files="in")
    handle = kptn.source_views(kptn.Stage("bronze", a))
    node = SourceViewsNode(spec=handle.__kptn__, name=handle.__name__)
    [entry] = build_plan(_resolved(node), FakeStateStore())
    assert (entry.action, entry.reason) == (PlanAction.RUN, "always runs")


def test_config_values_for_plan_skips_duckdb() -> None:
    opened = []
    conf = ConfigNode(spec={"duckdb": (lambda: opened.append(1), "engine"), "config": lambda: {"k": 1}})  # ty: ignore[invalid-argument-type]
    assert config_values_for_plan(Graph(nodes=[conf], edges=[])) == {"config": {"k": 1}}
    assert opened == []


def test_config_values_for_plan_failure_is_none(capsys) -> None:
    def broken():
        raise RuntimeError("no config.json")

    conf = ConfigNode(spec={"config": broken})
    assert config_values_for_plan(Graph(nodes=[conf], edges=[])) is None
    assert "no config.json" in capsys.readouterr().err


def test_no_config_node_gives_empty_values() -> None:
    assert config_values_for_plan(Graph(nodes=[], edges=[])) == {}


def test_table_in_shared_cache_is_planned_as_a_restore(tmp_path: Path) -> None:
    from kptn.sources.shared import SharedCache

    work = tmp_path / "w"
    _write(work, "in/a.csv", "x\n1\n")
    node = SourceNode(spec=kptn.source("s", files=str(work / "in")).__kptn__, name="s")
    cache = SharedCache(tmp_path / "cache")
    with pytest.warns(UserWarning):
        execute(
            _resolved(node), FakeStateStore(), cwd=work, shared=cache,
            emitter=EventEmitter("r", "default", None, _Sink()),
        )

    fresh = FakeStateStore()  # a working directory that never ran
    [entry] = build_plan(_resolved(node), fresh, config_values={}, root=work, shared=cache)
    assert (entry.action, entry.reason) == (PlanAction.RUN, "in shared cache")
    [entry] = build_plan(_resolved(node), fresh, config_values={}, root=work)
    assert (entry.action, entry.reason) == (PlanAction.RUN, "inputs changed")
