from __future__ import annotations

from pathlib import Path

import pytest

import kptn
from kptn.graph.graph import Graph
from kptn.graph.nodes import ConfigNode, SourceNode
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


def test_table_in_shared_bronze_is_planned_as_a_restore(tmp_path: Path) -> None:
    from kptn.sources.shared import SharedBronze

    work = tmp_path / "w"
    _write(work, "in/a.csv", "x\n1\n")
    node = SourceNode(spec=kptn.source("s", files=str(work / "in")).__kptn__, name="s")
    (tmp_path / "cache").mkdir()
    cache = SharedBronze(tmp_path / "cache")
    with pytest.warns(UserWarning):
        execute(
            _resolved(node), FakeStateStore(), cwd=work, shared=cache,
            emitter=EventEmitter("r", "default", None, _Sink()),
        )

    fresh = FakeStateStore()  # a working directory that never ran
    [entry] = build_plan(_resolved(node), fresh, config_values={}, root=work, shared=cache)
    assert (entry.action, entry.reason) == (PlanAction.RUN, "in shared bronze")
    [entry] = build_plan(_resolved(node), fresh, config_values={}, root=work)
    assert (entry.action, entry.reason) == (PlanAction.RUN, "inputs changed")


def test_rendered_plan_names_a_restore() -> None:
    import io

    from kptn.runner.plan import PlanEntry, render_plan

    out = io.StringIO()
    render_plan(
        [
            PlanEntry("s[a]", PlanAction.RUN, reason="in shared bronze"),
            PlanEntry("s[b]", PlanAction.RUN, reason="inputs changed"),
        ],
        stream=out,
    )
    assert out.getvalue().splitlines() == ["[RUN] s[a] — in shared bronze", "[RUN] s[b]"]


def _report() -> None:
    pass


def _downstream_of_source(tmp_path: Path, store: FakeStateStore) -> ResolvedGraph:
    """``upstream >> s >> report >> summary``, with s built and the three
    tasks cached."""
    from kptn.change_detector.hasher import hash_task_source
    from kptn.graph.decorators import TaskSpec
    from kptn.graph.nodes import TaskNode

    _write(tmp_path, "in/a.csv", "x\n1\n")
    handle = kptn.source("s", files=str(tmp_path / "in"))
    source = SourceNode(spec=handle.__kptn__, name="s")
    upstream = TaskNode(fn=_report, spec=TaskSpec(outputs=[]), name="upstream")
    report = TaskNode(fn=_report, spec=TaskSpec(outputs=[]), name="report")
    summary = TaskNode(fn=_report, spec=TaskSpec(outputs=[]), name="summary")
    with pytest.warns(UserWarning):
        execute(_resolved(source), store, cwd=tmp_path, emitter=EventEmitter("r", "default", None, _Sink()))
    for name in ("upstream", "report", "summary"):
        store.write_hash("kptn", "default", name, hash_task_source(_report))
    graph = Graph(
        nodes=[upstream, source, report, summary],
        edges=[(upstream, source), (source, report), (report, summary)],
    )
    return ResolvedGraph(graph=graph, pipeline="default", storage_key="kptn")


def test_a_table_to_build_makes_downstream_run(tmp_path: Path) -> None:
    store = FakeStateStore()
    resolved = _downstream_of_source(tmp_path, store)
    for parquet in (tmp_path / ".kptn" / "_bronze").rglob("*.parquet"):
        parquet.unlink()

    entries = build_plan(resolved, store, config_values={}, root=tmp_path)
    assert [(e.task_name, e.action, e.reason) for e in entries] == [
        ("upstream", PlanAction.SKIP, "cached"),
        ("s[a]", PlanAction.RUN, "inputs changed"),
        ("report", PlanAction.RUN, "upstream changed"),
        ("summary", PlanAction.RUN, "upstream changed"),
    ]


def test_cached_source_leaves_downstream_cached(tmp_path: Path) -> None:
    store = FakeStateStore()
    resolved = _downstream_of_source(tmp_path, store)

    entries = build_plan(resolved, store, config_values={}, root=tmp_path)
    assert [(e.task_name, e.action) for e in entries] == [
        ("upstream", PlanAction.SKIP),
        ("s[a]", PlanAction.SKIP),
        ("report", PlanAction.SKIP),
        ("summary", PlanAction.SKIP),
    ]


def test_upstream_change_passes_through_a_cached_source(tmp_path: Path) -> None:
    store = FakeStateStore()
    resolved = _downstream_of_source(tmp_path, store)
    store.write_hash("kptn", "default", "upstream", "stale")

    entries = build_plan(resolved, store, config_values={}, root=tmp_path)
    assert [(e.task_name, e.action) for e in entries] == [
        ("upstream", PlanAction.RUN),
        ("s[a]", PlanAction.SKIP),
        ("report", PlanAction.RUN),
        ("summary", PlanAction.RUN),
    ]
