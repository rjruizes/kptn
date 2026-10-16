from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import kptn
from kptn.exceptions import TaskError
from kptn.graph.decorators import TaskSpec
from kptn.graph.graph import Graph
from kptn.graph.nodes import ConfigNode, SourceNode, TaskNode
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.events import EventEmitter, EventKind
from kptn.runner.executor import execute
from kptn.sources.build import plan_tables
from kptn.sources.shared import SharedBronze
from tests.fakes import FakeStateStore


class RecordingSink:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


def _write(root: Path, rel: str, text: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def _source_node(root: Path, name: str = "s") -> SourceNode:
    handle = kptn.source(name, files=str(root / "in"))
    return SourceNode(spec=handle.__kptn__, name=name)


def _task_node(name: str) -> TaskNode:
    fn = MagicMock(return_value=None)
    fn.__name__ = name
    return TaskNode(fn=fn, spec=TaskSpec(outputs=[]), name=name)


def _run(graph: Graph, store: FakeStateStore, root: Path, **kwargs) -> RecordingSink:
    sink = RecordingSink()
    resolved = ResolvedGraph(graph=graph, pipeline="default", storage_key="kptn")
    with pytest.warns(UserWarning) if kwargs.pop("expect_warning", True) else _null():
        execute(resolved, store, cwd=root, emitter=EventEmitter("run-1", "default", None, sink), **kwargs)
    return sink


class _null:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _names(sink: RecordingSink, kind: EventKind) -> list[str]:
    return [e.task_name for e in sink.events if e.kind is kind]


@pytest.fixture
def project(tmp_path: Path) -> Path:
    _write(tmp_path, "in/a.csv", "x\n1\n")
    _write(tmp_path, "in/b.csv", "y\n2\n")
    return tmp_path


def test_first_run_builds_every_table(project: Path) -> None:
    store = FakeStateStore()
    sink = _run(Graph(nodes=[_source_node(project)], edges=[]), store, project)
    assert (project / "_bronze/s/a.parquet").exists()
    assert (project / "_bronze/s/b.parquet").exists()
    assert sorted(store.list_tasks("kptn", "default")) == ["s[a]", "s[b]"]
    started = [e for e in sink.events if e.kind is EventKind.TASK_STARTED]
    assert started[0].task_name == "s" and started[0].payload["mode"] == "source"
    assert started[0].payload["count"] == 2
    finished = [e for e in sink.events if e.kind is EventKind.TASK_FINISHED and e.task_name == "s"]
    assert finished[0].payload["cached"] is False


def test_second_run_skips_every_table(project: Path) -> None:
    store = FakeStateStore()
    node = _source_node(project)
    _run(Graph(nodes=[node], edges=[]), store, project)
    sink = _run(Graph(nodes=[node], edges=[]), store, project, expect_warning=False)
    assert _names(sink, EventKind.TASK_SKIPPED) == ["s[a]", "s[b]"]
    finished = [e for e in sink.events if e.kind is EventKind.TASK_FINISHED and e.task_name == "s"]
    assert finished[0].payload["cached"] is True


def test_changed_file_rebuilds_only_its_table(project: Path) -> None:
    store = FakeStateStore()
    node = _source_node(project)
    _run(Graph(nodes=[node], edges=[]), store, project)
    _write(project, "in/b.csv", "y\n33\n")
    sink = _run(Graph(nodes=[node], edges=[]), store, project)
    assert _names(sink, EventKind.TASK_SKIPPED) == ["s[a]"]
    assert "s[b]" in _names(sink, EventKind.TASK_STARTED)


def test_upstream_dirtiness_does_not_rebuild_a_source(project: Path) -> None:
    store = FakeStateStore()
    upstream = _task_node("init_database")
    node = _source_node(project)
    graph = Graph(nodes=[upstream, node], edges=[(upstream, node)])
    _run(graph, store, project)
    with patch("kptn.runner.executor.is_stale", return_value=(True, "code changed")):
        sink = _run(graph, store, project, expect_warning=False)
    assert _names(sink, EventKind.TASK_SKIPPED) == ["s[a]", "s[b]"]


def test_downstream_reruns_only_when_a_table_changes(project: Path) -> None:
    store = FakeStateStore()
    node = _source_node(project)
    downstream = _task_node("consumer")
    graph = Graph(nodes=[node, downstream], edges=[(node, downstream)])
    _run(graph, store, project)
    with patch("kptn.runner.executor.is_stale", return_value=(False, "cached")):
        sink = _run(graph, store, project, expect_warning=False)
        assert "consumer" in _names(sink, EventKind.TASK_SKIPPED)
        _write(project, "in/a.csv", "x\n99\n")
        sink = _run(graph, store, project)
        assert "consumer" in _names(sink, EventKind.TASK_STARTED)


def test_force_rebuilds_and_no_cache_writes_no_state(project: Path) -> None:
    store = FakeStateStore()
    node = _source_node(project)
    _run(Graph(nodes=[node], edges=[]), store, project)
    sink = _run(Graph(nodes=[node], edges=[]), store, project, force=True)
    assert _names(sink, EventKind.TASK_SKIPPED) == []
    fresh = FakeStateStore()
    _run(Graph(nodes=[node], edges=[]), fresh, project, no_cache=True)
    assert fresh.list_tasks("kptn", "default") == []


def test_removed_table_parquet_is_deleted(project: Path) -> None:
    store = FakeStateStore()
    node = _source_node(project)
    _run(Graph(nodes=[node], edges=[]), store, project)
    (project / "in/b.csv").unlink()
    _run(Graph(nodes=[node], edges=[]), store, project, expect_warning=False)
    assert not (project / "_bronze/s/b.parquet").exists()


def test_failed_table_raises_and_records_nothing(project: Path) -> None:
    store = FakeStateStore()
    node = _source_node(project)
    with patch("kptn.sources.build.run_checks", side_effect=TaskError("bad")):
        with pytest.raises(TaskError, match="bad"):
            _run(Graph(nodes=[node], edges=[]), store, project, expect_warning=False)
    assert store.list_tasks("kptn", "default") == []


def test_missing_files_raise(tmp_path: Path) -> None:
    node = SourceNode(spec=kptn.source("s", files=str(tmp_path / "nope")).__kptn__, name="s")
    with pytest.raises(TaskError, match="matched no files"):
        _run(Graph(nodes=[node], edges=[]), FakeStateStore(), tmp_path, expect_warning=False)


def test_files_from_config(project: Path) -> None:
    conf = ConfigNode(spec={"config": lambda: {"s": {"in": str(project / "in")}}})
    handle = kptn.source("s", files=lambda config: config["s"]["in"])
    node = SourceNode(spec=handle.__kptn__, name="s")
    _run(Graph(nodes=[conf, node], edges=[(conf, node)]), FakeStateStore(), project)
    assert (project / "_bronze/s/a.parquet").exists()


# -- the shared bronze --------------------------------------------------------


def _finished(sink: RecordingSink, name: str):
    [event] = [e for e in sink.events if e.kind is EventKind.TASK_FINISHED and e.task_name == name]
    return event


def _graph(node: SourceNode) -> Graph:
    return Graph(nodes=[node], edges=[])


def test_miss_publishes_and_links(project: Path) -> None:
    (project / "cache").mkdir()
    cache = SharedBronze(project / "cache")
    sink = _run(_graph(_source_node(project)), FakeStateStore(), project, shared=cache)
    for table in ("a", "b"):
        parquet = project / "_bronze" / "s" / f"{table}.parquet"
        assert parquet.is_symlink() and parquet.resolve().is_relative_to(cache.root.resolve())
        assert _finished(sink, f"s[{table}]").payload["shared"] == "published"


def test_hit_restores_without_building(project: Path) -> None:
    (project / "cache").mkdir()
    cache = SharedBronze(project / "cache")
    node = _source_node(project)
    _run(_graph(node), FakeStateStore(), project, shared=cache)
    shutil.rmtree(project / "_bronze")  # another working directory, in effect
    with (
        patch("kptn.sources.shared.build_table") as published,
        patch("kptn.runner.executor.build_table") as local,
    ):
        sink = _run(_graph(node), FakeStateStore(), project, shared=cache, expect_warning=False)
    published.assert_not_called()
    local.assert_not_called()
    [started] = [e for e in sink.events if e.kind is EventKind.TASK_STARTED and e.task_name == "s[a]"]
    assert started.payload["shared"] == "restored"
    assert _finished(sink, "s[a]").payload["shared"] == "restored"
    assert _finished(sink, "s").payload["cached"] is False  # a restore is a change
    assert (project / "_bronze" / "s" / "a.parquet").is_symlink()


def test_restored_table_is_current_next_run(project: Path) -> None:
    (project / "cache").mkdir()
    cache = SharedBronze(project / "cache")
    node = _source_node(project)
    _run(_graph(node), FakeStateStore(), project, shared=cache)
    shutil.rmtree(project / "_bronze")
    store = FakeStateStore()
    _run(_graph(node), store, project, shared=cache, expect_warning=False)
    sink = _run(_graph(node), store, project, shared=cache, expect_warning=False)
    assert _names(sink, EventKind.TASK_SKIPPED) == ["s[a]", "s[b]"]


@pytest.mark.parametrize("flag", ["force", "no_cache"])
def test_force_and_no_cache_build_locally(project: Path, flag: str) -> None:
    (project / "cache").mkdir()
    cache = SharedBronze(project / "cache")
    node = _source_node(project)
    _run(_graph(node), FakeStateStore(), project, shared=cache)
    [plan_a, _] = plan_tables("s", node.spec, {}, root=project)
    entry = cache.lookup(plan_a)
    assert entry is not None
    before = entry.stat().st_mtime_ns
    sink = _run(_graph(node), FakeStateStore(), project, shared=cache, **{flag: True})
    parquet = project / "_bronze" / "s" / "a.parquet"
    assert parquet.is_file() and not parquet.is_symlink()
    assert entry.stat().st_mtime_ns == before  # the entry is untouched
    assert "shared" not in _finished(sink, "s[a]").payload


def test_dangling_link_rebuilds_and_republishes(project: Path) -> None:
    (project / "cache").mkdir()
    cache = SharedBronze(project / "cache")
    node = _source_node(project)
    store = FakeStateStore()
    _run(_graph(node), store, project, shared=cache)
    shutil.rmtree(cache.root / "s" / "a")  # someone deleted a wrong entry
    sink = _run(_graph(node), store, project, shared=cache)
    assert _finished(sink, "s[a]").payload["shared"] == "published"
    assert _names(sink, EventKind.TASK_SKIPPED) == ["s[b]"]
    assert (project / "_bronze" / "s" / "a.parquet").exists()


def test_failed_publish_raises_and_records_nothing(project: Path) -> None:
    (project / "cache").mkdir()
    cache = SharedBronze(project / "cache")
    store = FakeStateStore()
    with patch("kptn.sources.shared.build_table", side_effect=RuntimeError("check failed")):
        with pytest.raises(TaskError, match="check failed"):
            _run(_graph(_source_node(project)), store, project, shared=cache, expect_warning=False)
    assert store.read_hash("kptn", "default", "s[a]") is None
    assert not (project / "_bronze" / "s" / "a.parquet").exists()


def test_without_a_shared_bronze_tables_are_local_files(project: Path) -> None:
    sink = _run(_graph(_source_node(project)), FakeStateStore(), project)
    assert not (project / "_bronze" / "s" / "a.parquet").is_symlink()
    assert "shared" not in _finished(sink, "s[a]").payload
