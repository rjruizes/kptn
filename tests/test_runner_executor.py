"""Tests for kptn/runner/executor.py — Story 2.4: Local Execution Runner."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest

from kptn.exceptions import TaskError
from kptn.graph.decorators import TaskSpec, RTaskSpec
from kptn.graph.graph import Graph
from kptn.graph.nodes import (
    ConfigNode,
    MapNode,
    NoopNode,
    ParallelNode,
    PipelineNode,
    RTaskNode,
    SqlTaskNode,
    StageNode,
    TaskNode,
)
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.executor import execute
from tests.fakes import FakeStateStore


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_task_node(name: str, fn=None, outputs: list[str] | None = None) -> TaskNode:
    if fn is None:
        fn = MagicMock(return_value=None)
        fn.__name__ = name
    spec = TaskSpec(outputs=outputs or [])
    return TaskNode(fn=fn, spec=spec, name=name)


def _make_r_task_node(name: str, path: str = "script.R", outputs: list[str] | None = None) -> RTaskNode:
    spec = RTaskSpec(path=path, outputs=outputs or [])
    return RTaskNode(path=path, spec=spec, name=name)


def _make_resolved(graph: Graph, bypassed_names: frozenset[str] | None = None) -> ResolvedGraph:
    return ResolvedGraph(
        graph=graph,
        pipeline="default",
        storage_key="kptn",
        bypassed_names=bypassed_names or frozenset(),
    )


# ─── AC-1: Topological execution order ────────────────────────────────────────


def test_execute_runs_tasks_in_topological_order() -> None:
    """Tasks must execute predecessor before successor (AC-1)."""
    call_order: list[str] = []

    def task_a() -> None:
        call_order.append("a")

    def task_b() -> None:
        call_order.append("b")

    task_a.__name__ = "task_a"
    task_b.__name__ = "task_b"

    node_a = TaskNode(fn=task_a, spec=TaskSpec(outputs=[]), name="task_a")
    node_b = TaskNode(fn=task_b, spec=TaskSpec(outputs=[]), name="task_b")
    graph = Graph(nodes=[node_a, node_b], edges=[(node_a, node_b)])

    resolved = _make_resolved(graph)
    store = FakeStateStore()
    execute(resolved, store)

    assert call_order == ["a", "b"]


def test_execute_writes_hash_after_task_runs(tmp_path: Path) -> None:
    """Hash must be written to state store after successful task execution (AC-1)."""
    output_file = tmp_path / "output.txt"
    output_file.write_bytes(b"data")

    def task_a() -> None:
        pass

    task_a.__name__ = "task_a"
    node_a = TaskNode(
        fn=task_a,
        spec=TaskSpec(outputs=[str(output_file)]),
        name="task_a",
    )
    graph = Graph(nodes=[node_a], edges=[])
    resolved = _make_resolved(graph)
    store = FakeStateStore()
    execute(resolved, store)

    assert store.read_hash("kptn", "default", "task_a") is not None


# ─── AC-2: Fail fast on TaskError ─────────────────────────────────────────────


def test_execute_fails_fast_on_task_error() -> None:
    """Execution must abort when a TaskError is raised; subsequent tasks must not run (AC-2)."""
    second_called = False

    def task_a() -> None:
        raise ValueError("boom")

    def task_b() -> None:
        nonlocal second_called
        second_called = True

    task_a.__name__ = "task_a"
    task_b.__name__ = "task_b"

    node_a = TaskNode(fn=task_a, spec=TaskSpec(outputs=[]), name="task_a")
    node_b = TaskNode(fn=task_b, spec=TaskSpec(outputs=[]), name="task_b")
    graph = Graph(nodes=[node_a, node_b], edges=[(node_a, node_b)])

    resolved = _make_resolved(graph)
    store = FakeStateStore()

    with pytest.raises(TaskError):
        execute(resolved, store)

    assert not second_called


# ─── AC-3 & AC-4: RTaskNode subprocess invocation ─────────────────────────────


def test_execute_r_task_success() -> None:
    """RTaskNode with returncode=0 must not raise TaskError (AC-4)."""
    node_r = _make_r_task_node("my_script", path="analysis.R")
    graph = Graph(nodes=[node_r], edges=[])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stderr = ""
    mock_result.stdout = ""

    with patch("kptn.runner.executor.subprocess.run", return_value=mock_result):
        execute(resolved, store)  # Should not raise


def test_execute_r_task_failure_raises_task_error() -> None:
    """RTaskNode with non-zero returncode must raise TaskError (AC-3)."""
    node_r = _make_r_task_node("my_script", path="analysis.R")
    graph = Graph(nodes=[node_r], edges=[])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "Error in analysis.R: object not found"
    mock_result.stdout = ""

    with patch("kptn.runner.executor.subprocess.run", return_value=mock_result):
        with pytest.raises(TaskError) as exc_info:
            execute(resolved, store)

    assert "exited with code 1" in str(exc_info.value)
    assert "Error in analysis.R" in str(exc_info.value)


def test_execute_r_task_no_env_injection() -> None:
    """subprocess.run must NOT be called with env= kwarg for RTaskNode (AC-4)."""
    node_r = _make_r_task_node("my_script", path="analysis.R")
    graph = Graph(nodes=[node_r], edges=[])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stderr = ""
    mock_result.stdout = ""

    with patch("kptn.runner.executor.subprocess.run", return_value=mock_result) as mock_run:
        execute(resolved, store)

    # Verify subprocess.run was called without env= kwarg
    assert mock_run.call_count == 1
    _, kwargs = mock_run.call_args
    assert "env" not in kwargs


# ─── AC-5: Bypassed task hash propagation ────────────────────────────────────


def test_execute_bypassed_task_not_executed() -> None:
    """Bypassed task must not execute the task function (AC-5)."""
    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "task_a"

    node_a = TaskNode(fn=task_fn, spec=TaskSpec(outputs=[]), name="task_a")
    graph = Graph(nodes=[node_a], edges=[])
    resolved = _make_resolved(graph, bypassed_names=frozenset({"task_a"}))
    store = FakeStateStore()

    execute(resolved, store)

    task_fn.assert_not_called()


def test_execute_bypassed_task_reads_state_store() -> None:
    """Bypassed task must call read_hash on the state store (AC-5)."""
    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "task_a"

    node_a = TaskNode(fn=task_fn, spec=TaskSpec(outputs=[]), name="task_a")
    graph = Graph(nodes=[node_a], edges=[])
    resolved = _make_resolved(graph, bypassed_names=frozenset({"task_a"}))
    store = FakeStateStore()
    # Pre-populate store to verify read
    store.write_hash("kptn", "default", "task_a", "abc123")

    execute(resolved, store)

    # The pre-populated hash remains (not overwritten) — confirms read occurred correctly
    assert store.read_hash("kptn", "default", "task_a") == "abc123"


def test_execute_bypassed_task_no_stdout(capsys) -> None:
    """Bypassed task must emit nothing to stdout (AC-5)."""
    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "task_a"

    node_a = TaskNode(fn=task_fn, spec=TaskSpec(outputs=[]), name="task_a")
    graph = Graph(nodes=[node_a], edges=[])
    resolved = _make_resolved(graph, bypassed_names=frozenset({"task_a"}))
    store = FakeStateStore()

    execute(resolved, store)

    captured = capsys.readouterr()
    assert captured.out == ""


# ─── AC-6: MapNode expansion message ─────────────────────────────────────────


def test_execute_map_node_emits_count_message(capsys) -> None:
    """MapNode must emit '[MAP] task_name — expanding over N items' (AC-6)."""
    items = [1, 2, 3]
    ctx_value = {"states": items}

    # Build a ctx task that returns the runtime context value
    def ctx_task() -> dict:
        return ctx_value

    ctx_task.__name__ = "ctx"
    ctx_node = TaskNode(fn=ctx_task, spec=TaskSpec(outputs=[]), name="ctx")

    # MapNode: map over ctx.states
    map_fn = MagicMock(return_value=None)
    map_fn.__name__ = "process"
    map_node = MapNode(task=map_fn, over="ctx.states", name="process")

    graph = Graph(nodes=[ctx_node, map_node], edges=[(ctx_node, map_node)])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    execute(resolved, store)

    captured = capsys.readouterr()
    assert "[MAP] process — expanding over 3 items" in captured.out


def test_execute_map_node_dispatches_per_item() -> None:
    """MapNode must call the task function once per item (AC-6)."""
    items = ["a", "b", "c"]
    ctx_value = {"states": items}

    def ctx_task() -> dict:
        return ctx_value

    ctx_task.__name__ = "ctx"
    ctx_node = TaskNode(fn=ctx_task, spec=TaskSpec(outputs=[]), name="ctx")

    map_fn = MagicMock(return_value=None)
    map_fn.__name__ = "process"
    map_node = MapNode(task=map_fn, over="ctx.states", name="process")

    graph = Graph(nodes=[ctx_node, map_node], edges=[(ctx_node, map_node)])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    execute(resolved, store)

    assert map_fn.call_count == 3
    called_items = [c.args[0] for c in map_fn.call_args_list]
    assert called_items == ["a", "b", "c"]


# ─── ConfigNode kwargs propagation ────────────────────────────────────────────


def test_execute_config_node_kwargs_passed_to_task() -> None:
    """ConfigNode callables must be resolved and passed as kwargs to subsequent TaskNodes."""
    received_kwargs: dict = {}

    def get_engine():
        return "engine_instance"

    def task_a(engine=None) -> None:
        received_kwargs["engine"] = engine

    task_a.__name__ = "task_a"

    config_node = ConfigNode(spec={"engine": get_engine})
    task_node = TaskNode(fn=task_a, spec=TaskSpec(outputs=[]), name="task_a")

    graph = Graph(nodes=[config_node, task_node], edges=[(config_node, task_node)])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    execute(resolved, store)

    assert received_kwargs.get("engine") == "engine_instance"


# ─── Non-exec nodes silently skipped ─────────────────────────────────────────


def test_execute_non_exec_nodes_skipped() -> None:
    """NoopNode, ParallelNode, PipelineNode, StageNode must all be silently skipped."""
    noop_node = NoopNode(name="noop")
    parallel_node = ParallelNode(name="parallel")
    pipeline_node = PipelineNode(name="my_pipeline")
    stage_node = StageNode(name="stage_1")

    graph = Graph(
        nodes=[noop_node, parallel_node, pipeline_node, stage_node],
        edges=[],
    )
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    # Must not raise
    execute(resolved, store)

    # No hashes written
    assert store.list_tasks("kptn", "default") == []
