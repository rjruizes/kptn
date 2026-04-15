"""Tests for Story 2.5: Cache Hit Detection & Skip Behavior."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from kptn.change_detector.hasher import hash_file, hash_task_source
from kptn.graph.decorators import TaskSpec
from kptn.graph.graph import Graph
from kptn.graph.nodes import MapNode, StageNode, TaskNode
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.executor import execute
from tests.fakes import FakeStateStore


# ─── Helpers ──────────────────────────────────────────────────────────────────


def make_graph(*nodes: TaskNode | MapNode) -> Graph:
    """Build a sequential Graph from a list of nodes (each depends on the prior)."""
    node_list = list(nodes)
    edges = [(node_list[i], node_list[i + 1]) for i in range(len(node_list) - 1)]
    return Graph(nodes=node_list, edges=edges)


def make_task_node(name: str, fn=None, outputs: list[str] | None = None) -> TaskNode:
    """Create a TaskNode with TaskSpec(outputs=outputs)."""
    if fn is None:
        fn = MagicMock(return_value=None)
        fn.__name__ = name
    spec = TaskSpec(outputs=outputs or [])
    return TaskNode(fn=fn, spec=spec, name=name)


def make_resolved(graph: Graph) -> ResolvedGraph:
    return ResolvedGraph(
        graph=graph,
        pipeline="default",
        storage_key="kptn",
        bypassed_names=frozenset(),
    )


def _composite_hash(file_path: str) -> str:
    """Compute the same composite hash that _compute_hash / _compute_hash_for_map_item produces."""
    h = hash_file(file_path)
    return hashlib.sha256(h.encode()).hexdigest()


# ─── TaskNode cache-hit tests ─────────────────────────────────────────────────


def test_task_skips_when_hash_matches(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    """Task function must NOT be called when stored hash matches current output (AC-1)."""
    output_file = tmp_path / "out.txt"
    output_file.write_bytes(b"data")

    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "my_task"
    node = make_task_node("my_task", fn=task_fn, outputs=[str(output_file)])

    store = FakeStateStore()
    store.write_hash("kptn", "default", "my_task", _composite_hash(str(output_file)))

    execute(make_resolved(make_graph(node)), store)

    task_fn.assert_not_called()
    captured = capsys.readouterr()
    assert "[SKIP] my_task \u2014 cached" in captured.out


def test_task_runs_when_no_stored_hash(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    """Task function MUST be called when no hash is stored (first run, AC-3)."""
    output_file = tmp_path / "out.txt"
    output_file.write_bytes(b"data")

    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "my_task"
    node = make_task_node("my_task", fn=task_fn, outputs=[str(output_file)])

    store = FakeStateStore()  # empty — no stored hash

    execute(make_resolved(make_graph(node)), store)

    task_fn.assert_called_once()
    captured = capsys.readouterr()
    assert "[RUN] my_task" in captured.out


def test_task_runs_when_hash_differs(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    """Task function MUST be called when stored hash does not match current output (AC-2)."""
    output_file = tmp_path / "out.txt"
    output_file.write_bytes(b"data")

    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "my_task"
    node = make_task_node("my_task", fn=task_fn, outputs=[str(output_file)])

    store = FakeStateStore()
    store.write_hash("kptn", "default", "my_task", "wrong_hash_value")

    execute(make_resolved(make_graph(node)), store)

    task_fn.assert_called_once()
    captured = capsys.readouterr()
    assert "[RUN] my_task" in captured.out


def test_hash_error_during_staleness_check_treated_as_stale(
    capsys: pytest.CaptureFixture, tmp_path: Path
) -> None:
    """HashError during pre-run is_stale() must be caught and treated as stale (AC-3).

    The output file does NOT exist before the task runs, causing is_stale() to raise
    HashError.  The executor must catch it, treat the task as stale, and run it.
    The task creates the file so that the post-run hash write succeeds.
    """
    output_file = tmp_path / "out.txt"  # does NOT exist yet — triggers HashError in is_stale

    called: list[bool] = []

    def task_fn() -> None:
        called.append(True)
        output_file.write_bytes(b"data")  # create file so post-run _compute_hash succeeds

    node = make_task_node("my_task", fn=task_fn, outputs=[str(output_file)])

    store = FakeStateStore()
    store.write_hash("kptn", "default", "my_task", "some_stored_hash")

    execute(make_resolved(make_graph(node)), store)

    assert called == [True], "task function must be called when HashError treated as stale"
    captured = capsys.readouterr()
    assert "[RUN] my_task" in captured.out


# ─── Exact format tests ───────────────────────────────────────────────────────


def test_emit_skip_exact_format(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    """emit_skip must produce exactly '[SKIP] task_name \u2014 cached\\n'."""
    output_file = tmp_path / "out.txt"
    output_file.write_bytes(b"data")

    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "my_task"
    node = make_task_node("my_task", fn=task_fn, outputs=[str(output_file)])

    store = FakeStateStore()
    store.write_hash("kptn", "default", "my_task", _composite_hash(str(output_file)))

    execute(make_resolved(make_graph(node)), store)

    captured = capsys.readouterr()
    assert captured.out == "[SKIP] my_task \u2014 cached\n"


def test_emit_run_exact_format(capsys: pytest.CaptureFixture) -> None:
    """emit_run must produce exactly '[RUN] task_name\\n'."""
    task_fn = MagicMock(return_value=None)
    task_fn.__name__ = "my_task"
    # Uninspectable fn + no outputs → always runs (no code hash fallback)
    node = make_task_node("my_task", fn=task_fn, outputs=[])

    store = FakeStateStore()

    execute(make_resolved(make_graph(node)), store)

    captured = capsys.readouterr()
    assert captured.out == "[RUN] my_task\n"


# ─── Code-hash caching for tasks with outputs=[] ─────────────────────────────


def _no_output_fn():
    """Real function whose source is inspectable."""
    return 42


def test_no_output_task_skips_on_second_run(capsys: pytest.CaptureFixture) -> None:
    """Task with outputs=[] must be skipped on second run when code hasn't changed."""
    node = make_task_node("my_task", fn=_no_output_fn, outputs=[])
    store = FakeStateStore()
    resolved = make_resolved(make_graph(node))

    execute(resolved, store)
    capsys.readouterr()  # discard first-run output

    execute(resolved, store)

    captured = capsys.readouterr()
    assert "[SKIP] my_task \u2014 cached" in captured.out


def test_no_output_task_runs_when_code_hash_outdated(capsys: pytest.CaptureFixture) -> None:
    """Task with outputs=[] must run when the stored hash is stale."""
    node = make_task_node("my_task", fn=_no_output_fn, outputs=[])
    store = FakeStateStore()
    store.write_hash("kptn", "default", "my_task", "outdated_hash_value")
    resolved = make_resolved(make_graph(node))

    execute(resolved, store)

    captured = capsys.readouterr()
    assert "[RUN] my_task" in captured.out


# ─── MapNode cache-hit tests ──────────────────────────────────────────────────


def _make_map_setup(tmp_path: Path, items: list[str], name: str = "process_state"):
    """Build a graph with a ctx task + MapNode, and return (graph, resolved, map_node)."""
    output_file = tmp_path / "out.txt"
    output_file.write_bytes(b"data")

    # ctx task feeds the collection into runtime_ctx under key "states"
    # (MapNode is wired with over="ctx.states")
    ctx_fn = MagicMock(return_value={"states": items})
    ctx_fn.__name__ = "ctx"
    ctx_node = make_task_node("ctx", fn=ctx_fn, outputs=[])

    # MapNode task with __kptn__ that has outputs
    map_fn = MagicMock(return_value=None)
    map_fn.__kptn__ = TaskSpec(outputs=[str(output_file)])

    map_node = MapNode(task=map_fn, over="ctx.states", name=name)

    graph = Graph(nodes=[ctx_node, map_node], edges=[(ctx_node, map_node)])
    resolved = ResolvedGraph(
        graph=graph,
        pipeline="default",
        storage_key="kptn",
        bypassed_names=frozenset(),
    )
    return graph, resolved, map_node, map_fn, output_file


def test_map_node_all_items_skip_on_second_run(
    capsys: pytest.CaptureFixture, tmp_path: Path
) -> None:
    """All map items must skip when hashes match for all keys (AC-4, AC-5)."""
    items = ["ca", "tx", "ny"]
    _, resolved, _, map_fn, output_file = _make_map_setup(tmp_path, items)

    expected_hash = _composite_hash(str(output_file))
    store = FakeStateStore()
    for item in items:
        store.write_hash("kptn", "default", f"process_state[{item}]", expected_hash)

    execute(resolved, store)

    map_fn.assert_not_called()
    captured = capsys.readouterr()
    for item in items:
        assert f"[SKIP] process_state[{item}] \u2014 cached" in captured.out


def test_map_node_partial_skip(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    """Items with matching hash skip; items with no stored hash run (AC-4)."""
    items = ["ca", "tx", "ny"]
    _, resolved, _, map_fn, output_file = _make_map_setup(tmp_path, items)

    expected_hash = _composite_hash(str(output_file))
    store = FakeStateStore()
    # Pre-populate only ca and tx — ny has no stored hash (will run)
    store.write_hash("kptn", "default", "process_state[ca]", expected_hash)
    store.write_hash("kptn", "default", "process_state[tx]", expected_hash)

    execute(resolved, store)

    # map_fn called once for "ny" only
    assert map_fn.call_count == 1
    assert map_fn.call_args[0][0] == "ny"
    captured = capsys.readouterr()
    assert "[SKIP] process_state[ca] \u2014 cached" in captured.out
    assert "[SKIP] process_state[tx] \u2014 cached" in captured.out
    assert "[RUN] process_state[ny]" in captured.out


def test_map_node_first_run_writes_per_item_hashes(tmp_path: Path) -> None:
    """First run must write individual State Store entries for each item (AC-6)."""
    items = ["ca", "tx", "ny"]
    _, resolved, _, map_fn, output_file = _make_map_setup(tmp_path, items)

    store = FakeStateStore()

    execute(resolved, store)

    assert map_fn.call_count == 3
    expected_hash = _composite_hash(str(output_file))
    for item in items:
        assert store.read_hash("kptn", "default", f"process_state[{item}]") == expected_hash


def test_map_node_state_store_key_format(tmp_path: Path) -> None:
    """State Store key for each item must be '<task_name>[<item>]' (AC-5)."""
    items = ["ca", "tx", "ny"]
    _, resolved, _, map_fn, output_file = _make_map_setup(tmp_path, items)

    store = FakeStateStore()

    execute(resolved, store)

    written_tasks = store.list_tasks("kptn", "default")
    # ctx task has no outputs → no hash written for it
    # Each map item writes: process_state[ca], process_state[tx], process_state[ny]
    for item in items:
        key = f"process_state[{item}]"
        assert key in written_tasks, f"Expected key {key!r} in state store"
        # Verify it is NOT using alternative separator formats
        assert f"process_state:{item}" not in written_tasks
        assert f"process_state.{item}" not in written_tasks


# ─── Cascade invalidation tests ───────────────────────────────────────────────


def _upstream_fn():
    """Real inspectable function used as an upstream task."""
    pass


def test_downstream_task_forced_when_upstream_ran(tmp_path: Path) -> None:
    """If task A runs, downstream task B must also run even when B's cache is valid."""
    b_output = tmp_path / "b_out.txt"
    b_output.write_bytes(b"data")

    task_b_fn = MagicMock(return_value=None)
    task_b_fn.__name__ = "task_b"

    node_a = make_task_node("task_a", fn=_upstream_fn, outputs=[])
    node_b = make_task_node("task_b", fn=task_b_fn, outputs=[str(b_output)])

    store = FakeStateStore()
    # A has no stored code hash → will run; B has a valid stored hash → would normally skip
    store.write_hash("kptn", "default", "task_b", _composite_hash(str(b_output)))

    graph = make_graph(node_a, node_b)
    execute(make_resolved(graph), store)

    task_b_fn.assert_called_once()


def test_downstream_task_skips_when_upstream_also_skipped(tmp_path: Path) -> None:
    """If task A is cached (skipped), task B downstream can also skip."""
    b_output = tmp_path / "b_out.txt"
    b_output.write_bytes(b"data")

    task_b_fn = MagicMock(return_value=None)
    task_b_fn.__name__ = "task_b"

    node_a = make_task_node("task_a", fn=_upstream_fn, outputs=[])
    node_b = make_task_node("task_b", fn=task_b_fn, outputs=[str(b_output)])

    store = FakeStateStore()
    store.write_hash("kptn", "default", "task_a", hash_task_source(_upstream_fn))
    store.write_hash("kptn", "default", "task_b", _composite_hash(str(b_output)))

    graph = make_graph(node_a, node_b)
    execute(make_resolved(graph), store)

    task_b_fn.assert_not_called()


def test_cascade_propagates_through_stage_node(tmp_path: Path) -> None:
    """Cascade must propagate through StageNode (non-exec) to reach downstream tasks."""
    b_output = tmp_path / "b_out.txt"
    b_output.write_bytes(b"data")

    task_b_fn = MagicMock(return_value=None)
    task_b_fn.__name__ = "task_b"

    node_a = make_task_node("task_a", fn=_upstream_fn, outputs=[])
    stage = StageNode(name="my_stage")
    node_b = make_task_node("task_b", fn=task_b_fn, outputs=[str(b_output)])

    store = FakeStateStore()
    store.write_hash("kptn", "default", "task_b", _composite_hash(str(b_output)))

    graph = Graph(
        nodes=[node_a, stage, node_b],
        edges=[(node_a, stage), (stage, node_b)],
    )
    execute(make_resolved(graph), store)

    task_b_fn.assert_called_once()


def test_cascade_is_transitive(tmp_path: Path) -> None:
    """If A runs → B runs → C must also run (transitive cascade)."""
    c_output = tmp_path / "c_out.txt"
    c_output.write_bytes(b"data")

    task_c_fn = MagicMock(return_value=None)
    task_c_fn.__name__ = "task_c"

    node_a = make_task_node("task_a", fn=_upstream_fn, outputs=[])
    node_b = make_task_node("task_b", fn=_upstream_fn, outputs=[])
    node_c = make_task_node("task_c", fn=task_c_fn, outputs=[str(c_output)])

    store = FakeStateStore()
    # B has a valid code hash → would skip; C has a valid output hash → would skip
    store.write_hash("kptn", "default", "task_b", hash_task_source(_upstream_fn))
    store.write_hash("kptn", "default", "task_c", _composite_hash(str(c_output)))

    graph = make_graph(node_a, node_b, node_c)
    execute(make_resolved(graph), store)

    # A runs (no stored hash) → B must run (cascade) → C must run (cascade from B)
    task_c_fn.assert_called_once()
