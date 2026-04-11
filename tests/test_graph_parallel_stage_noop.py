import pytest

import kptn
from kptn.graph.nodes import ParallelNode, StageNode, NoopNode, TaskNode
from kptn.graph.graph import Graph
from kptn.graph.topo import topo_sort
from kptn.graph.composition import parallel, Stage
from kptn.graph.decorators import noop, task


# ─── Helper ──────────────────────────────────────────────────────────────── #


def _task(name: str):
    """Create a @kptn.task decorated function with the given name."""

    @task(outputs=[f"duckdb://schema.{name}"])
    def fn():
        pass

    fn.__name__ = name
    return fn


# ─── AC-3: noop() ────────────────────────────────────────────────────────── #


def test_noop_returns_graph():
    assert isinstance(noop(), Graph)


def test_noop_graph_contains_noop_node():
    result = noop()
    assert len(result.nodes) == 1
    assert isinstance(result.nodes[0], NoopNode)


def test_noop_node_name():
    result = noop()
    assert result.nodes[0].name == "noop"


def test_noop_composes_between_tasks():
    A = _task("A")
    B = _task("B")
    graph = A >> kptn.noop() >> B
    assert isinstance(graph, Graph)
    assert len(graph.nodes) == 3
    noop_node = next((n for n in graph.nodes if isinstance(n, NoopNode)), None)
    assert noop_node is not None, "expected NoopNode in graph"
    a_node = next((n for n in graph.nodes if isinstance(n, TaskNode) and n.name == "A"), None)
    assert a_node is not None, "expected TaskNode 'A' in graph"
    b_node = next((n for n in graph.nodes if isinstance(n, TaskNode) and n.name == "B"), None)
    assert b_node is not None, "expected TaskNode 'B' in graph"
    assert (a_node, noop_node) in graph.edges
    assert (noop_node, b_node) in graph.edges


def test_noop_standalone_composes_with_rshift():
    A = _task("A")
    graph = kptn.noop() >> A
    assert isinstance(graph, Graph)
    assert len(graph.nodes) == 2


# ─── AC-1: parallel() ────────────────────────────────────────────────────── #


def test_parallel_returns_graph():
    A = _task("A")
    B = _task("B")
    assert isinstance(parallel(A, B), Graph)


def test_parallel_contains_parallel_node():
    A = _task("A")
    B = _task("B")
    graph = parallel(A, B)
    assert any(isinstance(n, ParallelNode) for n in graph.nodes)


def test_parallel_all_branches_present():
    A = _task("A")
    B = _task("B")
    C = _task("C")
    graph = parallel(A, B, C)
    node_names = {n.name for n in graph.nodes if isinstance(n, TaskNode)}
    assert node_names == {"A", "B", "C"}


def test_parallel_named_node():
    A = _task("A")
    B = _task("B")
    graph = parallel("etl", A, B)
    p_node = next((n for n in graph.nodes if isinstance(n, ParallelNode)), None)
    assert p_node is not None, "expected ParallelNode in graph"
    assert p_node.name == "etl"


def test_parallel_anonymous_default_name():
    A = _task("A")
    B = _task("B")
    graph = parallel(A, B)
    p_node = next((n for n in graph.nodes if isinstance(n, ParallelNode)), None)
    assert p_node is not None, "expected ParallelNode in graph"
    assert p_node.name == "parallel"


def test_parallel_head_is_parallel_node():
    A = _task("A")
    B = _task("B")
    graph = parallel(A, B)
    heads = graph._heads()
    assert len(heads) == 1
    assert isinstance(heads[0], ParallelNode)


def test_parallel_tails_are_branch_nodes():
    A = _task("A")
    B = _task("B")
    graph = parallel(A, B)
    tails = graph._tails()
    assert len(tails) == 2
    assert all(isinstance(n, TaskNode) for n in tails)
    tail_names = {n.name for n in tails}
    assert tail_names == {"A", "B"}


def test_parallel_downstream_cross_edges():
    A = _task("A")
    B = _task("B")
    C = _task("C")
    graph = parallel(A, B) >> C
    c_node = next((n for n in graph.nodes if isinstance(n, TaskNode) and n.name == "C"), None)
    assert c_node is not None, "expected TaskNode 'C' in graph"
    incoming = [dst for _, dst in graph.edges if dst is c_node]
    assert len(incoming) == 2


def test_parallel_topo_sort():
    U = _task("U")
    A = _task("A")
    B = _task("B")
    D = _task("D")
    graph = U >> parallel(A, B) >> D
    result = topo_sort(graph)
    names = [n.name for n in result]
    assert names[0] == "U"
    assert names[-1] == "D"
    a_idx = names.index("A")
    b_idx = names.index("B")
    d_idx = names.index("D")
    assert a_idx < d_idx
    assert b_idx < d_idx


def test_parallel_single_branch():
    A = _task("A")
    graph = parallel(A)
    assert len(graph.nodes) == 2
    assert any(isinstance(n, ParallelNode) for n in graph.nodes)
    assert any(isinstance(n, TaskNode) for n in graph.nodes)


def test_parallel_zero_branches_raises():
    with pytest.raises(ValueError, match="at least one branch"):
        parallel()


def test_parallel_named_zero_branches_raises():
    with pytest.raises(ValueError, match="at least one branch"):
        parallel("etl")


# ─── AC-2: Stage() ───────────────────────────────────────────────────────── #


def test_stage_returns_graph():
    A = _task("A")
    B = _task("B")
    assert isinstance(Stage("data_sources", A, B), Graph)


def test_stage_contains_stage_node():
    A = _task("A")
    B = _task("B")
    graph = Stage("data_sources", A, B)
    assert any(isinstance(n, StageNode) for n in graph.nodes)


def test_stage_node_name():
    A = _task("A")
    B = _task("B")
    graph = Stage("data_sources", A, B)
    s_node = next((n for n in graph.nodes if isinstance(n, StageNode)), None)
    assert s_node is not None, "expected StageNode in graph"
    assert s_node.name == "data_sources"


def test_stage_all_branches_present():
    A = _task("A")
    B = _task("B")
    graph = Stage("s", A, B)
    task_nodes = [n for n in graph.nodes if isinstance(n, TaskNode)]
    assert len(task_nodes) == 2
    names = {n.name for n in task_nodes}
    assert names == {"A", "B"}


def test_stage_head_is_stage_node():
    A = _task("A")
    B = _task("B")
    graph = Stage("s", A, B)
    heads = graph._heads()
    assert len(heads) == 1
    assert isinstance(heads[0], StageNode)


def test_stage_tails_are_branch_nodes():
    A = _task("A")
    B = _task("B")
    graph = Stage("s", A, B)
    tails = graph._tails()
    assert len(tails) == 2
    assert all(isinstance(n, TaskNode) for n in tails)
    tail_names = {n.name for n in tails}
    assert tail_names == {"A", "B"}


def test_stage_topo_sort():
    A = _task("A")
    B = _task("B")
    graph = Stage("s", A, B)
    result = topo_sort(graph)
    assert len(result) == 3
    assert isinstance(result[0], StageNode)


def test_stage_no_branch_pruned():
    A = _task("A")
    B = _task("B")
    graph = Stage("s", A, B)
    assert len(graph.nodes) == 3


def test_stage_zero_branches_raises():
    with pytest.raises(ValueError, match="at least one branch"):
        Stage("s")


# ─── AC-4: topo_sort with all new node types ─────────────────────────────── #


def test_topo_sort_parallel_and_stage_and_noop():
    A = _task("A")
    B = _task("B")
    C = _task("C")
    D = _task("D")
    E = _task("E")
    graph = A >> parallel(B, C) >> kptn.noop() >> Stage("s", D, E)
    result = topo_sort(graph)
    assert len(result) == len(graph.nodes)


def test_topo_sort_parallel_downstream_after_all_branches():
    A = _task("A")
    B = _task("B")
    C = _task("C")
    D = _task("D")
    graph = A >> parallel(B, C) >> D
    result = topo_sort(graph)
    names = [n.name for n in result]
    b_idx = names.index("B")
    c_idx = names.index("C")
    d_idx = names.index("D")
    assert b_idx < d_idx
    assert c_idx < d_idx


# ─── Public API surface ───────────────────────────────────────────────────── #


def test_kptn_noop_public():
    assert hasattr(kptn, "noop")
    assert callable(kptn.noop)


def test_kptn_parallel_public():
    assert hasattr(kptn, "parallel")
    assert callable(kptn.parallel)


def test_kptn_stage_public():
    assert hasattr(kptn, "Stage")
    assert callable(kptn.Stage)
