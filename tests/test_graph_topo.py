import pytest
from kptn.graph.decorators import task
from kptn.graph.graph import Graph
from kptn.graph.nodes import TaskNode
from kptn.graph.topo import topo_sort
from kptn.exceptions import GraphError


def _make_task(name: str):  # type: ignore[return]
    @task(outputs=[f"duckdb://schema.{name}"])
    def fn() -> str:
        return name

    fn.__name__ = name
    return fn


def test_topo_sort_two_nodes() -> None:
    """AC-3: topo_sort(A >> B) returns [A_node, B_node] in order."""
    a = _make_task("a")
    b = _make_task("b")

    g = a >> b
    result = topo_sort(g)

    assert len(result) == 2
    assert result[0].fn is a
    assert result[1].fn is b


def test_topo_sort_three_nodes() -> None:
    """AC-4: topo_sort(A >> B >> C) returns [A, B, C]."""
    a = _make_task("a")
    b = _make_task("b")
    c = _make_task("c")

    g = a >> b >> c
    result = topo_sort(g)

    assert len(result) == 3
    assert result[0].fn is a
    assert result[1].fn is b
    assert result[2].fn is c


def test_topo_sort_no_duplicates() -> None:
    """No node appears twice in the sorted result."""
    a = _make_task("a")
    b = _make_task("b")
    c = _make_task("c")

    g = a >> b >> c
    result = topo_sort(g)

    assert len(result) == len({id(n) for n in result})


def test_topo_sort_cycle_raises() -> None:
    """AC-5: GraphError raised when a cycle is present."""
    spec_a = _make_task("a").__kptn__
    spec_b = _make_task("b").__kptn__

    def fn_a() -> None:
        pass

    def fn_b() -> None:
        pass

    node_a = TaskNode(fn=fn_a, spec=spec_a, name="a")
    node_b = TaskNode(fn=fn_b, spec=spec_b, name="b")

    # Manually create a cyclic graph: a -> b -> a
    cyclic_graph = Graph(
        nodes=[node_a, node_b],
        edges=[(node_a, node_b), (node_b, node_a)],
    )

    with pytest.raises(GraphError):
        topo_sort(cyclic_graph)


def test_topo_sort_cycle_message_identifies_nodes() -> None:
    """Error message includes the names of nodes involved in the cycle."""
    spec_a = _make_task("alpha").__kptn__
    spec_b = _make_task("beta").__kptn__

    def fn_alpha() -> None:
        pass

    def fn_beta() -> None:
        pass

    node_a = TaskNode(fn=fn_alpha, spec=spec_a, name="alpha")
    node_b = TaskNode(fn=fn_beta, spec=spec_b, name="beta")

    cyclic_graph = Graph(
        nodes=[node_a, node_b],
        edges=[(node_a, node_b), (node_b, node_a)],
    )

    with pytest.raises(GraphError, match="alpha|beta"):
        topo_sort(cyclic_graph)


def test_topo_sort_single_node() -> None:
    """A single node graph sorts to [node]."""
    a = _make_task("solo")
    g = Graph._from_node(a)
    result = topo_sort(g)
    assert len(result) == 1
    assert result[0].fn is a
