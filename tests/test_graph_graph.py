import pytest
from kptn.graph.decorators import task
from kptn.graph.graph import Graph
from kptn.graph.nodes import TaskNode


def _make_task(name: str):  # type: ignore[return]
    """Helper: create a @kptn.task-decorated function with the given name."""

    @task(outputs=[f"duckdb://schema.{name}"])
    def fn() -> str:
        return name

    fn.__name__ = name
    # Re-attach spec with correct name (name was assigned after decoration)
    return fn


def test_rshift_produces_graph() -> None:
    """AC-3: A >> B is a Graph."""
    a = _make_task("a")
    b = _make_task("b")

    result = a >> b

    assert isinstance(result, Graph)


def test_rshift_order_preserved() -> None:
    """Tails of A connect to heads of B."""
    a = _make_task("a")
    b = _make_task("b")

    g = a >> b

    # There must be exactly one edge: a_node -> b_node
    assert len(g.edges) == 1
    src, dst = g.edges[0]
    assert src.fn is a
    assert dst.fn is b


def test_rshift_chain_three_nodes() -> None:
    """A >> B >> C has 3 nodes, 2 edges."""
    a = _make_task("a")
    b = _make_task("b")
    c = _make_task("c")

    g = a >> b >> c

    assert len(g.nodes) == 3
    assert len(g.edges) == 2


def test_rshift_is_left_associative() -> None:
    """(A >> B) >> C has the same topology as A >> (B >> C)."""
    a = _make_task("a")
    b = _make_task("b")
    c = _make_task("c")

    left = (a >> b) >> c
    right = a >> (b >> c)

    # Same number of nodes and edges
    assert len(left.nodes) == len(right.nodes) == 3
    assert len(left.edges) == len(right.edges) == 2

    # Node order should be A, B, C in both
    left_names = [n.fn.__name__ for n in left.nodes]
    right_names = [n.fn.__name__ for n in right.nodes]
    assert left_names == right_names == ["a", "b", "c"]


def test_graph_heads_and_tails() -> None:
    """_heads returns entry nodes, _tails returns exit nodes."""
    a = _make_task("a")
    b = _make_task("b")

    g = a >> b

    head_fns = [n.fn for n in g._heads()]
    tail_fns = [n.fn for n in g._tails()]

    assert a in head_fns
    assert b in tail_fns


def test_rshift_no_node_duplication() -> None:
    """Chaining the same graph objects does not duplicate nodes."""
    a = _make_task("a")
    b = _make_task("b")
    c = _make_task("c")

    g = a >> b >> c

    # Each node should appear exactly once
    assert len(g.nodes) == len(set(id(n) for n in g.nodes))
