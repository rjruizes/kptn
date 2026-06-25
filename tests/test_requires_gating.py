from __future__ import annotations

import kptn
from kptn.graph.graph import Graph
from kptn.graph.requires import expand_requires, gate_disjunctive


@kptn.task(outputs=["A"])
def A() -> None: ...


@kptn.task(outputs=["B"])
def B() -> None: ...


def _names(graph) -> set[str]:
    return {n.name for n in graph.nodes}


def _build(graph: Graph) -> Graph:
    """Apply both transforms the way the runner does (expand, then gate)."""
    return gate_disjunctive(expand_requires(graph))


def test_disjunctive_satisfied_when_member_present() -> None:
    @kptn.task(outputs=["c"], requires=[kptn.any_of(A, B)])
    def consumer() -> None: ...

    g = _build(A >> consumer)
    assert "consumer" in _names(g)
    assert "B" not in _names(g)  # any_of never pulls


def test_disjunctive_drops_consumer_when_none_present() -> None:
    @kptn.task(outputs=["c2"], requires=[kptn.any_of(A, B)])
    def consumer2() -> None: ...

    g = _build(Graph._from_node(consumer2))
    assert "consumer2" not in _names(g)


def test_structural_successor_survives_via_bypass() -> None:
    @kptn.task(outputs=["c3"], requires=[kptn.any_of(A, B)])
    def gated() -> None: ...

    @kptn.task(outputs=["t"])
    def tail() -> None: ...

    # `tail` follows `gated` via >> but does NOT require it
    g = _build(gated >> tail)
    assert "gated" not in _names(g)
    assert "tail" in _names(g)


def test_requirer_of_dropped_node_is_propagated() -> None:
    @kptn.task(outputs=["c4"], requires=[kptn.any_of(A, B)])
    def gated2() -> None: ...

    @kptn.task(outputs=["d"], requires=[gated2])
    def dependent() -> None: ...

    g = _build(Graph._from_node(dependent))
    # gated2 has no present member → dropped; dependent conjunctively required it → also dropped
    assert "gated2" not in _names(g)
    assert "dependent" not in _names(g)


def test_fixpoint_chained_drops() -> None:
    @kptn.task(outputs=["m"], requires=[kptn.any_of(A, B)])
    def mid() -> None: ...

    @kptn.task(outputs=["e"], requires=[kptn.any_of(mid, B)])
    def edge() -> None: ...

    # neither A nor B present → mid dropped → edge's any_of(mid,B) now empty → edge dropped
    g = _build(Graph._from_node(edge))
    assert "mid" not in _names(g)
    assert "edge" not in _names(g)


def test_no_disjunctive_is_identity() -> None:
    @kptn.task(outputs=["p"])
    def plain() -> None: ...

    g = gate_disjunctive(expand_requires(Graph._from_node(plain)))
    assert _names(g) == {"plain"}
