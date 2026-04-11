import pytest

import kptn
from kptn.graph.nodes import MapNode, TaskNode
from kptn.graph.graph import Graph
from kptn.graph.topo import topo_sort
from kptn.graph.composition import map
from kptn.graph.decorators import task


# ─── Helper ──────────────────────────────────────────────────────────────── #


def _task(name: str):
    """Create a @kptn.task decorated function with the given name."""

    @task(outputs=[f"duckdb://schema.{name}"])
    def fn():
        pass

    fn.__name__ = name
    return fn


# ─── AC-1: map() returns Graph with MapNode carrying task + over ─────────── #


def test_map_returns_graph():
    process_item = _task("process_item")
    result = kptn.map(process_item, over="ctx.states")
    assert isinstance(result, Graph)


def test_map_graph_contains_map_node():
    process_item = _task("process_item")
    result = kptn.map(process_item, over="ctx.states")
    assert len(result.nodes) == 1
    assert isinstance(result.nodes[0], MapNode)


def test_map_node_task_field():
    process_item = _task("process_item")
    result = kptn.map(process_item, over="ctx.states")
    map_node = result.nodes[0]
    assert map_node.task is process_item


def test_map_node_over_field():
    process_item = _task("process_item")
    result = kptn.map(process_item, over="ctx.states")
    map_node = result.nodes[0]
    assert map_node.over == "ctx.states"


def test_map_node_name_from_callable():
    process_item = _task("process_item")
    result = kptn.map(process_item, over="ctx.states")
    map_node = result.nodes[0]
    assert map_node.name == "process_item"


def test_map_rejects_undecorated_callable():
    """map() raises TypeError when task_fn lacks __kptn__ — fail-fast guard."""
    class NoNameCallable:
        def __call__(self):
            pass

    with pytest.raises(TypeError, match="@kptn.task"):
        kptn.map(NoNameCallable(), over="ctx.items")


def test_map_node_name_fallback_for_kptn_tagged_nameless_callable():
    """MapNode.name uses repr() fallback when __kptn__-tagged callable lacks __name__."""
    class KptnTaggedNoName:
        __kptn__ = object()  # satisfies the __kptn__ guard

        def __call__(self):
            pass

    obj = KptnTaggedNoName()
    result = kptn.map(obj, over="ctx.items")
    map_node = result.nodes[0]
    assert map_node.name == repr(obj)


def test_map_no_edges():
    process_item = _task("process_item")
    result = kptn.map(process_item, over="ctx.states")
    assert result.edges == []


def test_map_over_is_keyword_only():
    """map(fn, "ctx.key") must raise TypeError — over must be keyword-only."""
    process_item = _task("process_item")
    with pytest.raises(TypeError):
        kptn.map(process_item, "ctx.states")  # type: ignore[call-arg]


# ─── AC-2: MapNode is a deferred expansion point; graph is valid ─────────── #


def test_map_single_node_graph_is_valid():
    process_item = _task("process_item")
    result = kptn.map(process_item, over="ctx.states")
    # topo_sort succeeds — no GraphError
    ordered = topo_sort(result)
    assert len(ordered) == 1
    assert isinstance(ordered[0], MapNode)


def test_map_no_graph_error_on_valid_graph():
    from kptn.exceptions import GraphError

    process_item = _task("process_item")
    result = kptn.map(process_item, over="ctx.states")
    try:
        topo_sort(result)
    except GraphError:
        pytest.fail("topo_sort raised GraphError on valid map graph")


def test_map_composes_downstream_with_rshift():
    process_item = _task("process_item")
    downstream = _task("downstream")
    graph = kptn.map(process_item, over="ctx.states") >> downstream
    assert isinstance(graph, Graph)
    assert len(graph.nodes) == 2


def test_map_composes_upstream_with_rshift():
    provider = _task("provider")
    process_item = _task("process_item")
    graph = provider >> kptn.map(process_item, over="ctx.states")
    assert isinstance(graph, Graph)
    assert len(graph.nodes) == 2


# ─── AC-3: topo_sort orders MapNode correctly ────────────────────────────── #


def test_map_topo_sort_after_provider_before_downstream():
    provider = _task("provider")
    process_item = _task("process_item")
    downstream = _task("downstream")
    graph = provider >> kptn.map(process_item, over="ctx.states") >> downstream

    ordered = topo_sort(graph)
    names = [n.name for n in ordered]

    assert names[0] == "provider"
    assert names[-1] == "downstream"
    map_idx = names.index("process_item")
    provider_idx = names.index("provider")
    downstream_idx = names.index("downstream")
    assert provider_idx < map_idx < downstream_idx


def test_map_topo_sort_standalone():
    process_item = _task("process_item")
    graph = kptn.map(process_item, over="ctx.states")
    ordered = topo_sort(graph)
    assert len(ordered) == 1
    assert isinstance(ordered[0], MapNode)


def test_map_topo_sort_map_node_before_downstream():
    process_item = _task("process_item")
    downstream = _task("downstream")
    graph = kptn.map(process_item, over="ctx.states") >> downstream

    ordered = topo_sort(graph)
    names = [n.name for n in ordered]

    map_idx = names.index("process_item")
    downstream_idx = names.index("downstream")
    assert map_idx < downstream_idx


# ─── Public API surface ───────────────────────────────────────────────────── #


def test_kptn_map_public():
    assert hasattr(kptn, "map")
    assert callable(kptn.map)


def test_kptn_map_in_all():
    assert "map" in kptn.__all__


def test_map_node_importable_from_kptn_graph():
    from kptn.graph import MapNode as _MapNode
    assert _MapNode is MapNode


def test_map_importable_from_kptn_graph():
    from kptn.graph import map as _map
    assert _map is map
