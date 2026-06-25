from __future__ import annotations

import pytest

import kptn
from kptn.graph.decorators import RTaskSpec, SqlTaskSpec, TaskSpec, r_task, sql_task, task
from kptn.graph.graph import Graph
from kptn.graph.requires import AnyOf, any_of


@task(outputs=["duckdb://a"])
def a() -> None: ...


@task(outputs=["duckdb://b"])
def b() -> None: ...


def test_task_requires_default_none() -> None:
    @task(outputs=["duckdb://x"])
    def x() -> None: ...

    assert x.__kptn__.requires is None


def test_task_requires_stores_handles() -> None:
    @task(outputs=["duckdb://x"], requires=[a, b])
    def x() -> None: ...

    assert isinstance(x.__kptn__, TaskSpec)
    assert x.__kptn__.requires == [a, b]


def test_sql_and_r_task_requires_field() -> None:
    s = sql_task("q.sql", outputs=["duckdb://s"], requires=[a])
    r = r_task("s.R", outputs=["duckdb://r"], requires=[a])
    assert isinstance(s.__kptn__, SqlTaskSpec)
    assert isinstance(r.__kptn__, RTaskSpec)
    assert s.__kptn__.requires == [a]
    assert r.__kptn__.requires == [a]


def test_any_of_holds_members() -> None:
    grp = any_of(a, b)
    assert isinstance(grp, AnyOf)
    assert grp.members == (a, b)


def test_any_of_empty_raises() -> None:
    with pytest.raises(ValueError):
        any_of()


def test_any_of_rejects_non_handle() -> None:
    with pytest.raises(TypeError):
        any_of(a, 123)


def test_any_of_exported_from_package() -> None:
    assert kptn.any_of is any_of


def test_graph_requires_edges_defaults_empty() -> None:
    g = Graph(nodes=[], edges=[])
    assert g.requires_edges == set()


def test_graph_requires_edges_settable() -> None:
    g = Graph(nodes=[], edges=[], requires_edges={(1, 2)})
    assert g.requires_edges == {(1, 2)}


# ---------------------------------------------------------------------------
# Task 3: expand_requires + Pipeline wiring
# ---------------------------------------------------------------------------

from kptn.graph.nodes import PipelineNode, TaskNode
from kptn.graph.topo import topo_sort
from kptn.exceptions import GraphError


@task(outputs=["duckdb://idx"])
def build_index() -> None: ...


@task(outputs=["duckdb://raw"])
def load_raw() -> None: ...


def _names(graph) -> list[str]:
    return [n.name for n in graph.nodes]


def test_requires_pulls_in_and_orders_before_consumer() -> None:
    @task(outputs=["rep"], requires=[build_index])
    def report() -> None: ...

    pipe = kptn.Pipeline("p", report)
    assert "build_index" in _names(pipe)
    order = [n.name for n in topo_sort(pipe)]
    assert order.index("build_index") < order.index("report")


def test_requires_no_op_when_user_placed() -> None:
    @task(outputs=["rep2"], requires=[build_index])
    def report2() -> None: ...

    pipe = kptn.Pipeline("p", build_index >> report2)
    # exactly one build_index node, no requires-edge injected (user already wired it)
    assert _names(pipe).count("build_index") == 1
    assert pipe.requires_edges == set()


def test_requires_shared_single_node_two_edges() -> None:
    @task(outputs=["c1"], requires=[build_index])
    def cons1() -> None: ...

    @task(outputs=["c2"], requires=[build_index])
    def cons2() -> None: ...

    pipe = kptn.Pipeline("p", cons1 >> cons2)
    assert _names(pipe).count("build_index") == 1
    # one tagged edge per requirer
    assert len(pipe.requires_edges) == 2
    order = [n.name for n in topo_sort(pipe)]
    assert order.index("build_index") < order.index("cons1")
    assert order.index("build_index") < order.index("cons2")


def test_requires_transitive() -> None:
    @task(outputs=["duckdb://idx2"], requires=[load_raw])
    def build_index2() -> None: ...

    @task(outputs=["rep3"], requires=[build_index2])
    def report3() -> None: ...

    pipe = kptn.Pipeline("p", report3)
    names = _names(pipe)
    assert {"load_raw", "build_index2", "report3"} <= set(names)
    order = [n.name for n in topo_sort(pipe)]
    assert order.index("load_raw") < order.index("build_index2") < order.index("report3")


def test_requires_none_is_noop() -> None:
    @task(outputs=["x"])
    def x() -> None: ...

    pipe = kptn.Pipeline("p", x)
    assert pipe.requires_edges == set()


def test_requires_cycle_raises_on_topo() -> None:
    # mutual requirement among INJECTED nodes → cycle once both edges injected
    @task(outputs=["q1"])
    def q1() -> None: ...

    @task(outputs=["q2"])
    def q2() -> None: ...

    @task(outputs=["entry"], requires=[q2])
    def entry() -> None: ...

    # entry pulls in q2; q2 requires q1; q1 requires q2 → q1<->q2 cycle (neither user-placed)
    q2.__kptn__.requires = [q1]
    q1.__kptn__.requires = [q2]
    pipe = kptn.Pipeline("p", entry)
    with pytest.raises(GraphError):
        topo_sort(pipe)
