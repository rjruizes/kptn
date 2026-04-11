import pytest
import kptn
from kptn.graph.decorators import SqlTaskSpec, RTaskSpec, sql_task, r_task, task
from kptn.graph.nodes import SqlTaskNode, RTaskNode, TaskNode
from kptn.graph.graph import Graph
from kptn.graph.topo import topo_sort


# ──────────────────────────────────────────────────────────────────────────── #
# AC-1: sql_task                                                                #
# ──────────────────────────────────────────────────────────────────────────── #

def test_sql_task_rshift_produces_graph() -> None:
    """AC-1: sql_task(...) >> other produces a Graph"""
    sq1 = sql_task("path/to/query.sql", outputs=["duckdb://schema.table"])
    sq2 = sql_task("path/to/query2.sql", outputs=["duckdb://schema.table2"])
    result = sq1 >> sq2
    assert isinstance(result, Graph)


def test_sql_task_node_type() -> None:
    """AC-1: internal node is SqlTaskNode"""
    sq = sql_task("path/to/query.sql", outputs=["duckdb://schema.table"])
    g = Graph._from_node(sq)
    assert len(g.nodes) == 1
    assert isinstance(g.nodes[0], SqlTaskNode)


def test_sql_task_node_outputs() -> None:
    """AC-3: outputs attribute returns declared list"""
    sq = sql_task("path/to/query.sql", outputs=["duckdb://schema.table"])
    g = Graph._from_node(sq)
    node = g.nodes[0]
    assert isinstance(node, SqlTaskNode)
    assert node.spec.outputs == ["duckdb://schema.table"]


def test_sql_task_name_from_path_stem() -> None:
    """name is derived from the file basename without extension"""
    sq = sql_task("path/to/my_query.sql", outputs=["x"])
    g = Graph._from_node(sq)
    assert g.nodes[0].name == "my_query"


def test_sql_task_composes_with_python_task() -> None:
    """AC-1: sql_task >> @kptn.task works; Graph has both nodes, edge sql→python"""

    @task(outputs=["duckdb://schema.output"])
    def my_python_task() -> None:
        pass

    sq = sql_task("path/to/query.sql", outputs=["duckdb://schema.table"])
    g = sq >> my_python_task

    assert isinstance(g, Graph)
    assert len(g.nodes) == 2
    assert len(g.edges) == 1
    sql_node = g.nodes[0]
    python_node = g.nodes[1]
    assert isinstance(sql_node, SqlTaskNode)
    assert isinstance(python_node, TaskNode)


def test_sql_task_is_not_task_node() -> None:
    """isinstance(sql_task_result, TaskNode) is False"""
    sq = sql_task("path/to/query.sql", outputs=["x"])
    assert not isinstance(sq, TaskNode)


# ──────────────────────────────────────────────────────────────────────────── #
# AC-2: r_task                                                                  #
# ──────────────────────────────────────────────────────────────────────────── #

def test_r_task_rshift_produces_graph() -> None:
    """AC-2: r_task(...) >> other produces a Graph"""
    rt1 = r_task("scripts/process.R", outputs=["duckdb://schema.out"])
    rt2 = r_task("scripts/process2.R", outputs=["duckdb://schema.out2"])
    result = rt1 >> rt2
    assert isinstance(result, Graph)


def test_r_task_node_type() -> None:
    """AC-2: internal node is RTaskNode"""
    rt = r_task("scripts/process.R", outputs=["duckdb://schema.out"])
    g = Graph._from_node(rt)
    assert len(g.nodes) == 1
    assert isinstance(g.nodes[0], RTaskNode)


def test_r_task_node_stores_compute() -> None:
    """AC-2: RTaskNode.spec.compute == "4gb" """
    rt = r_task("scripts/process.R", outputs=["duckdb://schema.out"], compute="4gb")
    g = Graph._from_node(rt)
    node = g.nodes[0]
    assert isinstance(node, RTaskNode)
    assert node.spec.compute == "4gb"


def test_r_task_node_compute_default_none() -> None:
    """compute defaults to None when not provided"""
    rt = r_task("scripts/process.R", outputs=["duckdb://schema.out"])
    g = Graph._from_node(rt)
    node = g.nodes[0]
    assert isinstance(node, RTaskNode)
    assert node.spec.compute is None


def test_r_task_node_outputs() -> None:
    """AC-3: outputs attribute returns declared list"""
    rt = r_task("scripts/process.R", outputs=["duckdb://schema.out"])
    g = Graph._from_node(rt)
    node = g.nodes[0]
    assert isinstance(node, RTaskNode)
    assert node.spec.outputs == ["duckdb://schema.out"]


def test_r_task_name_from_path_stem() -> None:
    """name is derived from the file basename without extension"""
    rt = r_task("scripts/process.R", outputs=["x"])
    g = Graph._from_node(rt)
    assert g.nodes[0].name == "process"


def test_r_task_is_not_task_node() -> None:
    """isinstance(r_task_result, TaskNode) is False"""
    rt = r_task("scripts/process.R", outputs=["x"])
    assert not isinstance(rt, TaskNode)


# ──────────────────────────────────────────────────────────────────────────── #
# AC-2: cross-type composition                                                  #
# ──────────────────────────────────────────────────────────────────────────── #

def test_mixed_graph_python_sql_r() -> None:
    """python_task >> sql_task >> r_task forms a valid 3-node Graph"""

    @task(outputs=["duckdb://schema.a"])
    def python_task() -> None:
        pass

    sq = sql_task("query.sql", outputs=["duckdb://schema.b"])
    rt = r_task("script.R", outputs=["duckdb://schema.c"])

    g = python_task >> sq >> rt
    assert isinstance(g, Graph)


def test_mixed_graph_node_count() -> None:
    """3 tasks compose into a Graph with exactly 3 nodes and 2 edges"""

    @task(outputs=["duckdb://schema.a"])
    def python_task() -> None:
        pass

    sq = sql_task("query.sql", outputs=["duckdb://schema.b"])
    rt = r_task("script.R", outputs=["duckdb://schema.c"])

    g = python_task >> sq >> rt
    assert len(g.nodes) == 3
    assert len(g.edges) == 2


def test_mixed_graph_topo_sort_order() -> None:
    """topo_sort respects declaration order for a linear chain"""

    @task(outputs=["duckdb://schema.a"])
    def python_task() -> None:
        pass

    sq = sql_task("query.sql", outputs=["duckdb://schema.b"])
    rt = r_task("script.R", outputs=["duckdb://schema.c"])

    g = python_task >> sq >> rt
    sorted_nodes = topo_sort(g)

    assert len(sorted_nodes) == 3
    assert isinstance(sorted_nodes[0], TaskNode)
    assert isinstance(sorted_nodes[1], SqlTaskNode)
    assert isinstance(sorted_nodes[2], RTaskNode)


# ──────────────────────────────────────────────────────────────────────────── #
# Public API surface                                                             #
# ──────────────────────────────────────────────────────────────────────────── #

def test_kptn_sql_task_public() -> None:
    """import kptn; kptn.sql_task is accessible"""
    assert hasattr(kptn, "sql_task")
    assert callable(kptn.sql_task)


def test_kptn_r_task_public() -> None:
    """import kptn; kptn.r_task is accessible"""
    assert hasattr(kptn, "r_task")
    assert callable(kptn.r_task)
