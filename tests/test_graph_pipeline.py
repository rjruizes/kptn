import kptn
from kptn.graph.nodes import PipelineNode, TaskNode
from kptn.graph.graph import Graph
from kptn.graph.pipeline import Pipeline
from kptn.graph.topo import topo_sort
from kptn.graph.decorators import task


# ─── Helper ──────────────────────────────────────────────────────────────── #


def _task(name: str):
    """Create a @kptn.task decorated function with the given name."""

    @task(outputs=[f"duckdb://schema.{name}"])
    def fn():
        pass

    fn.__name__ = name
    return fn


# ─── AC-1: Pipeline("name", inner) composed with >> → all nodes in order ─── #


def test_pipeline_returns_pipeline_instance():
    a = _task("a")
    b = _task("b")
    p = kptn.Pipeline("ingest", a >> b)
    assert isinstance(p, Pipeline)


def test_pipeline_is_graph_subclass():
    a = _task("a")
    b = _task("b")
    p = kptn.Pipeline("ingest", a >> b)
    assert isinstance(p, Graph)


def test_pipeline_contains_sentinel_and_inner_nodes():
    a = _task("a")
    b = _task("b")
    c = _task("c")
    p = kptn.Pipeline("ingest", a >> b >> c)
    node_names = [n.name for n in p.nodes]
    assert "ingest" in node_names
    assert "a" in node_names
    assert "b" in node_names
    assert "c" in node_names
    assert len(p.nodes) == 4


def test_pipeline_composed_with_rshift_all_nodes_present():
    a = _task("a")
    b = _task("b")
    c = _task("c")
    d = _task("d")
    ingest = kptn.Pipeline("ingest", a >> b >> c)
    result = ingest >> d
    node_names = [n.name for n in result.nodes]
    assert "ingest" in node_names
    assert "a" in node_names
    assert "b" in node_names
    assert "c" in node_names
    assert "d" in node_names


def test_two_pipelines_composed_with_rshift():
    a = _task("a")
    b = _task("b")
    c = _task("c")
    d = _task("d")
    ingest = kptn.Pipeline("ingest", a >> b)
    transform = kptn.Pipeline("transform", c >> d)
    result = ingest >> transform
    node_names = [n.name for n in result.nodes]
    assert "ingest" in node_names
    assert "transform" in node_names
    assert "a" in node_names
    assert "b" in node_names
    assert "c" in node_names
    assert "d" in node_names


def test_pipeline_composed_correct_dependency_order():
    a = _task("a")
    b = _task("b")
    c = _task("c")
    d = _task("d")
    ingest = kptn.Pipeline("ingest", a >> b >> c)
    result = ingest >> d
    ordered = topo_sort(result)
    names = [n.name for n in ordered]
    assert names.index("ingest") < names.index("a")
    assert names.index("a") < names.index("b")
    assert names.index("b") < names.index("c")
    assert names.index("c") < names.index("d")


# ─── AC-2: Nested pipelines + topo_sort; operators work inside Pipeline ───── #


def test_nested_pipeline_topo_sort_flat_ordered():
    a = _task("a")
    b = _task("b")
    c = _task("c")
    d = _task("d")
    inner = kptn.Pipeline("inner", a >> b)
    outer = kptn.Pipeline("outer", inner >> c >> d)
    ordered = topo_sort(outer)
    names = [n.name for n in ordered]
    # outer sentinel first, then inner sentinel, then a, b, c, d
    assert names.index("outer") < names.index("inner")
    assert names.index("inner") < names.index("a")
    assert names.index("a") < names.index("b")
    assert names.index("b") < names.index("c")
    assert names.index("c") < names.index("d")


def test_pipeline_with_parallel_inside():
    a = _task("a")
    b = _task("b")
    c = _task("c")
    inner = kptn.parallel(a, b) >> c
    p = kptn.Pipeline("p", inner)
    ordered = topo_sort(p)
    names = [n.name for n in ordered]
    assert "p" in names
    assert "a" in names
    assert "b" in names
    assert "c" in names
    assert names.index("c") == len(names) - 1


def test_pipeline_with_stage_inside():
    a = _task("a")
    b = _task("b")
    c = _task("c")
    inner = a >> kptn.Stage("dev", b, c)
    p = kptn.Pipeline("p", inner)
    ordered = topo_sort(p)
    names = [n.name for n in ordered]
    assert "p" in names
    assert "a" in names
    assert "dev" in names
    assert names.index("p") < names.index("a")
    assert names.index("a") < names.index("dev")


def test_pipeline_with_map_inside():
    process = _task("process")
    m = kptn.map(process, over="ctx.items")
    p = kptn.Pipeline("p", m)
    ordered = topo_sort(p)
    names = [n.name for n in ordered]
    assert "p" in names
    assert "process" in names


def test_pipeline_topo_sort_returns_all_nodes():
    a = _task("a")
    b = _task("b")
    p = kptn.Pipeline("p", a >> b)
    ordered = topo_sort(p)
    assert len(ordered) == 3  # PipelineNode sentinel + a + b


# ─── AC-3: Pipeline.name attribute ───────────────────────────────────────── #


def test_pipeline_name_attribute():
    a = _task("a")
    b = _task("b")
    p = kptn.Pipeline("ingest", a >> b)
    assert p.name == "ingest"


def test_pipeline_name_attribute_different_names():
    a = _task("a")
    b = _task("b")
    c = _task("c")
    d = _task("d")
    p1 = kptn.Pipeline("alpha", a >> b)
    p2 = kptn.Pipeline("beta", c >> d)
    assert p1.name == "alpha"
    assert p2.name == "beta"


def test_pipeline_sentinel_node_name_matches_pipeline_name():
    a = _task("a")
    b = _task("b")
    p = kptn.Pipeline("my_pipeline", a >> b)
    sentinel = p.nodes[0]
    assert isinstance(sentinel, PipelineNode)
    assert sentinel.name == "my_pipeline"


# ─── Public API surface ───────────────────────────────────────────────────── #


def test_kptn_pipeline_public():
    assert hasattr(kptn, "Pipeline")
    assert callable(kptn.Pipeline)


def test_pipeline_in_kptn_all():
    assert "Pipeline" in kptn.__all__


def test_pipeline_importable_from_kptn_graph():
    from kptn.graph import Pipeline as _Pipeline
    assert _Pipeline is Pipeline


def test_pipeline_node_importable_from_kptn_graph():
    from kptn.graph import PipelineNode as _PipelineNode
    assert _PipelineNode is PipelineNode


def test_pipeline_node_in_kptn_graph_all():
    from kptn import graph
    assert "PipelineNode" in graph.__all__
    assert "Pipeline" in graph.__all__
