from __future__ import annotations

from kptn.graph.graph import Graph
from kptn.graph.nodes import PipelineNode


class Pipeline(Graph):
    """Named pipeline group — composes with >> like any Graph.

    Usage:
        ingest = kptn.Pipeline("ingest", task_a >> task_b >> task_c)
        transform = kptn.Pipeline("transform", task_d >> task_e)
        full_graph = ingest >> transform   # returns a Graph

    Pipeline wraps the inner graph with a PipelineNode sentinel head.
    The runner (Epic 2) uses PipelineNode to identify pipeline scope.
    """

    def __init__(self, name: str, graph: Graph) -> None:
        # Auto-wrap graph in Graph.from_node if user passed a single node instead of a Graph, for convenience.
        # This allows users to write kptn.Pipeline("foo", task_a) instead of kptn.Pipeline("foo", kptn.Graph.from_node(task_a)).
        if not isinstance(graph, Graph):
            graph = Graph._from_node(graph)
        sentinel = PipelineNode(name=name)
        all_nodes = [sentinel] + graph.nodes
        cross_edges = [(sentinel, h) for h in graph._heads()]
        all_edges = graph.edges + cross_edges
        super().__init__(nodes=all_nodes, edges=all_edges)
        self._name = name  # stored separately — not a dataclass field

    @property
    def name(self) -> str:
        return self._name

    def run(self, *, profile: str | None = None, keep_db_open: bool = False):
        """Run this pipeline — equivalent to kptn.run(pipeline, ...).

        Parameters
        ----------
        profile:
            Optional profile name to resolve from ``kptn.yaml``.
        keep_db_open:
            When ``True`` and the pipeline declares ``kptn.config(duckdb=get_engine)``,
            the DuckDB connection is left open and returned.  Useful in test fixtures::

                @pytest.fixture(scope="module")
                def engine():
                    return main_pipeline.run(keep_db_open=True)

        Returns
        -------
        The live ``duckdb.DuckDBPyConnection`` when ``keep_db_open=True`` and a
        duckdb factory was declared; ``None`` otherwise.
        """
        from kptn.runner.api import run as _run  # lazy import avoids circular dependency
        return _run(self, profile=profile, keep_db_open=keep_db_open)
