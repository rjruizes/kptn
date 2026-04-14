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
        sentinel = PipelineNode(name=name)
        all_nodes = [sentinel] + graph.nodes
        cross_edges = [(sentinel, h) for h in graph._heads()]
        all_edges = graph.edges + cross_edges
        super().__init__(nodes=all_nodes, edges=all_edges)
        self._name = name  # stored separately — not a dataclass field

    @property
    def name(self) -> str:
        return self._name

    def run(self, *, profile: str | None = None) -> None:
        """Run this pipeline — equivalent to kptn.run(pipeline, profile=profile)."""
        from kptn.runner.api import run as _run  # lazy import avoids circular dependency
        _run(self, profile=profile)
