from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Union

from kptn.graph.nodes import TaskNode, SqlTaskNode, RTaskNode, ParallelNode, StageNode, NoopNode, MapNode, PipelineNode, AnyNode
from kptn.graph.decorators import SqlTaskSpec, RTaskSpec


def _to_task_node(obj: Any) -> AnyNode:
    """Auto-wrap a kptn-tagged handle or node into the correct node type."""
    if isinstance(obj, (TaskNode, SqlTaskNode, RTaskNode, ParallelNode, StageNode, NoopNode, MapNode, PipelineNode)):
        return obj
    if not hasattr(obj, "__kptn__"):
        raise TypeError(
            f"Expected a @kptn.task-decorated callable, a kptn task handle "
            f"(sql_task/r_task), or a kptn graph operator result (parallel/Stage/noop/map/Pipeline), "
            f"got {type(obj).__name__!r}. "
            "Use @kptn.task, kptn.sql_task(), kptn.r_task(), kptn.noop(), "
            "kptn.parallel(), kptn.Stage(), kptn.map(), or kptn.Pipeline() before composing with >>."
        )
    spec = obj.__kptn__
    if isinstance(spec, SqlTaskSpec):
        return SqlTaskNode(path=spec.path, spec=spec, name=obj.__name__)
    elif isinstance(spec, RTaskSpec):
        return RTaskNode(path=spec.path, spec=spec, name=obj.__name__)
    else:
        # TaskSpec — standard Python task
        return TaskNode(fn=obj, spec=spec, name=obj.__name__)


@dataclass
class Graph:
    nodes: list[AnyNode] = field(default_factory=list)
    edges: list[tuple[AnyNode, AnyNode]] = field(default_factory=list)

    @classmethod
    def _from_node(cls, obj: Any) -> "Graph":
        """Create a single-node Graph from a kptn-tagged callable."""
        node = _to_task_node(obj)
        return cls(nodes=[node], edges=[])

    def _heads(self) -> list[AnyNode]:
        """Nodes with no incoming edges in this graph."""
        has_incoming = {id(dst) for _, dst in self.edges}
        return [n for n in self.nodes if id(n) not in has_incoming]

    def _tails(self) -> list[AnyNode]:
        """Nodes with no outgoing edges in this graph."""
        has_outgoing = {id(src) for src, _ in self.edges}
        return [n for n in self.nodes if id(n) not in has_outgoing]

    def __rshift__(self, other: Union["Graph", Any]) -> "Graph":
        """
        Sequential composition operator.
        Always returns a new Graph — never mutates self.
        `other` can be a Graph or a __kptn__-tagged callable.
        """
        if isinstance(other, Graph):
            other_graph = other
        else:
            other_graph = Graph._from_node(other)

        # Deduplicate nodes by identity
        seen: set[int] = set()
        merged_nodes: list[AnyNode] = []
        for n in self.nodes + other_graph.nodes:
            if id(n) not in seen:
                seen.add(id(n))
                merged_nodes.append(n)

        # Cross edges: each tail of self → each head of other
        cross_edges = [
            (tail, head)
            for tail in self._tails()
            for head in other_graph._heads()
        ]

        merged_edges = self.edges + other_graph.edges + cross_edges

        return Graph(nodes=merged_nodes, edges=merged_edges)

    def __rrshift__(self, other: Any) -> "Graph":
        """Support `fn >> Graph(...)` when fn is not a Graph."""
        left = Graph._from_node(other)
        return left >> self
