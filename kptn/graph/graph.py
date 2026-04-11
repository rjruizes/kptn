from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Union

from kptn.graph.nodes import TaskNode
from kptn.graph.decorators import TaskSpec


def _to_task_node(obj: Any) -> TaskNode:
    """Auto-wrap a kptn-tagged function into a TaskNode."""
    if isinstance(obj, TaskNode):
        return obj
    if not hasattr(obj, "__kptn__"):
        raise TypeError(
            f"Expected a @kptn.task-decorated callable, got {type(obj).__name__!r}. "
            "Decorate your function with @kptn.task before composing with >>."
        )
    spec: TaskSpec = obj.__kptn__
    return TaskNode(fn=obj, spec=spec, name=obj.__name__)


@dataclass
class Graph:
    nodes: list[TaskNode] = field(default_factory=list)
    edges: list[tuple[TaskNode, TaskNode]] = field(default_factory=list)

    @classmethod
    def _from_node(cls, obj: Any) -> "Graph":
        """Create a single-node Graph from a kptn-tagged callable."""
        node = _to_task_node(obj)
        return cls(nodes=[node], edges=[])

    def _heads(self) -> list[TaskNode]:
        """Nodes with no incoming edges in this graph."""
        has_incoming = {id(dst) for _, dst in self.edges}
        return [n for n in self.nodes if id(n) not in has_incoming]

    def _tails(self) -> list[TaskNode]:
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
        merged_nodes: list[TaskNode] = []
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
