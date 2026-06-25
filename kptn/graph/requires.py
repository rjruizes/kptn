from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

from kptn.graph.graph import Graph, _to_task_node
from kptn.graph.nodes import AnyNode, RTaskNode, SqlTaskNode, TaskNode


@dataclass(frozen=True)
class AnyOf:
    """A disjunctive requirement group: satisfied if any member is present.

    Created via :func:`any_of`. Holds kptn task handles (objects carrying
    ``__kptn__``). Never pulls tasks into the run — see ``gate_disjunctive``.
    """

    members: tuple[Any, ...]

_TASK_NODES = (TaskNode, SqlTaskNode, RTaskNode)


def _conjunctive_handles(spec: Any) -> list[Any]:
    """Return the conjunctive (handle) entries of a spec's requires list."""
    reqs = getattr(spec, "requires", None) or []
    return [r for r in reqs if not isinstance(r, AnyOf)]


def expand_requires(graph: Graph) -> Graph:
    """Pull conjunctive ``requires`` prerequisites into the graph (transitively).

    For every task node whose spec lists conjunctive requirements:
      - A prerequisite already present in the graph (user-placed) is a no-op.
      - Otherwise it is injected as exactly one shared node (deduped by name),
        with a tagged requires-edge to every requirer.
    Disjunctive (``any_of``) entries are ignored here — see ``gate_disjunctive``.

    Returns a new Graph; never mutates the input.
    """
    original_names = {n.name for n in graph.nodes}
    nodes: list[AnyNode] = list(graph.nodes)
    edges: list[tuple[AnyNode, AnyNode]] = list(graph.edges)
    requires_edges: set[tuple[int, int]] = set(graph.requires_edges)
    injected: dict[str, AnyNode] = {}
    existing_edges: set[tuple[int, int]] = {(id(s), id(d)) for s, d in edges}

    queue: deque[TaskNode | SqlTaskNode | RTaskNode] = deque(
        n for n in graph.nodes if isinstance(n, _TASK_NODES)
    )
    while queue:
        requirer = queue.popleft()
        for handle in _conjunctive_handles(requirer.spec):
            name = handle.__name__
            if name in original_names:
                continue  # user-placed: existing wiring governs ordering
            node = injected.get(name)
            if node is None:
                node = _to_task_node(handle)
                injected[name] = node
                nodes.append(node)
                if isinstance(node, (TaskNode, SqlTaskNode, RTaskNode)):
                    queue.append(node)  # transitive closure
            key = (id(node), id(requirer))
            if key not in existing_edges:
                edges.append((node, requirer))
                existing_edges.add(key)
                requires_edges.add(key)
    return Graph(nodes=nodes, edges=edges, requires_edges=requires_edges)


def any_of(*handles: Any) -> AnyOf:
    """Group task handles into a disjunctive (OR) requirement.

    Usage::

        @kptn.task(outputs=["combined"], requires=[kptn.any_of(A, B)])
        def consumer(): ...

    Raises:
        ValueError: if no handles are given.
        TypeError: if a member is not a kptn task handle.
    """
    if not handles:
        raise ValueError("any_of() requires at least one task")
    for h in handles:
        if not hasattr(h, "__kptn__"):
            raise TypeError(
                f"any_of() expects @kptn.task / sql_task / r_task handles, "
                f"got {type(h).__name__!r}."
            )
    return AnyOf(members=tuple(handles))
