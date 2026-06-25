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


def _any_of_groups(node: AnyNode) -> list[AnyOf]:
    if not isinstance(node, _TASK_NODES):
        return []
    reqs = getattr(node.spec, "requires", None) or []
    return [r for r in reqs if isinstance(r, AnyOf)]


def gate_disjunctive(graph: Graph) -> Graph:
    """Drop consumers whose disjunctive (``any_of``) requirements are unmet.

    A node with ``any_of`` group(s) survives only if every group has at least
    one member present (by name) in the surviving graph. Dropping a node:
      - bypass-reconnects its structural (``>>``) predecessors to its successors,
      - propagates the drop along requires-edges (a task that conjunctively
        required the dropped node is dropped too).
    Iterates to a fixpoint. Returns a new Graph; never mutates the input.
    """
    requires_edge_set: set[tuple[int, int]] = set(graph.requires_edges)

    # requires-successor map: prereq id -> [requirer ids]
    requires_succ: dict[int, list[int]] = {}
    for src_id, dst_id in requires_edge_set:
        requires_succ.setdefault(src_id, []).append(dst_id)

    dropped: set[int] = set()

    def _drop(node_id: int) -> None:
        if node_id in dropped:
            return
        dropped.add(node_id)
        for requirer_id in requires_succ.get(node_id, []):
            _drop(requirer_id)  # propagate through requires-edges

    changed = True
    while changed:
        changed = False
        present_names = {n.name for n in graph.nodes if id(n) not in dropped}
        for node in graph.nodes:
            if id(node) in dropped:
                continue
            groups = _any_of_groups(node)
            if not groups:
                continue
            unmet = any(
                not any(member.__name__ in present_names for member in group.members)
                for group in groups
            )
            if unmet:
                before = len(dropped)
                _drop(id(node))
                if len(dropped) != before:
                    changed = True

    if not dropped:
        return graph

    # Structural (non-requires) adjacency, for bypass reconnection.
    structural_pred: dict[int, list[AnyNode]] = {id(n): [] for n in graph.nodes}
    structural_succ: dict[int, list[AnyNode]] = {id(n): [] for n in graph.nodes}
    for src, dst in graph.edges:
        if (id(src), id(dst)) in requires_edge_set:
            continue
        structural_succ[id(src)].append(dst)
        structural_pred[id(dst)].append(src)

    def _surviving(start: AnyNode, adj: dict[int, list[AnyNode]]) -> list[AnyNode]:
        out: list[AnyNode] = []
        seen: set[int] = {id(start)}
        queue: list[AnyNode] = list(adj[id(start)])
        while queue:
            n = queue.pop(0)
            if id(n) in seen:
                continue
            seen.add(id(n))
            if id(n) not in dropped:
                out.append(n)
            else:
                queue.extend(adj[id(n)])
        return out

    existing: set[tuple[int, int]] = {(id(s), id(d)) for s, d in graph.edges}
    bypass: list[tuple[AnyNode, AnyNode]] = []
    for node in graph.nodes:
        if id(node) not in dropped:
            continue
        for pred in _surviving(node, structural_pred):
            for succ in _surviving(node, structural_succ):
                key = (id(pred), id(succ))
                if key not in existing:
                    existing.add(key)
                    bypass.append((pred, succ))

    keep_ids = {id(n) for n in graph.nodes} - dropped
    new_nodes = [n for n in graph.nodes if id(n) in keep_ids]
    new_edges = [(s, d) for s, d in graph.edges if id(s) in keep_ids and id(d) in keep_ids] + bypass
    new_requires_edges = {
        (s, d) for (s, d) in requires_edge_set if s in keep_ids and d in keep_ids
    }
    return Graph(nodes=new_nodes, edges=new_edges, requires_edges=new_requires_edges)


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
