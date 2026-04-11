from __future__ import annotations

from collections import deque

from kptn.graph.graph import Graph
from kptn.graph.nodes import TaskNode
from kptn.exceptions import GraphError


def topo_sort(graph: Graph) -> list[TaskNode]:
    """
    Topological sort of graph nodes using Kahn's algorithm (BFS-based).
    Raises GraphError if a cycle is detected.
    Returns nodes in dependency order (predecessors before successors).
    """
    # Build adjacency and in-degree maps keyed by object identity
    in_degree: dict[int, int] = {id(n): 0 for n in graph.nodes}
    successors: dict[int, list[TaskNode]] = {id(n): [] for n in graph.nodes}
    node_by_id: dict[int, TaskNode] = {id(n): n for n in graph.nodes}

    for src, dst in graph.edges:
        if id(src) not in in_degree or id(dst) not in in_degree:
            missing = [n.name for n in (src, dst) if id(n) not in in_degree]
            raise GraphError(
                f"Graph edge references node(s) not registered in graph.nodes: {missing}"
            )
        successors[id(src)].append(dst)
        in_degree[id(dst)] += 1

    queue: deque[TaskNode] = deque(
        n for n in graph.nodes if in_degree[id(n)] == 0
    )

    result: list[TaskNode] = []

    while queue:
        node = queue.popleft()
        result.append(node)
        for successor in successors[id(node)]:
            in_degree[id(successor)] -= 1
            if in_degree[id(successor)] == 0:
                queue.append(successor)

    if len(result) < len(graph.nodes):
        remaining = [
            node_by_id[nid]
            for nid, deg in in_degree.items()
            if deg > 0
        ]
        raise GraphError(
            f"Cycle detected in graph. Nodes not sorted: {[n.name for n in remaining]}"
        )

    return result
