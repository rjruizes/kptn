from __future__ import annotations

from kptn.graph.graph import Graph
from kptn.graph.nodes import AnyNode
from kptn.exceptions import GraphError


def topo_sort(graph: Graph) -> list[AnyNode]:
    """
    Topological sort of graph nodes using a depth-first variant of Kahn's algorithm.

    Uses a LIFO stack instead of a FIFO queue so that sequential chains (e.g. all
    tasks inside a Pipeline branch) are kept together in the output rather than
    being interleaved with sibling branches.  Successors are pushed in reversed
    declaration order so the first-declared successor is processed first.

    Raises GraphError if a cycle is detected.
    Returns nodes in dependency order (predecessors before successors).
    """
    # Build adjacency and in-degree maps keyed by object identity
    in_degree: dict[int, int] = {id(n): 0 for n in graph.nodes}
    successors: dict[int, list[AnyNode]] = {id(n): [] for n in graph.nodes}
    node_by_id: dict[int, AnyNode] = {id(n): n for n in graph.nodes}

    for src, dst in graph.edges:
        if id(src) not in in_degree or id(dst) not in in_degree:
            missing = [n.name for n in (src, dst) if id(n) not in in_degree]
            raise GraphError(
                f"Graph edge references node(s) not registered in graph.nodes: {missing}"
            )
        successors[id(src)].append(dst)
        in_degree[id(dst)] += 1

    # Seed the stack in reversed declaration order so the first-declared node
    # ends up on top and is processed first.
    stack: list[AnyNode] = list(
        reversed([n for n in graph.nodes if in_degree[id(n)] == 0])
    )

    result: list[AnyNode] = []

    while stack:
        node = stack.pop()
        result.append(node)
        # Push successors in reversed declaration order so the first-declared
        # successor lands on top of the stack and is processed next, keeping
        # each pipeline's task chain contiguous in the output.
        for successor in reversed(successors[id(node)]):
            in_degree[id(successor)] -= 1
            if in_degree[id(successor)] == 0:
                stack.append(successor)

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
