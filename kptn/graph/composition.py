from __future__ import annotations

from typing import TYPE_CHECKING, Any

from kptn.graph.nodes import AnyNode, ParallelNode, StageNode

if TYPE_CHECKING:
    from kptn.graph.graph import Graph   # avoids circular at runtime


def parallel(*args: Any) -> "Graph":
    """Group branches as always-active parallel tasks.

    Usage:
        kptn.parallel(A, B, C)           # anonymous parallel group (name="parallel")
        kptn.parallel("etl", A, B, C)    # named parallel group

    Returns a Graph with:
      - ParallelNode as sentinel head (fan-out point)
      - Edges: ParallelNode → head(branch) for each branch
      - Branch tails become the Graph's tails (fan-in when composed with >>)

    P0 execution: sequential in declaration order; parallelization deferred to P2+.
    The graph model correctly expresses the parallel intent.
    """
    from kptn.graph.graph import Graph

    # Parse optional name as first positional arg (str only)
    if args and isinstance(args[0], str):
        name = args[0]
        branch_args = args[1:]
    else:
        name = "parallel"
        branch_args = args

    if not branch_args:
        raise ValueError("parallel() requires at least one branch")

    sentinel = ParallelNode(name=name)

    all_nodes: list[AnyNode] = [sentinel]
    all_edges: list[tuple[AnyNode, AnyNode]] = []
    seen_ids: set[int] = {id(sentinel)}

    for branch in branch_args:
        branch_graph: Graph = branch if isinstance(branch, Graph) else Graph._from_node(branch)

        for n in branch_graph.nodes:
            if id(n) not in seen_ids:
                seen_ids.add(id(n))
                all_nodes.append(n)

        all_edges.extend(branch_graph.edges)

        # Fan-out: sentinel → each head of the branch sub-graph
        for head in branch_graph._heads():
            all_edges.append((sentinel, head))

    return Graph(nodes=all_nodes, edges=all_edges)


def Stage(name: str, *branches: Any) -> "Graph":
    """Group branches as profile-conditional selection options.

    Usage:
        kptn.Stage("data_sources", A, B)

    Returns a Graph with:
      - StageNode(name=name) as sentinel head
      - Edges: StageNode → head(branch) for each branch
      - All branches retained — no pruning at graph-build time

    Selection is deferred to Epic 3 (ResolvedGraph / profile resolver).
    Stage atomicity: if start_from/stop_after reference a node within a Stage group,
    the ENTIRE group is the cursor unit. This is enforced in Epic 3.
    """
    from kptn.graph.graph import Graph

    if not branches:
        raise ValueError("Stage() requires at least one branch")

    sentinel = StageNode(name=name)

    all_nodes: list[AnyNode] = [sentinel]
    all_edges: list[tuple[AnyNode, AnyNode]] = []
    seen_ids: set[int] = {id(sentinel)}

    for branch in branches:
        branch_graph: Graph = branch if isinstance(branch, Graph) else Graph._from_node(branch)

        for n in branch_graph.nodes:
            if id(n) not in seen_ids:
                seen_ids.add(id(n))
                all_nodes.append(n)

        all_edges.extend(branch_graph.edges)

        for head in branch_graph._heads():
            all_edges.append((sentinel, head))

    return Graph(nodes=all_nodes, edges=all_edges)


# TODO: Story 1.5+ — map() factory (MapNode, dynamic fanout)
