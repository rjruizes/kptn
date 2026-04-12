from __future__ import annotations

import difflib
from typing import Any

from kptn.exceptions import ProfileError
from kptn.graph.graph import Graph
from kptn.graph.nodes import AnyNode, TaskNode, SqlTaskNode, RTaskNode, StageNode
from kptn.graph.pipeline import Pipeline
from kptn.graph.topo import topo_sort
from kptn.profiles.resolved import ResolvedGraph
from kptn.profiles.schema import KptnConfig, ProfileSpec


def _linearize(
    name: str,
    profiles: dict[str, ProfileSpec],
    seen: set[str],
    path: tuple[str, ...],
) -> list[str]:
    """Return profile names in ancestor-first topological order, deduplicated.

    Each profile appears at most once; diamond ancestors are scheduled via the
    first path that reaches them and skipped on subsequent paths.
    """
    if name not in profiles:
        if path:
            raise ProfileError(f"Profile '{path[-1]}' extends unknown profile '{name}'")
        raise ProfileError(f"Unknown profile '{name}'")
    if name in path:
        chain = " → ".join((*path, name))
        raise ProfileError(f"Circular extends detected: {chain}")
    if name in seen:
        return []

    spec = profiles[name]
    parents = (
        [spec.extends] if isinstance(spec.extends, str) else list(spec.extends)
    ) if spec.extends else []

    result: list[str] = []
    inner = (*path, name)
    for parent_name in parents:
        result.extend(_linearize(parent_name, profiles, seen, inner))

    seen.add(name)
    result.append(name)
    return result


def _resolve(name: str, profiles: dict[str, ProfileSpec]) -> ProfileSpec:
    order = _linearize(name, profiles, set(), ())

    merged_stage_selections: dict[str, list[str]] = {}
    merged_optional_groups: dict[str, bool] = {}
    merged_args: dict[str, dict[str, Any]] = {}

    for profile_name in order:
        spec = profiles[profile_name]
        # stage_selections: additive — append own branches
        for stage, branches in spec.stage_selections.items():
            merged_stage_selections.setdefault(stage, []).extend(branches)
        # optional_groups: last-write-wins
        merged_optional_groups.update(spec.optional_groups)
        # args: per-task deep merge — rightmost wins per param key
        for task, params in spec.args.items():
            if task not in merged_args:
                merged_args[task] = {}
            merged_args[task].update(params)

    child_spec = profiles[name]
    return ProfileSpec(
        extends=None,
        stage_selections=merged_stage_selections,
        optional_groups=merged_optional_groups,
        args=merged_args,
        start_from=child_spec.start_from,
        stop_after=child_spec.stop_after,
    )


def _prune(graph: Graph, profile: ProfileSpec) -> Graph:
    """Return a new Graph with inactive Stage branches and disabled optional nodes removed.

    Two-phase:
    1. Seed dead set: inactive stage branch heads + disabled optional task nodes.
    2. Forward propagation: any non-seed node whose ALL predecessors are dead becomes dead.

    Nodes with zero predecessors (source nodes) can only be dead if explicitly seeded.
    StageNode and ParallelNode sentinels are never seeded — they are always retained.
    """
    predecessors: dict[int, list[AnyNode]] = {id(n): [] for n in graph.nodes}
    for src, dst in graph.edges:
        predecessors[id(dst)].append(src)

    dead_ids: set[int] = set()

    # Phase 1a: Stage branch pruning
    for src, dst in graph.edges:
        if isinstance(src, StageNode):
            active_names = profile.stage_selections.get(src.name)
            if active_names is not None and dst.name not in active_names:
                dead_ids.add(id(dst))

    # Phase 1b: Optional group pruning
    for node in graph.nodes:
        if isinstance(node, (TaskNode, SqlTaskNode, RTaskNode)):
            opt = node.spec.optional
            if opt is not None and not profile.optional_groups.get(f"*.{opt}", False):
                dead_ids.add(id(node))

    # Phase 2: Forward propagation
    changed = True
    while changed:
        changed = False
        for node in graph.nodes:
            if id(node) in dead_ids:
                continue
            preds = predecessors[id(node)]
            if preds and all(id(p) in dead_ids for p in preds):
                dead_ids.add(id(node))
                changed = True

    return Graph(
        nodes=[n for n in graph.nodes if id(n) not in dead_ids],
        edges=[(s, d) for s, d in graph.edges if id(s) not in dead_ids and id(d) not in dead_ids],
    )


def _validate_stage_refs(
    graph: Graph,
    profile: ProfileSpec,
    profile_name: str,
) -> None:
    """Raise ProfileError if any stage_selections branch name is absent from the graph.

    Validates that every branch referenced in profile.stage_selections exists as
    a direct child of the corresponding StageNode in the graph.

    Silent-skip behavior: if a stage_name in stage_selections has no corresponding
    StageNode in the graph, it is skipped (consistent with _prune's behavior).

    Must be called BEFORE _prune() so all branches are still visible in the graph.
    """
    stage_branches: dict[str, set[str]] = {}
    for src, dst in graph.edges:
        if isinstance(src, StageNode):
            stage_branches.setdefault(src.name, set()).add(dst.name)

    for stage_name, branch_refs in profile.stage_selections.items():
        if stage_name not in stage_branches:
            continue  # stage absent from graph — silently ignored (mirrors _prune)
        valid = stage_branches[stage_name]
        for ref in branch_refs:
            if ref not in valid:
                matches = difflib.get_close_matches(ref, list(valid), n=1, cutoff=0.6)
                suggestion = f" Did you mean '{matches[0]}'?" if matches else ""
                raise ProfileError(
                    f"profile '{profile_name}' stage '{stage_name}' "
                    f"references unknown pipeline '{ref}'.{suggestion}"
                )


def _apply_cursors(graph: Graph, profile: ProfileSpec) -> tuple[Graph, frozenset[str]]:
    """Apply start_from and stop_after cursor operations to a pruned Graph.

    start_from:  all nodes topologically before the cursor are bypass-flagged.
                 They remain in the graph but are recorded in bypassed_names.
    stop_after:  all nodes topologically after the cursor are pruned from the graph.

    Stage atomicity: if a cursor references a direct child of a StageNode OR the
    StageNode sentinel itself:
      - start_from: cursor anchor = the StageNode sentinel (bypass before it)
      - stop_after:  cursor anchor = last branch of the Stage group in topo order
                     (so all branches are retained)

    Single-node slice: start_from == stop_after on the same node is valid.
    Predecessors are bypass-flagged; successors are pruned; the named node runs.

    Raises ProfileError on unknown cursor targets or start/stop conflict (stop
    is strictly before start in topo order).

    Returns: (modified_graph, bypassed_names)
    """
    if profile.start_from is None and profile.stop_after is None:
        return graph, frozenset()

    ordered: list[AnyNode] = topo_sort(graph)
    idx_of: dict[int, int] = {id(n): i for i, n in enumerate(ordered)}
    name_to_node: dict[str, AnyNode] = {n.name: n for n in ordered}

    # Map branch name → its direct StageNode parent
    stage_of: dict[str, StageNode] = {}
    stage_branches: dict[str, list[AnyNode]] = {}
    for src, dst in graph.edges:
        if isinstance(src, StageNode):
            stage_of[dst.name] = src
            stage_branches.setdefault(src.name, []).append(dst)

    def _did_you_mean(name: str) -> str:
        matches = difflib.get_close_matches(name, list(name_to_node), n=1, cutoff=0.6)
        return f" Did you mean '{matches[0]}'?" if matches else ""

    def _resolve_start_idx(cursor_name: str) -> int:
        if cursor_name not in name_to_node:
            raise ProfileError(
                f"'start_from' references unknown node '{cursor_name}'.{_did_you_mean(cursor_name)}"
            )
        if cursor_name in stage_of:
            # Stage atomicity: cursor = StageNode sentinel
            return idx_of[id(stage_of[cursor_name])]
        return idx_of[id(name_to_node[cursor_name])]

    def _resolve_stop_idx(cursor_name: str) -> int:
        if cursor_name not in name_to_node:
            raise ProfileError(
                f"'stop_after' references unknown node '{cursor_name}'.{_did_you_mean(cursor_name)}"
            )
        if cursor_name in stage_of:
            # Stage atomicity for branch cursor: stop after last branch in topo order
            return max(idx_of[id(b)] for b in stage_branches[stage_of[cursor_name].name])
        if isinstance(name_to_node[cursor_name], StageNode):
            # Stage atomicity for StageNode-name cursor: stop after last branch
            branches = stage_branches.get(cursor_name, [])
            if branches:
                return max(idx_of[id(b)] for b in branches)
        return idx_of[id(name_to_node[cursor_name])]

    start_idx: int | None = None
    stop_idx: int | None = None

    if profile.start_from is not None:
        start_idx = _resolve_start_idx(profile.start_from)
    if profile.stop_after is not None:
        stop_idx = _resolve_stop_idx(profile.stop_after)

    # Conflict: stop_after before start_from in topo order
    if start_idx is not None and stop_idx is not None and stop_idx < start_idx:
        raise ProfileError(
            f"'stop_after' ({profile.stop_after!r}) is topologically before "
            f"'start_from' ({profile.start_from!r})"
        )

    bypassed_names: frozenset[str] = (
        frozenset(ordered[i].name for i in range(start_idx))
        if start_idx is not None
        else frozenset()
    )

    if stop_idx is not None:
        keep_ids = {id(ordered[i]) for i in range(stop_idx + 1)}
        pruned_nodes = [n for n in graph.nodes if id(n) in keep_ids]
        pruned_edges = [(s, d) for s, d in graph.edges if id(s) in keep_ids and id(d) in keep_ids]
        graph = Graph(nodes=pruned_nodes, edges=pruned_edges)

    return graph, bypassed_names


class ProfileResolver:
    """Resolves ProfileSpec `extends` chains into a fully merged ProfileSpec."""

    def __init__(self, config: KptnConfig) -> None:
        self._profiles = dict(config.profiles)
        self._settings = config.settings

    def resolve(self, name: str) -> ProfileSpec:
        """Return a fully merged ProfileSpec for the named profile.

        Raises:
            ProfileError: if name is unknown, any parent is unknown, or a cycle is detected.
        """
        if name not in self._profiles:
            raise ProfileError(f"Unknown profile '{name}'")
        return _resolve(name, self._profiles)

    def compile(self, pipeline: Pipeline, profile_name: str) -> ResolvedGraph:
        """Compile a Pipeline + profile into a pruned, immutable ResolvedGraph.

        Raises:
            ProfileError: if profile_name is unknown (delegated to resolve()).
        """
        profile = self.resolve(profile_name)
        _validate_stage_refs(pipeline, profile, profile_name)
        pruned = _prune(pipeline, profile)
        storage_key = self._settings.db_path or ".kptn/kptn.db"
        final_graph, bypassed_names = _apply_cursors(pruned, profile)
        return ResolvedGraph(
            graph=final_graph,
            pipeline=pipeline.name,
            storage_key=storage_key,
            bypassed_names=bypassed_names,
            profile_args=dict(profile.args),
        )
