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


def _optional_group_enabled(
    optional_groups: dict[str, bool],
    opt: str,
    pipeline_name: str | None = None,
) -> bool:
    """Return True if the optional group *opt* is enabled.

    Key matching priority (last matching key wins, mirroring resolve merge order):
    - Bare key: ``"validate"`` → matches opt unconditionally
    - Wildcard-scoped: ``"*.validate"`` → matches opt in any pipeline
    - Pipeline-scoped: ``"ci.validate"`` → matches opt only when pipeline_name == "ci"

    This allows the YAML shorthand flat notation (``validate: true``) and the
    explicit nested form (``optional_groups: {"*.validate": true}``) to both work.
    """
    result = False
    for key, enabled in optional_groups.items():
        if "." in key:
            scope, group = key.split(".", 1)
            if group != opt:
                continue
            if scope == "*" or scope == pipeline_name:
                result = enabled
        elif key == opt:
            result = enabled
    return result


def _prune(graph: Graph, profile: ProfileSpec, pipeline_name: str | None = None) -> Graph:
    """Return a new Graph with inactive Stage branches and disabled optional nodes removed.

    Four phases:
    1a. Seed dead_ids: inactive stage branch heads (Stage branch pruning).
    1b. Seed optional_dead_ids (separate set): disabled optional task nodes.
        This set is intentionally separate from dead_ids — bypass injection applies
        ONLY to optional_dead_ids nodes (D-01 guard).
    2.  Forward cascade. optional_dead_ids nodes normally act as a firewall:
        a node whose only dead predecessors are optional-dead is NOT cascade-killed
        (D-02 guard). Exception (D-02b): if an optional-dead node itself has ALL
        predecessors in dead_ids, it is inside a fully dead stage branch — it is
        moved from optional_dead_ids into dead_ids so cascade continues through it.
    3.  Bypass injection: for each optional-dead node, BFS backwards through optional-dead
        intermediaries to find transitive surviving predecessors, and BFS forwards through
        optional-dead intermediaries to find transitive surviving successors. Add a bypass
        edge for every surviving_pred × surviving_succ pair not already in the graph (D-03,
        D-04 dedup via seen_bypass set). Also inject D-05 stage-exit bypass edges.

    D-05 (Stage exit): If ALL branches of a Stage (N >= 2) are dead, any node immediately
    downstream (connected via branch-tail → node edges) is NOT cascade-killed. The exit node
    is detected because its backward BFS through dead nodes reaches ALL N branch heads of that
    Stage. Internal branch nodes only trace to ONE branch head and are correctly cascade-killed.
    A bypass edge StageNode → exit_node is injected so the pipeline continues past the empty
    Stage.

    Nodes with zero predecessors (source nodes) can only be dead if explicitly seeded.
    StageNode and ParallelNode sentinels are never seeded — they are always retained.
    """
    predecessors: dict[int, list[AnyNode]] = {id(n): [] for n in graph.nodes}
    successors: dict[int, list[AnyNode]] = {id(n): [] for n in graph.nodes}
    for src, dst in graph.edges:
        predecessors[id(dst)].append(src)
        successors[id(src)].append(dst)

    # Phase 1a: Stage branch pruning — seeds dead_ids (stage-selection-dead only).
    # seeded_dead_ids tracks the exact set of branch heads killed here; cascade-dead nodes
    # added in Phase 2 are NOT in seeded_dead_ids. This distinction drives the D-05 check.
    dead_ids: set[int] = set()
    seeded_dead_ids: set[int] = set()
    for src, dst in graph.edges:
        if isinstance(src, StageNode):
            active_names = profile.stage_selections.get(src.name)
            if active_names is not None and dst.name not in active_names:
                dead_ids.add(id(dst))
                seeded_dead_ids.add(id(dst))

    # Phase 1b: Optional group pruning — seeds optional_dead_ids (separate from dead_ids)
    # D-01: bypass injection applies only to optional_dead_ids; stage-killed nodes must not
    # receive bypass treatment.
    optional_dead_ids: set[int] = set()
    for node in graph.nodes:
        if isinstance(node, (TaskNode, SqlTaskNode, RTaskNode)):
            opt = node.spec.optional
            if opt is not None and not _optional_group_enabled(profile.optional_groups, opt, pipeline_name):
                optional_dead_ids.add(id(node))

    # Phase 2: Forward cascade from dead_ids only.
    # D-02: optional_dead_ids nodes are NOT in dead_ids by default, so a node that has an
    # optional-dead predecessor is NOT cascade-killed by that predecessor (optional-dead acts
    # as a firewall for alive predecessor chains — bypass injection handles those in Phase 3).
    # D-02b: If an optional-dead node itself has ALL predecessors in dead_ids, it is inside a
    # fully dead branch and no bypass can help it (no alive source to route around it). Move it
    # from optional_dead_ids into dead_ids so the cascade can continue through it.
    # D-05 (Stage exit): If ALL branches of a Stage (N >= 2) are dead and a node's backward BFS
    # through dead nodes reaches ALL branch heads of that Stage, it is a "stage exit" node —
    # downstream of every dead branch tail. Do NOT cascade-kill it; inject a bypass
    # StageNode → exit_node instead. Internal branch nodes only trace to ONE branch head and
    # are correctly cascade-killed.

    # Pre-compute fully-dead stages for D-05.
    # A Stage is fully-dead when ALL its direct successors are in seeded_dead_ids.
    # We require N >= 2 branches: for single-branch stages every internal node also traces
    # back to that one branch head, so exit detection would be ambiguous.
    _stage_branch_heads: dict[int, list[int]] = {}  # stage_id → [seeded branch head ids]
    _stage_node_by_id: dict[int, StageNode] = {}
    for src, dst in graph.edges:
        if isinstance(src, StageNode):
            _stage_node_by_id[id(src)] = src  # type: ignore[assignment]
            _stage_branch_heads.setdefault(id(src), []).append(id(dst))

    fully_dead_stages: list[tuple[StageNode, frozenset[int]]] = []
    for stage_id, branch_head_ids in _stage_branch_heads.items():
        if len(branch_head_ids) >= 2 and all(bhid in seeded_dead_ids for bhid in branch_head_ids):
            fully_dead_stages.append(
                (_stage_node_by_id[stage_id], frozenset(branch_head_ids))
            )

    def _find_seeded_dead_roots(start: AnyNode) -> set[int]:
        """BFS backward from start through dead nodes; collect seeded-dead roots."""
        roots: set[int] = set()
        queue: list[AnyNode] = list(predecessors[id(start)])
        visited: set[int] = {id(start)}
        while queue:
            n = queue.pop(0)
            if id(n) in visited:
                continue
            visited.add(id(n))
            if id(n) in seeded_dead_ids:
                roots.add(id(n))
            elif id(n) in dead_ids:
                for pred in predecessors[id(n)]:
                    if id(pred) not in visited:
                        queue.append(pred)
        return roots

    def _d05_stage_ancestor(node: AnyNode) -> "StageNode | None":
        """Return the StageNode if node is a D-05 stage exit; otherwise None.

        A stage exit has backward roots that cover ALL seeded branch heads of some
        fully-dead Stage. Internal branch nodes trace to only ONE branch's head.
        """
        roots = _find_seeded_dead_roots(node)
        for stage_node, branch_heads in fully_dead_stages:
            if branch_heads.issubset(roots):
                return stage_node
        return None

    stage_exits: list[tuple[AnyNode, AnyNode]] = []
    stage_exit_ids: set[int] = set()  # avoid re-processing in subsequent while iterations

    changed = True
    while changed:
        changed = False
        for node in graph.nodes:
            if id(node) in dead_ids or id(node) in stage_exit_ids:
                continue
            preds = predecessors[id(node)]
            if not preds or not all(id(p) in dead_ids for p in preds):
                continue
            if id(node) in optional_dead_ids:
                # D-02b: fully-dead-branch optional node — cascade continues through it
                dead_ids.add(id(node))
                optional_dead_ids.discard(id(node))
                changed = True
            else:
                # D-05: check if backward roots cover all branches of a fully-dead Stage
                stage_anc = _d05_stage_ancestor(node)
                if stage_anc is not None:
                    stage_exits.append((stage_anc, node))
                    stage_exit_ids.add(id(node))
                else:
                    dead_ids.add(id(node))
                    changed = True

    # Phase 3: Bypass injection.
    # D-03: BFS is transitive — it traverses through chained optional-dead intermediaries
    # to find the nearest surviving predecessors and successors.
    # D-04: seen_bypass is seeded from all original edges so that bypass never duplicates
    # an edge that already exists in the graph.
    all_dead_ids: set[int] = dead_ids | optional_dead_ids

    def _bfs_surviving_preds(start: AnyNode) -> list[AnyNode]:
        """BFS backwards: collect surviving predecessors, traverse through optional-dead nodes."""
        result: list[AnyNode] = []
        queue: list[AnyNode] = [start]
        visited: set[int] = {id(start)}
        while queue:
            node = queue.pop(0)
            for pred in predecessors[id(node)]:
                if id(pred) in visited:
                    continue
                visited.add(id(pred))
                if id(pred) not in all_dead_ids:
                    result.append(pred)
                elif id(pred) in optional_dead_ids:
                    # Continue BFS through chained optional-dead intermediaries (D-03)
                    queue.append(pred)
                # Stage-cascade-dead preds: do not collect, do not traverse
        return result

    def _bfs_surviving_succs(start: AnyNode) -> list[AnyNode]:
        """BFS forwards: collect surviving successors, traverse through optional-dead nodes."""
        result: list[AnyNode] = []
        queue: list[AnyNode] = [start]
        visited: set[int] = {id(start)}
        while queue:
            node = queue.pop(0)
            for succ in successors[id(node)]:
                if id(succ) in visited:
                    continue
                visited.add(id(succ))
                if id(succ) not in all_dead_ids:
                    result.append(succ)
                elif id(succ) in optional_dead_ids:
                    queue.append(succ)
                # Stage-cascade-dead succs: do not collect, do not traverse
        return result

    # Seed dedup set from ALL original edges (covers D-04: skip bypass if direct edge exists)
    seen_bypass: set[tuple[int, int]] = {(id(s), id(d)) for s, d in graph.edges}
    bypass_edges: list[tuple[AnyNode, AnyNode]] = []
    for node in graph.nodes:
        if id(node) not in optional_dead_ids:
            continue
        surv_preds = _bfs_surviving_preds(node)
        surv_succs = _bfs_surviving_succs(node)
        for pred in surv_preds:
            for succ in surv_succs:
                key = (id(pred), id(succ))
                if key not in seen_bypass:
                    seen_bypass.add(key)
                    bypass_edges.append((pred, succ))

    # D-05: inject stage-exit bypass edges (StageNode → exit_node) collected in Phase 2.
    for stage_node, exit_node in stage_exits:
        key = (id(stage_node), id(exit_node))
        if key not in seen_bypass:
            seen_bypass.add(key)
            bypass_edges.append((stage_node, exit_node))

    surviving_ids: set[int] = {id(n) for n in graph.nodes} - all_dead_ids
    return Graph(
        nodes=[n for n in graph.nodes if id(n) in surviving_ids],
        edges=[
            (s, d) for s, d in graph.edges
            if id(s) in surviving_ids and id(d) in surviving_ids
        ] + bypass_edges,
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
        pruned = _prune(pipeline, profile, pipeline.name if isinstance(pipeline, Pipeline) else None)
        storage_key = self._settings.db_path or ".kptn/kptn.db"
        final_graph, bypassed_names = _apply_cursors(pruned, profile)
        return ResolvedGraph(
            graph=final_graph,
            pipeline=pipeline.name,
            storage_key=storage_key,
            bypassed_names=bypassed_names,
            profile_args=dict(profile.args),
        )
