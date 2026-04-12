from __future__ import annotations

from typing import Any

from kptn.exceptions import ProfileError
from kptn.graph.graph import Graph
from kptn.graph.nodes import AnyNode, TaskNode, SqlTaskNode, RTaskNode, StageNode
from kptn.graph.pipeline import Pipeline
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
        pruned = _prune(pipeline, profile)
        storage_key = self._settings.db_path or ".kptn/kptn.db"
        return ResolvedGraph(
            graph=pruned,
            pipeline=pipeline.name,
            storage_key=storage_key,
            bypassed_names=frozenset(),  # start_from/stop_after: Story 3.4
            profile_args=dict(profile.args),
        )
