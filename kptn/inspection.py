"""Read-only, profile-aware pipeline inspection model.

``inspect_pipeline`` builds a :class:`PipelineInspection` describing every
node in a pipeline's graph, in the same order the runner and ``kptn plan``
use (``kptn.graph.topo.topo_sort`` over the profile-resolved graph). This is
a pure read model: it never executes tasks, never mutates the pipeline or
its graph, and never infers documentation metadata that was not explicitly
declared on a task, ``Stage``, or ``Pipeline``.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kptn.graph.graph import Graph
from kptn.graph.nodes import (
    AnyNode,
    ConfigNode,
    MapNode,
    NoopNode,
    ParallelNode,
    PipelineNode,
    RTaskNode,
    SqlTaskNode,
    StageNode,
    TaskNode,
)
from kptn.graph.pipeline import Pipeline
from kptn.graph.topo import topo_sort
from kptn.profiles.resolver import ProfileResolver
from kptn.profiles.schema import KptnConfig

# Nodes that represent a unit of actual work (or a dynamic fan-out of work),
# as opposed to structural sentinels (Stage/Parallel/Pipeline/Noop/Config).
_EXECUTABLE_NODE_TYPES = (TaskNode, SqlTaskNode, RTaskNode, MapNode)

_KIND_NAMES: dict[type, str] = {
    TaskNode: "python",
    SqlTaskNode: "sql",
    RTaskNode: "r",
    MapNode: "map",
    ParallelNode: "parallel",
    StageNode: "stage",
    PipelineNode: "pipeline",
    NoopNode: "noop",
    ConfigNode: "config",
}


@dataclass(frozen=True)
class InspectionItem:
    """One node in a pipeline's resolved graph, as a read-only record."""

    name: str
    kind: str
    sequence: int | None
    executable: bool
    bypassed: bool
    description: str | None
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    source_path: Path | None
    source_line: int | None
    docs_path: Path | None
    docs_anchor: str | None
    predecessors: tuple[str, ...]
    successors: tuple[str, ...]


@dataclass(frozen=True)
class PipelineInspection:
    """The full read-only inspection result for one pipeline + profile."""

    pipeline: str
    profile: str | None
    items: tuple[InspectionItem, ...]


def resolve_docs_path(project_root: Path, candidate: str) -> Path:
    """Resolve a project-relative Markdown docs path.

    Raises:
        ValueError: if the resolved path would fall outside ``project_root``.
    """
    root = project_root.resolve()
    resolved = (root / candidate).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(
            f"Docs path {candidate!r} must stay within project root {root}."
        )
    return resolved


def _split_docs(docs: str | None, project_root: Path) -> tuple[Path | None, str | None]:
    """Split a ``docs`` string ("path/to/file.md#anchor") into (path, anchor)."""
    if not docs:
        return None, None
    path_part, sep, anchor = docs.partition("#")
    docs_path = resolve_docs_path(project_root, path_part) if path_part else None
    docs_anchor = anchor if sep else None
    return docs_path, docs_anchor


def _underlying_task(node: AnyNode) -> Any | None:
    """Return the __kptn__-tagged callable/handle backing a node, if any."""
    if isinstance(node, MapNode):
        return node.task
    if isinstance(node, TaskNode):
        return node.fn
    if isinstance(node, (SqlTaskNode, RTaskNode)):
        return node
    return None


def _spec_for(node: AnyNode) -> Any | None:
    """Return the TaskSpec/SqlTaskSpec/RTaskSpec backing a node, if any."""
    task = _underlying_task(node)
    if task is None:
        return None
    return getattr(task, "__kptn__", None)


def _stripped_docstring(fn: Any) -> str | None:
    doc = inspect.getdoc(fn)
    return doc.strip() if doc else None


def _description_for(node: AnyNode) -> str | None:
    """Explicit metadata, then stripped docstring (Python tasks only), then None."""
    spec = _spec_for(node)
    if spec is not None:
        if spec.doc.description:
            return spec.doc.description
        if isinstance(node, TaskNode):
            return _stripped_docstring(inspect.unwrap(node.fn))
        if isinstance(node, MapNode):
            underlying = getattr(node.task, "__wrapped__", None)
            if underlying is not None:
                return _stripped_docstring(inspect.unwrap(underlying))
        return None
    if isinstance(node, (StageNode, PipelineNode)):
        return node.description
    return None


def _docs_for(node: AnyNode) -> str | None:
    spec = _spec_for(node)
    if spec is not None:
        return spec.doc.docs
    if isinstance(node, (StageNode, PipelineNode)):
        return node.docs
    return None


def _inputs_for(node: AnyNode) -> tuple[str, ...]:
    """Declared data inputs only — never inferred."""
    spec = _spec_for(node)
    if spec is None:
        return ()
    return spec.doc.inputs


def _outputs_for(node: AnyNode) -> tuple[str, ...]:
    spec = _spec_for(node)
    if spec is None:
        return ()
    return tuple(spec.outputs)


def _source_for(node: AnyNode) -> tuple[Path | None, int | None]:
    if isinstance(node, TaskNode):
        try:
            fn = inspect.unwrap(node.fn)
            source_file = inspect.getsourcefile(fn)
            _, lineno = inspect.getsourcelines(fn)
        except (OSError, TypeError):
            return None, None
        return (Path(source_file) if source_file else None), lineno
    if isinstance(node, MapNode):
        try:
            fn = inspect.unwrap(getattr(node.task, "__wrapped__", node.task))
            source_file = inspect.getsourcefile(fn)
            _, lineno = inspect.getsourcelines(fn)
        except (OSError, TypeError):
            return None, None
        return (Path(source_file) if source_file else None), lineno
    if isinstance(node, (SqlTaskNode, RTaskNode)):
        return Path(node.path), None
    return None, None


def inspect_pipeline(
    pipeline: Pipeline,
    config: KptnConfig,
    profile: str | None,
    project_root: Path,
) -> PipelineInspection:
    """Build a read-only, profile-aware inspection of a pipeline.

    Mirrors ``kptn.runner.api.resolve_pipeline``'s profile handling: when
    ``profile`` is given, the pipeline is compiled through
    :class:`~kptn.profiles.resolver.ProfileResolver` (pruning inactive Stage
    branches and applying ``start_from``/``stop_after`` cursors); when
    ``profile`` is ``None``, the raw pipeline graph is inspected unmodified.

    Node order follows ``kptn.graph.topo.topo_sort`` over the resolved graph —
    the same ordering the runner and ``kptn plan`` use. This function only
    reads the graph and its node specs; it never executes tasks or mutates
    the pipeline.
    """
    if profile is not None:
        resolved = ProfileResolver(config).compile(pipeline, profile)
        graph: Graph = resolved.graph
        bypassed_names = resolved.bypassed_names
    else:
        graph = pipeline
        bypassed_names = frozenset()

    ordered = topo_sort(graph)

    predecessors: dict[int, list[str]] = {id(n): [] for n in graph.nodes}
    successors: dict[int, list[str]] = {id(n): [] for n in graph.nodes}
    for src, dst in graph.edges:
        successors[id(src)].append(dst.name)
        predecessors[id(dst)].append(src.name)

    items: list[InspectionItem] = []
    sequence = 0
    for node in ordered:
        kind = _KIND_NAMES.get(type(node), type(node).__name__)
        bypassed = node.name in bypassed_names
        executable = isinstance(node, _EXECUTABLE_NODE_TYPES) and not bypassed

        seq: int | None = None
        if executable:
            sequence += 1
            seq = sequence

        docs_path, docs_anchor = _split_docs(_docs_for(node), project_root)
        source_path, source_line = _source_for(node)

        items.append(
            InspectionItem(
                name=node.name,
                kind=kind,
                sequence=seq,
                executable=executable,
                bypassed=bypassed,
                description=_description_for(node),
                inputs=_inputs_for(node),
                outputs=_outputs_for(node),
                source_path=source_path,
                source_line=source_line,
                docs_path=docs_path,
                docs_anchor=docs_anchor,
                predecessors=tuple(predecessors[id(node)]),
                successors=tuple(successors[id(node)]),
            )
        )

    pipeline_name = pipeline.name if isinstance(pipeline, Pipeline) else ""
    return PipelineInspection(
        pipeline=pipeline_name,
        profile=profile,
        items=tuple(items),
    )
