from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, TextIO, cast

from kptn.change_detector.detector import is_stale
from kptn.exceptions import HashError, TaskError
from kptn.graph.config import invoke_config
from kptn.graph.nodes import (
    ConfigNode,
    MapNode,
    NoopNode,
    ParallelNode,
    PipelineNode,
    SourceNode,
    StageNode,
)
from kptn.graph.topo import topo_sort
from kptn.profiles.resolved import ResolvedGraph
from kptn.sources.build import is_current, plan_tables
from kptn.sources.shared import SharedBronze
from kptn.state_store.protocol import StateStoreBackend

_PLAN_NON_EXEC = (ParallelNode, StageNode, NoopNode, PipelineNode, ConfigNode)
_DEFAULT_STREAM = sys.stdout


class PlanAction(StrEnum):
    RUN = "run"
    SKIP = "skip"
    MAP = "map"


@dataclass(frozen=True)
class PlanEntry:
    task_name: str
    action: PlanAction
    reason: str = ""
    provider: str | None = None


def _format_plan_status_line(
    action: PlanAction,
    task_name: str,
    *,
    provider: str | None = None,
    timestamp: bool = False,
) -> str:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""

    if action is PlanAction.RUN:
        return f"[RUN]{ts} {task_name}"
    if action is PlanAction.SKIP:
        return f"[SKIP]{ts} {task_name} — cached"
    if provider is None:
        raise ValueError(f"Map plan status for '{task_name}' is missing a provider.")
    return f"[MAP]{ts} {task_name} — dynamic, expands after {provider}"


def emit_map(task_name: str, count: int) -> None:
    print(f"[MAP] {task_name} — expanding over {count} items", flush=True)


def emit_map_plan(task_name: str, provider: str) -> None:
    print(
        _format_plan_status_line(
            PlanAction.MAP,
            task_name,
            provider=provider,
            timestamp=False,
        ),
        flush=True,
    )


def emit_fail(task_name: str, reason: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[FAIL]{ts} {task_name} — {reason}", file=sys.stderr, flush=True)


def emit_skip(task_name: str, timestamp: bool = False) -> None:
    print(
        _format_plan_status_line(
            PlanAction.SKIP,
            task_name,
            provider=None,
            timestamp=timestamp,
        ),
        flush=True,
    )


def emit_run(task_name: str, timestamp: bool = False) -> None:
    print(
        _format_plan_status_line(
            PlanAction.RUN,
            task_name,
            provider=None,
            timestamp=timestamp,
        ),
        flush=True,
    )


def emit_backup_start(task_name: str, dest: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[BACKUP_START]{ts} {task_name} → {dest}", flush=True)


def emit_backup_end(task_name: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[BACKUP_END]{ts} {task_name}", flush=True)


def emit_restore_start(src: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[RESTORE_START]{ts} {src}", flush=True)


def emit_restore_end(elapsed_s: float, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[RESTORE_END]{ts} — {elapsed_s:.1f}s", flush=True)


def emit_checkpoint_select(task_name: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[CHECKPOINT_SELECT]{ts} {task_name}", flush=True)


def emit_checkpoint_stale(task_name: str, stale_task: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[CHECKPOINT_STALE]{ts} {task_name} — {stale_task} is stale, backup deleted", flush=True)


class _PrefetchedHashes:
    """Answers ``read_hash`` from one bulk ``read_hashes`` query.

    A plan reads a hash for nearly every task, and in factory mode each
    ``read_hash`` opens a fresh connection through the project's factory. The
    query runs on the first read, so a plan that reads nothing touches nothing;
    reads for any other key go to the store itself. Only for planning: a run
    writes hashes between reads, which a snapshot would miss.
    """

    def __init__(self, state_store: StateStoreBackend, storage_key: str, pipeline: str) -> None:
        self._store = state_store
        self._key = (storage_key, pipeline)
        self._hashes: dict[str, str | None] | None = None

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None:
        read_hashes = getattr(self._store, "read_hashes", None)
        if (storage_key, pipeline) != self._key or read_hashes is None:
            return self._store.read_hash(storage_key, pipeline, task)
        if self._hashes is None:
            self._hashes = read_hashes(storage_key, pipeline)
        return self._hashes.get(task)


#: Why a table that is not current will be restored rather than built.
SHARED_REASON = "in shared bronze"


def config_values_for_plan(graph: Any) -> dict[str, Any] | None:
    """The config node's values, for resolving config-derived source files.

    The ``duckdb`` entry is skipped so planning never opens the pipeline
    database. Returns None (and says why on stderr) if a config callable fails.
    """
    for node in getattr(graph, "nodes", []):
        if isinstance(node, ConfigNode):
            spec = {k: v for k, v in node.spec.items() if k != "duckdb"}
            try:
                return invoke_config(ConfigNode(spec=spec))
            except TaskError as exc:
                detail = f"{exc}: {exc.__cause__}" if exc.__cause__ is not None else str(exc)
                print(f"[PLAN] config unavailable, sources shown as run: {detail}", file=sys.stderr)
                return None
    return {}


def _plan_source(
    node: SourceNode,
    reader: Any,
    resolved: ResolvedGraph,
    config_values: Mapping[str, Any] | None,
    root: Path,
    shared: SharedBronze | None = None,
) -> list[PlanEntry]:
    """One entry per table of *node*, or one entry for the source when its
    tables cannot be known without running anything.

    A table the shared bronze already has still runs -- restoring it changes
    the local file and reruns what reads it -- but says it will not be built.
    """
    if config_values is None and callable(node.spec.files):
        return [PlanEntry(node.name, PlanAction.RUN, reason="files depend on config")]
    try:
        entries: list[PlanEntry] = []
        for plan in plan_tables(node.name, node.spec, config_values or {}, root=root):
            if is_current(plan, reader, resolved.storage_key, resolved.pipeline):
                entries.append(PlanEntry(plan.state_name, PlanAction.SKIP, reason="cached"))
            elif shared is not None and shared.lookup(plan) is not None:
                entries.append(PlanEntry(plan.state_name, PlanAction.RUN, reason=SHARED_REASON))
            else:
                entries.append(PlanEntry(plan.state_name, PlanAction.RUN, reason="inputs changed"))
        return entries
    except Exception as exc:  # includes errors raised by a files= or table= callable
        return [PlanEntry(node.name, PlanAction.RUN, reason=str(exc))]


class HashReader(Protocol):
    """All a plan needs of a state store: to read a task's hash. A full
    backend fits, and so does a read-only view of a DuckDB file
    (:class:`kptn.runner.checkpoint.TaskStateReader`)."""

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None: ...


def build_plan(
    resolved: ResolvedGraph,
    state_store: StateStoreBackend | HashReader,
    *,
    config_values: Mapping[str, Any] | None = None,
    root: Path | None = None,
    shared: SharedBronze | None = None,
) -> list[PlanEntry]:
    reader: StateStoreBackend | HashReader | _PrefetchedHashes = state_store
    if callable(getattr(state_store, "read_hashes", None)):
        reader = _PrefetchedHashes(cast(StateStoreBackend, state_store), resolved.storage_key, resolved.pipeline)
    entries: list[PlanEntry] = []
    # The executor's cascade: a source with a table to build or restore, or a
    # stale task, makes everything downstream run, whatever its own hashes
    # say. A task that runs only for want of outputs ("no outputs") does not.
    predecessors: dict[str, list[str]] = {}
    for src, dst in resolved.graph.edges:
        predecessors.setdefault(dst.name, []).append(src.name)
    dirty: set[str] = set()
    for node in topo_sort(resolved.graph):
        upstream_dirty = any(p in dirty for p in predecessors.get(node.name, ()))
        if isinstance(node, _PLAN_NON_EXEC):
            if upstream_dirty:
                dirty.add(node.name)
            continue
        if node.name in resolved.bypassed_names:
            continue
        if isinstance(node, SourceNode):
            # A source's tables are judged on their own files; upstream
            # dirtiness only passes through, as it does in the executor.
            source_entries = _plan_source(
                node, reader, resolved, config_values, root or Path.cwd(), shared
            )
            if upstream_dirty or any(e.action is PlanAction.RUN for e in source_entries):
                dirty.add(node.name)
            entries.extend(source_entries)
            continue
        if isinstance(node, MapNode):
            provider = node.over.split(".")[0]
            if not provider:
                raise ValueError(
                    f"MapNode '{node.name}' has an empty 'over' expression; "
                    "cannot determine provider for plan output."
                )
            if upstream_dirty:
                dirty.add(node.name)
            entries.append(PlanEntry(node.name, PlanAction.MAP, provider=provider))
            continue
        if upstream_dirty:
            dirty.add(node.name)
            entries.append(PlanEntry(node.name, PlanAction.RUN, reason="upstream changed"))
            continue
        try:
            stale, reason = is_stale(node, reader, resolved.storage_key, resolved.pipeline)
        except HashError:
            stale, reason = True, "hash unavailable"
        if stale:
            dirty.add(node.name)
        entries.append(
            PlanEntry(
                node.name,
                PlanAction.SKIP if not stale and reason == "cached" else PlanAction.RUN,
                reason=reason,
            )
        )
    return entries


def render_plan(entries: Iterable[PlanEntry], stream: TextIO = sys.stdout) -> None:
    if stream is _DEFAULT_STREAM:
        stream = sys.stdout
    for entry in entries:
        if entry.action is PlanAction.MAP:
            if entry.provider is None:
                raise ValueError(f"PlanEntry for map task '{entry.task_name}' is missing a provider.")
            print(
                _format_plan_status_line(
                    PlanAction.MAP,
                    entry.task_name,
                    provider=entry.provider,
                    timestamp=False,
                ),
                file=stream,
                flush=True,
            )
        elif entry.action is PlanAction.SKIP:
            print(
                _format_plan_status_line(
                    PlanAction.SKIP,
                    entry.task_name,
                    provider=None,
                    timestamp=False,
                ),
                file=stream,
                flush=True,
            )
        else:
            line = _format_plan_status_line(
                PlanAction.RUN,
                entry.task_name,
                provider=None,
                timestamp=False,
            )
            # Other reasons are the UI's; a restore is worth a terminal word
            # because it will not read any files.
            if entry.reason == SHARED_REASON:
                line += f" — {SHARED_REASON}"
            print(line, file=stream, flush=True)


def plan(
    resolved: ResolvedGraph,
    state_store: StateStoreBackend,
    *,
    config_values: Mapping[str, Any] | None = None,
    shared: SharedBronze | None = None,
) -> None:
    render_plan(build_plan(resolved, state_store, config_values=config_values, shared=shared))
