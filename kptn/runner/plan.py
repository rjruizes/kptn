from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Iterable, TextIO

from kptn.change_detector.detector import is_stale
from kptn.exceptions import HashError
from kptn.graph.nodes import (
    ConfigNode,
    MapNode,
    NoopNode,
    ParallelNode,
    PipelineNode,
    StageNode,
)
from kptn.graph.topo import topo_sort
from kptn.profiles.resolved import ResolvedGraph
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
        if (storage_key, pipeline) != self._key:
            return self._store.read_hash(storage_key, pipeline, task)
        if self._hashes is None:
            self._hashes = self._store.read_hashes(storage_key, pipeline)  # type: ignore[attr-defined]
        return self._hashes.get(task)


def build_plan(
    resolved: ResolvedGraph,
    state_store: StateStoreBackend,
) -> list[PlanEntry]:
    reader: StateStoreBackend | _PrefetchedHashes = state_store
    if callable(getattr(state_store, "read_hashes", None)):
        reader = _PrefetchedHashes(state_store, resolved.storage_key, resolved.pipeline)
    entries: list[PlanEntry] = []
    for node in topo_sort(resolved.graph):
        if isinstance(node, _PLAN_NON_EXEC):
            continue
        if node.name in resolved.bypassed_names:
            continue
        if isinstance(node, MapNode):
            provider = node.over.split(".")[0]
            if not provider:
                raise ValueError(
                    f"MapNode '{node.name}' has an empty 'over' expression; "
                    "cannot determine provider for plan output."
                )
            entries.append(PlanEntry(node.name, PlanAction.MAP, provider=provider))
            continue
        try:
            stale, reason = is_stale(node, reader, resolved.storage_key, resolved.pipeline)
        except HashError:
            stale, reason = True, "hash unavailable"
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
            print(
                _format_plan_status_line(
                    PlanAction.RUN,
                    entry.task_name,
                    provider=None,
                    timestamp=False,
                ),
                file=stream,
                flush=True,
            )


def plan(resolved: ResolvedGraph, state_store: StateStoreBackend) -> None:
    render_plan(build_plan(resolved, state_store))
