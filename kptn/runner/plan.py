from __future__ import annotations

import sys
from datetime import datetime

from kptn.change_detector.detector import is_stale
from kptn.exceptions import HashError
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
from kptn.graph.topo import topo_sort
from kptn.profiles.resolved import ResolvedGraph
from kptn.state_store.protocol import StateStoreBackend

_PLAN_NON_EXEC = (ParallelNode, StageNode, NoopNode, PipelineNode, ConfigNode)


def emit_map(task_name: str, count: int) -> None:
    print(f"[MAP] {task_name} — expanding over {count} items", flush=True)


def emit_map_plan(task_name: str, provider: str) -> None:
    print(f"[MAP] {task_name} \u2014 dynamic, expands after {provider}", flush=True)


def emit_fail(task_name: str, reason: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[FAIL]{ts} {task_name} — {reason}", file=sys.stderr, flush=True)


def emit_skip(task_name: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[SKIP]{ts} {task_name} \u2014 cached", flush=True)


def emit_run(task_name: str, timestamp: bool = False) -> None:
    ts = f" {datetime.now().strftime('%H:%M:%S')}" if timestamp else ""
    print(f"[RUN]{ts} {task_name}", flush=True)


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


def plan(resolved: ResolvedGraph, state_store: StateStoreBackend) -> None:
    ordered: list[AnyNode] = topo_sort(resolved.graph)
    for node in ordered:
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
            emit_map_plan(node.name, provider)
            continue
        # TaskNode, RTaskNode, SqlTaskNode
        stale = True
        reason = ""
        try:
            stale, reason = is_stale(node, state_store, resolved.storage_key, resolved.pipeline)
        except HashError:
            stale = True
        if not stale and reason == "cached":
            emit_skip(node.name)
        else:
            emit_run(node.name)
