from __future__ import annotations

import hashlib
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

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
from kptn.graph.config import invoke_config
from kptn.profiles.resolved import ResolvedGraph
from kptn.state_store.protocol import StateStoreBackend
from kptn.change_detector.hasher import hash_file, hash_sqlite_table, hash_duckdb_table
from kptn.exceptions import HashError, TaskError
from kptn.runner.plan import emit_map, emit_fail

logger = logging.getLogger(__name__)

_NON_EXEC_NODES = (ParallelNode, StageNode, NoopNode, PipelineNode)


def _dispatch_task(
    node: TaskNode,
    config_kwargs: dict[str, Any],
    profile_kwargs: dict[str, Any],
) -> Any:
    kwargs = {**config_kwargs, **profile_kwargs}
    try:
        return node.fn(**kwargs)
    except TaskError:
        raise
    except Exception as exc:
        raise TaskError(f"Task '{node.name}' raised an error: {exc}") from exc


def _dispatch_r_task(node: RTaskNode, cwd: Path) -> None:
    result = subprocess.run(
        ["Rscript", node.path],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if result.returncode != 0:
        output = "\n".join(filter(None, [result.stderr, result.stdout]))
        msg = f"Rscript {node.path!r} exited with code {result.returncode}\n{output}"
        raise TaskError(msg)


def _compute_hash(node: AnyNode) -> str | None:
    if not isinstance(node, (TaskNode, SqlTaskNode, RTaskNode)):
        return None

    outputs = node.spec.outputs
    if not outputs:
        return None

    hashes: list[str] = []
    for output in outputs:
        if output.startswith("duckdb://"):
            h = hash_duckdb_table(output[len("duckdb://"):])
        elif output.startswith("sqlite://"):
            h = hash_sqlite_table(output[len("sqlite://"):])
        else:
            h = hash_file(output)
        hashes.append(h)

    hashes.sort()
    combined = ":".join(hashes)
    return hashlib.sha256(combined.encode()).hexdigest()


def _resolve_collection(over: str, runtime_ctx: dict[str, Any]) -> list[Any]:
    parts = over.split(".")
    obj = runtime_ctx.get(parts[0])
    if obj is None:
        logger.warning("MapNode over=%r: task %r not in runtime context", over, parts[0])
        return []
    for part in parts[1:]:
        if isinstance(obj, dict):
            obj = obj.get(part)
        else:
            obj = getattr(obj, part, None)
        if obj is None:
            logger.warning("MapNode over=%r: attribute %r not found", over, part)
            return []
    if obj is None:
        return []
    try:
        return list(obj)
    except TypeError:
        logger.warning("MapNode over=%r: resolved value %r is not iterable", over, obj)
        return []


def execute(
    resolved: ResolvedGraph,
    state_store: StateStoreBackend,
    cwd: Path | None = None,
) -> None:
    if cwd is None:
        cwd = Path.cwd()

    ordered: list[AnyNode] = topo_sort(resolved.graph)
    config_kwargs: dict[str, Any] = {}
    runtime_ctx: dict[str, Any] = {}

    for node in ordered:
        # Non-exec nodes — pass through silently
        if isinstance(node, _NON_EXEC_NODES):
            continue

        # Bypassed nodes — read hash to keep fresh; no execution
        if node.name in resolved.bypassed_names:
            state_store.read_hash(resolved.storage_key, resolved.pipeline, node.name)
            continue

        # ConfigNode — invoke and accumulate config kwargs
        if isinstance(node, ConfigNode):
            config_kwargs.update(invoke_config(node))
            continue

        # MapNode — resolve collection and dispatch per-item
        if isinstance(node, MapNode):
            collection = _resolve_collection(node.over, runtime_ctx)
            emit_map(node.name, len(collection))
            for item in collection:
                item_task_name = f"{node.name}[{item}]"
                kwargs = {
                    **config_kwargs,
                    **resolved.profile_args.get(node.name, {}),
                }
                try:
                    result = node.task(item, **kwargs)
                except TaskError as exc:
                    emit_fail(item_task_name, str(exc))
                    raise
                except Exception as exc:
                    task_err = TaskError(
                        f"MapNode item '{item_task_name}' raised an error: {exc}"
                    )
                    emit_fail(item_task_name, str(task_err))
                    raise task_err from exc
                hash_ = _compute_hash_for_map_item(node, item_task_name)
                if hash_ is not None:
                    state_store.write_hash(
                        resolved.storage_key, resolved.pipeline, item_task_name, hash_
                    )
            continue

        # TaskNode
        if isinstance(node, TaskNode):
            profile_kwargs = resolved.profile_args.get(node.name, {})
            try:
                result = _dispatch_task(node, config_kwargs, profile_kwargs)
            except TaskError as exc:
                emit_fail(node.name, str(exc))
                raise
            runtime_ctx[node.name] = result
            try:
                hash_ = _compute_hash(node)
            except HashError as exc:
                emit_fail(node.name, str(exc))
                raise TaskError(f"Hash computation failed for '{node.name}': {exc}") from exc
            if hash_ is not None:
                state_store.write_hash(
                    resolved.storage_key, resolved.pipeline, node.name, hash_
                )
            continue

        # RTaskNode
        if isinstance(node, RTaskNode):
            try:
                _dispatch_r_task(node, cwd)
            except TaskError as exc:
                emit_fail(node.name, str(exc))
                raise
            try:
                hash_ = _compute_hash(node)
            except HashError as exc:
                emit_fail(node.name, str(exc))
                raise TaskError(f"Hash computation failed for '{node.name}': {exc}") from exc
            if hash_ is not None:
                state_store.write_hash(
                    resolved.storage_key, resolved.pipeline, node.name, hash_
                )
            continue

        # SqlTaskNode — out of scope for local runner v0.2.0
        if isinstance(node, SqlTaskNode):
            logger.warning(
                "SqlTaskNode %r encountered — SQL dispatch is out of scope for v0.2.0 local runner; skipping",
                node.name,
            )
            try:
                hash_ = _compute_hash(node)
            except HashError as exc:
                logger.warning("Hash computation failed for SqlTaskNode %r: %s", node.name, exc)
                continue
            if hash_ is not None:
                state_store.write_hash(
                    resolved.storage_key, resolved.pipeline, node.name, hash_
                )
            continue


def _compute_hash_for_map_item(node: MapNode, item_task_name: str) -> str | None:
    """MapNode items don't have outputs tracked — return None for now.

    Story 2.5 extends this with per-item output tracking.
    """
    return None
