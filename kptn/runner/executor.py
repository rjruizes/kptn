from __future__ import annotations

import hashlib
import inspect
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

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
from kptn.change_detector.detector import is_stale
from kptn.exceptions import HashError, TaskError
from kptn.runner.plan import emit_map, emit_fail, emit_skip, emit_run

logger = logging.getLogger(__name__)

_NON_EXEC_NODES = (ParallelNode, StageNode, NoopNode, PipelineNode)


def _filter_kwargs(fn: Callable, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Return only the kwargs that *fn* declares in its signature.

    Tasks should only receive the config/profile values they explicitly ask for.
    If the function accepts **kwargs it receives everything; otherwise only the
    intersection of the supplied dict and the function's parameter names is passed.
    """
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        return kwargs  # can't introspect — pass everything and let Python raise
    params = sig.parameters
    if any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        for p in params.values()
    ):
        return kwargs  # fn accepts **kwargs — pass everything
    return {k: v for k, v in kwargs.items() if k in params}


def _dispatch_task(
    node: TaskNode,
    config_kwargs: dict[str, Any],
    profile_kwargs: dict[str, Any],
) -> Any:
    kwargs = _filter_kwargs(node.fn, {**config_kwargs, **profile_kwargs})
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


def _dispatch_sql_task(node: SqlTaskNode, conn: Any, cwd: Path) -> None:
    """Execute a SQL file against *conn*.

    The file is split on ``;`` and each non-empty statement is executed
    individually.  Semicolons inside string literals or SQL comments are not
    handled — keep SQL files simple or use single-statement files.
    """
    sql_path = Path(node.path)
    if not sql_path.is_absolute():
        sql_path = cwd / sql_path
    try:
        sql_text = sql_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise TaskError(f"SQL task '{node.name}' could not read {sql_path}: {exc}") from exc
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for i, stmt in enumerate(statements, start=1):
        try:
            conn.execute(stmt)
        except Exception as exc:
            preview = stmt[:120] + ("…" if len(stmt) > 120 else "")
            raise TaskError(
                f"SQL task '{node.name}' failed at statement {i}/{len(statements)}: {exc}\n"
                f"  Statement: {preview}"
            ) from exc


def _compute_hash(node: AnyNode, duckdb_conn: "Any | None" = None) -> str | None:
    if not isinstance(node, (TaskNode, SqlTaskNode, RTaskNode)):
        return None

    outputs = node.spec.outputs
    if not outputs:
        return None

    hashes: list[str] = []
    for output in outputs:
        if output.startswith("duckdb://"):
            if duckdb_conn is None:
                raise HashError(
                    f"Task declares 'duckdb://' output but no DuckDB connection is available. "
                    f"Add kptn.config(duckdb=get_engine) to your pipeline."
                )
            h = hash_duckdb_table(output[len("duckdb://"):], conn=duckdb_conn)
        elif output.startswith("sqlite://"):
            h = hash_sqlite_table(output[len("sqlite://"):])
        else:
            h = hash_file(output)
        hashes.append(h)

    hashes.sort()
    combined = ":".join(hashes)
    return hashlib.sha256(combined.encode()).hexdigest()


def _compute_hash_for_map_item(node: MapNode, item_task_name: str, duckdb_conn: "Any | None" = None) -> str | None:
    spec = getattr(node.task, "__kptn__", None)
    if spec is None or not spec.outputs:
        return None
    hashes: list[str] = []
    for output in spec.outputs:
        if output.startswith("duckdb://"):
            if duckdb_conn is None:
                raise HashError(
                    f"MapNode declares 'duckdb://' output but no DuckDB connection is available. "
                    f"Add kptn.config(duckdb=get_engine) to your pipeline."
                )
            h = hash_duckdb_table(output[len("duckdb://"):], conn=duckdb_conn)
        elif output.startswith("sqlite://"):
            h = hash_sqlite_table(output[len("sqlite://"):])
        else:
            h = hash_file(output)
        hashes.append(h)
    hashes.sort()
    return hashlib.sha256(":".join(hashes).encode()).hexdigest()


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
    *,
    duckdb_factory: "Callable[[], Any] | None" = None,
    duckdb_alias: str | None = None,
    keep_db_open: bool = False,
    no_cache: bool = False,
    extra_kwargs: "dict[str, Any] | None" = None,
) -> "Any | None":
    """Execute the resolved pipeline graph.

    When *duckdb_factory* is supplied (because the user declared
    ``kptn.config(duckdb=(get_engine, "engine"))``), kptn calls the factory
    directly and populates ``config_kwargs[duckdb_alias]`` (e.g. ``"engine"``)
    rather than using the generic ``invoke_config`` path for the duckdb key.

    After all nodes have run:
    * ``keep_db_open=False`` (default) — close the connection via the factory.
    * ``keep_db_open=True`` — leave it open and return the live connection.

    Args:
        resolved: The resolved pipeline graph to execute.
        state_store: Backend store for persisting task hashes and state.
        cwd: Working directory for RTaskNode subprocess execution and SqlTaskNode SQL file
             path resolution; defaults to current directory.
        duckdb_factory: Callable that returns a DuckDB connection instance when provided.
        duckdb_alias: Key name for the DuckDB connection in config kwargs (default: "duckdb").
        keep_db_open: When True, return the live DuckDB connection instead of closing it.
        no_cache:
            When ``True``, bypass all state-store hash reads and writes — every task
            runs unconditionally and no hashes are persisted. (Behaviour added in Task 2.)
        extra_kwargs:
            Additional key/value pairs merged into ``config_kwargs`` before execution.
            Caller-supplied values take precedence over ConfigNode-resolved values.

    Returns the DuckDB connection when ``keep_db_open=True`` and a factory was
    provided; ``None`` otherwise.
    """
    if cwd is None:
        cwd = Path.cwd()

    _duckdb_alias = duckdb_alias or "duckdb"

    ordered: list[AnyNode] = topo_sort(resolved.graph)
    config_kwargs: dict[str, Any] = {}
    runtime_ctx: dict[str, Any] = {}

    if extra_kwargs is not None:
        config_kwargs.update(extra_kwargs)

    for node in ordered:
        # Non-exec nodes — pass through silently
        if isinstance(node, _NON_EXEC_NODES):
            continue

        # Bypassed nodes — read hash to keep fresh; no execution
        if node.name in resolved.bypassed_names:
            state_store.read_hash(resolved.storage_key, resolved.pipeline, node.name)
            continue

        # ConfigNode — invoke and accumulate config kwargs.
        # When duckdb_factory is set, the "duckdb" key is handled by the factory
        # (already wired into the state store); skip it in invoke_config to avoid
        # opening a second connection, and inject under the user's alias instead.
        if isinstance(node, ConfigNode):
            if duckdb_factory is not None and "duckdb" in node.spec:
                other_spec = {k: v for k, v in node.spec.items() if k != "duckdb"}
                from kptn.graph.nodes import ConfigNode as _CN
                if other_spec:
                    config_kwargs.update(invoke_config(_CN(spec=other_spec)))
                config_kwargs[_duckdb_alias] = duckdb_factory()
            else:
                config_kwargs.update(invoke_config(node))
            if extra_kwargs is not None:
                config_kwargs.update(extra_kwargs)
            continue

        # MapNode — resolve collection and dispatch per-item
        if isinstance(node, MapNode):
            collection = _resolve_collection(node.over, runtime_ctx)
            emit_map(node.name, len(collection))
            for item in collection:
                item_task_name = f"{node.name}[{item}]"
                if not no_cache:
                    stored_hash = state_store.read_hash(
                        resolved.storage_key, resolved.pipeline, item_task_name
                    )
                    if stored_hash is not None:
                        try:
                            current_hash = _compute_hash_for_map_item(node, item_task_name, duckdb_factory() if duckdb_factory else None)
                        except HashError:
                            current_hash = None  # treat as stale
                        if current_hash is not None and current_hash == stored_hash:
                            emit_skip(item_task_name)
                            continue
                emit_run(item_task_name)
                kwargs = _filter_kwargs(node.task, {
                    **config_kwargs,
                    **resolved.profile_args.get(node.name, {}),
                })
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
                if not no_cache:
                    try:
                        hash_ = _compute_hash_for_map_item(node, item_task_name, duckdb_factory() if duckdb_factory else None)
                    except HashError:
                        logger.warning(
                            "Hash computation failed for map item %r — skipping hash write",
                            item_task_name,
                        )
                        hash_ = None
                    if hash_ is not None:
                        state_store.write_hash(
                            resolved.storage_key, resolved.pipeline, item_task_name, hash_
                        )
            continue

        # TaskNode
        if isinstance(node, TaskNode):
            if not no_cache:
                stale, reason = False, ""
                try:
                    stale, reason = is_stale(node, state_store, resolved.storage_key, resolved.pipeline, duckdb_conn=duckdb_factory() if duckdb_factory else None)
                except HashError:
                    stale = True  # outputs unreadable/missing — treat as stale (first run or deleted)
                if not stale and reason == "cached":
                    emit_skip(node.name)
                    continue
            emit_run(node.name)
            profile_kwargs = resolved.profile_args.get(node.name, {})
            try:
                result = _dispatch_task(node, config_kwargs, profile_kwargs)
            except TaskError as exc:
                emit_fail(node.name, str(exc))
                raise
            runtime_ctx[node.name] = result
            if not no_cache:
                try:
                    hash_ = _compute_hash(node, duckdb_factory() if duckdb_factory else None)
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
            if not no_cache:
                stale, reason = False, ""
                try:
                    stale, reason = is_stale(node, state_store, resolved.storage_key, resolved.pipeline, duckdb_conn=duckdb_factory() if duckdb_factory else None)
                except HashError:
                    stale = True  # outputs unreadable/missing — treat as stale (first run or deleted)
                if not stale and reason == "cached":
                    emit_skip(node.name)
                    continue
            emit_run(node.name)
            try:
                _dispatch_r_task(node, cwd)
            except TaskError as exc:
                emit_fail(node.name, str(exc))
                raise
            if not no_cache:
                try:
                    hash_ = _compute_hash(node, duckdb_factory() if duckdb_factory else None)
                except HashError as exc:
                    emit_fail(node.name, str(exc))
                    raise TaskError(f"Hash computation failed for '{node.name}': {exc}") from exc
                if hash_ is not None:
                    state_store.write_hash(
                        resolved.storage_key, resolved.pipeline, node.name, hash_
                    )
            continue

        # SqlTaskNode — dispatch SQL file against DuckDB connection
        if isinstance(node, SqlTaskNode):
            if duckdb_factory is None:
                logger.warning(
                    "SqlTaskNode %r encountered but no DuckDB connection is available — "
                    "add kptn.config(duckdb=get_engine) to your pipeline; skipping",
                    node.name,
                )
                continue
            conn = duckdb_factory()  # acquire once; factory should return a cached singleton
            if not no_cache:
                stale, reason = False, ""
                try:
                    stale, reason = is_stale(node, state_store, resolved.storage_key, resolved.pipeline, duckdb_conn=conn)
                except HashError:
                    stale = True  # outputs unreadable/missing — treat as stale (first run or deleted)
                if not stale and reason == "cached":
                    emit_skip(node.name)
                    continue
            emit_run(node.name)
            try:
                _dispatch_sql_task(node, conn, cwd)
            except TaskError as exc:
                emit_fail(node.name, str(exc))
                raise
            if not no_cache:
                try:
                    hash_ = _compute_hash(node, conn)
                except HashError as exc:
                    emit_fail(node.name, str(exc))
                    raise TaskError(f"Hash computation failed for '{node.name}': {exc}") from exc
                if hash_ is not None:
                    state_store.write_hash(
                        resolved.storage_key, resolved.pipeline, node.name, hash_
                    )
            continue

    # Post-run: manage duckdb connection lifecycle
    if duckdb_factory is not None:
        if keep_db_open:
            return duckdb_factory()
        else:
            try:
                duckdb_factory().close()
            except Exception:
                logger.debug("Failed to close duckdb connection after pipeline run")
    return None
