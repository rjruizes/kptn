"""Staleness detection using deterministic content hashes."""

import hashlib
import logging
from typing import Any

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
from kptn.state_store.protocol import StateStoreBackend

from kptn.change_detector.hasher import hash_duckdb_table, hash_file, hash_sqlite_table

logger = logging.getLogger(__name__)

_NON_TASK_NODES = (ParallelNode, StageNode, NoopNode, ConfigNode, PipelineNode, MapNode)


def _hash_outputs(node: AnyNode, duckdb_conn: Any = None) -> str | None:
    if isinstance(node, (ParallelNode, StageNode, NoopNode, ConfigNode, PipelineNode, MapNode)):
        return None

    spec = getattr(node, "spec", None)
    if spec is None or not spec.outputs:
        return None

    individual_hashes: list[str] = []
    for output in spec.outputs:
        if output.startswith("duckdb://"):
            table_name = output[len("duckdb://"):]
            if duckdb_conn is None:
                raise HashError(
                    f"Task declares 'duckdb://' output but no DuckDB connection is available. "
                    f"Add kptn.config(duckdb=get_engine) to your pipeline."
                )
            individual_hashes.append(hash_duckdb_table(table_name, conn=duckdb_conn))
        elif output.startswith("sqlite://"):
            table_uri = output[len("sqlite://"):]
            individual_hashes.append(hash_sqlite_table(table_uri))
        else:
            individual_hashes.append(hash_file(output))

    sorted_hashes = sorted(individual_hashes)
    composite_input = ":".join(sorted_hashes)
    return hashlib.sha256(composite_input.encode()).hexdigest()


def is_stale(
    node: AnyNode,
    state_store: StateStoreBackend,
    storage_key: str,
    pipeline: str,
    *,
    duckdb_conn: Any = None,
) -> tuple[bool, str]:
    """Return ``(stale, reason)`` for *node*.

    Raises:
        HashError: if output hashing fails (file missing, DB inaccessible, etc.).
    """
    if isinstance(node, _NON_TASK_NODES):
        return (False, "non-task node")

    task_name = node.name

    current = _hash_outputs(node, duckdb_conn=duckdb_conn)
    if current is None:
        return (False, "no outputs")

    stored = state_store.read_hash(storage_key, pipeline, task_name)
    if stored is None:
        return (True, "no cached hash")

    if current == stored:
        return (False, "cached")

    return (True, f"output changed (stored={stored[:8]}… current={current[:8]}…)")
