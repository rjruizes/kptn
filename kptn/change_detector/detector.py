"""Staleness detection using deterministic content hashes."""

import hashlib
import logging

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

from kptn.change_detector.hasher import hash_file, hash_sqlite_table, hash_task_source

logger = logging.getLogger(__name__)

_NON_TASK_NODES = (ParallelNode, StageNode, NoopNode, ConfigNode, PipelineNode, MapNode)


def _hash_outputs(node: AnyNode) -> str | None:
    if isinstance(node, (ParallelNode, StageNode, NoopNode, ConfigNode, PipelineNode, MapNode)):
        return None

    spec = getattr(node, "spec", None)
    if spec is None or not spec.outputs:
        return None

    individual_hashes: list[str] = []
    for output in spec.outputs:
        if output.startswith("duckdb://"):
            continue  # duckdb outputs are not hashed; rely on upstream-dirty propagation
        elif output.startswith("sqlite://"):
            table_uri = output[len("sqlite://"):]
            individual_hashes.append(hash_sqlite_table(table_uri))
        else:
            individual_hashes.append(hash_file(output))

    if not individual_hashes:
        return None

    sorted_hashes = sorted(individual_hashes)
    composite_input = ":".join(sorted_hashes)
    return hashlib.sha256(composite_input.encode()).hexdigest()


def _hash_code(node: AnyNode) -> str | None:
    """Return a deterministic hash of the node's code, or None if not possible.

    Used as a cache key fallback when a task declares no outputs.
    """
    try:
        if isinstance(node, TaskNode):
            return hash_task_source(node.fn)
        if isinstance(node, (RTaskNode, SqlTaskNode)):
            return hash_file(node.path)
    except HashError:
        pass
    return None


def is_stale(
    node: AnyNode,
    state_store: StateStoreBackend,
    storage_key: str,
    pipeline: str,
) -> tuple[bool, str]:
    """Return ``(stale, reason)`` for *node*.

    Raises:
        HashError: if output hashing fails (file missing, DB inaccessible, etc.).
    """
    if isinstance(node, _NON_TASK_NODES):
        return (False, "non-task node")

    task_name = node.name

    current = _hash_outputs(node)
    if current is None:
        code_hash = _hash_code(node)
        if code_hash is None:
            return (False, "no outputs")
        stored = state_store.read_hash(storage_key, pipeline, task_name)
        if stored is None:
            return (True, "no cached code hash")
        if code_hash == stored:
            return (False, "cached")
        return (True, f"code changed (stored={stored[:8]}… current={code_hash[:8]}…)")

    stored = state_store.read_hash(storage_key, pipeline, task_name)
    if stored is None:
        return (True, "no cached hash")

    if current == stored:
        return (False, "cached")

    return (True, f"output changed (stored={stored[:8]}… current={current[:8]}…)")
