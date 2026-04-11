"""Tests for kptn.change_detector.detector — staleness detection (AC 6–7)."""

import pytest

from kptn.change_detector.detector import is_stale
from kptn.change_detector.hasher import hash_file
from kptn.graph.decorators import TaskSpec
from kptn.graph.nodes import MapNode, NoopNode, ParallelNode, TaskNode
from tests.fakes import FakeStateStore


# ---------------------------------------------------------------------------
# AC-6: is_stale returns (False, "cached") when hash matches
# ---------------------------------------------------------------------------


def test_is_stale_returns_false_cached_when_hash_matches(tmp_path):
    tmp_file = tmp_path / "output.txt"
    tmp_file.write_text("data")
    real_hash = hash_file(str(tmp_file))
    store = FakeStateStore()
    store.write_hash("ns", "pipe", "my_task", real_hash)
    node = TaskNode(fn=lambda: None, spec=TaskSpec(outputs=[str(tmp_file)]), name="my_task")
    # Single output — composite hash is sha256 of the single hash
    import hashlib
    composite = hashlib.sha256(real_hash.encode()).hexdigest()
    store.write_hash("ns", "pipe", "my_task", composite)
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is False
    assert reason == "cached"


# ---------------------------------------------------------------------------
# AC-7: is_stale returns (True, reason) when no stored hash
# ---------------------------------------------------------------------------


def test_is_stale_returns_true_when_no_stored_hash(tmp_path):
    tmp_file = tmp_path / "output.txt"
    tmp_file.write_text("data")
    store = FakeStateStore()
    node = TaskNode(fn=lambda: None, spec=TaskSpec(outputs=[str(tmp_file)]), name="t1")
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is True
    assert reason == "no cached hash"


# ---------------------------------------------------------------------------
# AC-7: is_stale returns (True, reason) when output changed
# ---------------------------------------------------------------------------


def test_is_stale_returns_true_when_output_changed(tmp_path):
    tmp_file = tmp_path / "output.txt"
    tmp_file.write_text("original")

    import hashlib
    old_file_hash = hash_file(str(tmp_file))
    old_composite = hashlib.sha256(old_file_hash.encode()).hexdigest()

    store = FakeStateStore()
    store.write_hash("ns", "pipe", "t2", old_composite)
    node = TaskNode(fn=lambda: None, spec=TaskSpec(outputs=[str(tmp_file)]), name="t2")

    # Mutate file
    tmp_file.write_text("changed content")

    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is True
    assert "output changed" in reason


# ---------------------------------------------------------------------------
# Non-task node tests — always not stale
# ---------------------------------------------------------------------------


def test_is_stale_noop_node_never_stale():
    store = FakeStateStore()
    node = NoopNode()
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is False
    assert reason == "non-task node"


def test_is_stale_map_node_never_stale():
    store = FakeStateStore()
    node = MapNode(task=lambda: None, over="ctx.items", name="my_map")
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is False
    assert reason == "non-task node"


def test_is_stale_task_with_no_outputs_never_stale_on_first_run():
    """A task with no outputs must return (False, "no outputs") even with no stored hash."""
    store = FakeStateStore()
    node = TaskNode(fn=lambda: None, spec=TaskSpec(outputs=[]), name="fresh_task")
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is False
    assert reason == "no outputs"


def test_is_stale_parallel_node_never_stale():
    store = FakeStateStore()
    node = ParallelNode()
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is False
    assert reason == "non-task node"


# ---------------------------------------------------------------------------
# Task with no outputs — never stale
# ---------------------------------------------------------------------------


def test_is_stale_task_with_no_outputs_never_stale():
    store = FakeStateStore()
    store.write_hash("ns", "pipe", "empty_task", "some_hash")
    node = TaskNode(fn=lambda: None, spec=TaskSpec(outputs=[]), name="empty_task")
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is False
    assert reason == "no outputs"
