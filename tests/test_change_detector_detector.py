"""Tests for kptn.change_detector.detector — staleness detection (AC 6–7)."""

import pytest
from unittest.mock import MagicMock

from kptn.change_detector.detector import is_stale
from kptn.change_detector.hasher import hash_file, hash_task_source
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
    """A task with uninspectable fn and no outputs returns (False, 'no outputs') on first run."""
    mock_fn = MagicMock()
    mock_fn.__name__ = "fresh_task"
    store = FakeStateStore()
    node = TaskNode(fn=mock_fn, spec=TaskSpec(outputs=[]), name="fresh_task")
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
    """A task with uninspectable fn and no outputs returns (False, 'no outputs') even with a stored hash."""
    mock_fn = MagicMock()
    mock_fn.__name__ = "empty_task"
    store = FakeStateStore()
    store.write_hash("ns", "pipe", "empty_task", "some_hash")
    node = TaskNode(fn=mock_fn, spec=TaskSpec(outputs=[]), name="empty_task")
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is False
    assert reason == "no outputs"


# ---------------------------------------------------------------------------
# Code-hash caching for tasks with no declared outputs
# ---------------------------------------------------------------------------


def _a_real_task():
    pass


def test_is_stale_no_outputs_stale_on_first_run():
    """Real fn, no outputs, no stored hash → stale so the task runs on first call."""
    store = FakeStateStore()
    node = TaskNode(fn=_a_real_task, spec=TaskSpec(outputs=[]), name="t")
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is True
    assert reason == "no cached code hash"


def test_is_stale_no_outputs_cached_when_code_unchanged():
    """Real fn, no outputs → skipped when stored hash matches current code hash."""
    code_hash = hash_task_source(_a_real_task)
    store = FakeStateStore()
    store.write_hash("ns", "pipe", "t", code_hash)
    node = TaskNode(fn=_a_real_task, spec=TaskSpec(outputs=[]), name="t")
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is False
    assert reason == "cached"


def test_is_stale_no_outputs_stale_when_code_changed():
    """Real fn, no outputs → stale when stored hash doesn't match current code."""
    store = FakeStateStore()
    store.write_hash("ns", "pipe", "t", "outdated_hash_value")
    node = TaskNode(fn=_a_real_task, spec=TaskSpec(outputs=[]), name="t")
    stale, reason = is_stale(node, store, "ns", "pipe")
    assert stale is True
    assert "code changed" in reason
