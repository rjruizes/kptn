"""Tests for kptn.runner.checkpoint.find_restore_candidate."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from kptn.change_detector.hasher import hash_task_source
from kptn.graph.decorators import TaskSpec, _KptnCallable
from kptn.graph.nodes import TaskNode
from kptn.runner.checkpoint import checkpoint_path, find_restore_candidate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _node(name: str, fn, *, duckdb_checkpoint: bool = False) -> TaskNode:
    return TaskNode(fn=fn, spec=TaskSpec(outputs=[], duckdb_checkpoint=duckdb_checkpoint), name=name)


def _make_backup_db(path: Path, hashes: dict[str, str], storage_key: str, pipeline: str) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS _kptn")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _kptn.task_state (
            storage_key   TEXT NOT NULL,
            pipeline_name TEXT NOT NULL,
            task_name     TEXT NOT NULL,
            output_hash   TEXT,
            status        TEXT,
            ran_at        TEXT,
            PRIMARY KEY (storage_key, pipeline_name, task_name)
        )
    """)
    for task_name, h in hashes.items():
        conn.execute(
            "INSERT INTO _kptn.task_state VALUES (?, ?, ?, ?, ?, ?)",
            (storage_key, pipeline, task_name, h, "success", "2024-01-01"),
        )
    conn.commit()
    conn.execute("CHECKPOINT")
    conn.close()


def _load_fn(path: Path, fn_name: str):
    """Load a named function from a .py file on disk via importlib."""
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, fn_name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_restore_candidate_found_when_all_cached(tmp_path: Path) -> None:
    """Baseline: checkpoint is returned when init and big_task are both cached."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    tasks_py = pkg / "tasks.py"
    tasks_py.write_text(
        "def init():\n"
        "    pass\n"
        "\n"
        "def helper():\n"
        "    return 1\n"
        "\n"
        "def big_task():\n"
        "    return helper()\n"
    )

    init_fn = _load_fn(tasks_py, "init")
    big_task_fn = _load_fn(tasks_py, "big_task")

    ordered = [
        _node("init", init_fn),
        _node("big_task", big_task_fn, duckdb_checkpoint=True),
        _node("stuff", lambda: None),
    ]

    db_path = tmp_path / "pipeline.ddb"
    cp = checkpoint_path(db_path, "big_task")
    _make_backup_db(
        cp,
        {"init": hash_task_source(init_fn), "big_task": hash_task_source(big_task_fn)},
        "kptn",
        "default",
    )

    assert find_restore_candidate(ordered, "kptn", "default", db_path) == cp


def test_restore_candidate_none_when_helper_changes(tmp_path: Path) -> None:
    """Restore is skipped when a callee of the checkpoint task changes."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    tasks_py = pkg / "tasks.py"
    tasks_py.write_text(
        "def init():\n"
        "    pass\n"
        "\n"
        "def helper():\n"
        "    return 1\n"
        "\n"
        "def big_task():\n"
        "    return helper()\n"
    )

    init_fn = _load_fn(tasks_py, "init")
    big_task_fn = _load_fn(tasks_py, "big_task")

    ordered = [
        _node("init", init_fn),
        _node("big_task", big_task_fn, duckdb_checkpoint=True),
        _node("stuff", lambda: None),
    ]

    db_path = tmp_path / "pipeline.ddb"
    cp = checkpoint_path(db_path, "big_task")
    _make_backup_db(
        cp,
        {"init": hash_task_source(init_fn), "big_task": hash_task_source(big_task_fn)},
        "kptn",
        "default",
    )

    # Mutate the helper — big_task's transitive hash must now differ from backup
    tasks_py.write_text(
        "def init():\n"
        "    pass\n"
        "\n"
        "def helper():\n"
        "    return 2\n"  # changed
        "\n"
        "def big_task():\n"
        "    return helper()\n"
    )

    assert find_restore_candidate(ordered, "kptn", "default", db_path) is None


def test_restore_candidate_none_when_helper_in_separate_module_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Restore is skipped when a callee imported from a separate module changes."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")

    helpers_py = pkg / "helpers.py"
    helpers_py.write_text("def helper():\n    return 1\n")

    tasks_py = pkg / "tasks.py"
    tasks_py.write_text(
        "from pkg.helpers import helper\n"
        "\n"
        "def init():\n"
        "    pass\n"
        "\n"
        "def big_task():\n"
        "    return helper()\n"
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    init_fn = _load_fn(tasks_py, "init")
    big_task_fn = _load_fn(tasks_py, "big_task")

    ordered = [
        _node("init", init_fn),
        _node("big_task", big_task_fn, duckdb_checkpoint=True),
        _node("stuff", lambda: None),
    ]

    db_path = tmp_path / "pipeline.ddb"
    cp = checkpoint_path(db_path, "big_task")
    _make_backup_db(
        cp,
        {"init": hash_task_source(init_fn), "big_task": hash_task_source(big_task_fn)},
        "kptn",
        "default",
    )

    # Mutate the helper module
    helpers_py.write_text("def helper():\n    return 2\n")

    assert find_restore_candidate(ordered, "kptn", "default", db_path) is None


def test_hash_task_source_follows_wrapped_for_transitive_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """hash_task_source must unwrap _KptnCallable to enable transitive hashing.

    Without inspect.unwrap, inspect.getfile raises TypeError on a _KptnCallable
    and the collector returns [] — changes to callees silently don't bust the cache.
    """
    pkg = tmp_path / "wpkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    helpers_py = pkg / "helpers.py"
    helpers_py.write_text("def helper():\n    return 1\n")
    tasks_py = pkg / "tasks.py"
    tasks_py.write_text(
        "from wpkg.helpers import helper\n"
        "\n"
        "def big_task():\n"
        "    return helper()\n"
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    big_task_fn = _load_fn(tasks_py, "big_task")
    wrapped = _KptnCallable(big_task_fn, TaskSpec(outputs=[]))

    hash_before = hash_task_source(wrapped)
    helpers_py.write_text("def helper():\n    return 99\n")
    hash_after = hash_task_source(wrapped)

    assert hash_before != hash_after, (
        "callee change must bust the hash even when fn is a _KptnCallable"
    )


def test_restore_candidate_none_when_callee_changes_through_kptn_wrapper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """find_restore_candidate rejects backup when a callee of a @kptn.task function changes.

    Reproduces the production bug where restore fired even after staging_schema.py
    was modified, because _KptnCallable blocked transitive hashing.
    """
    pkg = tmp_path / "wpkg2"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    helpers_py = pkg / "helpers.py"
    helpers_py.write_text("def helper():\n    return 1\n")
    tasks_py = pkg / "tasks.py"
    tasks_py.write_text(
        "from wpkg2.helpers import helper\n"
        "\n"
        "def init():\n"
        "    pass\n"
        "\n"
        "def big_task():\n"
        "    return helper()\n"
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    init_fn = _load_fn(tasks_py, "init")
    big_task_fn = _load_fn(tasks_py, "big_task")

    init_wrapped = _KptnCallable(init_fn, TaskSpec(outputs=[]))
    big_task_wrapped = _KptnCallable(big_task_fn, TaskSpec(outputs=[], duckdb_checkpoint=True))

    ordered = [
        TaskNode(fn=init_wrapped, spec=TaskSpec(outputs=[]), name="init"),
        TaskNode(fn=big_task_wrapped, spec=TaskSpec(outputs=[], duckdb_checkpoint=True), name="big_task"),
        _node("stuff", lambda: None),
    ]

    db_path = tmp_path / "pipeline.ddb"
    cp = checkpoint_path(db_path, "big_task")
    _make_backup_db(
        cp,
        {
            "init": hash_task_source(init_wrapped),
            "big_task": hash_task_source(big_task_wrapped),
        },
        "kptn",
        "default",
    )

    helpers_py.write_text("def helper():\n    return 99\n")

    assert find_restore_candidate(ordered, "kptn", "default", db_path) is None
