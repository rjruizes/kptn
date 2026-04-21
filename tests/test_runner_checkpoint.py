"""Tests for kptn.runner.checkpoint.find_restore_candidate and _try_restore."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

duckdb = pytest.importorskip("duckdb")

from kptn.change_detector.hasher import hash_task_source
from kptn.graph.decorators import TaskSpec, _KptnCallable
from kptn.graph.nodes import TaskNode
from kptn.runner.checkpoint import checkpoint_path, find_restore_candidate
from kptn.runner.executor import _try_restore


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


# ---------------------------------------------------------------------------
# _try_restore: only restore when the database was deleted
# ---------------------------------------------------------------------------


def _make_resolved(storage_key: str = "kptn", pipeline: str = "default"):
    """Minimal ResolvedGraph stand-in for _try_restore tests."""
    from kptn.profiles.resolved import ResolvedGraph
    from kptn.graph.graph import Graph

    resolved = MagicMock(spec=ResolvedGraph)
    resolved.storage_key = storage_key
    resolved.pipeline = pipeline
    return resolved


def test_try_restore_skips_when_db_has_existing_state(tmp_path: Path) -> None:
    """_try_restore must NOT restore when the database file exists with pipeline state.

    Regression: a backup file's mere existence was enough to trigger an
    unconditional restore, overwriting a live database.
    """
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    tasks_py = pkg / "tasks.py"
    tasks_py.write_text("def big_task():\n    pass\n")
    big_task_fn = _load_fn(tasks_py, "big_task")

    ordered = [_node("big_task", big_task_fn, duckdb_checkpoint=True)]

    db_path = tmp_path / "pipeline.ddb"

    # Seed the live database with pipeline state so it appears intact.
    live_conn = duckdb.connect(str(db_path))
    live_conn.execute("CREATE SCHEMA IF NOT EXISTS _kptn")
    live_conn.execute("""
        CREATE TABLE IF NOT EXISTS _kptn.task_state (
            storage_key TEXT NOT NULL, pipeline_name TEXT NOT NULL,
            task_name TEXT NOT NULL, output_hash TEXT, status TEXT, ran_at TEXT,
            PRIMARY KEY (storage_key, pipeline_name, task_name)
        )
    """)
    live_conn.execute(
        "INSERT INTO _kptn.task_state VALUES (?, ?, ?, ?, ?, ?)",
        ("kptn", "default", "big_task", "abc123", "success", "2024-01-01"),
    )
    live_conn.commit()
    live_conn.execute("CHECKPOINT")
    live_conn.close()

    # Create a valid backup alongside the live database.
    cp = checkpoint_path(db_path, "big_task")
    _make_backup_db(cp, {"big_task": hash_task_source(big_task_fn)}, "kptn", "default")

    original_size = db_path.stat().st_size

    def factory():
        return duckdb.connect(str(db_path))

    resolved = _make_resolved()
    _try_restore(ordered, resolved, factory)

    # Live database must be untouched (backup was NOT restored).
    assert db_path.stat().st_size == original_size
    conn = duckdb.connect(str(db_path))
    row = conn.execute("SELECT output_hash FROM _kptn.task_state").fetchone()
    conn.close()
    assert row is not None and row[0] == "abc123"


def test_try_restore_restores_when_db_was_deleted(tmp_path: Path) -> None:
    """_try_restore MUST restore when the database file was deleted and a backup exists."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    tasks_py = pkg / "tasks.py"
    tasks_py.write_text("def big_task():\n    pass\n")
    big_task_fn = _load_fn(tasks_py, "big_task")

    ordered = [_node("big_task", big_task_fn, duckdb_checkpoint=True)]

    db_path = tmp_path / "pipeline.ddb"

    # Create a backup that would be valid for this task.
    cp = checkpoint_path(db_path, "big_task")
    _make_backup_db(cp, {"big_task": hash_task_source(big_task_fn)}, "kptn", "default")
    backup_size = cp.stat().st_size

    # db_path does NOT exist — simulates the user having deleted it.
    assert not db_path.exists()

    def factory():
        return duckdb.connect(str(db_path))

    resolved = _make_resolved()
    _try_restore(ordered, resolved, factory)

    # The database should now be the restored backup.
    assert db_path.exists()
    assert db_path.stat().st_size == backup_size
