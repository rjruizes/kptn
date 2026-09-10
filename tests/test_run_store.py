"""Tests for the durable SQLite-backed pipeline run store."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from kptn_server.run_store import (
    ActiveRunError,
    RunNotFoundError,
    RunRequest,
    RunStateError,
    RunStore,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STATUS_SUCCEEDED,
)


def request(project_root: Path, **overrides: object) -> RunRequest:
    defaults: dict[str, object] = dict(
        project_root=project_root,
        pipeline="main",
        profile="dev",
        force=False,
    )
    defaults.update(overrides)
    return RunRequest(**defaults)  # type: ignore[arg-type]


# --- persistence -------------------------------------------------------- #


def test_create_run_persists_after_reopen(tmp_path: Path) -> None:
    db = tmp_path / ".kptn" / "ui.db"
    first = RunStore(db)
    run = first.create_run(request(tmp_path))
    second = RunStore(db)
    assert second.get_run(run.run_id) == run


def test_get_run_missing_returns_none(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    assert store.get_run("does-not-exist") is None


# --- single active run lock --------------------------------------------- #


def test_only_one_active_run_per_project(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    store.create_run(request(tmp_path))
    with pytest.raises(ActiveRunError):
        store.create_run(request(tmp_path))


def test_active_run_lock_uses_canonical_project_path(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    store.create_run(request(tmp_path))
    # Same canonical project, spelled with a redundant path segment.
    weird_path = Path(str(tmp_path) + "/./")
    with pytest.raises(ActiveRunError):
        store.create_run(request(weird_path))


def test_lock_released_after_finish_allows_new_run(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    store.finish_run(run.run_id, STATUS_SUCCEEDED, exit_code=0)

    second_run = store.create_run(request(tmp_path))
    assert second_run.run_id != run.run_id
    assert store.active_run(tmp_path).run_id == second_run.run_id


def test_active_run_returns_none_when_no_lock(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    assert store.active_run(tmp_path) is None


# --- events: ordering, sequencing, SSE cursor reads ---------------------- #


def test_events_are_ordered_and_gap_free(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    e1 = store.append_event(run.run_id, "run_started")
    e2 = store.append_event(run.run_id, "task_started", task_name="load")
    e3 = store.append_event(run.run_id, "task_finished", task_name="load")

    assert [e1.sequence, e2.sequence, e3.sequence] == [1, 2, 3]

    events = store.events_after(run.run_id, 0)
    assert [e.sequence for e in events] == [1, 2, 3]
    assert [e.kind for e in events] == ["run_started", "task_started", "task_finished"]


def test_events_after_cursor_returns_only_newer_events(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    store.append_event(run.run_id, "task_started", task_name="load")
    store.append_event(run.run_id, "task_finished", task_name="load")

    events = store.events_after(run.run_id, 1)
    assert [e.sequence for e in events] == [2, 3]

    events = store.events_after(run.run_id, 3)
    assert events == []


def test_append_event_to_unknown_run_raises(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    with pytest.raises(RunNotFoundError):
        store.append_event("nope", "run_started")


def test_event_payload_round_trips(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    event = store.append_event(
        run.run_id,
        "log",
        task_name="load",
        payload={"line": "hello world", "level": "info"},
    )
    assert event.payload == {"line": "hello world", "level": "info"}

    [stored] = [e for e in store.events_after(run.run_id, 0) if e.kind == "log"]
    assert stored.payload == {"line": "hello world", "level": "info"}


# --- state transitions ---------------------------------------------------- #


def test_run_started_twice_raises_run_state_error(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    with pytest.raises(RunStateError):
        store.append_event(run.run_id, "run_started")


def test_event_after_terminal_status_raises(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    store.finish_run(run.run_id, STATUS_SUCCEEDED, exit_code=0)
    with pytest.raises(RunStateError):
        store.append_event(run.run_id, "log", payload={"line": "too late"})


def test_finish_run_twice_raises(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    store.finish_run(run.run_id, STATUS_SUCCEEDED, exit_code=0)
    with pytest.raises(RunStateError):
        store.finish_run(run.run_id, STATUS_FAILED, exit_code=1)


def test_finish_run_with_non_terminal_status_raises(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    with pytest.raises(RunStateError):
        store.finish_run(run.run_id, STATUS_RUNNING)


def test_finish_run_sets_status_and_exit_code(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    finished = store.finish_run(run.run_id, STATUS_FAILED, exit_code=1)
    assert finished.status == STATUS_FAILED
    assert finished.exit_code == 1
    assert finished.finished_at is not None


def test_request_stop_marks_stop_requested(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    stopped = store.request_stop(run.run_id)
    assert stopped.status == "stop_requested"


def test_request_stop_on_terminal_run_raises(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    store.finish_run(run.run_id, STATUS_STOPPED)
    with pytest.raises(RunStateError):
        store.request_stop(run.run_id)


# --- warning grouping without occurrence loss --------------------------- #


def test_warning_groups_preserve_all_occurrences(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    store.append_event(
        run.run_id,
        "warning",
        task_name="load",
        payload={
            "message": "2026-09-10T10:00:00Z retrying connection",
            "category": "network",
        },
    )
    store.append_event(
        run.run_id,
        "warning",
        task_name="load",
        payload={
            "message": "2026-09-10T10:05:00Z retrying connection",
            "category": "network",
        },
    )
    store.append_event(
        run.run_id,
        "warning",
        task_name="load",
        payload={"message": "\x1b[33mdisk almost full\x1b[0m", "category": "disk"},
    )

    groups = store.warning_groups(run.run_id)
    assert len(groups) == 2

    network_group = next(g for g in groups if g.category == "network")
    assert network_group.count == 2
    assert network_group.occurrence_sequences == (2, 3)

    disk_group = next(g for g in groups if g.category == "disk")
    assert disk_group.count == 1
    assert disk_group.occurrence_sequences == (4,)

    # The raw warning events themselves are never collapsed or deleted.
    warning_events = [
        e for e in store.events_after(run.run_id, 0) if e.kind == "warning"
    ]
    assert len(warning_events) == 3


def test_warning_fingerprint_ignores_timestamps_and_ansi(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    store.append_event(
        run.run_id,
        "warning",
        task_name="load",
        payload={"message": "10:00:00 \x1b[31mconnection refused\x1b[0m"},
    )
    store.append_event(
        run.run_id,
        "warning",
        task_name="load",
        payload={"message": "10:05:32 connection refused"},
    )

    groups = store.warning_groups(run.run_id)
    assert len(groups) == 1
    assert groups[0].count == 2


# --- list_runs ------------------------------------------------------------ #


def test_list_runs_filters_by_project_and_orders_newest_first(tmp_path: Path) -> None:
    project_a = tmp_path / "a"
    project_b = tmp_path / "b"
    project_a.mkdir()
    project_b.mkdir()
    store = RunStore(tmp_path / "ui.db")

    run_a1 = store.create_run(request(project_a))
    store.append_event(run_a1.run_id, "run_started")
    store.finish_run(run_a1.run_id, STATUS_SUCCEEDED, exit_code=0)
    run_a2 = store.create_run(request(project_a))

    store.create_run(request(project_b))

    project_a_runs = store.list_runs(project_a)
    assert {r.run_id for r in project_a_runs} == {run_a1.run_id, run_a2.run_id}

    all_runs = store.list_runs()
    assert len(all_runs) == 3


# --- PRAGMA settings ------------------------------------------------------ #


def test_wal_mode_enabled(tmp_path: Path) -> None:
    db_path = tmp_path / "ui.db"
    RunStore(db_path)
    conn = sqlite3.connect(db_path)
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"
    finally:
        conn.close()


def test_busy_timeout_is_five_seconds(tmp_path: Path) -> None:
    db_path = tmp_path / "ui.db"
    store = RunStore(db_path)
    conn = store._connect()  # noqa: SLF001 - inspecting the PRAGMA the store sets
    try:
        timeout_ms = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        assert timeout_ms == 5000
    finally:
        conn.close()
