"""Tests for the durable SQLite-backed pipeline run store."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from types import MappingProxyType

import pytest

from kptn_server.run_store import (
    COUNTED_EVENT_KINDS,
    ActiveRunError,
    RunNotFoundError,
    RunRequest,
    RunStateError,
    RunStore,
    STATUS_FAILED,
    STATUS_INTERRUPTED,
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


def test_unfinished_runs_filters_terminal_statuses_in_sql(tmp_path: Path) -> None:
    """A supervisor polls this every few seconds, so it must not hydrate
    every run ever recorded."""
    project_a = tmp_path / "a"
    project_b = tmp_path / "b"
    project_a.mkdir()
    project_b.mkdir()
    store = RunStore(tmp_path / "ui.db")

    finished = store.create_run(request(project_a))
    store.append_event(finished.run_id, "run_started")
    store.finish_run(finished.run_id, STATUS_SUCCEEDED, exit_code=0)

    queued = store.create_run(request(project_a))
    running = store.create_run(request(project_b))
    store.append_event(running.run_id, "run_started")

    assert [r.run_id for r in store.unfinished_runs()] == [
        queued.run_id,
        running.run_id,
    ]
    assert [r.run_id for r in store.unfinished_runs(project_b)] == [running.run_id]

    store.finish_run(running.run_id, STATUS_FAILED, exit_code=1)
    assert [r.run_id for r in store.unfinished_runs()] == [queued.run_id]


def test_unfinished_runs_excludes_interrupted(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    store.finish_run(run.run_id, STATUS_INTERRUPTED)
    assert store.unfinished_runs() == []


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


# --- payload types from the real event contract -------------------------- #


def test_append_event_accepts_mappingproxy_payload(tmp_path: Path) -> None:
    # kptn/runner/events.py builds RunEvent.payload as
    # MappingProxyType(dict(payload)) -- Task 5's worker sink will hand
    # append_event exactly this object, not a plain dict.
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")

    proxy_payload = MappingProxyType(
        {"message": "disk almost full", "category": "disk"}
    )
    event = store.append_event(
        run.run_id,
        "warning",
        task_name="load",
        payload=proxy_payload,
    )
    assert event.payload == {"message": "disk almost full", "category": "disk"}

    [stored] = [e for e in store.events_after(run.run_id, 0) if e.kind == "warning"]
    assert stored.payload == {"message": "disk almost full", "category": "disk"}


# --- concurrent first-creation of a brand-new database -------------------- #


def test_concurrent_first_creation_does_not_raise(tmp_path: Path) -> None:
    db_path = tmp_path / ".kptn" / "ui.db"
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def _create() -> None:
        try:
            barrier.wait(timeout=5)
            RunStore(db_path)
        except BaseException as exc:  # noqa: BLE001 - capture for the assertion below
            errors.append(exc)

    threads = [threading.Thread(target=_create) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []

    # The schema is usable afterwards regardless of which thread "won".
    store = RunStore(db_path)
    run = store.create_run(request(tmp_path))
    assert store.get_run(run.run_id) == run


# --- aggregated event counts -------------------------------------------- #


def _counted_run(store: RunStore, tmp_path: Path) -> str:
    record = store.create_run(request(tmp_path))
    store.append_event(record.run_id, "run_started")
    store.append_event(record.run_id, "task_started", task_name="alpha")
    for index in range(5):
        store.append_event(
            record.run_id,
            "log",
            task_name="alpha",
            payload={"stream": "stdout", "severity": "output"},
            log_start=index,
            log_end=index + 1,
        )
    store.append_event(
        record.run_id, "warning", task_name="alpha", payload={"message": "careful"}
    )
    store.append_event(
        record.run_id,
        "task_finished",
        task_name="alpha",
        payload={"status": "succeeded", "duration_seconds": 0.1},
    )
    store.append_event(record.run_id, "task_started", task_name="beta")
    store.append_event(
        record.run_id, "task_skipped", task_name="gamma", payload={"cached": True}
    )
    store.append_event(
        record.run_id,
        "task_finished",
        task_name="beta",
        payload={"status": "failed", "error": "boom"},
    )
    return record.run_id


def test_event_counts_tallies_by_kind_and_task_status(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    run_id = _counted_run(store, tmp_path)

    assert store.event_counts(run_id) == {
        ("task_started", None): 2,
        ("task_skipped", None): 1,
        ("warning", None): 1,
        ("task_finished", "succeeded"): 1,
        ("task_finished", "failed"): 1,
    }


def test_event_counts_excludes_log_events(tmp_path: Path) -> None:
    """``log`` is by far the most numerous kind and contributes no counts.

    Excluding it in SQL is why the run-history page can summarize fifty runs
    without hydrating every line of output they ever produced.
    """
    store = RunStore(tmp_path / "ui.db")
    run_id = _counted_run(store, tmp_path)

    assert not [key for key in store.event_counts(run_id) if key[0] == "log"]
    assert not [key for key in store.event_counts(run_id) if key[0] == "run_started"]


def test_event_counts_agrees_with_the_hydrated_events(tmp_path: Path) -> None:
    """The aggregate is a faster way to the same answer, not a different one."""
    store = RunStore(tmp_path / "ui.db")
    run_id = _counted_run(store, tmp_path)

    expected: dict[tuple[str, str | None], int] = {}
    for event in store.events_after(run_id):
        if event.kind not in COUNTED_EVENT_KINDS:
            continue
        status = event.payload.get("status")
        key = (event.kind, status if isinstance(status, str) else None)
        expected[key] = expected.get(key, 0) + 1

    assert store.event_counts(run_id) == expected


def test_event_counts_is_empty_for_a_run_with_no_counted_events(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "ui.db")
    record = store.create_run(request(tmp_path))
    assert store.event_counts(record.run_id) == {}
    # And for a run that does not exist at all: a count of nothing, not a raise.
    assert store.event_counts("nope") == {}


def test_the_stores_event_vocabulary_comes_from_the_runner() -> None:
    """Every kind this module tests for is an ``EventKind``, not a literal.

    Three sites keyed off bare strings: the counted kinds, and -- worse --
    ``append_event``'s status machine, where a rename in the runner would
    have broken a state transition rather than merely zeroing a count.
    Deriving them means a rename is an ``AttributeError`` at import.
    """
    from kptn.runner.events import EventKind
    from kptn_server import run_store

    known = {kind.value for kind in EventKind}
    assert set(COUNTED_EVENT_KINDS) <= known
    # ``log`` is the one work-shaped kind deliberately left out.
    assert EventKind.LOG.value not in COUNTED_EVENT_KINDS

    source = Path(run_store.__file__).read_text(encoding="utf-8")
    for kind in EventKind:
        # Both quotings: a kind spelled single-quoted inside an SQL string is
        # exactly as much of a rename hazard as a double-quoted Python one.
        for literal in (f'"{kind.value}"', f"'{kind.value}'"):
            assert literal not in source, (
                f"{kind.value!r} is spelled as a literal in run_store.py; "
                "derive it from EventKind instead"
            )


# --- connection reuse ---------------------------------------------------- #
#
# On an NFS-mounted project, opening a connection in WAL mode costs ~180ms and
# closing one ~40ms -- every fcntl lock is a network round trip. A store that
# opened a connection per call made every UI page and every captured output
# line pay that, so the store keeps one connection per thread instead.


def _counting_connect(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    opened: list[object] = []
    real_connect = sqlite3.connect

    def counting(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", counting)
    return opened


def test_store_reuses_one_connection_per_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened = _counting_connect(monkeypatch)
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    store.append_event(run.run_id, "run_started")
    for _ in range(10):
        store.get_run(run.run_id)
        store.events_after(run.run_id, 0)
        store.active_run(tmp_path)
    assert len(opened) == 1

    other_thread: list[int] = []
    worker = threading.Thread(
        target=lambda: other_thread.append(len(store.list_runs()))
    )
    worker.start()
    worker.join(timeout=10)
    assert other_thread == [1]
    assert len(opened) == 2


def test_reused_connection_sees_writes_from_other_connections(tmp_path: Path) -> None:
    db = tmp_path / "ui.db"
    reader = RunStore(db)
    writer = RunStore(db)
    run = writer.create_run(request(tmp_path))
    writer.append_event(run.run_id, "run_started")
    assert reader.get_run(run.run_id).status == STATUS_RUNNING  # type: ignore[union-attr]

    writer.finish_run(run.run_id, STATUS_SUCCEEDED)
    record = reader.get_run(run.run_id)
    assert record is not None and record.status == STATUS_SUCCEEDED
    assert reader.active_run(tmp_path) is None


def test_an_abandoned_transaction_does_not_poison_the_reused_connection(
    tmp_path: Path,
) -> None:
    store = RunStore(tmp_path / "ui.db")
    run = store.create_run(request(tmp_path))
    conn = store._acquire()  # noqa: SLF001 - simulating a call that died mid-transaction
    conn.execute("BEGIN IMMEDIATE")
    store._release(conn)  # noqa: SLF001

    store.append_event(run.run_id, "run_started")
    assert store.get_run(run.run_id).status == STATUS_RUNNING  # type: ignore[union-attr]
    # And the write lock was really let go: a second connection can write.
    RunStore(tmp_path / "ui.db").finish_run(run.run_id, STATUS_STOPPED)


def test_close_closes_every_thread_connection(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    store.list_runs()
    used, release = threading.Event(), threading.Event()

    def hold_a_connection() -> None:
        store.list_runs()
        used.set()
        release.wait(10)  # still alive, so its connection is still open

    worker = threading.Thread(target=hold_a_connection)
    worker.start()
    assert used.wait(10)
    connections = store._open_connections()  # noqa: SLF001
    assert len(connections) == 2

    store.close()
    release.set()
    worker.join(timeout=10)

    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
    # A closed store reopens on next use rather than failing.
    assert store.list_runs() == []


def test_a_finished_threads_connection_is_closed_not_kept(tmp_path: Path) -> None:
    """The UI's thread pool retires idle threads; their connections must go too."""
    import gc

    store = RunStore(tmp_path / "ui.db")
    opened: list[sqlite3.Connection] = []

    def use_store() -> None:
        store.list_runs()
        opened.append(store._acquire())  # noqa: SLF001 - the connection this thread got

    for _ in range(5):
        worker = threading.Thread(target=use_store)
        worker.start()
        worker.join(timeout=10)
    gc.collect()

    assert len(store._open_connections()) == 1  # noqa: SLF001 - only this thread's
    for conn in opened:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
