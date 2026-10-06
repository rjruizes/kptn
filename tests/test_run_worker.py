"""Tests for the detached pipeline worker and its output capture layer.

The worker is the process that actually executes a pipeline. Everything it
produces -- stdout, stderr, ``warnings.warn`` calls, ``logging`` records at
WARNING or above, and the runner's own structured events -- has to land in the
durable run store, because the browser, the notebook server, and FastAPI may
all restart
while the run is still going.

The primary test drives a real ``python -m kptn_server.worker`` subprocess: the
worker surviving as a detached process is the whole point, so at least one test
must not cheat by calling ``main()`` in-process.
"""

from __future__ import annotations

import io
import logging
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import warnings
from dataclasses import dataclass
from pathlib import Path

import pytest

from kptn.runner.events import EventKind, RunEvent
from kptn_server import worker
from kptn_server.capture import (
    SEVERITY_OUTPUT,
    SEVERITY_STDERR,
    STREAM_STDERR,
    STREAM_STDOUT,
    DurableWriteFailed,
    RunStoreSink,
    StructuredWarningHandler,
    _Reentry,
    _store_write_reentry,
    capture_worker_output,
    durable,
)
from kptn_server.run_files import read_span_text, run_file_text
from kptn_server.run_store import (
    STATUS_ERRORED,
    STATUS_FAILED,
    STATUS_STOPPED,
    STATUS_SUCCEEDED,
    RunNotFoundError,
    RunRequest,
    RunStore,
    RunStoreError,
    StoredEvent,
)

FIXTURE_PROJECT = Path(__file__).parent / "fixtures" / "ui_project"
REPO_ROOT = Path(__file__).resolve().parents[1]
POLL_TIMEOUT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.02


# -- fixtures and helpers -------------------------------------------------


@pytest.fixture(autouse=True)
def restore_process_state():
    """Guard the test process against anything the worker or capture leaks."""
    original_cwd = Path.cwd()
    original_path = sys.path.copy()
    original_modules = set(sys.modules)
    original_stdout, original_stderr = sys.stdout, sys.stderr
    original_showwarning = warnings.showwarning
    root = logging.getLogger()
    original_handlers = root.handlers.copy()

    yield

    os.chdir(original_cwd)
    sys.path[:] = original_path
    for name in set(sys.modules) - original_modules:
        sys.modules.pop(name, None)
    sys.stdout, sys.stderr = original_stdout, original_stderr
    warnings.showwarning = original_showwarning
    root.handlers[:] = original_handlers


@dataclass(frozen=True)
class FixtureRun:
    store: RunStore
    db_path: Path
    run_id: str
    project_root: Path
    log_path: Path

    def events(self) -> list[StoredEvent]:
        return self.store.events_after(self.run_id)

    def captured_text(self) -> str:
        """Every line of captured output in the run file, in order."""
        return run_file_text(self.log_path)

    def slice_log(self, event: StoredEvent) -> str:
        assert event.log_start is not None and event.log_end is not None
        return read_span_text(self.log_path, event.log_start, event.log_end)


@pytest.fixture
def ui_project(tmp_path: Path) -> Path:
    destination = tmp_path / "project"
    shutil.copytree(FIXTURE_PROJECT, destination)
    return destination


def create_fixture_run(
    ui_project: Path,
    *,
    profile: str,
    force: bool = False,
) -> FixtureRun:
    db_path = ui_project / ".kptn" / "ui.db"
    store = RunStore(db_path)
    record = store.create_run(
        RunRequest(
            project_root=ui_project,
            pipeline="fixture",
            profile=profile,
            force=force,
        )
    )
    return FixtureRun(
        store=store,
        db_path=db_path,
        run_id=record.run_id,
        project_root=record.project_root,
        log_path=record.log_path,
    )


def _worker_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env.pop("KPTN_UI_FIXTURE_RUN_LEVEL_WARNING", None)
    env.pop("KPTN_UI_FIXTURE_SENTINEL", None)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(REPO_ROOT) if not existing else f"{REPO_ROOT}{os.pathsep}{existing}"
    )
    env.update(extra or {})
    return env


def _worker_argv(run: FixtureRun) -> list[str]:
    return [
        sys.executable,
        "-m",
        "kptn_server.worker",
        "--db",
        str(run.db_path),
        "--run-id",
        run.run_id,
    ]


def run_worker(
    run: FixtureRun,
    *,
    env: dict[str, str] | None = None,
    timeout: float = 120.0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        _worker_argv(run),
        cwd=REPO_ROOT,
        env=_worker_env(env),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def spawn_worker(
    run: FixtureRun,
    *,
    env: dict[str, str] | None = None,
    extra_argv: list[str] | None = None,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        _worker_argv(run) + (extra_argv or []),
        cwd=REPO_ROOT,
        env=_worker_env(env),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def wait_until(predicate, *, message: str, timeout: float = POLL_TIMEOUT_SECONDS):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {message}")
        time.sleep(POLL_INTERVAL_SECONDS)


def warning_events(run: FixtureRun) -> list[StoredEvent]:
    return [e for e in run.events() if e.kind == EventKind.WARNING.value]


def log_events(run: FixtureRun) -> list[StoredEvent]:
    return [e for e in run.events() if e.kind == EventKind.LOG.value]


# -- worker end-to-end ----------------------------------------------------


def test_worker_persists_output_and_structured_warnings(ui_project: Path) -> None:
    run = create_fixture_run(ui_project, profile="success")

    result = run_worker(run)

    assert result.returncode == 0, result.stderr
    reopened = RunStore(run.db_path)
    record = reopened.get_run(run.run_id)
    assert record is not None
    assert record.status == STATUS_SUCCEEDED
    assert record.exit_code == 0
    assert record.finished_at is not None
    assert record.worker_pid is not None

    assert "ordinary output" in run.captured_text()
    # Captured output goes to the log file and the store, never back out
    # through the replaced Python stream.
    assert "ordinary output" not in result.stdout

    groups = reopened.warning_groups(run.run_id)
    assert {(g.category, g.count) for g in groups} == {
        ("UserWarning", 1),
        ("fixture", 1),
    }

    kinds = [e.kind for e in reopened.events_after(run.run_id)]
    assert kinds[0] == EventKind.RUN_STARTED.value
    assert kinds[-1] == EventKind.RUN_FINISHED.value
    assert EventKind.TASK_STARTED.value in kinds
    assert EventKind.TASK_FINISHED.value in kinds


def test_worker_records_failure_and_exits_nonzero(ui_project: Path) -> None:
    run = create_fixture_run(ui_project, profile="failure")

    result = run_worker(run)

    assert result.returncode != 0
    reopened = RunStore(run.db_path)
    record = reopened.get_run(run.run_id)
    assert record is not None
    assert record.status == STATUS_FAILED
    assert record.exit_code == result.returncode

    finished = [
        e
        for e in reopened.events_after(run.run_id)
        if e.kind == EventKind.RUN_FINISHED.value
    ]
    assert len(finished) == 1
    assert finished[0].payload["status"] == "failed"
    assert "fixture failure" in str(finished[0].payload["error"])
    # The traceback the worker prints must also reach the durable log.
    assert "fixture failure" in run.captured_text()


def test_worker_attributes_task_output_and_warnings_to_the_running_task(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")

    assert run_worker(run).returncode == 0

    warnings_seen = warning_events(run)
    assert warnings_seen, "expected structured warning events"
    assert {e.task_name for e in warnings_seen} == {"noisy_task"}
    assert {str(e.payload["category"]) for e in warnings_seen} == {
        "UserWarning",
        "fixture",
    }

    ordinary = [e for e in log_events(run) if run.slice_log(e) == "ordinary output\n"]
    assert len(ordinary) == 1
    assert ordinary[0].task_name == "noisy_task"


def test_worker_records_run_level_warning_without_task_attribution(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")

    result = run_worker(run, env={"KPTN_UI_FIXTURE_RUN_LEVEL_WARNING": "1"})

    assert result.returncode == 0, result.stderr
    run_level = [
        e for e in warning_events(run) if str(e.payload["category"]) == "RuntimeWarning"
    ]
    assert len(run_level) == 1
    assert run_level[0].task_name is None
    assert run_level[0].payload["message"] == "run-level warning"
    # Grouping keeps it separate from the in-task warnings.
    groups = run.store.warning_groups(run.run_id)
    assert ("RuntimeWarning", None) in {(g.category, g.task_name) for g in groups}


def test_worker_records_raw_stderr_as_log_output_not_a_warning(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")

    assert run_worker(run).returncode == 0

    stderr_logs = [e for e in log_events(run) if e.payload["stream"] == STREAM_STDERR]
    raw = [e for e in stderr_logs if run.slice_log(e) == "raw stderr output\n"]
    assert len(raw) == 1
    assert raw[0].payload["severity"] == SEVERITY_STDERR

    stdout_logs = [e for e in log_events(run) if e.payload["stream"] == STREAM_STDOUT]
    assert all(e.payload["severity"] == SEVERITY_OUTPUT for e in stdout_logs)

    # Raw stderr must never be reclassified as a warning.
    assert all(
        "raw stderr output" not in str(e.payload["message"])
        for e in warning_events(run)
    )


def test_worker_records_pid_and_heartbeat_while_a_slow_run_is_in_flight(
    ui_project: Path, tmp_path: Path
) -> None:
    sentinel = tmp_path / "release-slow-task"
    run = create_fixture_run(ui_project, profile="slow")
    process = spawn_worker(
        run,
        env={"KPTN_UI_FIXTURE_SENTINEL": str(sentinel)},
        extra_argv=["--heartbeat-interval", "0.05"],
    )

    try:
        record = wait_until(
            lambda: (
                lambda r: r if r is not None and r.worker_pid is not None else None
            )(run.store.get_run(run.run_id)),
            message="the worker to record its process identity",
        )
        assert record.worker_pid == process.pid
        assert record.worker_started_at is not None

        wait_until(
            lambda: run.store.get_run(run.run_id).current_task == "noisy_task",
            message="the slow task to start",
        )
        first_heartbeat = run.store.get_run(run.run_id).heartbeat_at
        assert first_heartbeat is not None
        # The daemon heartbeat thread keeps advancing it while the run blocks.
        wait_until(
            lambda: run.store.get_run(run.run_id).heartbeat_at > first_heartbeat,
            message="the heartbeat thread to advance heartbeat_at",
        )
        assert process.poll() is None, "worker exited before the sentinel appeared"

        sentinel.touch()
        assert process.wait(timeout=60) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)

    record = run.store.get_run(run.run_id)
    assert record.status == STATUS_SUCCEEDED
    log_text = run.captured_text()
    assert "slow task waiting for sentinel" in log_text


def test_worker_records_a_terminated_run_as_stopped(
    ui_project: Path, tmp_path: Path
) -> None:
    sentinel = tmp_path / "never-created"
    run = create_fixture_run(ui_project, profile="slow")
    process = spawn_worker(run, env={"KPTN_UI_FIXTURE_SENTINEL": str(sentinel)})

    try:
        wait_until(
            lambda: run.store.get_run(run.run_id).current_task == "noisy_task",
            message="the slow task to start",
        )
        process.terminate()
        returncode = process.wait(timeout=60)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)

    assert returncode != 0
    record = run.store.get_run(run.run_id)
    assert record.status == STATUS_STOPPED
    assert record.exit_code == returncode
    assert not sentinel.exists()


def test_worker_stops_and_exits_nonzero_when_a_durable_write_fails(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    real_append = RunStore.append_event
    calls = {"count": 0}

    def failing_append(self, *args, **kwargs):
        calls["count"] += 1
        if calls["count"] > 3:
            raise RunStoreError("simulated durable write failure")
        return real_append(self, *args, **kwargs)

    monkeypatch.setattr(RunStore, "append_event", failing_append)

    exit_code = worker.main(["--db", str(run.db_path), "--run-id", run.run_id])

    assert exit_code != 0
    monkeypatch.undo()
    reopened = RunStore(run.db_path)
    record = reopened.get_run(run.run_id)
    assert record is not None
    assert record.status == STATUS_ERRORED
    assert record.exit_code == exit_code


def test_worker_rejects_an_unknown_run_id(tmp_path: Path) -> None:
    db_path = tmp_path / ".kptn" / "ui.db"
    RunStore(db_path)

    exit_code = worker.main(["--db", str(db_path), "--run-id", "does-not-exist"])

    assert exit_code != 0


# -- RunStoreSink ---------------------------------------------------------


def _run_event(kind: EventKind, **payload: object) -> RunEvent:
    from datetime import datetime, timezone
    from types import MappingProxyType

    return RunEvent(
        run_id="ignored",
        sequence=1,
        timestamp=datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        kind=kind,
        pipeline="fixture",
        profile="success",
        task_name="noisy_task",
        payload=MappingProxyType(dict(payload)),
    )


def test_run_store_sink_translates_run_events_into_durable_rows(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    sink = RunStoreSink(run.store, run.run_id)

    sink.emit(_run_event(EventKind.RUN_STARTED))
    sink.emit(_run_event(EventKind.TASK_STARTED, mode="python"))

    events = run.events()
    assert [e.kind for e in events] == [
        EventKind.RUN_STARTED.value,
        EventKind.TASK_STARTED.value,
    ]
    assert events[1].task_name == "noisy_task"
    assert events[1].payload == {"mode": "python"}
    assert events[0].timestamp.isoformat() == "2024-01-02T03:04:05+00:00"
    # The store, not the in-memory emitter, owns the durable sequence numbers.
    assert [e.sequence for e in events] == [1, 2]


def test_run_store_sink_propagates_store_errors(ui_project: Path) -> None:
    run = create_fixture_run(ui_project, profile="success")
    sink = RunStoreSink(run.store, "no-such-run")

    with pytest.raises(RunStoreError):
        sink.emit(_run_event(EventKind.RUN_STARTED))


# -- capture layer --------------------------------------------------------


def test_capture_restores_streams_handlers_and_showwarning_when_body_raises(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    root = logging.getLogger()
    original_stdout, original_stderr = sys.stdout, sys.stderr
    original_showwarning = warnings.showwarning
    original_handlers = root.handlers.copy()
    original_filters = warnings.filters.copy()

    with pytest.raises(RuntimeError, match="boom"):
        with capture_worker_output(run.store, run.run_id):
            assert sys.stdout is not original_stdout
            assert sys.stderr is not original_stderr
            assert warnings.showwarning is not original_showwarning
            assert root.handlers != original_handlers
            print("before the failure")
            raise RuntimeError("boom")

    assert sys.stdout is original_stdout
    assert sys.stderr is original_stderr
    assert warnings.showwarning is original_showwarning
    assert root.handlers == original_handlers
    assert warnings.filters == original_filters
    # Output written before the failure is still durable.
    assert "before the failure\n" in run.captured_text()


def test_capture_buffers_partial_lines_until_newline_or_exit(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    with capture_worker_output(run.store, run.run_id) as capture:
        sys.stdout.write("par")
        capture.sink.flush()
        assert log_events(run) == []
        sys.stdout.write("tial\n")
        capture.sink.flush()
        assert len(log_events(run)) == 1
        sys.stdout.write("trailing without newline")
        capture.sink.flush()
        assert len(log_events(run)) == 1

    events = log_events(run)
    assert [run.slice_log(e) for e in events] == [
        "partial\n",
        "trailing without newline",
    ]
    assert run.captured_text() == "partial\ntrailing without newline"


def test_capture_records_log_offsets_that_match_the_log_file(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    with capture_worker_output(run.store, run.run_id):
        print("first")
        print("sécond")
        print("third", file=sys.stderr)

    events = log_events(run)
    assert [run.slice_log(e) for e in events] == ["first\n", "sécond\n", "third\n"]
    assert [e.payload["stream"] for e in events] == [
        STREAM_STDOUT,
        STREAM_STDOUT,
        STREAM_STDERR,
    ]
    # Each span is one whole line of the run file, by byte offset: the lines
    # follow one another, and the last one ends the file.
    assert events[0].log_end == events[1].log_start
    assert events[1].log_end == events[2].log_start
    assert events[2].log_end == run.log_path.stat().st_size


def test_capture_serializes_concurrent_writes_under_one_lock(
    ui_project: Path,
) -> None:
    """Captured text is handed to the sink one writer at a time.

    Proven deterministically rather than by timing luck: a hook fires inside
    the critical section, just before the hand-over, and blocks there. A
    second thread then tries to write. If the hand-over is covered by the
    lock, it cannot get in -- no second event. If it escaped the lock, the
    second thread would land its write and the assertions below would fail.
    """
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    inside = threading.Event()
    release = threading.Event()
    second_started = threading.Event()
    second_finished = threading.Event()

    with capture_worker_output(run.store, run.run_id) as capture:
        lock_held: list[bool] = []

        def hook() -> None:
            # Structural, not adjacency-based: the hook fires from inside
            # _commit_span, before the hand-over to the sink, and this asserts
            # the lock really is held at that point. If a future change moved
            # the hand-over out of the critical section, this fails even
            # though the hook itself did not move.
            lock_held.append(capture.state.lock_is_held())
            inside.set()
            assert release.wait(10), "critical-section hook was never released"

        capture.state._critical_section_hook = hook

        def first() -> None:
            capture.stdout.write("first line\n")

        def second() -> None:
            second_started.set()
            capture.stdout.write("second line\n")
            second_finished.set()

        thread_one = threading.Thread(target=first, daemon=True)
        thread_one.start()
        assert inside.wait(10), "first writer never reached the critical section"

        thread_two = threading.Thread(target=second, daemon=True)
        thread_two.start()
        assert second_started.wait(10)
        assert not second_finished.wait(0.25), (
            "second writer completed while the critical section was held"
        )
        # The first writer is inside the critical section and the second is
        # blocked behind it, so nothing has reached the store or the file.
        assert len(log_events(run)) == 0
        assert run.captured_text() == ""

        release.set()
        thread_one.join(timeout=10)
        assert second_finished.wait(10)
        thread_two.join(timeout=10)

    assert lock_held == [True], "the seam between the two halves was unlocked"

    events = log_events(run)
    assert len(events) == 2
    # Contiguous, non-overlapping, monotonic with durable sequence.
    assert events[0].log_end == events[1].log_start
    assert events[1].log_end == run.log_path.stat().st_size
    assert {run.slice_log(e) for e in events} == {"first line\n", "second line\n"}


def test_capture_records_logging_records_as_warnings_and_mirrors_the_text(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    logger = logging.getLogger("capture_fixture")

    with capture_worker_output(run.store, run.run_id):
        logger.info("quiet")
        logger.warning("loud")
        logger.error("louder")

    events = warning_events(run)
    assert [str(e.payload["message"]) for e in events] == ["loud", "louder"]
    assert {str(e.payload["category"]) for e in events} == {"capture_fixture"}
    log_text = run.captured_text()
    assert "loud\n" in log_text
    assert "quiet" not in log_text


class _Terminal(io.StringIO):
    """A stand-in terminal: records what reaches it and when it is flushed."""

    def __init__(self) -> None:
        super().__init__()
        self.flushes = 0

    def flush(self) -> None:
        self.flushes += 1
        super().flush()

    def isatty(self) -> bool:
        return True

    def fileno(self) -> int:
        return 42


def test_echoing_capture_passes_output_through_and_still_records_it(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    out, err = _Terminal(), _Terminal()
    sys.stdout, sys.stderr = out, err

    with capture_worker_output(run.store, run.run_id, echo=True):
        sys.stdout.write("progress 50%\r")
        # Partial lines reach the terminal at once, not at the next newline.
        assert out.getvalue() == "progress 50%\r"
        print("done", flush=True)
        print("oops", file=sys.stderr)
        assert sys.stdout.isatty() is True
        assert sys.stdout.fileno() == 42

    assert out.getvalue() == "progress 50%\rdone\n"
    assert out.flushes >= 1
    assert err.getvalue() == "oops\n"
    assert run.captured_text() == "progress 50%\rdone\noops\n"
    assert [e.payload["stream"] for e in log_events(run)] == [
        STREAM_STDOUT,
        STREAM_STDERR,
    ]


def test_capture_without_echo_writes_nothing_to_the_streams_it_replaced(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    out = _Terminal()
    sys.stdout = out

    with capture_worker_output(run.store, run.run_id):
        print("only in the log")
        assert sys.stdout.isatty() is False
        with pytest.raises(io.UnsupportedOperation):
            sys.stdout.fileno()

    assert out.getvalue() == ""
    assert run.captured_text() == "only in the log\n"


def test_echoing_capture_shows_a_repeated_warning_once_but_records_every_one(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    err = _Terminal()
    sys.stderr = err

    with capture_worker_output(run.store, run.run_id, echo=True):
        for _ in range(3):
            warnings.warn("again", UserWarning)

    assert err.getvalue().count("UserWarning: again") == 1
    assert run.captured_text().count("UserWarning: again") == 3
    assert len(warning_events(run)) == 3


def test_echoing_capture_leaves_a_logged_warning_to_the_handler_that_printed_it(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    err = _Terminal()
    sys.stderr = err
    # pytest's own capture handlers sit on the root logger; without them this
    # is a process whose logging nobody configured. The autouse fixture puts
    # them back.
    logging.getLogger().handlers[:] = []
    handled = logging.getLogger("capture_fixture.handled")
    printed = io.StringIO()
    own_handler = logging.StreamHandler(printed)
    handled.addHandler(own_handler)
    unhandled = logging.getLogger("capture_fixture.unhandled")

    try:
        with capture_worker_output(run.store, run.run_id, echo=True):
            handled.warning("printed by its own handler")
            unhandled.warning("printed in place of lastResort")
    finally:
        handled.removeHandler(own_handler)

    assert printed.getvalue() == "printed by its own handler\n"
    # The terminal sees each record once: the configured handler printed the
    # first, and the capture stands in for logging.lastResort on the second.
    assert err.getvalue() == "printed in place of lastResort\n"
    log_text = run.captured_text()
    assert "printed by its own handler\n" in log_text
    assert "printed in place of lastResort\n" in log_text
    assert len(warning_events(run)) == 2


def test_worker_echoes_progress_lines_without_recording_them_as_output(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``kptn run``'s in-process call: the terminal reads as it always did."""
    monkeypatch.delenv("KPTN_UI_FIXTURE_RUN_LEVEL_WARNING", raising=False)
    run = create_fixture_run(ui_project, profile="success")
    out, err = _Terminal(), _Terminal()
    sys.stdout, sys.stderr = out, err

    exit_code = worker.execute_run(
        run.store, run.store.get_run(run.run_id), heartbeat_interval=0.05, echo=True
    )

    assert exit_code == worker.EXIT_SUCCESS
    assert "[RUN]" in out.getvalue() and "noisy_task" in out.getvalue()
    assert "ordinary output\n" in out.getvalue()
    assert "raw stderr output\n" in err.getvalue()
    # Progress lines are events, rendered into the download from the stream;
    # in the log file they would appear twice there.
    assert "[RUN]" not in run.captured_text()
    assert "ordinary output\n" in run.captured_text()
    assert run.store.get_run(run.run_id).status == STATUS_SUCCEEDED


def test_worker_reports_a_profile_error_by_its_message_alone(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="no_such_profile")

    result = run_worker(run)

    assert result.returncode == worker.EXIT_FAILED
    assert run.store.get_run(run.run_id).status == STATUS_FAILED
    log_text = run.captured_text()
    assert "no_such_profile" in log_text
    assert "Traceback" not in log_text


def test_structured_warning_handler_ignores_records_below_warning(
    ui_project: Path,
) -> None:
    """The level floor is enforced in emit(), not only by the handler level."""
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    handler = StructuredWarningHandler(RunStoreSink(run.store, run.run_id))
    record = logging.LogRecord(
        name="direct",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="quiet",
        args=(),
        exc_info=None,
    )

    handler.emit(record)

    assert warning_events(run) == []

    record.levelno = logging.WARNING
    record.levelname = "WARNING"
    handler.emit(record)

    assert [str(e.payload["category"]) for e in warning_events(run)] == ["direct"]


# -- re-entrancy guard (fix round 1, finding 1) ---------------------------


def test_reentry_guard_blocks_every_nested_attempt_not_just_the_first() -> None:
    """A blocked attempt must not release a flag it never took.

    Clearing unconditionally in __exit__ disarms the guard: the first nested
    attempt is refused but hands the flag back on its way out, admitting the
    second.
    """
    assert not getattr(_store_write_reentry, "active", False)

    with _Reentry(_store_write_reentry) as outer:
        assert outer is True
        observed = []
        for _ in range(3):
            with _Reentry(_store_write_reentry) as nested:
                observed.append(nested)
        assert observed == [False, False, False]

    # The outer guard released it, so the path is usable again.
    with _Reentry(_store_write_reentry) as again:
        assert again is True
    assert not getattr(_store_write_reentry, "active", False)


def test_capture_blocks_every_nested_write_not_just_the_first(
    ui_project: Path,
) -> None:
    """Nested writes from inside the persistence path are all dropped.

    Admitting the second one would re-enter LogWriteState.write and open a
    second store transaction while the outer one still holds SQLite's write
    lock -- a self-inflicted busy_timeout stall that the worker would then
    misreport as a durable failure.
    """
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    attempts: list[int] = []

    with capture_worker_output(run.store, run.run_id) as capture:

        def hook() -> None:
            for index in range(3):
                attempts.append(capture.stdout.write(f"nested {index}\n"))

        capture.state._critical_section_hook = hook
        capture.stdout.write("outer\n")

    # write() still reports the characters it accepted; what must not happen
    # is any of them reaching the file or the store.
    assert len(attempts) == 3
    assert run.captured_text() == "outer\n"
    assert [run.slice_log(e) for e in log_events(run)] == ["outer\n"]


def test_capture_guards_the_warnings_path_symmetrically(
    ui_project: Path,
) -> None:
    """A warning raised inside the persistence path must not re-enter it."""
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    with capture_worker_output(run.store, run.run_id) as capture:

        def hook() -> None:
            warnings.warn("warned from inside the write path", UserWarning)
            logging.getLogger("inside").warning("logged from inside the write path")

        capture.state._critical_section_hook = hook
        capture.stdout.write("outer\n")

    assert warning_events(run) == []
    assert run.captured_text() == "outer\n"


# -- durable-failure classification (fix round 1, finding 2) --------------


def test_worker_reports_a_task_database_error_as_failed_not_errored(
    ui_project: Path,
) -> None:
    """sqlite3.Error from pipeline code is the pipeline failing, not the store.

    kptn's own default state store is SQLite (the fixture sets db: sqlite), so
    classifying durable failures by exception type would turn an ordinary task
    query error into `errored` while the event stream said `failed`.
    """
    run = create_fixture_run(ui_project, profile="db_error")

    result = run_worker(run)

    assert result.returncode == worker.EXIT_FAILED
    record = run.store.get_run(run.run_id)
    assert record.status == STATUS_FAILED
    assert record.exit_code == worker.EXIT_FAILED

    finished = [e for e in run.events() if e.kind == EventKind.RUN_FINISHED.value]
    assert len(finished) == 1
    # The run row and its own event stream must agree.
    assert finished[0].payload["status"] == "failed"
    assert record.status == finished[0].payload["status"]
    assert "fixture_user_query" in str(finished[0].payload["error"])


def test_durable_marks_store_failures_and_passes_others_through() -> None:
    def store_failure():
        raise RunStoreError("store is gone")

    def task_failure():
        raise sqlite3.OperationalError("no such table: user_table")

    with pytest.raises(DurableWriteFailed) as exc_info:
        durable(store_failure)
    assert isinstance(exc_info.value.cause, RunStoreError)
    assert isinstance(exc_info.value, RunStoreError)

    # durable() only marks what fails *inside it*; it is applied at the
    # persistence seam, so a task's own database error never reaches it.
    with pytest.raises(DurableWriteFailed):
        durable(task_failure)

    # An already-marked failure is not double-wrapped.
    original = DurableWriteFailed(RunStoreError("first"))

    def already_marked():
        raise original

    with pytest.raises(DurableWriteFailed) as exc_info:
        durable(already_marked)
    assert exc_info.value is original


# -- store heartbeat (fix round 1, finding 3) -----------------------------


def test_heartbeat_refuses_to_stamp_a_finished_run(ui_project: Path) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    assert run.store.heartbeat(run.run_id) is True
    beat = run.store.get_run(run.run_id).heartbeat_at
    assert beat is not None

    run.store.finish_run(run.run_id, STATUS_SUCCEEDED, exit_code=0)

    assert run.store.heartbeat(run.run_id) is False
    assert run.store.get_run(run.run_id).heartbeat_at == beat


def test_heartbeat_raises_for_an_unknown_run(ui_project: Path) -> None:
    run = create_fixture_run(ui_project, profile="success")

    with pytest.raises(RunNotFoundError):
        run.store.heartbeat("no-such-run")


# -- worker setup failures (fix round 1, elevated minor) ------------------


def test_worker_records_a_setup_failure_instead_of_crashing(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure before the pipeline starts still gets a documented outcome.

    It must not escape main() as a traceback with exit 1, and it must not
    leave the signal disposition rewritten.
    """
    run = create_fixture_run(ui_project, profile="success")

    def boom(self, *args, **kwargs):
        raise RuntimeError("heartbeat thread would not start")

    monkeypatch.setattr(worker._Heartbeat, "start", boom)
    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)

    exit_code = worker.main(["--db", str(run.db_path), "--run-id", run.run_id])

    assert exit_code == worker.EXIT_SETUP_FAILURE
    assert signal.getsignal(signal.SIGTERM) is original_sigterm
    assert signal.getsignal(signal.SIGINT) is original_sigint
    record = run.store.get_run(run.run_id)
    assert record.status == STATUS_ERRORED
    assert record.exit_code == worker.EXIT_SETUP_FAILURE
    # The identity write happened before the failure, so it is still recorded.
    assert record.worker_pid == os.getpid()


def test_worker_records_a_failed_worker_identity_write_as_errored(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = create_fixture_run(ui_project, profile="success")

    def boom(self, *args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(RunStore, "record_worker_start", boom)

    exit_code = worker.main(["--db", str(run.db_path), "--run-id", run.run_id])

    assert exit_code == worker.EXIT_DURABLE_WRITE_FAILURE
    monkeypatch.undo()
    record = RunStore(run.db_path).get_run(run.run_id)
    assert record.status == STATUS_ERRORED


# -- store heartbeat atomicity (fix round 1, finding 3) -------------------


class _RacingConnection:
    """Wraps a sqlite connection and fires a callback after one statement."""

    def __init__(self, inner, marker: str, on_seen) -> None:
        self._inner = inner
        self._marker = marker
        self._on_seen = on_seen

    def execute(self, sql, *args, **kwargs):
        result = self._inner.execute(sql, *args, **kwargs)
        if self._on_seen is not None and self._marker in sql:
            callback, self._on_seen = self._on_seen, None
            callback()
        return result

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_heartbeat_holds_a_write_transaction_across_its_status_check(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The status check and the heartbeat write must be one transaction.

    Proven by racing a real ``finish_run`` against the window between them: if
    ``heartbeat()`` holds ``BEGIN IMMEDIATE``, the competing writer cannot get
    in, so the run cannot become terminal underneath the check. Without the
    transaction the competing writer commits immediately and ``heartbeat()``
    stamps ``heartbeat_at`` onto a finished run while reporting success.
    """
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    competitor = RunStore(run.db_path)
    finished = threading.Event()
    started = threading.Event()
    blocked_as_expected: list[bool] = []

    def finish_in_background() -> None:
        started.set()
        try:
            competitor.finish_run(run.run_id, STATUS_SUCCEEDED, exit_code=0)
        finally:
            finished.set()

    def race() -> None:
        thread = threading.Thread(target=finish_in_background, daemon=True)
        thread.start()
        assert started.wait(10)
        # Must still be blocked on the write lock heartbeat() is holding.
        blocked_as_expected.append(not finished.wait(0.25))

    original_connect = RunStore._connect

    def racing_connect(self):
        conn = original_connect(self)
        if self is heartbeat_store:
            return _RacingConnection(conn, "SELECT status FROM runs", race)
        return conn

    heartbeat_store = RunStore(run.db_path)
    monkeypatch.setattr(RunStore, "_connect", racing_connect)
    # The store keeps its connection, so drop the one it opened unpatched.
    heartbeat_store.close()

    result = heartbeat_store.heartbeat(run.run_id)
    monkeypatch.undo()
    assert finished.wait(10)

    assert blocked_as_expected == [True], (
        "finish_run committed inside heartbeat's status-check window"
    )
    record = RunStore(run.db_path).get_run(run.run_id)
    # heartbeat() won the race, so it reported success; the run only became
    # terminal afterwards. What must never happen is True on a terminal run.
    assert result is True
    assert record.status == STATUS_SUCCEEDED


# -- warning filters (fix round 1, spec deviation) ------------------------


def test_capture_preserves_project_configured_warning_filters(
    ui_project: Path,
) -> None:
    """A project's warnings-as-errors setting must survive the worker.

    simplefilter() resets the whole filter list, so a pipeline relying on
    `filterwarnings = error` would behave differently under the worker than
    under `kptn run`. Appending leaves it in charge.
    """
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    with warnings.catch_warnings():
        warnings.resetwarnings()
        warnings.filterwarnings("error", category=UserWarning)

        with capture_worker_output(run.store, run.run_id):
            with pytest.raises(UserWarning):
                warnings.warn("escalated by the project", UserWarning)
            # Categories the project said nothing about still fall through to
            # the appended "always" entry, so every occurrence is recorded.
            warnings.warn("not escalated", RuntimeWarning)
            warnings.warn("not escalated", RuntimeWarning)

    assert [str(e.payload["category"]) for e in warning_events(run)] == [
        "RuntimeWarning",
        "RuntimeWarning",
    ]


# -- batched persistence of captured output --------------------------------
#
# One store transaction per captured line cost ~20ms on an NFS-mounted
# project (a write lock and an fsync, each a network round trip), which both
# slowed the pipeline down to the speed of its own logging and starved the UI
# of the same lock. Output is now written in batches.


def _counting_store_writes(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[str]]:
    calls: dict[str, list[str]] = {"append_event": [], "append_events": []}
    real_one, real_many = RunStore.append_event, RunStore.append_events

    def one(self, run_id, kind, **kwargs):
        calls["append_event"].append(kind)
        return real_one(self, run_id, kind, **kwargs)

    def many(self, run_id, events):
        calls["append_events"].append(",".join(e.kind for e in events))
        return real_many(self, run_id, events)

    monkeypatch.setattr(RunStore, "append_event", one)
    monkeypatch.setattr(RunStore, "append_events", many)
    return calls


def test_capture_writes_a_burst_of_output_in_a_few_transactions(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    calls = _counting_store_writes(monkeypatch)

    with capture_worker_output(run.store, run.run_id):
        for index in range(300):
            print(f"line {index}")
        warnings.warn("one warning among the output", UserWarning)

    assert calls["append_event"] == []
    assert 1 <= len(calls["append_events"]) <= 5
    events = log_events(run)
    assert [run.slice_log(e) for e in events][:300] == [f"line {i}\n" for i in range(300)]
    # Contiguous spans and contiguous sequences, exactly as unbatched.
    assert all(a.log_end == b.log_start for a, b in zip(events, events[1:]))
    sequences = [e.sequence for e in run.events()]
    assert sequences == list(range(1, len(sequences) + 1))
    assert [str(e.payload["message"]) for e in warning_events(run)] == [
        "one warning among the output"
    ]


def test_captured_output_reaches_the_store_without_an_explicit_flush(
    ui_project: Path,
) -> None:
    """The console is live: pending output is written on a short timer."""
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    with capture_worker_output(run.store, run.run_id):
        print("arrives on its own")
        deadline = time.monotonic() + 5
        while not log_events(run) and time.monotonic() < deadline:
            time.sleep(POLL_INTERVAL_SECONDS)
        seen = [run.slice_log(e) for e in log_events(run)]

    assert seen == ["arrives on its own\n"]


def test_pending_output_is_written_before_a_state_changing_event(
    ui_project: Path,
) -> None:
    """A task's output must never be sequenced after the event that ends it."""
    run = create_fixture_run(ui_project, profile="success")

    with capture_worker_output(run.store, run.run_id) as capture:
        capture.sink.emit(_run_event(EventKind.RUN_STARTED))
        print("before the task")
        capture.sink.emit(_run_event(EventKind.TASK_STARTED))
        print("inside the task")
        capture.sink.emit(_run_event(EventKind.TASK_FINISHED, status="succeeded"))

    assert [(e.kind, run.slice_log(e) if e.kind == "log" else None) for e in run.events()] == [
        (EventKind.RUN_STARTED.value, None),
        (EventKind.LOG.value, "before the task\n"),
        (EventKind.TASK_STARTED.value, None),
        (EventKind.LOG.value, "inside the task\n"),
        (EventKind.TASK_FINISHED.value, None),
    ]


def test_a_failed_batch_write_is_raised_to_the_worker(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A durable failure on the flush thread is not swallowed."""
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    def failing(self, run_id, events):
        raise RunStoreError("simulated batch failure")

    monkeypatch.setattr(RunStore, "append_events", failing)

    with pytest.raises(DurableWriteFailed):
        with capture_worker_output(run.store, run.run_id):
            print("cannot be stored")
            time.sleep(1.0)  # several flush ticks: the failure happens off-thread
            print("the next write reports it")


def test_worker_records_errored_when_captured_output_cannot_be_stored(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = create_fixture_run(ui_project, profile="success")

    def failing(self, run_id, events):
        raise RunStoreError("simulated batch failure")

    monkeypatch.setattr(RunStore, "append_events", failing)

    exit_code = worker.main(["--db", str(run.db_path), "--run-id", run.run_id])

    assert exit_code == worker.EXIT_DURABLE_WRITE_FAILURE
    monkeypatch.undo()
    record = RunStore(run.db_path).get_run(run.run_id)
    assert record is not None
    assert record.status == STATUS_ERRORED


def test_worker_sequences_task_output_inside_its_task(ui_project: Path) -> None:
    """Runner events and captured output share one ordered stream."""
    run = create_fixture_run(ui_project, profile="success")

    assert run_worker(run).returncode == 0

    events = run.events()
    started = next(e.sequence for e in events if e.kind == EventKind.TASK_STARTED.value and e.task_name == "noisy_task")
    finished = next(e.sequence for e in events if e.kind == EventKind.TASK_FINISHED.value and e.task_name == "noisy_task")
    task_output = [e.sequence for e in log_events(run) if e.task_name == "noisy_task"]
    assert task_output
    assert all(started < sequence < finished for sequence in task_output)
