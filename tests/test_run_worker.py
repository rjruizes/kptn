"""Tests for the detached pipeline worker and its output capture layer.

The worker is the process that actually executes a pipeline. Everything it
produces -- stdout, stderr, ``warnings.warn`` calls, ``logging`` records at
WARNING or above, and the runner's own structured events -- has to land in the
durable run store, because the browser, VS Code, and FastAPI may all restart
while the run is still going.

The primary test drives a real ``python -m kptn_server.worker`` subprocess: the
worker surviving as a detached process is the whole point, so at least one test
must not cheat by calling ``main()`` in-process.
"""

from __future__ import annotations

import logging
import os
import shutil
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
    RunStoreSink,
    StructuredWarningHandler,
    capture_worker_output,
)
from kptn_server.run_store import (
    STATUS_ERRORED,
    STATUS_FAILED,
    STATUS_STOPPED,
    STATUS_SUCCEEDED,
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

    def log_bytes(self) -> bytes:
        return self.log_path.read_bytes()

    def slice_log(self, event: StoredEvent) -> str:
        assert event.log_start is not None and event.log_end is not None
        return self.log_bytes()[event.log_start : event.log_end].decode("utf-8")


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

    assert "ordinary output" in Path(run.log_path).read_text()

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
    assert "fixture failure" in Path(run.log_path).read_text()


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
    log_text = run.log_path.read_text()
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
        with capture_worker_output(run.store, run.run_id, run.log_path):
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
    assert "before the failure\n" in run.log_path.read_text()


def test_capture_buffers_partial_lines_until_newline_or_exit(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    with capture_worker_output(run.store, run.run_id, run.log_path):
        sys.stdout.write("par")
        assert log_events(run) == []
        sys.stdout.write("tial\n")
        assert len(log_events(run)) == 1
        sys.stdout.write("trailing without newline")
        assert len(log_events(run)) == 1

    events = log_events(run)
    assert [run.slice_log(e) for e in events] == [
        "partial\n",
        "trailing without newline",
    ]
    assert run.log_bytes() == b"partial\ntrailing without newline"


def test_capture_records_log_offsets_that_match_the_log_file(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    with capture_worker_output(run.store, run.run_id, run.log_path):
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
    # Offsets are byte offsets, not character offsets.
    assert events[1].log_end - events[1].log_start == len("sécond\n".encode())


def test_capture_serializes_concurrent_writes_under_one_lock(
    ui_project: Path,
) -> None:
    """The byte write and the durable event must share one critical section.

    Proven deterministically rather than by timing luck: a hook fires at the
    exact seam between "bytes written" and "event appended" and blocks there.
    A second thread then tries to write. If both halves are covered by the
    same lock, it cannot get in -- no second byte range, no second event. If
    either half escaped the lock (say the event append moved outside it), the
    second thread would land its write and the assertions below would fail.
    """
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)

    inside = threading.Event()
    release = threading.Event()
    second_started = threading.Event()
    second_finished = threading.Event()

    with capture_worker_output(run.store, run.run_id, run.log_path) as capture:

        def hook() -> None:
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
        # The first writer's bytes are down but its event is not yet appended;
        # the second writer is blocked, so nothing of its own has landed in
        # either the file or the store.
        assert len(log_events(run)) == 0
        assert run.log_bytes() == b"first line\n"

        release.set()
        thread_one.join(timeout=10)
        assert second_finished.wait(10)
        thread_two.join(timeout=10)

    events = log_events(run)
    assert len(events) == 2
    # Contiguous, non-overlapping, monotonic with durable sequence.
    assert events[0].log_start == 0
    assert events[0].log_end == events[1].log_start
    assert events[1].log_end == len(run.log_bytes())
    assert {run.slice_log(e) for e in events} == {"first line\n", "second line\n"}


def test_capture_records_logging_records_as_warnings_and_mirrors_the_text(
    ui_project: Path,
) -> None:
    run = create_fixture_run(ui_project, profile="success")
    run.store.append_event(run.run_id, EventKind.RUN_STARTED.value)
    logger = logging.getLogger("capture_fixture")

    with capture_worker_output(run.store, run.run_id, run.log_path):
        logger.info("quiet")
        logger.warning("loud")
        logger.error("louder")

    events = warning_events(run)
    assert [str(e.payload["message"]) for e in events] == ["loud", "louder"]
    assert {str(e.payload["category"]) for e in events} == {"capture_fixture"}
    log_text = run.log_path.read_text()
    assert "loud\n" in log_text
    assert "quiet" not in log_text


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
