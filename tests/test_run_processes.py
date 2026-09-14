"""Tests for detached worker launch, identity-safe stop, and reconciliation.

Three things have to hold for a run to survive the browser, VS Code, and the
FastAPI server all restarting:

1. ``start()`` must detach the worker so nothing about the launcher's own
   lifetime can kill it.
2. ``stop()`` must signal *that* worker and nothing else -- matching the
   recorded PID alone is not enough, because PIDs get reused.
3. ``reconcile()`` must notice a worker a reboot (or a ``kill -9``) took out
   and mark its run terminal, releasing the project's active-run lock.

Nothing here synchronizes on ``sleep``: the reconciliation clock is injected
and ``heartbeat_at`` is written directly, so the 15-second grace period is
tested without waiting 15 seconds.
"""

from __future__ import annotations

import gc
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psutil
import pytest

from kptn_server.processes import (
    STALE_WORKER_GRACE_SECONDS,
    ProcessIdentity,
    RunProcessManager,
)
from kptn_server.run_store import (
    STATUS_INTERRUPTED,
    STATUS_RUNNING,
    STATUS_STOP_REQUESTED,
    STATUS_STOPPED,
    STATUS_SUCCEEDED,
    RunRecord,
    RunRequest,
    RunStateError,
    RunStore,
)

FIXTURE_PROJECT = Path(__file__).parent / "fixtures" / "ui_project"
POLL_TIMEOUT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.02


# -- process hygiene -------------------------------------------------------


def _own_children() -> set[tuple[int, float]]:
    try:
        return {
            (child.pid, child.create_time())
            for child in psutil.Process().children(recursive=True)
        }
    except psutil.Error:  # pragma: no cover - defensive
        return set()


@pytest.fixture(autouse=True)
def reap_spawned_workers():
    """Kill every child process a test leaves behind.

    This is deliberately structural rather than something each test opts into:
    a *failing* test must not be able to leak a detached worker onto the
    developer's machine or into CI, and the ``slow`` fixture profile blocks on
    a sentinel file forever if nobody releases it.
    """
    before = _own_children()

    yield

    leaked = [
        child
        for child in psutil.Process().children(recursive=True)
        if (child.pid, _safe_create_time(child)) not in before
    ]
    for child in leaked:
        _hard_kill(child)
    psutil.wait_procs(leaked, timeout=10)
    # Reap the zombies so the pytest process does not accumulate them.
    for child in leaked:
        try:
            os.waitpid(child.pid, os.WNOHANG)
        except (ChildProcessError, OSError):
            pass


def _safe_create_time(proc: psutil.Process) -> float:
    try:
        return proc.create_time()
    except psutil.Error:  # pragma: no cover - defensive
        return -1.0


def _hard_kill(proc: psutil.Process) -> None:
    try:
        if os.name == "nt":  # pragma: no cover - POSIX CI
            proc.kill()
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()
    except psutil.Error:
        pass


# -- store / project fixtures ---------------------------------------------


@pytest.fixture
def ui_project(tmp_path: Path) -> Path:
    destination = tmp_path / "project"
    shutil.copytree(FIXTURE_PROJECT, destination)
    return destination


@pytest.fixture
def store(ui_project: Path) -> RunStore:
    return RunStore(ui_project / ".kptn" / "ui.db")


def wait_until(predicate, *, message: str, timeout: float = POLL_TIMEOUT_SECONDS):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {message}")
        time.sleep(POLL_INTERVAL_SECONDS)


def create_run(store: RunStore, project_root: Path, *, profile: str) -> RunRecord:
    return store.create_run(
        RunRequest(project_root=project_root, pipeline="fixture", profile=profile)
    )


def fake_running_run(
    store: RunStore,
    project_root: Path,
    *,
    pid: int,
    worker_started_at: float,
    heartbeat_at: datetime | None = None,
    status: str = STATUS_RUNNING,
    profile: str = "success",
) -> RunRecord:
    """Forge a run that looks like it is being executed by ``pid``.

    A test-local helper on purpose: the real store has no business minting
    running runs that no worker is attached to.
    """
    record = create_run(store, project_root, profile=profile)
    conn = sqlite3.connect(store.path, timeout=5.0, isolation_level=None)
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE runs
            SET status = ?, started_at = ?, worker_pid = ?,
                worker_started_at = ?, heartbeat_at = ?
            WHERE run_id = ?
            """,
            (
                status,
                record.created_at.isoformat(),
                pid,
                worker_started_at,
                heartbeat_at.isoformat() if heartbeat_at is not None else None,
                record.run_id,
            ),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()
    refreshed = store.get_run(record.run_id)
    assert refreshed is not None
    return refreshed


def spawn_unrelated_process() -> psutil.Process:
    """A long-lived, detached process that is NOT one of our workers."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=(os.name != "nt"),
    )
    handle = psutil.Process(proc.pid)
    assert handle.is_running()
    return handle


@pytest.fixture
def slow_run(
    store: RunStore, ui_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> RunRecord:
    """A queued run whose task blocks on a sentinel file until released."""
    sentinel = tmp_path / "release-the-worker"
    monkeypatch.setenv("KPTN_UI_FIXTURE_SENTINEL", str(sentinel))
    monkeypatch.delenv("KPTN_UI_FIXTURE_RUN_LEVEL_WARNING", raising=False)
    return create_run(store, ui_project, profile="slow")


# -- detached launch -------------------------------------------------------


def test_worker_outlives_launcher_object(store: RunStore, slow_run: RunRecord) -> None:
    manager = RunProcessManager(store)

    identity = manager.start(slow_run.run_id)

    del manager
    gc.collect()

    # The worker got far enough to claim its run, so it is genuinely
    # executing rather than a corpse we are holding an exit status for.
    record = wait_until(
        lambda: (
            store.get_run(slow_run.run_id)
            if store.get_run(slow_run.run_id).status == STATUS_RUNNING
            else None
        ),
        message="the detached worker to start its run",
    )
    assert record.worker_pid == identity.pid

    survivor = psutil.Process(identity.pid)
    assert survivor.is_running()
    assert survivor.status() != psutil.STATUS_ZOMBIE
    assert survivor.create_time() == pytest.approx(identity.started_at)


def test_start_persists_pid_and_os_creation_time(
    store: RunStore, slow_run: RunRecord
) -> None:
    manager = RunProcessManager(store)

    identity = manager.start(slow_run.run_id)

    assert isinstance(identity, ProcessIdentity)
    assert identity.started_at == pytest.approx(
        psutil.Process(identity.pid).create_time()
    )
    record = store.get_run(slow_run.run_id)
    assert record.worker_pid == identity.pid
    assert record.worker_started_at == pytest.approx(identity.started_at)


def test_start_detaches_worker_into_its_own_process_group(
    store: RunStore, slow_run: RunRecord
) -> None:
    if os.name == "nt":  # pragma: no cover - POSIX CI
        pytest.skip("process groups are POSIX-only")
    manager = RunProcessManager(store)

    identity = manager.start(slow_run.run_id)

    # Detached: the worker leads its own group/session, so a signal aimed at
    # the launcher's group cannot reach it.
    assert os.getpgid(identity.pid) == identity.pid
    assert os.getpgid(identity.pid) != os.getpgid(os.getpid())


def test_start_uses_an_argv_list_never_a_shell_string(
    store: RunStore, slow_run: RunRecord, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}
    real_popen = subprocess.Popen

    def spy(args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return real_popen(args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", spy)
    RunProcessManager(store).start(slow_run.run_id)

    assert captured["args"] == [
        sys.executable,
        "-m",
        "kptn_server.worker",
        "--db",
        str(store.path),
        "--run-id",
        slow_run.run_id,
    ]
    kwargs = captured["kwargs"]
    assert kwargs.get("shell") in (None, False)
    assert Path(kwargs["cwd"]) == slow_run.project_root
    assert kwargs["stdin"] is subprocess.DEVNULL
    assert kwargs["stdout"] is subprocess.DEVNULL
    assert kwargs["stderr"] is subprocess.DEVNULL
    assert kwargs["start_new_session"] is (os.name != "nt")


def workers_for_run(run_id: str) -> list[psutil.Process]:
    """Every child process whose argv names *run_id*."""
    found = []
    for child in psutil.Process().children(recursive=True):
        try:
            argv = child.cmdline()
        except psutil.Error:  # pragma: no cover - defensive
            continue
        if run_id in argv:
            found.append(child)
    return found


def test_start_refuses_to_launch_a_second_worker_for_the_same_run(
    store: RunStore, slow_run: RunRecord
) -> None:
    """A double POST must not produce two workers for one run.

    The second launch would overwrite the store's record of the first one's
    identity, orphaning a live process that stop() can no longer signal and
    reconcile() can no longer see.
    """
    manager = RunProcessManager(store)
    identity = manager.start(slow_run.run_id)
    wait_until(
        lambda: store.get_run(slow_run.run_id).status == STATUS_RUNNING,
        message="the first worker to reach running",
    )

    with pytest.raises(RunStateError):
        manager.start(slow_run.run_id)

    assert store.get_run(slow_run.run_id).worker_pid == identity.pid
    assert [p.pid for p in workers_for_run(slow_run.run_id)] == [identity.pid]


# -- identity-safe stop ----------------------------------------------------


def test_stop_terminates_the_matching_worker(
    store: RunStore, slow_run: RunRecord
) -> None:
    manager = RunProcessManager(store)
    identity = manager.start(slow_run.run_id)
    wait_until(
        lambda: store.get_run(slow_run.run_id).status == STATUS_RUNNING,
        message="the worker to reach running",
    )

    assert manager.stop(slow_run.run_id) is True

    gone = psutil.wait_procs([psutil.Process(identity.pid)], timeout=30)[0]
    assert gone, "worker did not exit after SIGTERM"
    record = wait_until(
        lambda: (
            store.get_run(slow_run.run_id)
            if store.get_run(slow_run.run_id).finished_at is not None
            else None
        ),
        message="the stopped run to be finished durably",
    )
    # `interrupted` is only ever written by reconcile(), which this test never
    # calls -- accepting it here would let a real regression slip through.
    assert record.status == STATUS_STOPPED


def test_stop_refuses_to_signal_a_reused_pid(store: RunStore, ui_project: Path) -> None:
    """A live process whose creation time does not match is NOT ours."""
    victim = spawn_unrelated_process()
    run = fake_running_run(
        store,
        ui_project,
        pid=victim.pid,
        # Deliberately wrong: this run's worker was created at a different
        # time, so `victim` is a PID that got reused.
        worker_started_at=victim.create_time() - 500.0,
        heartbeat_at=datetime.now(timezone.utc),
    )

    signalled = RunProcessManager(store).stop(run.run_id)

    assert signalled is False
    assert victim.is_running(), "stop() signalled an unrelated process"
    assert victim.status() != psutil.STATUS_ZOMBIE


def test_stop_records_the_stop_request_even_without_a_worker(
    store: RunStore, ui_project: Path
) -> None:
    run = create_run(store, ui_project, profile="success")

    assert RunProcessManager(store).stop(run.run_id) is False
    assert store.get_run(run.run_id).status == STATUS_STOP_REQUESTED


def test_stop_tolerates_the_worker_finishing_first(
    store: RunStore, ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Clicking Stop as the pipeline finishes is routine, not a 500.

    ``stop()`` reads the status and requests the stop in two separate
    transactions, so the worker can legitimately finish in between.
    """
    run = fake_running_run(
        store,
        ui_project,
        pid=999999,
        worker_started_at=1.0,
        heartbeat_at=datetime.now(timezone.utc),
    )
    real_request_stop = RunStore.request_stop

    def worker_wins_the_race(self: RunStore, run_id: str):
        self.finish_run(run_id, STATUS_SUCCEEDED, exit_code=0)
        return real_request_stop(self, run_id)

    monkeypatch.setattr(RunStore, "request_stop", worker_wins_the_race)

    assert RunProcessManager(store).stop(run.run_id) is False
    assert store.get_run(run.run_id).status == STATUS_SUCCEEDED


# -- reconciliation --------------------------------------------------------


def _manager_at(store: RunStore, moment: datetime) -> RunProcessManager:
    return RunProcessManager(store, now=lambda: moment)


def test_reconcile_marks_missing_worker_interrupted(
    store: RunStore, ui_project: Path
) -> None:
    run = fake_running_run(store, ui_project, pid=999999, worker_started_at=1.0)

    interrupted = RunProcessManager(store).reconcile()

    assert interrupted == [run.run_id]
    assert store.get_run(run.run_id).status == STATUS_INTERRUPTED


def test_reconcile_marks_pid_start_time_mismatch_interrupted(
    store: RunStore, ui_project: Path
) -> None:
    """The PID is alive, but it is a different process than the run's worker."""
    impostor = spawn_unrelated_process()
    run = fake_running_run(
        store,
        ui_project,
        pid=impostor.pid,
        worker_started_at=impostor.create_time() - 500.0,
    )

    assert RunProcessManager(store).reconcile() == [run.run_id]
    assert store.get_run(run.run_id).status == STATUS_INTERRUPTED
    assert impostor.is_running(), "reconcile() must never signal anything"


def test_reconcile_leaves_a_run_with_a_fresh_heartbeat_alone(
    store: RunStore, ui_project: Path
) -> None:
    """A heartbeat inside the grace window wins over a missing process.

    The process lookup can race a worker that has registered its PID but not
    yet been observed, so a fresh heartbeat is authoritative.
    """
    now = datetime.now(timezone.utc)
    run = fake_running_run(
        store,
        ui_project,
        pid=999999,
        worker_started_at=1.0,
        heartbeat_at=now,
    )

    assert _manager_at(store, now).reconcile() == []
    assert store.get_run(run.run_id).status == STATUS_RUNNING


@pytest.mark.parametrize(
    ("heartbeat_age", "expect_interrupted"),
    [
        (STALE_WORKER_GRACE_SECONDS - 0.5, False),
        (STALE_WORKER_GRACE_SECONDS + 0.5, True),
    ],
    ids=["inside-grace", "past-grace"],
)
def test_reconcile_applies_the_fifteen_second_grace(
    store: RunStore,
    ui_project: Path,
    heartbeat_age: float,
    expect_interrupted: bool,
) -> None:
    now = datetime.now(timezone.utc)
    run = fake_running_run(
        store,
        ui_project,
        pid=999999,
        worker_started_at=1.0,
        heartbeat_at=now - timedelta(seconds=heartbeat_age),
    )

    reconciled = _manager_at(store, now).reconcile()

    assert (reconciled == [run.run_id]) is expect_interrupted
    expected = STATUS_INTERRUPTED if expect_interrupted else STATUS_RUNNING
    assert store.get_run(run.run_id).status == expected


def test_reconcile_keeps_a_run_whose_process_it_cannot_inspect(
    store: RunStore, ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``AccessDenied`` means "unknown", never "dead".

    Burying a run on an inconclusive answer releases the project's active-run
    lock while the worker keeps writing, which then permits a second
    concurrent run on the same project.
    """
    inspectable = spawn_unrelated_process()
    now = datetime.now(timezone.utc)
    run = fake_running_run(
        store,
        ui_project,
        pid=inspectable.pid,
        worker_started_at=inspectable.create_time(),
        # Well past the grace window, so the ONLY thing that can keep this run
        # alive is refusing to call an un-inspectable process dead.
        heartbeat_at=now - timedelta(seconds=STALE_WORKER_GRACE_SECONDS + 600),
    )

    def denied(self: psutil.Process) -> float:
        raise psutil.AccessDenied(self.pid)

    monkeypatch.setattr(psutil.Process, "create_time", denied)

    assert _manager_at(store, now).reconcile() == []
    assert store.get_run(run.run_id).status == STATUS_RUNNING
    assert store.active_run(ui_project).run_id == run.run_id


def test_stop_refuses_to_signal_a_process_it_cannot_identify(
    store: RunStore, ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other side of "unknown": never signal on an inconclusive answer."""
    victim = spawn_unrelated_process()
    run = fake_running_run(
        store,
        ui_project,
        pid=victim.pid,
        worker_started_at=victim.create_time(),
        heartbeat_at=datetime.now(timezone.utc),
    )

    def denied(self: psutil.Process) -> float:
        raise psutil.AccessDenied(self.pid)

    monkeypatch.setattr(psutil.Process, "create_time", denied)

    assert RunProcessManager(store).stop(run.run_id) is False
    monkeypatch.undo()
    assert victim.is_running(), "stop() signalled an unidentifiable process"


def test_reconcile_releases_the_active_project_lock(
    store: RunStore, ui_project: Path
) -> None:
    run = fake_running_run(store, ui_project, pid=999999, worker_started_at=1.0)
    assert store.active_run(ui_project).run_id == run.run_id

    RunProcessManager(store).reconcile()

    assert store.active_run(ui_project) is None
    # And the project can take a new run again.
    replacement = create_run(store, ui_project, profile="success")
    assert store.active_run(ui_project).run_id == replacement.run_id


def test_reconcile_leaves_a_live_worker_running(
    store: RunStore, slow_run: RunRecord
) -> None:
    manager = RunProcessManager(store)
    identity = manager.start(slow_run.run_id)
    wait_until(
        lambda: store.get_run(slow_run.run_id).status == STATUS_RUNNING,
        message="the worker to reach running",
    )

    assert manager.reconcile() == []
    assert psutil.Process(identity.pid).is_running()
    assert store.get_run(slow_run.run_id).status == STATUS_RUNNING


def test_reconcile_ignores_terminal_runs(store: RunStore, ui_project: Path) -> None:
    run = fake_running_run(store, ui_project, pid=999999, worker_started_at=1.0)
    store.finish_run(run.run_id, STATUS_SUCCEEDED, exit_code=0)

    assert RunProcessManager(store).reconcile() == []
    assert store.get_run(run.run_id).status == STATUS_SUCCEEDED


def test_reconcile_gives_a_queued_run_the_grace_period_before_giving_up(
    store: RunStore, ui_project: Path
) -> None:
    """A queued run has no PID yet; the launcher may be mid-spawn."""
    run = create_run(store, ui_project, profile="success")
    created_at = store.get_run(run.run_id).created_at

    inside = created_at + timedelta(seconds=STALE_WORKER_GRACE_SECONDS - 0.5)
    assert _manager_at(store, inside).reconcile() == []

    past = created_at + timedelta(seconds=STALE_WORKER_GRACE_SECONDS + 0.5)
    assert _manager_at(store, past).reconcile() == [run.run_id]
    assert store.get_run(run.run_id).status == STATUS_INTERRUPTED


def test_manager_owns_no_background_threads(store: RunStore) -> None:
    """Reconciliation cadence belongs to the caller, not to the manager.

    A manager that started its own timer thread would keep running (and keep
    the store open) after the object it belongs to is gone.
    """
    import threading

    before = {t.ident for t in threading.enumerate()}
    manager = RunProcessManager(store)
    manager.reconcile()
    assert {t.ident for t in threading.enumerate()} == before
