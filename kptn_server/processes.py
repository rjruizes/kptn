"""Supervisor for detached pipeline-run workers.

This module is the seam between the FastAPI service (a process that restarts
whenever the developer saves a file) and the worker that actually executes a
pipeline (a process that must not). Three operations make that split durable:

``start(run_id)``
    Launch ``python -m kptn_server.worker`` as a genuinely detached process --
    its own session/process group on POSIX, a new process group on Windows,
    with all three standard streams pointed at the null device. Nothing about
    the launcher's own lifetime can then take the worker down, so the browser,
    VS Code, and the server can all restart mid-run.

``stop(run_id)``
    Signal *that* worker and nothing else. PIDs are recycled, so a recorded
    PID alone is not an identity: the process's OS creation time has to match
    the value the worker registered as well. If it does not, this refuses to
    signal, records the stop request, and lets reconciliation clean up. There
    is no process-name matching and no ``pkill``/``killall`` anywhere here --
    both would be able to kill an innocent process that merely looks similar.

``reconcile()``
    One synchronous pass that finds runs whose worker is gone -- a host reboot,
    an OOM kill, a ``kill -9`` -- and marks them ``interrupted`` through
    :meth:`RunStore.finish_run`, which drops the project's active-run lock in
    the same transaction so the project is not wedged forever.

The reconciliation *cadence* is the caller's business (the FastAPI lifespan
runs it every :data:`RECONCILE_INTERVAL_SECONDS`). :class:`RunProcessManager`
deliberately owns no threads and no timers: it holds nothing that has to
outlive the object, which is exactly why a launched worker survives the
manager being garbage-collected.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import psutil

from kptn_server.run_store import (
    STATUS_INTERRUPTED,
    TERMINAL_STATUSES,
    RunNotFoundError,
    RunRecord,
    RunStateError,
    RunStore,
)

#: How often the caller should invoke :meth:`RunProcessManager.reconcile`.
RECONCILE_INTERVAL_SECONDS = 5.0

#: How long a run may go without a heartbeat before a missing worker is
#: treated as gone. Comfortably more than the worker's two-second heartbeat, so
#: a busy machine cannot make a healthy worker look dead.
STALE_WORKER_GRACE_SECONDS = 15.0

#: Tolerance when comparing a stored ``worker_started_at`` against a live
#: process's ``create_time()``. Both come from the same clock via psutil, so
#: this only absorbs float round-tripping through SQLite's REAL column.
_CREATE_TIME_TOLERANCE_SECONDS = 0.001


@dataclass(frozen=True)
class ProcessIdentity:
    """A process, identified strongly enough to be safe to signal.

    ``started_at`` is the OS process-creation timestamp
    (``psutil.Process(pid).create_time()``), not the moment the run was
    registered. The pair is what makes a recycled PID distinguishable from the
    worker that originally claimed it.
    """

    pid: int
    started_at: float


class ProcessLaunchError(Exception):
    """Raised when a worker process could not be launched or identified."""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


class RunProcessManager:
    """Launches, stops, and reconciles detached run workers."""

    def __init__(
        self,
        store: RunStore,
        *,
        now: Callable[[], datetime] = _utcnow,
        python_executable: str | None = None,
    ) -> None:
        self._store = store
        self._now = now
        self._python = python_executable or sys.executable
        # Popen handles are kept only so the launcher can reap the zombie a
        # finished worker leaves in its process table. They are never used to
        # wait on, kill, or otherwise control a worker -- dropping this manager
        # must not disturb a running worker in any way.
        self._handles: dict[str, subprocess.Popen[bytes]] = {}

    # -- launch ------------------------------------------------------------

    def worker_argv(self, run_id: str) -> list[str]:
        """The exact argv used to launch a worker -- a list, never a string."""
        return [
            self._python,
            "-m",
            "kptn_server.worker",
            "--db",
            str(self._store.path),
            "--run-id",
            run_id,
        ]

    def start(self, run_id: str) -> ProcessIdentity:
        """Launch a detached worker for *run_id* and record its identity."""
        record = self._require_run(run_id)
        if record.status in TERMINAL_STATUSES:
            raise RunStateError(f"run {run_id} is already finished ({record.status!r})")

        proc = subprocess.Popen(
            self.worker_argv(run_id),
            cwd=record.project_root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=(os.name != "nt"),
            creationflags=(
                subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            ),
        )
        self._handles[run_id] = proc

        try:
            created_at = psutil.Process(proc.pid).create_time()
        except psutil.Error as exc:
            # The worker died before we could read its creation time, so we
            # have no identity for it and can never safely signal that PID.
            # Fail the run rather than leave the project locked by a ghost.
            self._finish_quietly(run_id, STATUS_INTERRUPTED, exit_code=proc.poll())
            raise ProcessLaunchError(
                f"worker for run {run_id} vanished before it could be identified: {exc}"
            ) from exc

        identity = ProcessIdentity(pid=proc.pid, started_at=created_at)
        # The worker registers the same pair itself during startup; writing it
        # here too means a run is never briefly "running with no known PID",
        # which is the window in which reconciliation would have to guess.
        try:
            self._store.record_worker_start(
                run_id, pid=identity.pid, started_at=identity.started_at
            )
        except RunStateError:
            # The worker got there first and has already finished -- nothing
            # left to claim, and the identity we return is still correct.
            pass
        return identity

    # -- stop --------------------------------------------------------------

    def stop(self, run_id: str) -> bool:
        """Ask the worker for *run_id* to stop. Returns whether it was signalled.

        The stop request is recorded durably first, so a worker that is
        mid-startup (or a reconciliation pass that runs next) still sees the
        intent even when there is nothing to signal yet.
        """
        record = self._require_run(run_id)
        if record.status in TERMINAL_STATUSES:
            return False

        record = self._store.request_stop(run_id)

        identity = self._recorded_identity(record)
        if identity is None:
            return False
        if not self._is_live(identity):
            return False
        return self._signal(identity)

    @staticmethod
    def _signal(identity: ProcessIdentity) -> bool:
        try:
            if os.name == "nt":  # pragma: no cover - POSIX CI
                os.kill(identity.pid, signal.CTRL_BREAK_EVENT)
            else:
                # The worker leads its own process group, so this reaches the
                # worker and anything it spawned (an R or SQL subprocess),
                # without ever touching the launcher's own group.
                os.killpg(os.getpgid(identity.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            return False
        return True

    # -- reconciliation ----------------------------------------------------

    def reconcile(self) -> list[str]:
        """Mark runs whose worker is gone as ``interrupted``.

        One synchronous pass; the caller decides how often to make it. Returns
        the run ids that were marked, in the order they were processed.
        """
        interrupted: list[str] = []
        for record in self._store.list_runs():
            if record.status in TERMINAL_STATUSES:
                continue
            if self._looks_alive(record):
                continue
            if self._finish_quietly(record.run_id, STATUS_INTERRUPTED):
                interrupted.append(record.run_id)
        self._reap_finished_handles()
        return interrupted

    def _looks_alive(self, record: RunRecord) -> bool:
        """Is *record*'s worker plausibly still running?

        A live process whose PID *and* creation time match what the worker
        registered is alive, full stop. Otherwise the run gets the benefit of
        the doubt only while it is inside the grace window, because a false
        "it is dead" verdict is destructive: it releases the project lock and
        marks a run terminal while its worker may still be writing.
        """
        identity = self._recorded_identity(record)
        if identity is not None and self._is_live(identity):
            return True
        return self._within_grace(record, has_identity=identity is not None)

    def _within_grace(self, record: RunRecord, *, has_identity: bool) -> bool:
        if has_identity:
            # ``record_worker_start`` seeds ``heartbeat_at``, so a run with a
            # registered worker and no heartbeat at all has lost its process
            # without ever reporting in -- there is nothing to wait for.
            reference = record.heartbeat_at
        else:
            # No PID yet: the launcher may be mid-spawn, or the worker may not
            # have reached its identity write. Time it from run creation.
            reference = record.heartbeat_at or record.created_at
        if reference is None:
            return False
        age = (self._now() - _as_utc(reference)).total_seconds()
        return age <= STALE_WORKER_GRACE_SECONDS

    # -- identity ----------------------------------------------------------

    @staticmethod
    def _recorded_identity(record: RunRecord) -> ProcessIdentity | None:
        if record.worker_pid is None or record.worker_started_at is None:
            return None
        return ProcessIdentity(
            pid=record.worker_pid, started_at=float(record.worker_started_at)
        )

    @staticmethod
    def _is_live(identity: ProcessIdentity) -> bool:
        """Does a running process with exactly this identity exist?

        A zombie counts as dead: the worker has exited and only its exit status
        is still around, so signalling it would be pointless.
        """
        try:
            proc = psutil.Process(identity.pid)
            if proc.status() == psutil.STATUS_ZOMBIE:
                return False
            created_at = proc.create_time()
        except psutil.Error:
            return False
        return abs(created_at - identity.started_at) <= _CREATE_TIME_TOLERANCE_SECONDS

    # -- store plumbing ----------------------------------------------------

    def _require_run(self, run_id: str) -> RunRecord:
        record = self._store.get_run(run_id)
        if record is None:
            raise RunNotFoundError(f"no such run: {run_id}")
        return record

    def _finish_quietly(
        self, run_id: str, status: str, *, exit_code: int | None = None
    ) -> bool:
        """Finish a run, tolerating a worker that got there first.

        Routed through ``finish_run`` on purpose: it sets the status and
        deletes the project's active-run lock in one transaction, so a
        reconciled run can never leave the project locked.
        """
        try:
            self._store.finish_run(run_id, status, exit_code=exit_code)
        except (RunStateError, RunNotFoundError):
            return False
        return True

    def _reap_finished_handles(self) -> None:
        for run_id, proc in list(self._handles.items()):
            if proc.poll() is not None:
                self._handles.pop(run_id, None)


__all__ = [
    "ProcessIdentity",
    "ProcessLaunchError",
    "RECONCILE_INTERVAL_SECONDS",
    "RunProcessManager",
    "STALE_WORKER_GRACE_SECONDS",
]
