"""Detached worker process that executes one pipeline run.

Spawned as::

    python -m kptn_server.worker --db PATH --run-id ID

The worker is deliberately the only process that touches a run's execution. It
looks the run up by id, moves into the run's immutable project root, loads the
project's pipeline, and calls :func:`kptn.run` with a
:class:`~kptn_server.capture.RunStoreSink`. Everything it learns goes straight
into the durable store, so the browser, VS Code, and the FastAPI service can
all restart -- or never be running at all -- without the run noticing.

A daemon thread heartbeats every two seconds while the run is in flight; a
supervisor uses that heartbeat, together with the recorded PID, to tell a live
worker from one a host reboot killed.

Exit codes:

===== ==========================================================
0     the pipeline succeeded
1     the pipeline raised -- the run is recorded as ``failed``
2     usage problem (unopenable store, unknown or finished run)
3     a durable write failed -- the run is recorded as ``errored``
130   the worker was interrupted or terminated -- ``stopped``
===== ==========================================================
"""

from __future__ import annotations

import argparse
import os
import signal
import sqlite3
import sys
import threading
import time
import traceback
from pathlib import Path

import kptn
from kptn.project import load_pipeline
from kptn_server.capture import RunStoreSink, capture_worker_output
from kptn_server.run_store import (
    STATUS_ERRORED,
    STATUS_FAILED,
    STATUS_STOPPED,
    STATUS_SUCCEEDED,
    TERMINAL_STATUSES,
    RunRecord,
    RunStore,
    RunStoreError,
)

#: The plan fixes the worker heartbeat at two seconds. Staleness grace periods
#: and reconciliation cadence belong to the supervisor, not here.
HEARTBEAT_INTERVAL_SECONDS = 2.0

EXIT_SUCCESS = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_DURABLE_WRITE_FAILURE = 3
EXIT_STOPPED = 130

#: Failures of the durable store itself. These are fatal: a run whose events
#: are not being recorded is worse than no run at all, so the worker stops.
_DURABLE_ERRORS = (RunStoreError, sqlite3.Error)

_TERMINATION_SIGNALS = (signal.SIGTERM, signal.SIGINT)


class _Heartbeat:
    """Daemon thread that refreshes the run's heartbeat while it executes."""

    def __init__(
        self,
        store: RunStore,
        run_id: str,
        interval: float = HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        self._store = store
        self._run_id = run_id
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"kptn-heartbeat-{run_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval * 2)

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                if not self._store.heartbeat(self._run_id):
                    return  # run already finished; nothing left to report
            except _DURABLE_ERRORS:
                # The main thread's own store writes will surface the problem
                # with a proper status and exit code; a heartbeat is not the
                # place to tear the process down.
                return


def _install_termination_handlers():
    """Turn SIGTERM/SIGINT into ``KeyboardInterrupt`` so a stop is recorded.

    Returns the previous handlers so they can be restored -- ``main()`` is
    callable in-process (tests do it), and it must not leave the caller's
    signal disposition rewritten.
    """

    def handler(signum: int, frame: object) -> None:
        raise KeyboardInterrupt(f"worker terminated by signal {signum}")

    previous: dict[int, object] = {}
    for signal_number in _TERMINATION_SIGNALS:
        try:
            previous[signal_number] = signal.signal(signal_number, handler)
        except (ValueError, OSError):
            # Not the main thread, or the platform lacks the signal.
            pass
    return previous


def _restore_termination_handlers(previous) -> None:
    for signal_number, original in previous.items():
        try:
            signal.signal(signal_number, original)
        except (ValueError, OSError):
            pass


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m kptn_server.worker",
        description="Execute one durable kptn pipeline run.",
    )
    parser.add_argument(
        "--db",
        required=True,
        type=Path,
        help="Path to the UI run store (normally <project>/.kptn/ui.db).",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        dest="run_id",
        help="Identifier of the queued run to execute.",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=HEARTBEAT_INTERVAL_SECONDS,
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def _fail_usage(message: str) -> int:
    print(f"kptn worker: {message}", file=sys.stderr, flush=True)
    return EXIT_USAGE


def execute_run(
    store: RunStore,
    record: RunRecord,
    *,
    heartbeat_interval: float = HEARTBEAT_INTERVAL_SECONDS,
) -> int:
    """Run one pipeline to completion and record its outcome durably."""
    run_id = record.run_id
    os.chdir(record.project_root)
    store.record_worker_start(run_id, pid=os.getpid(), started_at=time.time())

    heartbeat = _Heartbeat(store, run_id, heartbeat_interval)
    previous_handlers = _install_termination_handlers()
    heartbeat.start()

    status = STATUS_SUCCEEDED
    exit_code = EXIT_SUCCESS
    durable_failure: BaseException | None = None

    try:
        with capture_worker_output(store, run_id, record.log_path):
            try:
                pipeline = load_pipeline(record.project_root)
                kptn.run(
                    pipeline,
                    profile=record.profile,
                    force=record.force,
                    event_sink=RunStoreSink(store, run_id),
                    run_id=run_id,
                )
            except _DURABLE_ERRORS:
                raise
            except KeyboardInterrupt:
                status, exit_code = STATUS_STOPPED, EXIT_STOPPED
                print("run stopped", file=sys.stderr, flush=True)
            except BaseException:
                status, exit_code = STATUS_FAILED, EXIT_FAILED
                # Into the captured stderr, so the failure is in the run log
                # and not only in the worker's own (unwatched) stderr.
                traceback.print_exc(file=sys.stderr)
    except _DURABLE_ERRORS as exc:
        status = STATUS_ERRORED
        exit_code = EXIT_DURABLE_WRITE_FAILURE
        durable_failure = exc
    finally:
        heartbeat.stop()
        _restore_termination_handlers(previous_handlers)

    if durable_failure is not None:
        print(
            f"kptn worker: durable event write failed: {durable_failure}",
            file=sys.stderr,
            flush=True,
        )

    try:
        store.finish_run(run_id, status, exit_code=exit_code)
    except _DURABLE_ERRORS as exc:
        print(
            f"kptn worker: could not record final status {status!r}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return exit_code or EXIT_DURABLE_WRITE_FAILURE

    return exit_code


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        store = RunStore(args.db)
    except (sqlite3.Error, OSError) as exc:
        return _fail_usage(f"cannot open run store {args.db}: {exc}")

    try:
        record = store.get_run(args.run_id)
    except _DURABLE_ERRORS as exc:
        return _fail_usage(f"cannot read run {args.run_id}: {exc}")

    if record is None:
        return _fail_usage(f"no such run: {args.run_id}")
    if record.status in TERMINAL_STATUSES:
        return _fail_usage(f"run {args.run_id} is already finished ({record.status!r})")

    return execute_run(store, record, heartbeat_interval=args.heartbeat_interval)


if __name__ == "__main__":
    raise SystemExit(main())
