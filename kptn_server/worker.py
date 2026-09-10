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
4     the run could not be started at all -- also ``errored``
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
from kptn_server.capture import (
    DurableWriteFailed,
    RunStoreSink,
    capture_worker_output,
    durable,
)
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
EXIT_SETUP_FAILURE = 4
EXIT_STOPPED = 130

#: Store errors, for the direct store calls the worker itself makes -- opening
#: the database, reading the run, recording the final status. Pipeline code is
#: never on those paths, so matching by type is unambiguous there.
#:
#: It is NOT safe around the pipeline: kptn's own default state store is
#: SQLite, so a failing task query raises ``sqlite3.Error`` too. Failures of
#: the *durable event stream* are identified by the ``DurableWriteFailed``
#: marker that ``kptn_server.capture.durable()`` raises at the persistence
#: seam, never by exception type.
_STORE_ERRORS = (RunStoreError, sqlite3.Error, OSError)

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
        if self._thread.is_alive() or self._thread.ident is not None:
            self._thread.join(timeout=self._interval * 2)

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                if not self._store.heartbeat(self._run_id):
                    return  # run already finished; nothing left to report
            except _STORE_ERRORS:
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
    status = STATUS_SUCCEEDED
    exit_code = EXIT_SUCCESS
    aborted_by: BaseException | None = None
    abort_reason = ""

    # Everything that can fail -- including the chdir, the worker-identity
    # write, the signal install, and starting the heartbeat thread -- runs
    # inside the try, so a setup failure is recorded and given a documented
    # exit code instead of escaping main() as a traceback with the signal
    # disposition still rewritten.
    heartbeat: _Heartbeat | None = None
    previous_handlers = None

    try:
        os.chdir(record.project_root)
        durable(
            lambda: store.record_worker_start(
                run_id, pid=os.getpid(), started_at=time.time()
            )
        )
        previous_handlers = _install_termination_handlers()
        heartbeat = _Heartbeat(store, run_id, heartbeat_interval)
        heartbeat.start()

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
            except DurableWriteFailed:
                # The durable event stream itself broke. Nothing else can be
                # trusted, so stop rather than press on with a partial record.
                raise
            except KeyboardInterrupt:
                status, exit_code = STATUS_STOPPED, EXIT_STOPPED
                print("run stopped", file=sys.stderr, flush=True)
            except BaseException:
                # Any other exception is the pipeline's -- including a
                # sqlite3.Error from a task's own query, which must NOT be
                # mistaken for the run store failing.
                status, exit_code = STATUS_FAILED, EXIT_FAILED
                # Into the captured stderr, so the failure is in the run log
                # and not only in the worker's own (unwatched) stderr.
                traceback.print_exc(file=sys.stderr)
    except DurableWriteFailed as exc:
        status = STATUS_ERRORED
        exit_code = EXIT_DURABLE_WRITE_FAILURE
        aborted_by, abort_reason = exc, "durable event write failed"
    except KeyboardInterrupt:
        status, exit_code = STATUS_STOPPED, EXIT_STOPPED
    except BaseException as exc:
        status = STATUS_ERRORED
        exit_code = EXIT_SETUP_FAILURE
        aborted_by, abort_reason = exc, "could not start the run"
    finally:
        if heartbeat is not None:
            heartbeat.stop()
        if previous_handlers is not None:
            _restore_termination_handlers(previous_handlers)

    if aborted_by is not None:
        print(
            f"kptn worker: {abort_reason}: {aborted_by}",
            file=sys.stderr,
            flush=True,
        )

    try:
        store.finish_run(run_id, status, exit_code=exit_code)
    except _STORE_ERRORS as exc:
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
    except _STORE_ERRORS as exc:
        return _fail_usage(f"cannot read run {args.run_id}: {exc}")

    if record is None:
        return _fail_usage(f"no such run: {args.run_id}")
    if record.status in TERMINAL_STATUSES:
        return _fail_usage(f"run {args.run_id} is already finished ({record.status!r})")

    return execute_run(store, record, heartbeat_interval=args.heartbeat_interval)


if __name__ == "__main__":
    raise SystemExit(main())
