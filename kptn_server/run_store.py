"""Durable, SQLite-backed store for pipeline run history.

A detached worker process appends run lifecycle events here; the FastAPI UI
reads the same database. All UI state lives on disk under
``<project_root>/.kptn/ui.db`` and ``<project_root>/.kptn/runs/`` so a run
survives a browser, notebook server, or FastAPI restart -- nothing is kept in process
memory.

The store is also the only writer of the run files other people read (see
:mod:`kptn_server.run_files`). Every event it commits is appended to the run's
``<run_id>.jsonl`` inside the same write transaction, captured output
included, so the file and the database agree on order and nothing reaches
one without the other. ``index.json`` is republished from the database after
each state change, and at a bounded rate while a run is going.

Only one run may be active per canonical project path at a time, *among the
runs this store knows about*. Creating a second run for a project that
already has one raises :class:`ActiveRunError`. ``kptn run`` in a terminal
records its run here too, and so takes the same lock; ``kptn run --no-record``
never reaches this store and is therefore neither blocked by the lock nor
counted by it.
Invalid run state transitions (e.g. finishing an already-terminal run) raise
:class:`RunStateError` rather than being silently coerced.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from kptn.runner.events import EventKind
from kptn_server import run_files
from kptn_server.run_files import (
    INDEX_FILENAME,
    INDEX_LIMIT,
    INDEX_PUBLISH_INTERVAL_SECONDS,
    RUN_FILE_SUFFIX,
)

_LOGGER = logging.getLogger(__name__)

JSONValue = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_INITIAL_MIGRATION = _MIGRATIONS_DIR / "001_runs.sql"

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_STOP_REQUESTED = "stop_requested"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_STOPPED = "stopped"
STATUS_ERRORED = "errored"
#: Recorded by the supervisor, never by a worker: the process executing the
#: run is gone (a reboot, a SIGKILL, a container restart) and no outcome was
#: ever written, so the run's real fate is unknown.
STATUS_INTERRUPTED = "interrupted"

#: The event kinds :meth:`RunStore.event_counts` aggregates. Deliberately
#: excludes ``log``, which is by far the most numerous kind and contributes
#: nothing to a run's task or warning counts.
#:
#: Derived from :class:`~kptn.runner.events.EventKind` rather than spelled
#: out, along with every other kind this module tests for: the runner owns
#: the vocabulary, and a rename there has to break loudly here instead of
#: quietly zeroing a count or skipping a state transition.
COUNTED_EVENT_KINDS = (
    EventKind.TASK_STARTED.value,
    EventKind.TASK_SKIPPED.value,
    EventKind.WARNING.value,
    EventKind.TASK_FINISHED.value,
)

TERMINAL_STATUSES = frozenset(
    {
        STATUS_SUCCEEDED,
        STATUS_FAILED,
        STATUS_STOPPED,
        STATUS_ERRORED,
        STATUS_INTERRUPTED,
    }
)


class RunStoreError(Exception):
    """Base class for run store errors."""


class ActiveRunError(RunStoreError):
    """Raised when a project already has an active (unfinished) run."""


class RunStateError(RunStoreError):
    """Raised when an operation would coerce an invalid run state transition."""


class RunNotFoundError(RunStoreError):
    """Raised when an operation references a run_id that does not exist."""


@dataclass(frozen=True)
class RunRequest:
    project_root: Path
    pipeline: str
    profile: str | None = None
    force: bool = False


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    project_root: Path
    pipeline: str
    profile: str | None
    force: bool
    status: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    worker_pid: int | None
    worker_started_at: float | None
    heartbeat_at: datetime | None
    current_task: str | None
    exit_code: int | None
    log_path: Path


@dataclass(frozen=True)
class StoredEvent:
    run_id: str
    sequence: int
    timestamp: datetime
    kind: str
    task_name: str | None
    payload: Mapping[str, JSONValue]
    log_start: int | None
    log_end: int | None
    #: The captured output itself, when it is in hand: an event read out of a
    #: run file, or one just appended. Events read from ``ui.db`` leave it
    #: ``None`` and are read back through ``log_start``/``log_end``.
    text: str | None = None


@dataclass(frozen=True)
class PendingEvent:
    """An event waiting to be written by :meth:`RunStore.append_events`.

    Carries its own timestamp because it is written later than it happened.
    """

    kind: str
    timestamp: datetime
    task_name: str | None = None
    payload: Mapping[str, JSONValue] = field(default_factory=dict)
    log_start: int | None = None
    log_end: int | None = None
    #: Captured output, for a ``log`` event. Written to the run file, never
    #: to ``ui.db``; the row records where in the file it went.
    text: str | None = None


@dataclass(frozen=True)
class _FileEntry:
    """One event on its way into a run file, sequence already assigned."""

    sequence: int
    timestamp: datetime
    kind: str
    task_name: str | None
    payload: Mapping[str, JSONValue]
    text: str | None
    log_start: int | None
    log_end: int | None


#: Kinds that move a run's state machine. :meth:`RunStore.append_events`
#: refuses them: each has its own transition rule in
#: :meth:`RunStore.append_event`, and a batch is for the high-volume kinds
#: that only stamp the heartbeat.
_STATE_CHANGING_KINDS = frozenset(
    {
        EventKind.RUN_STARTED.value,
        EventKind.TASK_STARTED.value,
        EventKind.RUN_FINISHED.value,
    }
)


@dataclass(frozen=True)
class WarningGroup:
    run_id: str
    task_name: str | None
    category: str
    fingerprint: str
    sample_message: str
    count: int
    first_sequence: int
    last_sequence: int
    occurrence_sequences: tuple[int, ...] = field(default_factory=tuple)


@dataclass
class _WarningTally:
    """One warning group while it is still being counted.

    A mutable dataclass rather than a ``dict[str, object]``: the counters are
    read back out as ints and the sequences as a list, and a bag of ``object``
    made every one of those reads a cast.
    """

    sample_message: str
    first_sequence: int
    last_sequence: int
    count: int = 0
    occurrence_sequences: list[int] = field(default_factory=list)


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
    r"|\d{2}:\d{2}:\d{2}(?:\.\d+)?"
)


def _normalize_warning_text(text: str) -> str:
    """Strip ANSI presentation codes and timestamps for fingerprinting.

    Nothing else about the text is normalized -- leading/trailing whitespace,
    punctuation, and wording differences still produce different
    fingerprints.
    """
    without_ansi = _ANSI_RE.sub("", text)
    return _TIMESTAMP_RE.sub("<TS>", without_ansi)


def _dt_to_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _text_to_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _required_dt(value: str | None) -> datetime:
    """Read a column the schema declares ``NOT NULL``.

    ``created_at`` and an event's ``timestamp`` cannot be null, and the
    dataclasses that hold them say so. Asserting it here keeps that promise
    checkable instead of quietly widening every reader to ``| None``.
    """
    if value is None:
        raise RunStoreError("a NOT NULL timestamp column was NULL")
    return datetime.fromisoformat(value)


class _ThreadConnection:
    """One thread's connection, closed when the thread that owns it ends.

    Held only by that thread's ``threading.local`` slot (the store keeps a
    weak reference, for :meth:`RunStore.close`), so when the thread exits --
    the UI's thread pool retires idle workers -- the slot is cleared, this is
    collected, and the connection is closed rather than kept open for the
    life of the process.
    """

    __slots__ = ("conn", "pid", "__weakref__")

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.pid = os.getpid()

    def close(self) -> None:
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    def __del__(self) -> None:
        self.close()


def _close_all(held: weakref.WeakSet[_ThreadConnection]) -> None:
    for entry in list(held):
        entry.close()


class RunStore:
    """SQLite-backed store for pipeline run history."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._held: weakref.WeakSet[_ThreadConnection] = weakref.WeakSet()
        # Closes what is still open when the store is collected (or at exit),
        # rather than leaving each connection to its own finalizer.
        weakref.finalize(self, _close_all, self._held)
        # When this store last published each project's index.json, by
        # project root, for the rate limit on publishes nothing forces.
        self._published_at: dict[str, float] = {}
        self._publish_lock = threading.Lock()
        self._publish_failure_reported = False
        # Apply the migration (idempotently) up front so later connections
        # never race on schema creation.
        conn = self._acquire()
        self._release(conn)

    @property
    def path(self) -> Path:
        """Filesystem location of the store, for passing to a worker."""
        return self._path

    # -- connection management ------------------------------------------------
    #
    # One connection per thread, opened on first use and kept for as long as
    # the thread lives. Opening is the expensive part where it matters: on an
    # NFS-mounted project every fcntl lock is a network round trip, and a
    # WAL-mode open-and-close took ~220ms there against ~10ms for a read on a
    # connection already open. A connection per *call* put that on every page
    # render, every stream poll, and every line of captured worker output.
    #
    # Per thread because a sqlite3 connection must not be used by two threads
    # at once, and the UI calls the store from a thread pool. Nothing goes
    # stale by being kept: in autocommit mode each statement or BEGIN starts
    # a fresh read of the database, so another process's commits are seen on
    # the next call exactly as they were with a new connection.

    def _acquire(self) -> sqlite3.Connection:
        """This thread's connection, opened on first use."""
        held: _ThreadConnection | None = getattr(self._local, "held", None)
        # A forked child inherits the parent's thread-local; a SQLite
        # connection must never be used across fork, so it gets its own.
        if held is None or held.pid != os.getpid():
            held = _ThreadConnection(self._connect())
            self._local.held = held
            self._held.add(held)
        return held.conn

    def _release(self, conn: sqlite3.Connection) -> None:
        """Hand a connection back after a call; it stays open for the next one.

        Every write path rolls back on failure, but a ROLLBACK can itself
        fail. A connection left inside a transaction would hold SQLite's write
        lock for as long as the thread lives and fail its next BEGIN, so a
        transaction still open here is rolled back -- and if even that fails,
        the connection is dropped rather than reused.
        """
        if not conn.in_transaction:
            return
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            held = getattr(self._local, "held", None)
            if held is not None and held.conn is conn:
                self._local.held = None
                self._held.discard(held)
            try:
                conn.close()
            except sqlite3.Error:
                pass

    def _open_connections(self) -> list[sqlite3.Connection]:
        """The connections still open, for tests and diagnostics."""
        return [entry.conn for entry in list(self._held)]

    def close(self) -> None:
        """Close every connection this store has opened, on any thread.

        Optional: connections are closed anyway when their thread ends or the
        store is collected. A store used after ``close`` reopens.
        """
        _close_all(self._held)
        self._held.clear()
        # A fresh thread-local, so every thread -- not only this one -- opens
        # a new connection on its next call instead of reusing a closed one.
        self._local = threading.local()

    def _connect(self) -> sqlite3.Connection:
        """Open and configure a new connection.

        ``check_same_thread=False`` only so that :meth:`close` may close a
        connection from a thread other than the one that opened it; each
        connection is still used by exactly one thread.
        """
        conn = sqlite3.connect(
            self._path, timeout=5.0, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        self._ensure_schema(conn)
        return conn

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
        ).fetchone()
        if row is not None:
            return
        sql = _INITIAL_MIGRATION.read_text()
        # executescript() commits any pending transaction before it starts,
        # then runs the given statements in sequence. To make the
        # check-then-create atomic across processes/connections racing to
        # initialize a brand-new database, wrap the migration itself in an
        # explicit BEGIN IMMEDIATE/COMMIT inside the same script: the first
        # connection to acquire the write lock creates the schema and
        # commits; a connection that was blocked waiting for the lock then
        # re-runs the same script against an already-migrated database and
        # deterministically fails on "table schema_version already exists",
        # which we treat as "someone else already did this" rather than an
        # error.
        script = f"BEGIN IMMEDIATE;\n{sql}\nCOMMIT;\n"
        try:
            conn.executescript(script)
        except sqlite3.OperationalError as exc:
            if "already exists" not in str(exc):
                raise
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass

    # -- row <-> dataclass conversion -----------------------------------------

    @staticmethod
    def _row_to_run(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=row["run_id"],
            project_root=Path(row["project_root"]),
            pipeline=row["pipeline"],
            profile=row["profile"],
            force=bool(row["force"]),
            status=row["status"],
            created_at=_required_dt(row["created_at"]),
            started_at=_text_to_dt(row["started_at"]),
            finished_at=_text_to_dt(row["finished_at"]),
            worker_pid=row["worker_pid"],
            worker_started_at=row["worker_started_at"],
            heartbeat_at=_text_to_dt(row["heartbeat_at"]),
            current_task=row["current_task"],
            exit_code=row["exit_code"],
            log_path=Path(row["log_path"]),
        )

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> StoredEvent:
        return StoredEvent(
            run_id=row["run_id"],
            sequence=row["sequence"],
            timestamp=_required_dt(row["timestamp"]),
            kind=row["kind"],
            task_name=row["task_name"],
            payload=json.loads(row["payload_json"]),
            log_start=row["log_start"],
            log_end=row["log_end"],
        )

    # -- writes ----------------------------------------------------------------

    def create_run(self, request: RunRequest) -> RunRecord:
        canonical_root = Path(request.project_root).resolve()
        run_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc)
        runs_dir = canonical_root / ".kptn" / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        log_path = runs_dir / f"{run_id}{RUN_FILE_SUFFIX}"

        conn = self._acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT run_id FROM project_run_locks WHERE project_root = ?",
                (str(canonical_root),),
            ).fetchone()
            if existing is not None:
                conn.execute("ROLLBACK")
                raise ActiveRunError(
                    f"project {canonical_root} already has an active run: {existing['run_id']}"
                )

            conn.execute(
                """
                INSERT INTO runs (
                    run_id, project_root, pipeline, profile, force, status,
                    created_at, started_at, finished_at, worker_pid,
                    worker_started_at, heartbeat_at, current_task, exit_code,
                    log_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?)
                """,
                (
                    run_id,
                    str(canonical_root),
                    request.pipeline,
                    request.profile,
                    int(request.force),
                    STATUS_QUEUED,
                    _dt_to_text(created_at),
                    str(log_path),
                ),
            )
            conn.execute(
                "INSERT INTO project_run_locks (project_root, run_id) VALUES (?, ?)",
                (str(canonical_root), run_id),
            )
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            self._release(conn)

        self.publish_index(canonical_root)
        return RunRecord(
            run_id=run_id,
            project_root=canonical_root,
            pipeline=request.pipeline,
            profile=request.profile,
            force=request.force,
            status=STATUS_QUEUED,
            created_at=created_at,
            started_at=None,
            finished_at=None,
            worker_pid=None,
            worker_started_at=None,
            heartbeat_at=None,
            current_task=None,
            exit_code=None,
            log_path=log_path,
        )

    def append_event(
        self,
        run_id: str,
        kind: str,
        *,
        timestamp: datetime | None = None,
        task_name: str | None = None,
        payload: Mapping[str, JSONValue] | None = None,
        log_start: int | None = None,
        log_end: int | None = None,
        text: str | None = None,
    ) -> StoredEvent:
        """Append one event, applying its state transition.

        *text* is a ``log`` event's captured output. It goes to the run file,
        and the row records the span it landed in as ``log_start``/``log_end``
        -- which therefore need not be passed alongside it.
        """
        event_timestamp = timestamp or datetime.now(timezone.utc)
        payload = payload or {}
        log_path: Path | None = None
        file_size: int | None = None

        conn = self._acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise RunNotFoundError(f"no such run: {run_id}")
            current_status = row["status"]

            if kind == EventKind.RUN_STARTED.value:
                if current_status != STATUS_QUEUED:
                    conn.execute("ROLLBACK")
                    raise RunStateError(
                        f"run {run_id} cannot start from status {current_status!r}"
                    )
                conn.execute(
                    "UPDATE runs SET status = ?, started_at = ?, heartbeat_at = ? WHERE run_id = ?",
                    (
                        STATUS_RUNNING,
                        _dt_to_text(event_timestamp),
                        _dt_to_text(event_timestamp),
                        run_id,
                    ),
                )
            elif kind == EventKind.TASK_STARTED.value:
                if current_status in TERMINAL_STATUSES:
                    conn.execute("ROLLBACK")
                    raise RunStateError(
                        f"run {run_id} cannot start a task from terminal status {current_status!r}"
                    )
                conn.execute(
                    "UPDATE runs SET current_task = ?, heartbeat_at = ? WHERE run_id = ?",
                    (task_name, _dt_to_text(event_timestamp), run_id),
                )
            elif kind == EventKind.RUN_FINISHED.value:
                if current_status not in (STATUS_RUNNING, STATUS_STOP_REQUESTED):
                    conn.execute("ROLLBACK")
                    raise RunStateError(
                        f"run {run_id} cannot finish from status {current_status!r}"
                    )
                conn.execute(
                    "UPDATE runs SET heartbeat_at = ? WHERE run_id = ?",
                    (_dt_to_text(event_timestamp), run_id),
                )
            else:
                if current_status in TERMINAL_STATUSES:
                    conn.execute("ROLLBACK")
                    raise RunStateError(
                        f"run {run_id} cannot accept {kind!r} event in terminal status {current_status!r}"
                    )
                conn.execute(
                    "UPDATE runs SET heartbeat_at = ? WHERE run_id = ?",
                    (_dt_to_text(event_timestamp), run_id),
                )

            sequence_row = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM run_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            sequence = sequence_row["next_sequence"]

            log_path = Path(row["log_path"])
            file_size, spans = self._record_in_run_file(
                log_path,
                [
                    _FileEntry(
                        sequence=sequence,
                        timestamp=event_timestamp,
                        kind=kind,
                        task_name=task_name,
                        payload=payload,
                        text=text,
                        log_start=log_start,
                        log_end=log_end,
                    )
                ],
            )
            log_start, log_end = spans[0]

            conn.execute(
                """
                INSERT INTO run_events (
                    run_id, sequence, timestamp, kind, task_name, payload_json,
                    log_start, log_end
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    sequence,
                    _dt_to_text(event_timestamp),
                    kind,
                    task_name,
                    json.dumps(dict(payload)),
                    log_start,
                    log_end,
                ),
            )
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self._undo_run_file(log_path, file_size)
            raise
        finally:
            self._release(conn)

        self.publish_index(
            row["project_root"],
            force=kind in (EventKind.RUN_STARTED.value, EventKind.RUN_FINISHED.value),
        )
        return StoredEvent(
            run_id=run_id,
            sequence=sequence,
            timestamp=event_timestamp,
            kind=kind,
            task_name=task_name,
            payload=payload,
            log_start=log_start,
            log_end=log_end,
            text=text,
        )

    def append_events(
        self, run_id: str, events: Sequence[PendingEvent]
    ) -> list[StoredEvent]:
        """Append several events in one transaction, in the order given.

        For the capture layer's ``log`` and ``warning`` events, which arrive a
        line at a time: one transaction per line costs a write lock and an
        fsync per line, ~20ms each on NFS, and a worker printing quickly spent
        its time waiting on the store. All of *events* land with contiguous
        sequence numbers, or -- if the run is gone or already terminal --
        none of them do.

        State-changing kinds are refused with ``ValueError``; send those
        through :meth:`append_event`.
        """
        if not events:
            return []
        refused = sorted({e.kind for e in events} & _STATE_CHANGING_KINDS)
        if refused:
            raise ValueError(
                f"append_events does not accept state-changing kinds: {refused}"
            )

        log_path: Path | None = None
        file_size: int | None = None
        conn = self._acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, project_root, log_path FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise RunNotFoundError(f"no such run: {run_id}")
            if row["status"] in TERMINAL_STATUSES:
                conn.execute("ROLLBACK")
                raise RunStateError(
                    f"run {run_id} cannot accept events in terminal status {row['status']!r}"
                )
            first = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM run_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
            log_path = Path(row["log_path"])
            file_size, spans = self._record_in_run_file(
                log_path,
                [
                    _FileEntry(
                        sequence=first + offset,
                        timestamp=event.timestamp,
                        kind=event.kind,
                        task_name=event.task_name,
                        payload=event.payload,
                        text=event.text,
                        log_start=event.log_start,
                        log_end=event.log_end,
                    )
                    for offset, event in enumerate(events)
                ],
            )
            conn.executemany(
                """
                INSERT INTO run_events (
                    run_id, sequence, timestamp, kind, task_name, payload_json,
                    log_start, log_end
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        first + offset,
                        _dt_to_text(event.timestamp),
                        event.kind,
                        event.task_name,
                        json.dumps(dict(event.payload)),
                        span[0],
                        span[1],
                    )
                    for offset, (event, span) in enumerate(zip(events, spans))
                ],
            )
            conn.execute(
                "UPDATE runs SET heartbeat_at = ? WHERE run_id = ?",
                (_dt_to_text(events[-1].timestamp), run_id),
            )
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self._undo_run_file(log_path, file_size)
            raise
        finally:
            self._release(conn)

        self.publish_index(row["project_root"], force=False)
        return [
            StoredEvent(
                run_id=run_id,
                sequence=first + offset,
                timestamp=event.timestamp,
                kind=event.kind,
                task_name=event.task_name,
                payload=event.payload,
                log_start=span[0],
                log_end=span[1],
                text=event.text,
            )
            for offset, (event, span) in enumerate(zip(events, spans))
        ]

    def record_worker_start(
        self,
        run_id: str,
        *,
        pid: int,
        started_at: float,
        timestamp: datetime | None = None,
    ) -> RunRecord:
        """Record the identity of the process executing *run_id*.

        ``started_at`` is the worker process's OS creation timestamp (i.e.
        ``psutil.Process(pid).create_time()``); together with ``pid`` it lets a
        supervisor tell a live worker from a recycled PID. Callers must not
        pass "when I got here" instead -- the supervisor compares this value
        against the live process's creation time. Also seeds ``heartbeat_at``
        so a freshly spawned worker is never mistaken for a stale one.
        """
        beat = timestamp or datetime.now(timezone.utc)
        conn = self._acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise RunNotFoundError(f"no such run: {run_id}")
            if row["status"] in TERMINAL_STATUSES:
                conn.execute("ROLLBACK")
                raise RunStateError(
                    f"run {run_id} is already finished ({row['status']!r})"
                )
            conn.execute(
                """
                UPDATE runs
                SET worker_pid = ?, worker_started_at = ?, heartbeat_at = ?
                WHERE run_id = ?
                """,
                (pid, started_at, _dt_to_text(beat), run_id),
            )
            conn.execute("COMMIT")
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            self._release(conn)
        self.publish_index(row["project_root"], force=False)
        return self._row_to_run(row)

    def heartbeat(self, run_id: str, *, timestamp: datetime | None = None) -> bool:
        """Refresh ``heartbeat_at`` for a still-running run.

        Returns ``True`` when the heartbeat landed and ``False`` when the run
        has already reached a terminal status -- a race a shutting-down worker
        can lose harmlessly, so it is not an error.

        The terminal-status test is part of the UPDATE and the whole thing runs
        in one ``BEGIN IMMEDIATE`` transaction: a separate check-then-write
        could see a running run, lose the race to ``finish_run``, and then
        stamp ``heartbeat_at`` onto a finished run while reporting success.
        """
        beat = timestamp or datetime.now(timezone.utc)
        terminal = tuple(sorted(TERMINAL_STATUSES))
        placeholders = ", ".join("?" for _ in terminal)
        conn = self._acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise RunNotFoundError(f"no such run: {run_id}")
            project_root = conn.execute(
                "SELECT project_root FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()["project_root"]
            cursor = conn.execute(
                f"""
                UPDATE runs SET heartbeat_at = ?
                WHERE run_id = ? AND status NOT IN ({placeholders})
                """,
                (_dt_to_text(beat), run_id, *terminal),
            )
            updated = cursor.rowcount > 0
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            self._release(conn)
        if updated:
            # The heartbeat is what keeps index.json current through a long
            # quiet task: nothing else is written then, and a reader in
            # another pod judges a run's liveness by this timestamp alone.
            self.publish_index(project_root, force=False)
        return updated

    def request_stop(self, run_id: str) -> RunRecord:
        conn = self._acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise RunNotFoundError(f"no such run: {run_id}")
            current_status = row["status"]
            if current_status in TERMINAL_STATUSES:
                conn.execute("ROLLBACK")
                raise RunStateError(
                    f"run {run_id} cannot be stopped from terminal status {current_status!r}"
                )
            if current_status != STATUS_STOP_REQUESTED:
                conn.execute(
                    "UPDATE runs SET status = ? WHERE run_id = ?",
                    (STATUS_STOP_REQUESTED, run_id),
                )
            conn.execute("COMMIT")
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            self._release(conn)
        self.publish_index(row["project_root"])
        return self._row_to_run(row)

    def finish_run(
        self,
        run_id: str,
        status: str,
        *,
        exit_code: int | None = None,
        timestamp: datetime | None = None,
    ) -> RunRecord:
        if status not in TERMINAL_STATUSES:
            raise RunStateError(f"{status!r} is not a terminal status")
        finished_at = timestamp or datetime.now(timezone.utc)

        conn = self._acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise RunNotFoundError(f"no such run: {run_id}")
            if row["status"] in TERMINAL_STATUSES:
                conn.execute("ROLLBACK")
                raise RunStateError(
                    f"run {run_id} is already finished ({row['status']!r})"
                )

            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ?, exit_code = ? WHERE run_id = ?",
                (status, _dt_to_text(finished_at), exit_code, run_id),
            )
            conn.execute(
                "DELETE FROM project_run_locks WHERE run_id = ?",
                (run_id,),
            )
            conn.execute("COMMIT")
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            self._release(conn)
        self.publish_index(row["project_root"])
        return self._row_to_run(row)

    # -- the published run files -------------------------------------------

    @staticmethod
    def _record_in_run_file(
        log_path: Path, entries: Sequence[_FileEntry]
    ) -> tuple[int | None, list[tuple[int | None, int | None]]]:
        """Append *entries* to the run's file, inside the caller's transaction.

        Returns the file's size before the append -- ``None`` when nothing was
        written, so there is nothing to undo -- and each entry's
        ``(log_start, log_end)``: the span its text landed in, or whatever the
        caller passed for an entry with no text.

        Called under ``BEGIN IMMEDIATE``, and every writer of a run's file
        comes through here, so SQLite's write lock is what orders the file's
        lines: they follow the sequence numbers exactly, across processes.

        A ``.jsonl`` run gets a line for every event. A run recorded before
        run files existed has a ``.log`` of raw output, and gets only the text,
        as it always did.

        Losing a line for an event without text costs a colleague one row of
        a console the owner still sees in full, and is logged rather than
        raised. Losing *text* loses output that is stored nowhere else, so
        that failure raises, and the caller's transaction rolls back with it.
        """
        jsonl = log_path.suffix == RUN_FILE_SUFFIX
        spans: list[tuple[int | None, int | None]] = [
            (entry.log_start, entry.log_end) for entry in entries
        ]
        lines: list[bytes] = []
        owners: list[int] = []
        for index, entry in enumerate(entries):
            if jsonl:
                lines.append(
                    run_files.encode_event(
                        sequence=entry.sequence,
                        timestamp=_dt_to_text(entry.timestamp) or "",
                        kind=entry.kind,
                        task_name=entry.task_name,
                        payload=entry.payload,
                        text=entry.text,
                    )
                )
            elif entry.text is not None:
                lines.append(entry.text.encode("utf-8", errors="replace"))
            else:
                continue
            owners.append(index)
        if not lines:
            return None, spans

        try:
            size, written = run_files.append_lines(log_path, lines)
        except OSError:
            if any(entry.text is not None for entry in entries):
                raise
            _LOGGER.debug("could not append to run file %s", log_path, exc_info=True)
            return None, spans

        for index, span in zip(owners, written):
            if entries[index].text is not None:
                spans[index] = span
        return size, spans

    @staticmethod
    def _undo_run_file(log_path: Path | None, size: int | None) -> None:
        """Cut lines whose transaction rolled back back out of the run file.

        Safe without a lock of its own: the caller still holds the write lock
        no other appender can get past, so nothing can have been appended
        after them.
        """
        if log_path is not None and size is not None:
            run_files.truncate(log_path, size)

    def publish_index(self, project_root: Path | str, *, force: bool = True) -> None:
        """Rewrite ``<project_root>/.kptn/runs/index.json`` from this database.

        Unforced calls are rate-limited to one per
        :data:`~kptn_server.run_files.INDEX_PUBLISH_INTERVAL_SECONDS` per
        project. State changes force one, so a reader never waits that long
        to see a run start or end.

        The read and the rename happen under ``BEGIN IMMEDIATE``. Without
        that, a publisher that read the database first could rename last,
        and a run that had finished would be listed as running for good --
        nothing publishes a finished run again. With it, each rename carries
        state at least as new as the one before it, across processes.

        Never raises: a colleague's view going stale is not a reason to fail
        a run, or a page.
        """
        root = Path(project_root)
        key = str(root)
        now = time.monotonic()
        with self._publish_lock:
            last = self._published_at.get(key)
            if not force and last is not None and now - last < INDEX_PUBLISH_INTERVAL_SECONDS:
                return
            self._published_at[key] = now

        conn = self._acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT * FROM runs WHERE project_root = ?
                ORDER BY created_at DESC, run_id DESC LIMIT ?
                """,
                (key, INDEX_LIMIT),
            ).fetchall()
            records = [self._row_to_run(row) for row in rows]
            tallies = self._tallies(conn, [record.run_id for record in records])
            run_files.write_index(
                root / ".kptn" / "runs",
                run_files.build_index(root, records, tallies),
            )
            conn.execute("COMMIT")
        except Exception as exc:  # noqa: BLE001 - publishing must never fail a caller
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self._report_publish_failure(root, exc)
        finally:
            self._release(conn)

    def ensure_index(self, project_root: Path | str) -> None:
        """Publish ``index.json`` if the project has none yet.

        For a project whose history predates run files: nothing republishes
        it until its next run, and until then a colleague would see no
        history at all.
        """
        if not (Path(project_root) / ".kptn" / "runs" / INDEX_FILENAME).exists():
            self.publish_index(project_root)

    def _report_publish_failure(self, root: Path, exc: Exception) -> None:
        # Once per store at WARNING, then quietly. Inside a worker the root
        # logger is the run's own warning capture, and a publish retried
        # every few seconds would bury the run's real warnings.
        if self._publish_failure_reported:
            _LOGGER.debug("could not publish the run index for %s: %s", root, exc)
            return
        self._publish_failure_reported = True
        _LOGGER.warning(
            "could not publish the run index for %s, so other people's kptn ui "
            "will not see this project's latest runs: %s",
            root,
            exc,
        )

    @staticmethod
    def _tallies(
        conn: sqlite3.Connection, run_ids: Sequence[str]
    ) -> dict[str, dict[tuple[str, str | None], int]]:
        """:meth:`event_counts` for several runs, in one query."""
        result: dict[str, dict[tuple[str, str | None], int]] = {
            run_id: {} for run_id in run_ids
        }
        if not run_ids:
            return result
        kinds = ", ".join("?" for _ in COUNTED_EVENT_KINDS)
        ids = ", ".join("?" for _ in run_ids)
        rows = conn.execute(
            f"""
            SELECT run_id, kind,
                   json_extract(payload_json, '$.status') AS status,
                   COUNT(*) AS total
            FROM run_events
            WHERE run_id IN ({ids}) AND kind IN ({kinds})
            GROUP BY run_id, kind, status
            """,
            (*run_ids, *COUNTED_EVENT_KINDS),
        ).fetchall()
        for row in rows:
            result[row["run_id"]][(row["kind"], row["status"])] = row["total"]
        return result

    # -- reads -------------------------------------------------------------

    def get_run(self, run_id: str) -> RunRecord | None:
        conn = self._acquire()
        try:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        finally:
            self._release(conn)
        return self._row_to_run(row) if row is not None else None

    def active_run(self, project_root: Path | str) -> RunRecord | None:
        canonical_root = Path(project_root).resolve()
        conn = self._acquire()
        try:
            row = conn.execute(
                """
                SELECT r.* FROM project_run_locks l
                JOIN runs r ON r.run_id = l.run_id
                WHERE l.project_root = ?
                """,
                (str(canonical_root),),
            ).fetchone()
        finally:
            self._release(conn)
        return self._row_to_run(row) if row is not None else None

    def list_runs(
        self,
        project_root: Path | str | None = None,
        *,
        limit: int | None = None,
    ) -> list[RunRecord]:
        conn = self._acquire()
        try:
            query = "SELECT * FROM runs"
            params: list[object] = []
            if project_root is not None:
                canonical_root = Path(project_root).resolve()
                query += " WHERE project_root = ?"
                params.append(str(canonical_root))
            query += " ORDER BY created_at DESC, run_id DESC"
            if limit is not None:
                query += " LIMIT ?"
                params.append(limit)
            rows = conn.execute(query, params).fetchall()
        finally:
            self._release(conn)
        return [self._row_to_run(row) for row in rows]

    def unfinished_runs(
        self, project_root: Path | str | None = None
    ) -> list[RunRecord]:
        """Runs that have not reached a terminal status, oldest first.

        A supervisor polls this on a fixed cadence, so it is filtered in SQL
        rather than by hydrating every run ever recorded and discarding almost
        all of them. Read-only: no transaction to hold.
        """
        terminal = tuple(sorted(TERMINAL_STATUSES))
        placeholders = ", ".join("?" for _ in terminal)
        query = f"SELECT * FROM runs WHERE status NOT IN ({placeholders})"
        params: list[object] = [*terminal]
        if project_root is not None:
            query += " AND project_root = ?"
            params.append(str(Path(project_root).resolve()))
        query += " ORDER BY created_at ASC, run_id ASC"
        conn = self._acquire()
        try:
            rows = conn.execute(query, params).fetchall()
        finally:
            self._release(conn)
        return [self._row_to_run(row) for row in rows]

    def events_after(self, run_id: str, after_sequence: int = 0) -> list[StoredEvent]:
        conn = self._acquire()
        try:
            rows = conn.execute(
                """
                SELECT * FROM run_events
                WHERE run_id = ? AND sequence > ?
                ORDER BY sequence ASC
                """,
                (run_id, after_sequence),
            ).fetchall()
        finally:
            self._release(conn)
        return [self._row_to_event(row) for row in rows]

    def event_counts(self, run_id: str) -> dict[tuple[str, str | None], int]:
        """Per-(kind, task status) event tallies for *run_id*, aggregated in SQL.

        The raw facts only: how many events of each counted kind exist, and --
        for ``task_finished``, whose outcome lives in the payload -- how many
        carried each ``status``. What those tallies *mean* (which of them is a
        "task", how many tasks never finished) is the caller's, so that the
        derivation stays in one place above this layer.

        This exists because the run-history page needs counts for every listed
        run, and hydrating them was quadratic in the wrong thing: a ``log``
        event is one row per captured output span, so a project with fifty
        real runs of a few thousand output lines each turned one page load
        into hundreds of thousands of dataclass constructions and JSON parses.
        ``COUNT(*) ... GROUP BY`` returns a handful of rows per run instead,
        and the filter on ``kind`` means the log rows are never even scanned
        for their payloads.

        ``json_extract`` is SQLite's own JSON1 function, so the status is read
        by the database rather than by parsing every payload in Python. Kinds
        with no status (everything but ``task_finished``) key on ``None``.

        Read-only, so no transaction: a caller that wanted counts and events
        to agree exactly would have to hold one, and nothing does -- a
        counter that is one event stale on a live run is refreshed by the next
        poll.
        """
        placeholders = ", ".join("?" for _ in COUNTED_EVENT_KINDS)
        conn = self._acquire()
        try:
            rows = conn.execute(
                f"""
                SELECT kind,
                       json_extract(payload_json, '$.status') AS status,
                       COUNT(*) AS total
                FROM run_events
                WHERE run_id = ? AND kind IN ({placeholders})
                GROUP BY kind, status
                """,
                (run_id, *COUNTED_EVENT_KINDS),
            ).fetchall()
        finally:
            self._release(conn)
        return {(row["kind"], row["status"]): row["total"] for row in rows}

    def warning_groups(self, run_id: str) -> list[WarningGroup]:
        conn = self._acquire()
        try:
            rows = conn.execute(
                """
                SELECT * FROM run_events
                WHERE run_id = ? AND kind = ?
                ORDER BY sequence ASC
                """,
                (run_id, EventKind.WARNING.value),
            ).fetchall()
        finally:
            self._release(conn)

        groups: dict[tuple[str | None, str, str], _WarningTally] = {}
        order: list[tuple[str | None, str, str]] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            message = str(payload.get("message", ""))
            category = str(payload.get("category") or "general")
            fingerprint = _normalize_warning_text(message)
            key = (row["task_name"], category, fingerprint)
            sequence = int(row["sequence"])
            entry = groups.get(key)
            if entry is None:
                entry = _WarningTally(
                    sample_message=message,
                    first_sequence=sequence,
                    last_sequence=sequence,
                )
                groups[key] = entry
                order.append(key)
            entry.count += 1
            entry.first_sequence = min(entry.first_sequence, sequence)
            entry.last_sequence = max(entry.last_sequence, sequence)
            entry.occurrence_sequences.append(sequence)

        result: list[WarningGroup] = []
        for key in order:
            task_name, category, fingerprint = key
            entry = groups[key]
            result.append(
                WarningGroup(
                    run_id=run_id,
                    task_name=task_name,
                    category=category,
                    fingerprint=fingerprint,
                    sample_message=entry.sample_message,
                    count=entry.count,
                    first_sequence=entry.first_sequence,
                    last_sequence=entry.last_sequence,
                    occurrence_sequences=tuple(entry.occurrence_sequences),
                )
            )
        return result


def read_run_file(
    path: Path, run_id: str, *, offset: int = 0
) -> tuple[list[StoredEvent], int]:
    """The events in a run's ``.jsonl`` file from byte *offset*, text included.

    Returns them with the offset to resume from (see
    :func:`~kptn_server.run_files.read_events`). They are the same events
    ``ui.db`` holds, so the console renders them with the same code; only
    ``text`` stands in for the offsets nobody outside the owner's pod could
    use. Raises what :func:`~kptn_server.run_files.read_untrusted` raises.
    """
    events, resume = run_files.read_events(path, offset=offset)
    return [
        StoredEvent(
            run_id=run_id,
            sequence=event.sequence,
            timestamp=event.timestamp,
            kind=event.kind,
            task_name=event.task_name,
            payload=event.payload,
            log_start=None,
            log_end=None,
            text=event.text,
        )
        for event in events
    ], resume


__all__ = [
    "COUNTED_EVENT_KINDS",
    "ActiveRunError",
    "PendingEvent",
    "RunNotFoundError",
    "RunRecord",
    "RunRequest",
    "RunStateError",
    "RunStore",
    "RunStoreError",
    "STATUS_ERRORED",
    "STATUS_FAILED",
    "STATUS_INTERRUPTED",
    "STATUS_QUEUED",
    "STATUS_RUNNING",
    "STATUS_STOP_REQUESTED",
    "STATUS_STOPPED",
    "STATUS_SUCCEEDED",
    "StoredEvent",
    "TERMINAL_STATUSES",
    "WarningGroup",
    "read_run_file",
]
