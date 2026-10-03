"""Durable, SQLite-backed store for pipeline run history.

A detached worker process appends run lifecycle events here; the FastAPI UI
reads the same database. All UI state lives on disk under
``<project_root>/.kptn/ui.db`` and ``<project_root>/.kptn/runs/`` so a run
survives a browser, notebook server, or FastAPI restart -- nothing is kept in process
memory.

Only one run may be active per canonical project path at a time, *among the
runs this store knows about*. Creating a second run for a project that
already has one raises :class:`ActiveRunError`. ``kptn run`` in a terminal
never reaches this store and is therefore neither blocked by the lock nor
counted by it.
Invalid run state transitions (e.g. finishing an already-terminal run) raise
:class:`RunStateError` rather than being silently coerced.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import uuid
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from kptn.runner.events import EventKind

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
        log_path = runs_dir / f"{run_id}.log"

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
    ) -> StoredEvent:
        event_timestamp = timestamp or datetime.now(timezone.utc)
        payload = payload or {}

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
            raise
        finally:
            self._release(conn)

        return StoredEvent(
            run_id=run_id,
            sequence=sequence,
            timestamp=event_timestamp,
            kind=kind,
            task_name=task_name,
            payload=payload,
            log_start=log_start,
            log_end=log_end,
        )

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
        return self._row_to_run(row)

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


__all__ = [
    "COUNTED_EVENT_KINDS",
    "ActiveRunError",
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
]
