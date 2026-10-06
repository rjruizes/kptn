"""Capture everything a pipeline worker produces and persist it durably.

A detached worker must survive a browser, notebook server, or FastAPI restart, so
nothing it produces may live in process memory. This module supplies the two
pieces that make that true:

:class:`RunStoreSink`
    An :class:`~kptn.runner.events.EventSink` that translates the runner's
    structured :class:`~kptn.runner.events.RunEvent` stream into
    :meth:`~kptn_server.run_store.RunStore.append_event` rows. It also carries
    the two extra entry points the capture layer needs -- :meth:`emit_log` and
    :meth:`emit_warning`. Durable write failures are never swallowed: they
    propagate so the worker can stop and exit nonzero.

:func:`capture_worker_output`
    A context manager that, for the duration of its body, replaces
    ``sys.stdout`` / ``sys.stderr``, overrides ``warnings.showwarning``, and
    installs a ``logging`` handler on the root logger. Captured text goes to
    the store as ``log`` events, and the store writes it into the run's file
    (see :mod:`kptn_server.run_files`) in the same transaction as the row.

Two rules govern the design:

* **No recursion.** Captured output is written *directly* to the log file and
  the store -- never back to the replaced Python stream. If the write path
  itself produced output on the captured stream we would recurse forever, so a
  thread-local guard drops any re-entrant write as a second line of defence.
* **No stderr guessing.** Raw stderr is ordinary log output carrying the
  ``stderr`` severity. Only ``warnings.warn`` calls and ``logging`` records at
  ``WARNING`` or above become ``warning`` events, and every occurrence is
  recorded -- grouping is the reader's job, not the writer's.
"""

from __future__ import annotations

import io
import logging
import sqlite3
import sys
import threading
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable, Iterator, TextIO

from contextlib import contextmanager

if TYPE_CHECKING:
    from _typeshed import SupportsWrite

from kptn.runner.events import EventKind, JSONValue, RunEvent, current_task_name
from kptn_server.run_store import PendingEvent, RunStore, RunStoreError

STREAM_STDOUT = "stdout"
STREAM_STDERR = "stderr"

#: Severity of ordinary output written to stdout.
SEVERITY_OUTPUT = "output"
#: Severity of output written to stderr. This distinguishes stderr from stdout
#: and nothing more -- it is deliberately *not* a claim that the text is a
#: warning or an error.
SEVERITY_STDERR = "stderr"

#: Set while this thread is inside a store-touching capture path. Every such
#: path -- captured writes, ``showwarning``, and the logging handler -- shares
#: this one flag, because the recursion we are defending against is "the act of
#: persisting produced more output/warnings/log records", and that can cross
#: from any one of those paths into any other.
_store_write_reentry = threading.local()


class _Reentry:
    """Thread-local guard that makes a code path non-re-entrant.

    ``__enter__`` returns whether *this* instance took the flag, and
    ``__exit__`` clears it only in that case. Clearing unconditionally would
    disarm the guard: the first nested attempt would be blocked but would then
    release the flag on its way out, admitting the second one.
    """

    def __init__(self, storage: threading.local) -> None:
        self._storage = storage
        self._acquired = False

    def __enter__(self) -> bool:
        if getattr(self._storage, "active", False):
            self._acquired = False
            return False
        self._storage.active = True
        self._acquired = True
        return True

    def __exit__(self, *exc_info: object) -> None:
        if self._acquired:
            self._acquired = False
            self._storage.active = False


class DurableWriteFailed(RunStoreError):
    """The run store rejected or could not accept a durable write.

    Raised at the exact seam that persists events, so the worker can tell "the
    durable store is broken" from "pipeline code raised a database error". The
    two are indistinguishable by exception type -- a pipeline may use SQLite
    itself, and kptn's own default state store does -- so the distinction has
    to be drawn by *which call failed*, not by what it raised.
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.cause = cause


#: Errors a store write can fail with. ``DurableWriteFailed`` subclasses
#: ``RunStoreError``, so it is excluded explicitly to avoid re-wrapping.
_STORE_ERRORS = (RunStoreError, sqlite3.Error, OSError)


def durable(action):
    """Run *action*, tagging any store failure as :class:`DurableWriteFailed`."""
    try:
        return action()
    except DurableWriteFailed:
        raise
    except _STORE_ERRORS as exc:
        raise DurableWriteFailed(exc) from exc


# -- durable sink ---------------------------------------------------------


#: How long captured output may wait before it is written to the store. The
#: console shows it no sooner than this.
FLUSH_INTERVAL_SECONDS = 0.25

#: Pending events that make the writing thread flush on the spot rather than
#: wait for the timer, so a burst of output cannot grow the buffer unbounded.
MAX_PENDING_EVENTS = 1000

#: Kinds a batching sink buffers. Everything else changes what the UI shows
#: about the run's state, and is written at once -- after whatever is pending.
_BATCHED_KINDS = frozenset({EventKind.LOG.value, EventKind.WARNING.value})


class RunStoreSink:
    """Persist runner events, captured output, and warnings into a run store.

    Satisfies the :class:`~kptn.runner.events.EventSink` protocol, so it can be
    handed straight to ``kptn.run(..., event_sink=...)``.

    With *flush_interval* set, ``log`` and ``warning`` events are buffered and
    written together by :meth:`flush` -- on a background timer started by
    :meth:`start`, before any other event, at :data:`MAX_PENDING_EVENTS`, and
    on :meth:`close`. One transaction per captured line held the store's write
    lock for every ``print`` and, on an NFS-mounted project, cost ~20ms each.
    Order is unchanged: buffered events are written in arrival order and
    always before the next state-changing event. Without *flush_interval*
    every event is written as it arrives.

    A failed write is never swallowed. On the calling thread it raises
    :class:`DurableWriteFailed`; on the timer thread it is kept, and raised to
    the next caller of any method here.
    """

    def __init__(
        self, store: RunStore, run_id: str, *, flush_interval: float | None = None
    ) -> None:
        self._store = store
        self._run_id = run_id
        self._flush_interval = flush_interval
        self._pending: list[PendingEvent] = []
        self._pending_lock = threading.Lock()
        # Held across a store write, so that sequence numbers follow the order
        # events were handed to this sink. Reentrant: a state-changing emit
        # flushes and then appends under one hold.
        self._write_lock = threading.RLock()
        # Set while this thread is inside a store write. Output produced
        # there is buffered, never flushed: a nested flush would open a second
        # transaction on the connection that already has one open.
        self._writing = threading.local()
        self._failure: DurableWriteFailed | None = None
        self._stop = threading.Event()
        self._flusher: threading.Thread | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    def _append(self, kind: str, **kwargs) -> None:
        self._raise_failure()
        with self._write_lock:
            self.flush()
            self._writing.active = True
            try:
                durable(lambda: self._store.append_event(self._run_id, kind, **kwargs))
            finally:
                self._writing.active = False

    def _buffer(self, kind: str, **kwargs) -> None:
        if self._flush_interval is None:
            self._append(kind, **kwargs)
            return
        self._raise_failure()
        kwargs.setdefault("timestamp", datetime.now(timezone.utc))
        with self._pending_lock:
            self._pending.append(PendingEvent(kind=kind, **kwargs))
            full = len(self._pending) >= MAX_PENDING_EVENTS
        if full:
            self.flush()

    def _raise_failure(self) -> None:
        if self._failure is not None:
            raise self._failure

    def flush(self) -> None:
        """Write every buffered event now, in one transaction."""
        if getattr(self._writing, "active", False):
            return
        with self._write_lock:
            self._raise_failure()
            with self._pending_lock:
                batch, self._pending = self._pending, []
            if not batch:
                return
            self._writing.active = True
            try:
                durable(lambda: self._store.append_events(self._run_id, batch))
            except DurableWriteFailed as exc:
                self._failure = exc
                raise
            finally:
                self._writing.active = False

    def start(self) -> None:
        """Start the background flush timer, for a sink with a flush interval."""
        if self._flush_interval is None or self._flusher is not None:
            return
        self._flusher = threading.Thread(
            target=self._flush_periodically,
            name=f"kptn-flush-{self._run_id}",
            daemon=True,
        )
        self._flusher.start()

    def _flush_periodically(self) -> None:
        assert self._flush_interval is not None
        while not self._stop.wait(self._flush_interval):
            # The same guard as every other persistence path: output this
            # thread produces while writing is dropped, never re-captured.
            with _Reentry(_store_write_reentry) as entered:
                if not entered:
                    continue
                try:
                    self.flush()
                except DurableWriteFailed:
                    return  # kept in self._failure for the worker's thread
                except Exception as exc:  # noqa: BLE001 - must reach the worker
                    self._failure = DurableWriteFailed(exc)
                    return

    def close(self) -> None:
        """Stop the timer and write whatever is still buffered."""
        self._stop.set()
        if self._flusher is not None:
            self._flusher.join()
            self._flusher = None
        self.flush()

    def emit(self, event: RunEvent) -> None:
        """Translate one runner event into a durable row.

        The store, not the in-process emitter, owns durable sequence numbers --
        that is what lets captured log and warning events interleave with
        runner events in the order they actually happened.

        A runner ``log`` event carries its output inline as ``message`` -- an R
        task's captured stdout and stderr. That text is moved into the run
        file like any other captured output rather than kept in the row, so
        it reaches the download and other people's views too.
        """
        kind = str(event.kind)
        payload = dict(event.payload)
        if kind == EventKind.LOG.value and isinstance(payload.get("message"), str):
            text = str(payload.pop("message"))
            stream = str(payload.get("stream") or STREAM_STDOUT)
            payload.setdefault(
                "severity",
                SEVERITY_STDERR if stream == STREAM_STDERR else SEVERITY_OUTPUT,
            )
            self._buffer(
                kind,
                timestamp=event.timestamp,
                task_name=event.task_name,
                payload=payload,
                text=text,
            )
            return
        write = self._buffer if kind in _BATCHED_KINDS else self._append
        write(
            kind,
            timestamp=event.timestamp,
            task_name=event.task_name,
            payload=payload,
        )

    def emit_log(
        self,
        *,
        text: str,
        stream: str,
        severity: str,
        task_name: str | None = None,
    ) -> None:
        """Record a span of captured output."""
        payload: dict[str, JSONValue] = {"stream": stream, "severity": severity}
        self._buffer(
            EventKind.LOG.value,
            task_name=task_name,
            payload=payload,
            text=text,
        )

    def emit_warning(
        self,
        *,
        task_name: str | None,
        category: str,
        message: str,
        filename: str | None = None,
        lineno: int | None = None,
    ) -> None:
        """Record one warning occurrence.

        One event per occurrence, always -- callers that want the grouped view
        read :meth:`~kptn_server.run_store.RunStore.warning_groups`.
        """
        payload: dict[str, JSONValue] = {
            "category": category,
            "message": message,
            "filename": filename,
            "lineno": lineno,
        }
        self._buffer(
            EventKind.WARNING.value,
            task_name=task_name,
            payload=payload,
        )


# -- raw output capture ---------------------------------------------------


class LogWriteState:
    """Hands captured text to the sink, one writer at a time, until closed.

    The text itself is written by the store, into the run's file, when the
    sink flushes; the store's write lock orders the file and the sequence
    numbers together. What is left here is the order the two captured
    streams hand text over in, and the point after which they may not.
    """

    def __init__(self, sink: RunStoreSink) -> None:
        self._sink = sink
        self._lock = threading.RLock()
        self._closed = False
        # Test seam: invoked inside the critical section, just before the text
        # is handed to the sink. Production code never sets it.
        self._critical_section_hook: Callable[[], None] | None = None

    def write(
        self,
        text: str,
        *,
        stream: str,
        severity: str,
        task_name: str | None,
    ) -> bool:
        """Record *text* as one ``log`` event.

        Returns ``False`` if there was nothing to write or the state is
        already closed.
        """
        if not text:
            return False
        with self._lock:
            if self._closed:
                return False
            self._commit_span(
                text, stream=stream, severity=severity, task_name=task_name
            )
            return True

    def lock_is_held(self) -> bool:
        """Whether the calling thread currently owns the critical section."""
        # ``_is_owned`` is the only way to ask an ``RLock`` this question and
        # it is not in typeshed's public surface. It has existed on CPython's
        # RLock since the module was written; the reentrancy guard this
        # answers for is not something a wrapper flag could track correctly.
        return bool(self._lock._is_owned())  # ty: ignore[unresolved-attribute]

    def _commit_span(
        self,
        text: str,
        *,
        stream: str,
        severity: str,
        task_name: str | None,
    ) -> None:
        """Hand *text* to the sink, with the lock held by the caller."""
        hook, self._critical_section_hook = self._critical_section_hook, None
        if hook is not None:
            hook()
        self._sink.emit_log(
            text=text,
            stream=stream,
            severity=severity,
            task_name=task_name,
        )

    def close(self) -> None:
        with self._lock:
            self._closed = True


class CapturedTextIO(io.TextIOBase):
    """A text stream that persists everything written to it.

    Partial lines are buffered until a newline arrives or the stream is closed,
    so one ``print()`` produces one durable ``log`` event rather than two.

    With *echo*, every write is also passed straight through to *original* --
    unbuffered, so a ``\r`` progress bar still animates. That is how ``kptn
    run`` keeps its terminal while recording the same log a UI run gets; the
    detached worker has no terminal and never echoes.
    """

    def __init__(
        self,
        state: LogWriteState,
        *,
        stream: str,
        severity: str,
        original: TextIO | None = None,
        echo: bool = False,
    ) -> None:
        self._state = state
        self._stream = stream
        self._severity = severity
        self._original = original
        self._echo = echo and original is not None
        self._buffer = ""
        self._buffer_lock = threading.RLock()

    # -- TextIOBase surface ---------------------------------------------

    @property
    def encoding(self) -> str:
        return "utf-8"

    @property
    def errors(self) -> str:
        return "replace"

    @property
    def name(self) -> str:
        return f"<kptn captured {self._stream}>"

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def isatty(self) -> bool:
        # An echoing stream answers for the terminal it echoes to, so a
        # pipeline looks the same under ``kptn run`` as it always did.
        if self._echo:
            assert self._original is not None
            return self._original.isatty()
        return False

    def fileno(self) -> int:
        if self._echo:
            # A terminal run has a real descriptor to give, and refusing it
            # would break tasks (``subprocess.run(stdout=sys.stdout)``,
            # ``faulthandler``) that work under plain ``kptn run``. What is
            # written there reaches the terminal but not the log.
            assert self._original is not None
            return self._original.fileno()
        # Deliberately unsupported: handing out the real descriptor would let a
        # caller write bytes that bypass the capture layer entirely.
        raise io.UnsupportedOperation("captured streams have no file descriptor")

    # -- writing ---------------------------------------------------------

    def write(self, text: str) -> int:
        return self._write(text, echo=self._echo)

    def write_log_only(self, text: str) -> int:
        """Persist *text* without echoing it, for text the terminal already has."""
        return self._write(text, echo=False)

    def _write(self, text: str, *, echo: bool) -> int:
        if not isinstance(text, str):
            raise TypeError(f"write() requires str, not {type(text).__name__}")
        if not text:
            return 0
        if echo:
            assert self._original is not None
            self._original.write(text)
        with self._buffer_lock:
            self._buffer += text
            newline_index = self._buffer.rfind("\n")
            if newline_index == -1:
                return len(text)
            chunk = self._buffer[: newline_index + 1]
            self._buffer = self._buffer[newline_index + 1 :]
        self._persist(chunk)
        return len(text)

    def writelines(self, lines) -> None:  # type: ignore[override]
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        """Flush the echo, if any; never the log.

        ``print(..., flush=True)`` is common, and flushing a partial line would
        split a single logical line across several durable events. Partial
        lines are released on the next newline or when the stream is closed.
        """
        if self._echo:
            assert self._original is not None
            self._original.flush()

    def close(self) -> None:
        with self._buffer_lock:
            chunk, self._buffer = self._buffer, ""
        if chunk:
            self._persist(chunk)

    def _persist(self, chunk: str) -> None:
        with _Reentry(_store_write_reentry) as entered:
            if not entered:
                # Something in the persistence path wrote to the captured
                # stream. Dropping the nested write is the only safe answer.
                return
            self._state.write(
                chunk,
                stream=self._stream,
                severity=self._severity,
                task_name=current_task_name(),
            )


# -- warnings and logging adapters ---------------------------------------


class StructuredWarningHandler(logging.Handler):
    """Turn ``logging`` records at WARNING or above into warning events.

    The record's logger name is used as the warning category, which is what
    lets the UI group ``logging``-sourced warnings by their origin. The
    formatted text is also mirrored to the captured stderr stream so the
    console transcript still reads normally -- without this the handler would
    displace ``logging.lastResort`` and the text would vanish.
    """

    def __init__(
        self, sink: RunStoreSink, mirror: SupportsWrite[str] | None = None
    ) -> None:
        super().__init__(level=logging.WARNING)
        self._sink = sink
        self._mirror = mirror

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < logging.WARNING:
            return
        # The mirror write takes the shared guard itself, inside _persist, so
        # it is not wrapped here -- wrapping the whole method would block the
        # mirror write and the text would never reach the log.
        if self._mirror is not None:
            text = self.format(record) + "\n"
            log_only = getattr(self._mirror, "write_log_only", None)
            if log_only is not None and self._shown_elsewhere(record):
                # Another handler already printed it; an echoing mirror would
                # print it to the terminal a second time.
                log_only(text)
            else:
                self._mirror.write(text)
        with _Reentry(_store_write_reentry) as entered:
            if not entered:
                return
            # A durable write failure must stop the run, so this deliberately
            # does not funnel through Handler.handleError().
            self._sink.emit_warning(
                task_name=current_task_name(),
                category=record.name,
                message=record.getMessage(),
                filename=record.pathname,
                lineno=record.lineno,
            )

    def _shown_elsewhere(self, record: logging.LogRecord) -> bool:
        """Would *record* reach a handler other than this one?

        The same walk ``Logger.callHandlers`` makes. When it finds nothing,
        ``logging.lastResort`` would have printed the record had this handler
        not been installed, and the mirror is what stands in for it.
        """
        logger: logging.Logger | None = logging.getLogger(record.name)
        while logger is not None:
            if any(handler is not self for handler in logger.handlers):
                return True
            if not logger.propagate:
                return False
            logger = logger.parent
        return False


@dataclass
class WorkerCapture:
    """Handles for an active capture context."""

    sink: RunStoreSink
    state: LogWriteState
    stdout: CapturedTextIO
    stderr: CapturedTextIO


@contextmanager
def capture_worker_output(
    store: RunStore,
    run_id: str,
    *,
    echo: bool = False,
) -> Iterator[WorkerCapture]:
    """Capture stdout, stderr, ``warnings``, and ``logging`` for a worker run.

    With *echo*, captured output also goes on to the streams it replaced (see
    :class:`CapturedTextIO`); ``kptn run`` uses it to record a run without
    taking its terminal away.

    Streams, the root logging handler, and ``warnings.showwarning`` are all
    restored on the way out, including when the body raises.
    """
    sink = RunStoreSink(store, run_id, flush_interval=FLUSH_INTERVAL_SECONDS)
    state = LogWriteState(sink)

    original_stdout: TextIO = sys.stdout
    original_stderr: TextIO = sys.stderr
    captured_stdout = CapturedTextIO(
        state,
        stream=STREAM_STDOUT,
        severity=SEVERITY_OUTPUT,
        original=original_stdout,
        echo=echo,
    )
    captured_stderr = CapturedTextIO(
        state,
        stream=STREAM_STDERR,
        severity=SEVERITY_STDERR,
        original=original_stderr,
        echo=echo,
    )
    handler = StructuredWarningHandler(sink, mirror=captured_stderr)
    root_logger = logging.getLogger()
    # Warnings echoed to the terminal so far. The "always" filter below makes
    # every occurrence reach the log; the terminal keeps Python's default of
    # showing each one once per location.
    echoed_warnings: set[tuple[str, type[Warning], str, int]] = set()

    def _showwarning(
        message: Warning | str,
        category: type[Warning],
        filename: str,
        lineno: int,
        file: TextIO | None = None,
        line: str | None = None,
    ) -> None:
        # The human-readable form goes to the captured stderr so the console
        # transcript is unchanged; the structured form goes to the store. The
        # mirror write takes the shared guard inside _persist; the store write
        # takes it here, symmetrically with StructuredWarningHandler.emit --
        # stdlib code (sqlite3 adapters among it) does warn from inside the
        # persistence path, and the appended "always" filter suppresses none
        # of it.
        text = warnings.formatwarning(message, category, filename, lineno, line)
        key = (str(message), category, str(filename), int(lineno))
        if key in echoed_warnings:
            captured_stderr.write_log_only(text)
        else:
            echoed_warnings.add(key)
            captured_stderr.write(text)
        with _Reentry(_store_write_reentry) as entered:
            if not entered:
                return
            sink.emit_warning(
                task_name=current_task_name(),
                category=getattr(category, "__name__", str(category)),
                message=str(message),
                filename=str(filename),
                lineno=int(lineno),
            )

    # catch_warnings() saves and restores both the filter list and
    # showwarning, so the "always" filter below cannot leak out of the run.
    with warnings.catch_warnings():
        # Append rather than reset: simplefilter() would discard the project's
        # own filters, so a pipeline relying on `filterwarnings = error` would
        # behave differently under the worker than under `kptn run`. Appending
        # leaves earlier filters in charge and only supplies "always" as the
        # fallback, which is what makes every otherwise-shown occurrence -- not
        # just the first per location -- reach the store.
        warnings.filterwarnings("always", append=True)
        # Replacing ``warnings.showwarning`` is the documented way to
        # intercept warnings; ty reads any assignment to a module-level
        # function as an implicit shadow and there is nowhere to annotate it.
        warnings.showwarning = _showwarning  # ty: ignore[invalid-assignment]
        sys.stdout = captured_stdout
        sys.stderr = captured_stderr
        root_logger.addHandler(handler)
        sink.start()
        try:
            yield WorkerCapture(
                sink=sink,
                state=state,
                stdout=captured_stdout,
                stderr=captured_stderr,
            )
        finally:
            root_logger.removeHandler(handler)
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            try:
                # Release any trailing partial lines while the store is still
                # writable -- the run is not yet in a terminal status.
                captured_stdout.close()
                captured_stderr.close()
                # After the streams, whose close releases trailing partial
                # lines into the buffer; before the worker records an outcome.
                sink.close()
            finally:
                state.close()


__all__ = [
    "CapturedTextIO",
    "DurableWriteFailed",
    "FLUSH_INTERVAL_SECONDS",
    "LogWriteState",
    "MAX_PENDING_EVENTS",
    "RunStoreSink",
    "SEVERITY_OUTPUT",
    "SEVERITY_STDERR",
    "STREAM_STDERR",
    "STREAM_STDOUT",
    "StructuredWarningHandler",
    "WorkerCapture",
    "capture_worker_output",
    "durable",
]
