"""Capture everything a pipeline worker produces and persist it durably.

A detached worker must survive a browser, VS Code, or FastAPI restart, so
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
    installs a ``logging`` handler on the root logger. Raw bytes go to the
    run's log file; byte offsets plus metadata go to the store.

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
import os
import sqlite3
import sys
import threading
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator, TextIO

from contextlib import contextmanager

if TYPE_CHECKING:
    from _typeshed import SupportsWrite

from kptn.runner.events import EventKind, JSONValue, RunEvent, current_task_name
from kptn_server.run_store import RunStore, RunStoreError

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


class RunStoreSink:
    """Persist runner events, captured output, and warnings into a run store.

    Satisfies the :class:`~kptn.runner.events.EventSink` protocol, so it can be
    handed straight to ``kptn.run(..., event_sink=...)``.
    """

    def __init__(self, store: RunStore, run_id: str) -> None:
        self._store = store
        self._run_id = run_id

    @property
    def run_id(self) -> str:
        return self._run_id

    def _append(self, kind: str, **kwargs) -> None:
        durable(lambda: self._store.append_event(self._run_id, kind, **kwargs))

    def emit(self, event: RunEvent) -> None:
        """Translate one runner event into a durable row.

        The store, not the in-process emitter, owns durable sequence numbers --
        that is what lets captured log and warning events interleave with
        runner events in the order they actually happened.
        """
        self._append(
            str(event.kind),
            timestamp=event.timestamp,
            task_name=event.task_name,
            payload=dict(event.payload),
        )

    def emit_log(
        self,
        *,
        stream: str,
        severity: str,
        log_start: int,
        log_end: int,
        task_name: str | None = None,
    ) -> None:
        """Record a span of captured output by its byte offsets in the log."""
        payload: dict[str, JSONValue] = {"stream": stream, "severity": severity}
        self._append(
            EventKind.LOG.value,
            task_name=task_name,
            payload=payload,
            log_start=log_start,
            log_end=log_end,
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
        self._append(
            EventKind.WARNING.value,
            task_name=task_name,
            payload=payload,
        )


# -- raw output capture ---------------------------------------------------


class LogWriteState:
    """Owns the log file, the byte cursor, and the lock that guards both.

    The byte write and the matching durable ``log`` event happen inside a
    single critical section. That is what makes ``log_start``/``log_end``
    monotonic with the durable sequence: without it, two threads could write
    bytes in one order and append events in the other, and every reader that
    slices the log by event offsets would see corruption.
    """

    def __init__(self, sink: RunStoreSink, log_path: Path) -> None:
        self._sink = sink
        self._path = Path(log_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self._path, "ab", buffering=0)
        self._offset = self._handle.seek(0, os.SEEK_END)
        self._lock = threading.RLock()
        self._closed = False
        # Test seam: invoked inside the critical section, after the bytes are
        # written and the cursor advanced but before the durable event is
        # appended -- i.e. exactly at the seam that must not be observable by
        # another thread. Production code never sets it.
        self._critical_section_hook: Callable[[], None] | None = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def offset(self) -> int:
        with self._lock:
            return self._offset

    def write(
        self,
        text: str,
        *,
        stream: str,
        severity: str,
        task_name: str | None,
    ) -> tuple[int, int] | None:
        """Append *text* as UTF-8 and record the span durably.

        Returns the ``(start, end)`` byte offsets, or ``None`` if there was
        nothing to write or the state is already closed.
        """
        data = text.encode("utf-8", errors="replace")
        if not data:
            return None
        with self._lock:
            if self._closed:
                return None
            return self._commit_span(
                data, stream=stream, severity=severity, task_name=task_name
            )

    def lock_is_held(self) -> bool:
        """Whether the calling thread currently owns the critical section."""
        # ``_is_owned`` is the only way to ask an ``RLock`` this question and
        # it is not in typeshed's public surface. It has existed on CPython's
        # RLock since the module was written; the reentrancy guard this
        # answers for is not something a wrapper flag could track correctly.
        return bool(self._lock._is_owned())  # ty: ignore[unresolved-attribute]

    def _commit_span(
        self,
        data: bytes,
        *,
        stream: str,
        severity: str,
        task_name: str | None,
    ) -> tuple[int, int]:
        """Write *data* and append its durable event as one indivisible step.

        The two halves live in this method precisely so they cannot drift
        apart: the caller holds the lock across the whole of it, and the test
        seam fires *between* them, so moving either half out of the critical
        section requires editing this method rather than merely re-indenting a
        line elsewhere.
        """
        start = self._offset
        self._handle.write(data)
        end = start + len(data)
        self._offset = end
        hook, self._critical_section_hook = self._critical_section_hook, None
        if hook is not None:
            hook()
        self._sink.emit_log(
            stream=stream,
            severity=severity,
            log_start=start,
            log_end=end,
            task_name=task_name,
        )
        return start, end

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._handle.close()


class CapturedTextIO(io.TextIOBase):
    """A text stream that persists everything written to it.

    Partial lines are buffered until a newline arrives or the stream is closed,
    so one ``print()`` produces one durable ``log`` event rather than two.
    """

    def __init__(
        self,
        state: LogWriteState,
        *,
        stream: str,
        severity: str,
        original: TextIO | None = None,
    ) -> None:
        self._state = state
        self._stream = stream
        self._severity = severity
        self._original = original
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
        return False

    def fileno(self) -> int:
        # Deliberately unsupported: handing out the real descriptor would let a
        # caller write bytes that bypass the capture layer entirely.
        raise io.UnsupportedOperation("captured streams have no file descriptor")

    # -- writing ---------------------------------------------------------

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError(f"write() requires str, not {type(text).__name__}")
        if not text:
            return 0
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
        """No-op by design.

        ``print(..., flush=True)`` is common, and flushing a partial line would
        split a single logical line across several durable events. Partial
        lines are released on the next newline or when the stream is closed.
        """

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
            self._mirror.write(self.format(record) + "\n")
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


@dataclass
class WorkerCapture:
    """Handles for an active capture context."""

    sink: RunStoreSink
    state: LogWriteState
    stdout: CapturedTextIO
    stderr: CapturedTextIO

    @property
    def log_path(self) -> Path:
        return self.state.path


@contextmanager
def capture_worker_output(
    store: RunStore,
    run_id: str,
    log_path: Path | str,
) -> Iterator[WorkerCapture]:
    """Capture stdout, stderr, ``warnings``, and ``logging`` for a worker run.

    Streams, the root logging handler, and ``warnings.showwarning`` are all
    restored on the way out, including when the body raises.
    """
    sink = RunStoreSink(store, run_id)
    state = LogWriteState(sink, Path(log_path))

    original_stdout: TextIO = sys.stdout
    original_stderr: TextIO = sys.stderr
    captured_stdout = CapturedTextIO(
        state,
        stream=STREAM_STDOUT,
        severity=SEVERITY_OUTPUT,
        original=original_stdout,
    )
    captured_stderr = CapturedTextIO(
        state,
        stream=STREAM_STDERR,
        severity=SEVERITY_STDERR,
        original=original_stderr,
    )
    handler = StructuredWarningHandler(sink, mirror=captured_stderr)
    root_logger = logging.getLogger()

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
        captured_stderr.write(
            warnings.formatwarning(message, category, filename, lineno, line)
        )
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
            finally:
                state.close()


__all__ = [
    "CapturedTextIO",
    "DurableWriteFailed",
    "LogWriteState",
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
