"""The run files a colleague's server can read.

Two kinds of file, both under ``<project>/.kptn/runs/``:

``<run_id>.jsonl``
    Every event of one run, one JSON object per line, appended in sequence
    order by :class:`~kptn_server.run_store.RunStore` inside the same
    transaction that commits the event. Captured output travels inline as
    ``text``: this file *is* the run's log, and there is no other. A run's
    console and its download can both be rendered from it alone.

``index.json``
    The project's most recent runs, one summary row each, rewritten whole --
    temp file, then rename -- when a run changes state, and at most every
    :data:`INDEX_PUBLISH_INTERVAL_SECONDS` while one is running.

Why files and not ``ui.db``: everyone's notebook server runs in its own pod,
and SQLite's WAL mode only works when every reader and writer is on one host
-- it coordinates through a memory-mapped ``-shm`` file that a network
filesystem cannot share between machines. Appending lines and renaming whole
files are things NFS does reliably. ``ui.db`` stays the owner's: it holds the
run lock, the state machine, and the byte offsets the owner's console reads
lines by.

Everything read here may have been written by somebody else. Readers open
with ``O_NOFOLLOW``, insist on a regular file, refuse a ``runs`` directory
reached through a symlink, cap what they parse, and never take a path from a
file's contents.
"""

from __future__ import annotations

import json
import os
import re
import stat
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Sequence, cast

from kptn.runner.events import EventKind

if TYPE_CHECKING:
    from kptn_server.run_store import RunRecord

#: A run's event file. Runs recorded before it existed have a ``.log`` of raw
#: captured bytes instead, and every reader here still understands those.
RUN_FILE_SUFFIX = ".jsonl"

INDEX_FILENAME = "index.json"

#: Bumped only for a change an older reader would misread. Readers refuse a
#: schema newer than theirs rather than guess at it.
INDEX_SCHEMA = 1

#: How many runs ``index.json`` lists: the same window the history page shows.
INDEX_LIMIT = 50

#: How stale ``index.json`` may get while a run is going. A state change
#: (created, started, stop requested, finished) always publishes at once.
INDEX_PUBLISH_INTERVAL_SECONDS = 5.0

#: Ceiling on the ``index.json`` a reader will parse. Fifty summary rows are a
#: few tens of kilobytes; anything near this is not an index.
MAX_INDEX_BYTES = 4 * 1024 * 1024

#: What ``RunStore.create_run`` mints (``uuid4().hex``). A run id from a URL
#: must match this before it is used to name anything.
RUN_ID_RE = re.compile(r"\A[0-9a-f]{32}\Z")

_TASK_OUTCOMES = ("succeeded", "failed")


class RunFileError(Exception):
    """A published run file exists but cannot be trusted or understood."""


# -- writing ---------------------------------------------------------------


def encode_event(
    *,
    sequence: int,
    timestamp: str,
    kind: str,
    task_name: str | None,
    payload: Mapping[str, Any],
    text: str | None,
) -> bytes:
    """One event as one line of its run file.

    ``json.dumps`` escapes every control character, newlines included, so the
    only ``\\n`` in the result is the one that ends it -- which is what lets a
    reader split the file on newlines and a writer append without framing.
    """
    record: dict[str, Any] = {"seq": sequence, "ts": timestamp, "kind": kind}
    if task_name is not None:
        record["task"] = task_name
    if payload:
        record["payload"] = dict(payload)
    if text is not None:
        record["text"] = text
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    # ``replace``: a lone surrogate from a pipeline's own output must cost one
    # character, not the durable write it is part of.
    return line.encode("utf-8", errors="replace")


def append_lines(path: Path, lines: Sequence[bytes]) -> tuple[int, list[tuple[int, int]]]:
    """Append *lines* to *path* in one write.

    Returns the file's size before the write -- what :func:`truncate` takes to
    undo it -- and each line's ``(start, end)`` byte span.
    """
    with open(path, "ab", buffering=0) as handle:
        start = handle.seek(0, os.SEEK_END)
        handle.write(b"".join(lines))
    spans: list[tuple[int, int]] = []
    cursor = start
    for line in lines:
        spans.append((cursor, cursor + len(line)))
        cursor += len(line)
    return start, spans


def truncate(path: Path, size: int) -> None:
    """Cut *path* back to *size*, undoing an append whose events were rolled back."""
    with suppress(OSError):
        os.truncate(path, size)


def write_atomically(path: Path, data: bytes) -> None:
    """Replace *path* with *data* so that a reader sees the old file or the new one.

    The temporary name is unique per process and call, so two writers never
    share one; whichever renames last wins, and both renamed a whole file.
    """
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        with open(temporary, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary)
        raise


# -- the index ---------------------------------------------------------------


def counters(tallies: Mapping[tuple[str, str | None], int]) -> dict[str, int]:
    """Task and warning counts for one run.

    *tallies* is what :meth:`~kptn_server.run_store.RunStore.event_counts`
    returns: ``(kind, task status) -> count``. Counted from lifecycle events
    and never from console text -- a task that *prints* the words
    ``task_started`` is output, not a task.

    ``unfinished``
        Tasks that started and never reported an outcome. That is the shape a
        stopped or interrupted run leaves behind, and the number that tells a
        reader where the run stopped being trustworthy. Floored at zero: the
        event log is written by a worker that can be killed mid-sequence, so
        "more finishes than starts" is a corrupt log, not a negative count.

    Only the two outcomes the executor emits are counted as outcomes. An
    unrecognized ``status`` surfaces as ``unfinished`` -- the honest answer
    for a task whose outcome this UI does not understand.

    ``tasks``, ``skipped`` and ``warnings`` are also the console header's
    counters, which ``app.js`` recounts off the DOM as events stream in. One
    function, here, so the history, the console, and ``index.json`` cannot
    disagree about what a task is.
    """
    counted = {
        "tasks": _tally(tallies, EventKind.TASK_STARTED.value),
        "skipped": _tally(tallies, EventKind.TASK_SKIPPED.value),
        "warnings": _tally(tallies, EventKind.WARNING.value),
    }
    for outcome in _TASK_OUTCOMES:
        counted[outcome] = tallies.get((EventKind.TASK_FINISHED.value, outcome), 0)
    counted["unfinished"] = max(
        counted["tasks"] - counted["succeeded"] - counted["failed"], 0
    )
    return counted


def _tally(tallies: Mapping[tuple[str, str | None], int], kind: str) -> int:
    """Every tally for *kind*, whatever status the rows happened to carry.

    Summed rather than read at ``(kind, None)``: nothing stops a ``warning``
    payload from growing a ``status`` field, which would split that kind
    across two grouped rows and silently undercount it.
    """
    return sum(count for (tallied, _status), count in tallies.items() if tallied == kind)


def build_index(
    project_root: Path,
    records: Sequence[RunRecord],
    tallies: Mapping[str, Mapping[tuple[str, str | None], int]],
    *,
    published_at: datetime | None = None,
) -> dict[str, Any]:
    """The ``index.json`` document for *records*, newest first as given.

    Worker PIDs are left out on purpose: they name processes in the owner's
    pod, and a reader in another pod could only misuse them.
    """
    return {
        "schema": INDEX_SCHEMA,
        "project_root": str(project_root),
        "published_at": _iso(published_at or datetime.now(timezone.utc)),
        "runs": [
            {
                "run_id": record.run_id,
                "pipeline": record.pipeline,
                "profile": record.profile,
                "force": record.force,
                "status": record.status,
                "created_at": _iso(record.created_at),
                "started_at": _iso(record.started_at),
                "finished_at": _iso(record.finished_at),
                "heartbeat_at": _iso(record.heartbeat_at),
                "current_task": record.current_task,
                "exit_code": record.exit_code,
                "counters": counters(tallies.get(record.run_id, {})),
            }
            for record in records
        ],
    }


def write_index(runs_dir: Path, document: Mapping[str, Any]) -> None:
    runs_dir.mkdir(parents=True, exist_ok=True)
    data = json.dumps(document, ensure_ascii=False, indent=1).encode("utf-8")
    write_atomically(runs_dir / INDEX_FILENAME, data)


@dataclass(frozen=True)
class IndexedRun:
    """One run as ``index.json`` describes it.

    Attribute names match :class:`~kptn_server.run_store.RunRecord` wherever
    the two overlap, so the templates that render a run row render this too.
    """

    run_id: str
    pipeline: str
    profile: str | None
    force: bool
    status: str
    created_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    heartbeat_at: datetime | None
    current_task: str | None
    exit_code: int | None
    counters: dict[str, int] = field(default_factory=dict)


def read_index(runs_dir: Path) -> list[IndexedRun]:
    """The runs ``index.json`` lists, newest first.

    Raises :class:`FileNotFoundError` when nothing has been published, and
    :class:`RunFileError` for a file that is there but cannot be trusted:
    reached through a symlink, oversized, malformed, or newer than this
    reader. Rows that are individually malformed are dropped rather than
    failing the page.
    """
    data = read_untrusted(runs_dir / INDEX_FILENAME, limit=MAX_INDEX_BYTES)
    try:
        document = json.loads(data)
    except ValueError as exc:
        raise RunFileError(f"{INDEX_FILENAME} is not valid JSON: {exc}") from exc
    document = _object(document)
    if document is None:
        raise RunFileError(f"{INDEX_FILENAME} is not a JSON object")
    schema = document.get("schema")
    if not isinstance(schema, int) or isinstance(schema, bool):
        raise RunFileError(f"{INDEX_FILENAME} has no schema version")
    if schema > INDEX_SCHEMA:
        raise RunFileError(
            f"{INDEX_FILENAME} uses schema {schema}; this kptn reads up to "
            f"{INDEX_SCHEMA}. Upgrade kptn to read it."
        )
    rows = document.get("runs")
    if not isinstance(rows, list):
        raise RunFileError(f"{INDEX_FILENAME} has no list of runs")
    runs = [_indexed_run(row) for row in rows]
    return [run for run in runs if run is not None]


def _indexed_run(value: object) -> IndexedRun | None:
    row = _object(value)
    if row is None:
        return None
    run_id = row.get("run_id")
    status = row.get("status")
    if not isinstance(run_id, str) or not RUN_ID_RE.match(run_id):
        return None
    if not isinstance(status, str):
        return None
    counted = {
        key: count
        for key, count in (_object(row.get("counters")) or {}).items()
        if isinstance(count, int) and not isinstance(count, bool)
    }
    exit_code = row.get("exit_code")
    return IndexedRun(
        run_id=run_id,
        pipeline=_text(row.get("pipeline")) or "",
        profile=_text(row.get("profile")),
        force=row.get("force") is True,
        status=status,
        created_at=_when(row.get("created_at")),
        started_at=_when(row.get("started_at")),
        finished_at=_when(row.get("finished_at")),
        heartbeat_at=_when(row.get("heartbeat_at")),
        current_task=_text(row.get("current_task")),
        exit_code=exit_code if isinstance(exit_code, int) and not isinstance(exit_code, bool) else None,
        counters={
            **{key: 0 for key in ("tasks", "skipped", "warnings", "succeeded", "failed", "unfinished")},
            **counted,
        },
    )


# -- reading a run file ------------------------------------------------------


@dataclass(frozen=True)
class FileEvent:
    """One line of a run file, decoded."""

    sequence: int
    timestamp: datetime
    kind: str
    task_name: str | None
    payload: Mapping[str, Any]
    text: str | None


def read_events(path: Path, *, offset: int = 0) -> tuple[list[FileEvent], int]:
    """The events from byte *offset* up to the last complete line.

    Returns them with the offset just past that line, which is where the
    next call should resume. A line still being written has no newline yet
    and is left for that next call. Lines that do not decode are skipped:
    one damaged line costs that event, not the run.
    """
    data = read_untrusted(path, offset=offset)
    end = data.rfind(b"\n")
    if end == -1:
        return [], offset
    events = [
        event
        for event in (_decode_line(line) for line in data[: end + 1].split(b"\n"))
        if event is not None
    ]
    return events, offset + end + 1


def read_span_text(path: Path, start: int, end: int) -> str:
    """The captured text of one ``log`` event, by the span ``ui.db`` recorded.

    Both formats: a ``.jsonl`` span is one whole line whose ``text`` is the
    output; a legacy ``.log`` span is the raw bytes themselves. A file that
    is gone, or shorter than the span, gives ``""`` -- a degraded console,
    never a failed page.
    """
    if end <= start:
        return ""
    try:
        with open(path, "rb") as handle:
            handle.seek(start)
            raw = handle.read(end - start)
    except OSError:
        return ""
    if path.suffix != RUN_FILE_SUFFIX:
        return raw.decode("utf-8", errors="replace")
    event = _decode_line(raw.rstrip(b"\n"))
    if event is None or event.text is None:
        return ""
    return event.text


def run_file_text(path: Path) -> str:
    """All the captured output in a run file, concatenated in order."""
    events, _ = read_events(path)
    return "".join(event.text for event in events if event.text is not None)


def _decode_line(line: bytes) -> FileEvent | None:
    if not line.strip():
        return None
    try:
        record = _object(json.loads(line))
    except ValueError:
        return None
    if record is None:
        return None
    sequence = record.get("seq")
    kind = record.get("kind")
    timestamp = _when(record.get("ts"))
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        return None
    if not isinstance(kind, str) or timestamp is None:
        return None
    text = record.get("text")
    return FileEvent(
        sequence=sequence,
        timestamp=timestamp,
        kind=kind,
        task_name=_text(record.get("task")),
        payload=_object(record.get("payload")) or {},
        text=text if isinstance(text, str) else None,
    )


# -- untrusted files ---------------------------------------------------------


def published_runs_dir(project_root: Path) -> Path:
    """``<project_root>/.kptn/runs``, refusing one reached through a symlink.

    *project_root* must already be resolved (the registry resolves every
    root). A ``.kptn`` or ``runs`` that is a link could point anywhere the
    *reader* can see -- their own pod's home directory included -- so the
    path has to be the literal one.
    """
    runs_dir = project_root / ".kptn" / "runs"
    if os.path.realpath(runs_dir) != str(runs_dir):
        raise RunFileError(f"{runs_dir} is reached through a symlink")
    return runs_dir


def read_untrusted(path: Path, *, offset: int = 0, limit: int | None = None) -> bytes:
    """The bytes of *path* from *offset*, if it is a regular file and not a link.

    Raises :class:`FileNotFoundError` for a file that is not there and
    :class:`RunFileError` for everything else that makes it unreadable.
    """
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise RunFileError(f"cannot open {path.name}: {exc.strerror or exc}") from exc
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise RunFileError(f"{path.name} is not a regular file")
        if limit is not None and info.st_size - offset > limit:
            raise RunFileError(f"{path.name} is larger than {limit} bytes")
        handle.seek(offset)
        return handle.read()


def _object(value: object) -> dict[str, Any] | None:
    """*value* as a JSON object, or ``None``. JSON object keys are always strings."""
    return cast(dict[str, Any], value) if isinstance(value, dict) else None


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _when(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _text(value: object) -> str | None:
    return value if isinstance(value, str) else None


__all__ = [
    "FileEvent",
    "INDEX_FILENAME",
    "INDEX_LIMIT",
    "INDEX_PUBLISH_INTERVAL_SECONDS",
    "INDEX_SCHEMA",
    "IndexedRun",
    "RUN_FILE_SUFFIX",
    "RUN_ID_RE",
    "RunFileError",
    "append_lines",
    "build_index",
    "counters",
    "encode_event",
    "published_runs_dir",
    "read_events",
    "read_index",
    "read_span_text",
    "read_untrusted",
    "run_file_text",
    "truncate",
    "write_atomically",
    "write_index",
]
