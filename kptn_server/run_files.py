"""The run files a colleague's server can read.

Two kinds of file, both under ``<project>/.kptn/state/runs/`` -- or, when
``kptn.yaml`` sets ``shared_logs`` and that directory exists, under
``<shared_logs>/<working directory name>/``, where they outlive the working
directory (see :func:`runs_dir_for`):

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

from kptn.exceptions import ProfileError
from kptn.layout import LEGACY_RUNS_DIR, RUNS_DIR
from kptn.profiles.loader import ProfileLoader
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

#: Where run files go when ``kptn.yaml`` names no ``shared_logs``.
LOCAL_RUNS_DIR = RUNS_DIR

#: Explicit modes for shared run logs, whatever the umask. Every account
#: creates its own subdirectory in the root, which whoever set it up made
#: group-writable; only the owner writes inside their own.
LOG_DIR_MODE = 0o2755
LOG_FILE_MODE = 0o644


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


def write_atomically(path: Path, data: bytes, *, mode: int | None = None) -> None:
    """Replace *path* with *data* so that a reader sees the old file or the new one.

    The temporary name is unique per process and call, so two writers never
    share one; whichever renames last wins, and both renamed a whole file.
    *mode*, when given, is set on the file before it is renamed into place.
    """
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        with open(temporary, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
            if mode is not None:
                os.fchmod(handle.fileno(), mode)
        os.replace(temporary, path)
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary)
        raise


# -- where run files live ----------------------------------------------------


def shared_logs_root(project_root: Path, setting: str | None) -> Path | None:
    """The shared logs root *setting* names, resolved; None when unset.

    Relative to *project_root*, the directory holding ``kptn.yaml``, as
    ``shared_bronze`` is. Touches nothing beyond ``resolve()``.
    """
    if setting is None:
        return None
    root = Path(setting).expanduser()
    if not root.is_absolute():
        root = project_root / root
    return root.resolve()


def runs_dir_of(project_root: Path, setting: str | None) -> Path:
    """Where *project_root*'s run files go under the ``shared_logs`` *setting*.

    The shared logs directory is used only where it exists: one ``kptn.yaml``
    serves a server where it was set up and a laptop where it never is.
    For a reader: creates nothing, unlike :func:`runs_dir_for`.
    """
    shared = shared_logs_root(project_root, setting)
    if shared is None or not shared.is_dir():
        return local_runs_dir(project_root)
    return shared / project_root.name


def local_runs_dir(project_root: Path) -> Path:
    """``.kptn/state/runs``, or ``.kptn/runs`` where only that exists.

    A working directory no kptn has run in since its run files moved to
    ``.kptn/state/`` still has them in ``.kptn/runs``. Its owner's next run
    moves them; a colleague's server only reads, so it reads them there.
    """
    runs = project_root / LOCAL_RUNS_DIR
    legacy = project_root / LEGACY_RUNS_DIR
    if not runs.is_dir() and legacy.is_dir():
        return legacy
    return runs


def runs_dir_for(project_root: Path) -> Path:
    """Where the next recorded run of *project_root* writes its files.

    ``<shared_logs>/<working directory name>`` when ``kptn.yaml`` sets
    ``shared_logs`` and that directory exists, else ``.kptn/state/runs``;
    created either way. The shared root itself is never created: whoever sets it up
    gives it the pipeline group, and its existence is what moves the logs.

    A ``kptn.yaml`` that does not load falls back to ``.kptn/state/runs``: the run
    fails on that same error when it loads its profile, and that failure is
    logged where its owner can read it.
    """
    shared = shared_logs_root(project_root, _shared_logs_setting(project_root))
    if shared is None or not shared.is_dir():
        runs = project_root / LOCAL_RUNS_DIR
        runs.mkdir(parents=True, exist_ok=True)
        return runs
    runs = shared / project_root.name
    _mkdir(runs, LOG_DIR_MODE)
    return runs


def missing_shared_logs(project_root: Path) -> str | None:
    """What to tell a recorded run whose ``kptn.yaml`` names a shared logs
    directory that does not exist, or None when there is nothing to say."""
    setting = _shared_logs_setting(project_root)
    shared = shared_logs_root(project_root, setting)
    if shared is None or shared.is_dir():
        return None
    return f"[LOGS] {setting} does not exist, so this run's log stays in {LOCAL_RUNS_DIR}"


def _shared_logs_setting(project_root: Path) -> str | None:
    try:
        return ProfileLoader.load(project_root / "kptn.yaml").settings.shared_logs
    except ProfileError:
        return None


def _mkdir(path: Path, mode: int) -> None:
    """Create one directory with *mode*, whatever the umask. One that already
    exists, perhaps another account's, is left as it is: chmod on it would
    fail."""
    try:
        path.mkdir()
    except FileExistsError:
        return
    os.chmod(path, mode)


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
    carried: Sequence[IndexedRun] = (),
    published_at: datetime | None = None,
) -> dict[str, Any]:
    """The ``index.json`` document for *records* and *carried*, newest first.

    *carried* are rows an earlier index listed for runs this database never
    stored -- the runs of a working directory that was deleted and recreated
    under the same name, whose logs stay in one shared logs directory. They
    are written back as they were read; nothing here can know more about
    them. At most :data:`INDEX_LIMIT` rows are kept, by ``created_at``.

    Worker PIDs are left out on purpose: they name processes in the owner's
    pod, and a reader in another pod could only misuse them.
    """
    rows: list[tuple[datetime | None, dict[str, Any]]] = [
        (
            record.created_at,
            {
                "run_id": record.run_id,
                "number": record.number,
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
            },
        )
        for record in records
    ]
    rows.extend((run.created_at, _indexed_row(run)) for run in carried)
    # Stable, so records keep the database's order among equal times.
    rows.sort(key=lambda pair: _sortable(pair[0]), reverse=True)
    return {
        "schema": INDEX_SCHEMA,
        "project_root": str(project_root),
        "published_at": _iso(published_at or datetime.now(timezone.utc)),
        "runs": [row for _created, row in rows[:INDEX_LIMIT]],
    }


def _indexed_row(run: IndexedRun) -> dict[str, Any]:
    """*run* as the index row it was read from: the inverse of ``_indexed_run``."""
    return {
        "run_id": run.run_id,
        "number": run.number,
        "pipeline": run.pipeline,
        "profile": run.profile,
        "force": run.force,
        "status": run.status,
        "created_at": _iso(run.created_at),
        "started_at": _iso(run.started_at),
        "finished_at": _iso(run.finished_at),
        "heartbeat_at": _iso(run.heartbeat_at),
        "current_task": run.current_task,
        "exit_code": run.exit_code,
        "counters": dict(run.counters),
    }


def _sortable(value: datetime | None) -> datetime:
    """*value* comparable with any other: aware, and a missing time oldest."""
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def write_index(runs_dir: Path, document: Mapping[str, Any]) -> None:
    runs_dir.mkdir(parents=True, exist_ok=True)
    data = json.dumps(document, ensure_ascii=False, indent=1).encode("utf-8")
    write_atomically(runs_dir / INDEX_FILENAME, data, mode=LOG_FILE_MODE)


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
    #: ``None`` in an index published before runs were numbered.
    number: int | None = None


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
    number = row.get("number")
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
        number=number if isinstance(number, int) and not isinstance(number, bool) and number > 0 else None,
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


def published_runs_dir(runs_dir: Path) -> Path:
    """*runs_dir*, refusing one reached through a symlink.

    *runs_dir* must already be resolved up to the directory the project
    names: the registry resolves every root, and a shared logs root. A
    ``.kptn``, ``runs`` or per-project logs directory that is a link could
    point anywhere the *reader* can see -- their own pod's home directory
    included -- so the path has to be the literal one.
    """
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
    "LOCAL_RUNS_DIR",
    "LOG_DIR_MODE",
    "LOG_FILE_MODE",
    "RUN_FILE_SUFFIX",
    "RUN_ID_RE",
    "RunFileError",
    "append_lines",
    "build_index",
    "counters",
    "encode_event",
    "missing_shared_logs",
    "published_runs_dir",
    "read_events",
    "read_index",
    "read_span_text",
    "read_untrusted",
    "run_file_text",
    "local_runs_dir",
    "runs_dir_for",
    "runs_dir_of",
    "shared_logs_root",
    "truncate",
    "write_atomically",
    "write_index",
]
