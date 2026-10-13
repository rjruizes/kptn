"""The files a project's next run starts from: its database and checkpoints.

A pipeline with a DuckDB factory keeps its tables -- and, with ``db: duckdb``,
kptn's own task state -- in one file. A run *continues* from that file: what
it records as up to date is skipped. A task with ``duckdb_checkpoint=True``
copies the file to ``<stem>.<task>.backup<suffix>`` after it runs, and a run
that finds the database gone *restores* the furthest checkpoint whose tasks
are all still up to date (``kptn.runner.checkpoint``). Deleting the database
starts the next run from a checkpoint; deleting the checkpoints too starts it
from nothing.

The UI cannot ask the pipeline where its database is: the factory is the
project's own code, and opening it takes the database's lock -- a page load
must never block a run, or create the file it is about to offer to delete.
So the path comes from the runs themselves: each reports the database it
opened in a ``database`` event, and the newest report is what the app bar
shows. A project that has not run since kptn learned to report it shows
nothing yet.
"""

from __future__ import annotations

import glob
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from kptn_server.run_store import RunStore

#: How many of the newest runs to search for a report. Runs older than the
#: last report do not move the database; the bound keeps a page load cheap
#: in a store full of runs from before the event existed.
_RUNS_SEARCHED = 20


@dataclass(frozen=True)
class DatabaseFile:
    """One file the app bar offers: the database, or one of its checkpoints."""

    name: str
    path: Path
    #: ``database`` or ``checkpoint``.
    kind: str
    #: The task a checkpoint was saved after.
    task: str
    size: str
    modified: str
    #: Sidecar files deleted with it: the database's write-ahead log.
    companions: tuple[Path, ...] = ()


def reported_database(store: RunStore, project_root: Path) -> Path | None:
    """The database the project's newest reporting run opened."""
    for run in store.list_runs(project_root, limit=_RUNS_SEARCHED):
        path = store.database_reported(run.run_id)
        if path:
            return Path(path)
    return None


def database_files(store: RunStore, project_root: Path) -> list[DatabaseFile]:
    """The database and its checkpoints, as they are on disk now.

    Each is listed only while it exists: a deleted database with checkpoints
    left beside it is exactly the state where the next run restores one.
    """
    database = reported_database(store, project_root)
    if database is None:
        return []
    files = []
    if database.is_file():
        files.append(_describe(database, "database", "", (Path(f"{database}.wal"),)))
    pattern = f"{glob.escape(database.stem)}.*.backup{glob.escape(database.suffix)}"
    prefix, suffix = f"{database.stem}.", f".backup{database.suffix}"
    for checkpoint in sorted(database.parent.glob(pattern)):
        if checkpoint.is_file():
            task = checkpoint.name[len(prefix) : -len(suffix)]
            files.append(_describe(checkpoint, "checkpoint", task))
    return files


def _describe(path: Path, kind: str, task: str, companions: tuple[Path, ...] = ()) -> DatabaseFile:
    stat = path.stat()
    return DatabaseFile(
        name=path.name,
        path=path,
        kind=kind,
        task=task,
        size=_size(stat.st_size),
        modified=_when(datetime.fromtimestamp(stat.st_mtime)),
        companions=companions,
    )


def _size(size: float) -> str:
    for unit in ("bytes", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.0f} {unit}" if unit == "bytes" else f"{size:,.1f} {unit}"
        size /= 1024
    return ""  # unreachable


def _when(moment: datetime) -> str:
    clock = moment.strftime("%I:%M %p").lstrip("0")
    if moment.date() == datetime.now().date():
        return f"today {clock}"
    return f"{moment:%b} {moment.day}, {clock}"


def delete_database_file(store: RunStore, project_root: Path, name: str) -> DatabaseFile | None:
    """Delete the listed file called *name*, and its companions.

    Only a file the app bar lists can go -- matched by name against the list
    rebuilt now, never a path from the request. Returns what was deleted,
    or ``None`` when nothing listed has that name.
    """
    for entry in database_files(store, project_root):
        if entry.name == name:
            entry.path.unlink(missing_ok=True)
            for companion in entry.companions:
                companion.unlink(missing_ok=True)
            return entry
    return None


__all__ = ["DatabaseFile", "database_files", "delete_database_file", "reported_database"]
