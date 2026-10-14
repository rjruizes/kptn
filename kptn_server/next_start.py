"""What the next run starts from: the state card in the sidebar's Next run pane.

A pipeline with a DuckDB factory keeps its tables -- and, with ``db: duckdb``,
kptn's own task state -- in one file. A run *continues* from that file,
skipping what it records as up to date. A task with ``duckdb_checkpoint=True``
copies the file to ``<stem>.<task>.backup<suffix>`` after it runs, and a run
that finds the database gone *restores* the furthest checkpoint whose tasks
are all still up to date. With neither, the run starts fresh.

The card says which of the three the next run gets, from kptn itself:
:func:`kptn.runner.checkpoint.starting_point` makes the runner's restore
decision without its side effects. Its trash button deletes that file, and
the card is read again: the next starting point is kptn's answer, never a
guess here.

The UI cannot ask the pipeline where its database is: the factory is the
project's own code, and calling it would open -- or create -- the file and
take its lock. So the path comes from the runs: each reports the database it
opened in a ``database`` event, and the newest report is the one used. The
files are only ever opened ``read_only``, and never while a run is in
progress: DuckDB lets a writer in only when no other process has the file
open.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from kptn.graph.nodes import ConfigNode
from kptn.graph.topo import topo_sort
from kptn.runner.checkpoint import starting_point
from kptn_server.project import ProjectContext, resolve_graph
from kptn_server.run_store import RunStore

#: How many of the newest runs to search for a report. Runs older than the
#: last report do not move the database; the bound keeps a lookup cheap in
#: a store full of runs from before the event existed.
_RUNS_SEARCHED = 20


@dataclass(frozen=True)
class NextStart:
    """Everything the state card shows."""

    #: ``database`` or ``checkpoint`` -- or ``busy`` (a run has the database
    #: open) and ``unreadable`` (something else does).
    state: str
    label: str
    name: str
    #: Why the card cannot say more, in the states where it cannot.
    facts: str = ""
    #: The file the trash button deletes, by name; empty for no button.
    file: str = ""
    #: The same file, in the words the button's label uses.
    what: str = ""
    path: Path | None = None


def reported_database(store: RunStore, project_root: Path) -> Path | None:
    """The database the project's newest reporting run opened."""
    for run in store.list_runs(project_root, limit=_RUNS_SEARCHED):
        path = store.database_reported(run.run_id)
        if path:
            return Path(path)
    return None


def uses_duckdb(project: ProjectContext) -> bool:
    """Does the pipeline declare ``kptn.config(duckdb=...)``? Without one
    there is no database for a run to continue from, and no card."""
    return any(isinstance(node, ConfigNode) and "duckdb" in node.spec for node in project.pipeline.nodes)


def next_start(
    project: ProjectContext, store: RunStore, profile: str | None
) -> NextStart | None:
    """What the next run of *profile* starts from, or ``None`` for no card.

    No card, too, when there is nothing to start from: no run has reported a
    database yet, or there is neither a database nor a checkpoint -- the
    next run starts fresh, and the pane shows Run and Plan alone.
    """
    if not uses_duckdb(project):
        return None
    database = reported_database(store, project.root)
    if database is None:
        return None
    if store.active_run(project.root) is not None:
        return NextStart("busy", "Starts from", database.name, "Known when this run ends.")

    resolved = resolve_graph(project, profile)
    try:
        start = starting_point(
            topo_sort(resolved.graph), resolved.storage_key, resolved.pipeline, database
        )
    except duckdb.Error:
        return NextStart(
            "unreadable",
            "Starts from",
            database.name,
            "Open in another process, so it cannot be read.",
        )

    if start.kind == "fresh":
        return None
    if start.kind == "checkpoint":
        return NextStart(
            "checkpoint",
            "Restores",
            f"{start.task} checkpoint",
            file=start.path.name if start.path else "",
            what=f"the {start.task} checkpoint",
            path=start.path,
        )
    return NextStart(
        "database",
        "Continues from",
        database.name,
        file=database.name,
        what=database.name,
        path=database,
    )


@dataclass(frozen=True)
class StateSource:
    """Where the next run reads its task state from, for the plan page."""

    #: The file to read; ``None`` when there is none and nothing is cached.
    path: Path | None
    #: What the plan page tells the reader about it.
    note: str


def state_source(project: ProjectContext, store: RunStore, profile: str | None) -> StateSource:
    """The file a pipeline with a DuckDB factory keeps its task state in --
    the runner always uses the factory's database when there is one -- as
    the next run would find it: the database, or the checkpoint it would
    restore. Opened by the caller ``read_only``; raises :class:`duckdb.Error`
    when the database cannot be read (another process has it open).
    """
    database = reported_database(store, project.root)
    if database is None:
        return StateSource(
            None,
            "No run has reported where this pipeline's database is yet, so nothing shows "
            "as cached. Run the pipeline once and the plan reads its task state.",
        )
    resolved = resolve_graph(project, profile)
    start = starting_point(topo_sort(resolved.graph), resolved.storage_key, resolved.pipeline, database)
    if start.kind == "database":
        return StateSource(database, f"Task state read from {database.name}.")
    if start.kind == "checkpoint":
        return StateSource(
            start.path,
            f"Task state read from the {start.task} checkpoint, which the next run restores.",
        )
    return StateSource(None, f"No {database.name} and no usable checkpoint: every task runs.")


def delete_start(project: ProjectContext, store: RunStore, profile: str | None, name: str) -> bool:
    """Delete the file the next run starts from, if it is still *name*.

    The name the card showed is checked against kptn's answer now, so a
    second click in a stale tab cannot delete whatever has taken its place.
    A database goes with its write-ahead log. Returns whether it deleted.
    """
    current = next_start(project, store, profile)
    if current is None or not current.file or current.file != name or current.path is None:
        return False
    current.path.unlink(missing_ok=True)
    if current.state == "database":
        Path(f"{current.path}.wal").unlink(missing_ok=True)
    return True


__all__ = [
    "NextStart",
    "StateSource",
    "delete_start",
    "next_start",
    "reported_database",
    "state_source",
    "uses_duckdb",
]
