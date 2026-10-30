"""Where each console event goes: which section it opens, and which it lands in.

The run log is one continuous scroll of *sections*, one per unit of work --
a task, a source's table, a mapped task's item -- each headed with where it
sits in the pipeline (``raw_ingest / sales /``), how it ran, and how it ended.
Between and around them are the run's own lines: its start, its end, and any
output no task owns. The layout is decided here, once, and both surfaces use
it: the run page feeds a run's whole history through :class:`ConsoleLayout`
to build what it renders, and the stream feeds the same layout one event at
a time and tells ``app.js`` exactly where to put each row. The browser never
works out structure for itself, so a reload and a live page cannot disagree.

The rules rely on one fact about the runner: tasks run one at a time, so a
task's output is the unbroken run of events between its ``task_started`` and
its ``task_finished``.

* ``task_started`` and ``task_skipped`` open the task's section. A source's
  or a mapped task's own start is only a marker: its tables or items open the
  sections, under a path that ends with its name.
* Any other event naming a task lands in that task's section. One naming a
  source or a map with no section -- a failure before any table, output while
  planning -- opens a section for it then.
* An event naming no task lands in the section still running, if there is
  one, and otherwise among the run's own lines.
* A task's start, finish and skip are kept in the page but hidden: the
  section's heading says the same, and the run's counters and the stream's
  resume cursor count every event's row.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from kptn.runner.events import EventKind
from kptn_server.run_store import StoredEvent

#: The id of the log's top-level list: the parent of the run's own lines and
#: of every section. Shared with ``_console.html``.
ROOT_ID = "console-events"

_RUN_KINDS = frozenset({EventKind.RUN_STARTED.value, EventKind.RUN_FINISHED.value})
_OPENING_KINDS = frozenset({EventKind.TASK_STARTED.value, EventKind.TASK_SKIPPED.value})

#: Task modes whose own events only mark where their items start and end.
_MARKER_MODES = frozenset({"map", "source"})
#: Modes of the items a marker task runs.
_ITEM_MODES = frozenset({"source_table", "map_item"})

#: How each mode reads in a section's heading.
_RUNNERS = {
    "python": "python",
    "sql": "sql",
    "r": "r",
    "source_table": "ingest",
    "map_item": "map",
    "views": "views",  # kptn.source_views, in older stored runs
    "source": "ingest",
    "map": "map",
}

#: A section's state, in the words its heading and the counters use.
STATE_WORDS = {
    "succeeded": "Succeeded",
    "failed": "Failed",
    "cached": "Cached",
    "running": "Running",
    "interrupted": "Interrupted",
}


@dataclass(eq=False)
class Section:
    """One unit of work's part of the log: a heading, then its lines."""

    id: str
    #: The task's name as events carry it: ``report``, ``sales[orders]``.
    key: str
    #: The stages, pipelines and source around it, outermost first.
    path: tuple[str, ...] = ()
    #: What the heading calls it: the task, or the table or item.
    label: str = ""
    runner: str = ""
    #: ``succeeded``, ``failed``, ``cached``... -- empty while it runs.
    status: str = ""
    #: The prepared ``task_finished`` row, for the heading's outcome.
    outcome: dict[str, Any] | None = None
    #: The section's rows, in order. Filled only when a whole history is
    #: built for the page.
    children: list[Any] = field(default_factory=list)
    lines: int = 0
    #: Lines the section shows, its output's and any failure's: none, and
    #: there is nothing to fold.
    shown: int = 0
    warnings: int = 0
    open: bool = True
    #: Why a cached section was skipped, for its heading.
    reason: str = ""

    @property
    def body_id(self) -> str:
        return f"{self.id}-lines"

    @property
    def parent_id(self) -> str:
        return ROOT_ID

    @property
    def state(self) -> str:
        return self.status or "running"


@dataclass(frozen=True)
class Placement:
    """Where one event goes."""

    #: The section the event's row is appended to; ``None`` for the run's own
    #: lines at the top level.
    container: Section | None
    #: The section this event opens, if any: appended to the log before the
    #: row is placed in it.
    opens: tuple[Section, ...] = ()
    #: The section this event finishes, when it is a ``task_finished``.
    finishes: Section | None = None
    #: Kept in the page for the counters and the cursor, but not shown.
    hidden: bool = False

    @property
    def parent_id(self) -> str:
        return self.container.body_id if self.container is not None else ROOT_ID

    @property
    def in_task(self) -> bool:
        """Is the row inside its task's own section? Then it need not name it."""
        return self.container is not None


class ConsoleLayout:
    """Places a run's events, in sequence order, into sections."""

    def __init__(self) -> None:
        self._sections: dict[str, Section] = {}
        #: The section still running, which output naming no task belongs to.
        self._current: Section | None = None
        #: Where each marker task (a source, a map) sits, from its start.
        self._marker_paths: dict[str, tuple[str, ...]] = {}
        #: Marker tasks that have opened items; their own outcome is then
        #: already told by their items' sections.
        self._marker_items: set[str] = set()

    def place(self, event: StoredEvent) -> Placement:
        if event.kind in _RUN_KINDS:
            self._current = None
            return Placement(container=None)

        name = event.task_name
        mode = event.payload.get("mode")
        if event.kind in _OPENING_KINDS and name:
            if mode in _MARKER_MODES:
                self._marker_paths[name] = _group_path(event)
                return Placement(container=None, hidden=True)
            return self._open(event, name, mode)

        if name:
            section = self._sections.get(name)
            opens: tuple[Section, ...] = ()
            if section is None:
                base = name.split("[", 1)[0]
                if (
                    name in self._marker_paths
                    and event.kind == EventKind.TASK_FINISHED.value
                    and (event.payload.get("status") != "failed" or name in self._marker_items)
                ):
                    # A source's or a map's own outcome: its items' sections
                    # have said it already.
                    return Placement(container=None, hidden=True)
                # Output from a task with no section yet -- a marker failing
                # before any item, or a task whose start was never recorded.
                section = self._new_section(
                    event,
                    name,
                    path=self._marker_paths.get(name, _group_path(event)),
                    label=name if base == name else name.split("[", 1)[1].rstrip("]"),
                    runner=_RUNNERS.get(str(mode), str(mode or "")),
                )
                opens = (section,)
            return self._land(event, section, opens)

        if self._current is not None and not self._current.status:
            return self._land(event, self._current, ())
        return Placement(container=None)

    def _open(self, event: StoredEvent, name: str, mode: Any) -> Placement:
        if mode in _ITEM_MODES and "[" in name:
            base, item = name.split("[", 1)
            self._marker_items.add(base)
            path = (*self._marker_paths.get(base, _group_path(event)), base)
            label = item.rstrip("]")
        else:
            path = _group_path(event)
            label = name
        section = self._new_section(
            event, name, path=path, label=label, runner=_RUNNERS.get(str(mode), str(mode or ""))
        )
        if event.kind == EventKind.TASK_SKIPPED.value:
            section.status = "cached"
            # A table is skipped when its files are as they were; a task,
            # when nothing it reads or runs has changed.
            section.reason = "source unchanged" if mode == "source_table" else "inputs unchanged"
        else:
            self._current = section
        return Placement(container=section, opens=(section,), hidden=True)

    def _new_section(
        self, event: StoredEvent, name: str, *, path: tuple[str, ...], label: str, runner: str
    ) -> Section:
        section = Section(
            id=f"sec-{event.sequence}", key=name, path=path, label=label, runner=runner
        )
        self._sections[name] = section
        return section

    def _land(self, event: StoredEvent, section: Section, opens: tuple[Section, ...]) -> Placement:
        if event.kind == EventKind.WARNING.value:
            section.warnings += 1
        if event.kind != EventKind.TASK_FINISHED.value:
            return Placement(container=section, opens=opens)
        section.status = str(event.payload.get("status") or "succeeded")
        if self._current is section:
            self._current = None
        # A failure is a line of its own; a success says nothing the heading
        # does not.
        return Placement(
            container=section,
            opens=opens,
            finishes=section,
            hidden=section.status != "failed",
        )


def _group_path(event: StoredEvent) -> tuple[str, ...]:
    groups = event.payload.get("groups")
    if not isinstance(groups, list):
        return ()
    return tuple(str(name) for name in groups if isinstance(name, str) and name)


__all__ = [
    "ROOT_ID",
    "STATE_WORDS",
    "ConsoleLayout",
    "Placement",
    "Section",
]
