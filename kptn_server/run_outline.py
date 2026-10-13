"""The run page's rail: the pipeline as a tree of tasks, with the run on it.

A step is one task, or a stage (or nested pipeline) with a branch per source
or module beneath it, and the tasks -- a source's tables, a module's tasks --
beneath those. Each task shows how it went and how long it took. The rail is
built from :func:`kptn.inspection.pipeline_outline`, so tasks that have not
run yet are there, and then the run's events are applied to it in order, here
on the server: the page renders the result, and the stream re-renders a
step's part of the rail whenever an event changes it. The browser only swaps
them in.

Every task in the rail carries ``data-unit``, the name its section in the log
is found by; a step's heading carries ``data-units``, every task beneath it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from kptn.runner.events import EventKind

from kptn_server.run_store import StoredEvent

#: Modes of the events a task's items emit: a source's tables, a map's items.
_ITEM_MODES = frozenset({"source_table", "map_item"})

#: Kinds that are part of data ingest, which the sidebar marks with an icon.
_INGEST_KINDS = frozenset({"source", "views"})

#: What each state says beside a step's name.
STATE_WORDS = {
    "succeeded": "done",
    "failed": "failed",
    "cached": "cached",
    "running": "running",
    "waiting": "waiting",
    "not_run": "not run",
    "bypassed": "bypassed",
    "interrupted": "interrupted",
}

_TASK_EVENTS = frozenset(
    {
        EventKind.TASK_STARTED.value,
        EventKind.TASK_SKIPPED.value,
        EventKind.TASK_FINISHED.value,
    }
)


class _Entry(Protocol):
    name: str
    kind: str
    groups: tuple[str, ...]
    bypassed: bool


#: States of a task that did not run in this run, which the rail's "Ran"
#: filter leaves out. ``app.js`` holds the same list.
_NOT_RAN = frozenset({"cached", "waiting", "not_run", "bypassed"})


def format_duration(seconds: object) -> str:
    """A run time, in the units a reader thinks in."""
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
        return ""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


@dataclass
class _Progress:
    """What the run has said about one task or item so far."""

    state: str = ""
    duration: str = ""
    #: Raised a warning -- most often a table with no check file.
    warned: bool = False

    def apply(self, event: StoredEvent) -> None:
        if event.kind == EventKind.TASK_STARTED.value:
            self.state = "running"
        elif event.kind == EventKind.TASK_SKIPPED.value:
            self.state = "cached"
        elif event.kind == EventKind.TASK_FINISHED.value:
            status = str(event.payload.get("status") or "succeeded")
            self.state = "cached" if status == "succeeded" and event.payload.get("cached") else status
            self.duration = format_duration(event.payload.get("duration_seconds"))


@dataclass
class _Task:
    name: str
    kind: str
    bypassed: bool = False
    progress: _Progress = field(default_factory=_Progress)
    #: A source's tables or a map's items, in the order they were reported.
    items: dict[str, _Progress] = field(default_factory=dict)


@dataclass
class OutlineRow:
    """One branch of a stage: a source, or a module pipeline's tasks."""

    key: str
    name: str
    tasks: list[_Task] = field(default_factory=list)


@dataclass
class OutlineStep:
    """One card in the sidebar: a task on its own, or a group of rows."""

    key: str
    name: str
    group: bool
    ingest: bool = False
    rows: list[OutlineRow] = field(default_factory=list)

    def tasks(self) -> list[_Task]:
        return [task for row in self.rows for task in row.tasks]


class RunOutline:
    """A pipeline's steps, and a run's events applied to them in order."""

    def __init__(self, entries: Iterable[_Entry] = ()) -> None:
        self.steps: list[OutlineStep] = []
        self.terminal = False
        #: The row or step holding the task that started most recently.
        self.focus: str | None = None
        self._tasks: dict[str, tuple[_Task, OutlineStep, OutlineRow]] = {}
        self._groups: dict[str, OutlineStep] = {}
        for entry in entries:
            self._add(entry.name, entry.kind, tuple(entry.groups), entry.bypassed)

    # -- building ----------------------------------------------------------

    def _add(
        self, name: str, kind: str, groups: tuple[str, ...], bypassed: bool = False
    ) -> tuple[_Task, OutlineStep, OutlineRow]:
        task = _Task(name=name, kind=kind, bypassed=bypassed)
        if not groups:
            step = OutlineStep(
                key=f"s{len(self.steps)}", name=name, group=False, ingest=kind in _INGEST_KINDS
            )
            row = OutlineRow(key=step.key, name=name, tasks=[task])
            step.rows.append(row)
            self.steps.append(step)
        else:
            step = self._groups.get(groups[0])
            if step is None:
                step = OutlineStep(key=f"s{len(self.steps)}", name=groups[0], group=True)
                self._groups[groups[0]] = step
                self.steps.append(step)
            step.ingest = step.ingest or kind == "source"
            row_name = groups[1] if len(groups) > 1 else name
            row = next((r for r in step.rows if r.name == row_name), None)
            if row is None:
                row = OutlineRow(key=f"{step.key}r{len(step.rows)}", name=row_name)
                step.rows.append(row)
            row.tasks.append(task)
        self._tasks[name] = (task, step, row)
        return self._tasks[name]

    # -- the run -----------------------------------------------------------

    def apply(self, event: StoredEvent) -> OutlineStep | None:
        """Apply one event; return the step whose card it changed, if any."""
        if event.kind == EventKind.RUN_FINISHED.value:
            self.terminal = True
            return None
        if event.kind == EventKind.WARNING.value and event.task_name:
            return self._warn(event.task_name)
        if event.kind not in _TASK_EVENTS or not event.task_name:
            return None
        name = event.task_name
        mode = event.payload.get("mode")
        if mode in _ITEM_MODES and "[" in name:
            parent = name.split("[", 1)[0]
            task, step, row = self._find(parent, "map" if mode == "map_item" else "source", event)
            task.items.setdefault(name, _Progress()).apply(event)
        else:
            task, step, row = self._find(name, str(mode or "python"), event)
            task.progress.apply(event)
        if event.kind == EventKind.TASK_STARTED.value:
            self.focus = row.key
        return step

    def _warn(self, name: str) -> OutlineStep | None:
        found = self._tasks.get(name.split("[", 1)[0])
        if found is None:
            return None
        task, step, _row = found
        progress = task.items.get(name, task.progress) if "[" in name else task.progress
        if progress.warned:
            return None
        progress.warned = True
        return step

    def _find(
        self, name: str, kind: str, event: StoredEvent
    ) -> tuple[_Task, OutlineStep, OutlineRow]:
        found = self._tasks.get(name)
        if found is not None:
            return found
        # A task the outline did not know: the pipeline changed since the
        # run, or it could not be loaded. Placed by the groups its event
        # carries, which is where the runner put it.
        raw = event.payload.get("groups")
        groups = tuple(str(g) for g in raw) if isinstance(raw, list) else ()
        return self._add(name, kind, groups)

    # -- what the page shows ----------------------------------------------

    def state(self, task: _Task) -> str:
        own = task.progress.state
        # A mapped task reports no finish of its own, so its items say how it
        # went. A source does report one; until then it is running.
        if task.items and (own == "" or (own == "running" and task.kind == "map")):
            own = _combine([self._settle(item.state) for item in task.items.values()]) or own
        if not own:
            own = "bypassed" if task.bypassed else "waiting"
        return self._settle(own)

    def _settle(self, state: str) -> str:
        """A state as of now: once the run is over nothing is still going."""
        if not self.terminal:
            return state or "waiting"
        if state in ("", "waiting"):
            return "not_run"
        if state == "running":
            return "interrupted"
        return state

    def _unit(self, name: str, label: str, progress: _Progress, state: str) -> dict[str, Any]:
        return {
            "unit": name,
            "label": label,
            "state": state,
            "duration": progress.duration if state in ("succeeded", "failed") else "",
            "warned": progress.warned,
        }

    def _units(self, task: _Task) -> list[dict[str, Any]]:
        """What the rail lists for *task*: its tables or items, or itself."""
        if task.items:
            return [
                self._unit(name, name.split("[", 1)[1].rstrip("]"), item, self._settle(item.state))
                for name, item in task.items.items()
            ]
        if task.kind in ("source", "map") and self.state(task) != "failed":
            # Its tables or items are not known until it runs. One that
            # failed before any is listed itself: its failure has a section.
            return []
        return [self._unit(task.name, task.name, task.progress, self.state(task))]

    def view(self, step: OutlineStep) -> dict[str, Any]:
        """Everything ``_outline.html`` needs to draw *step*'s part of the rail."""
        branches = []
        for row in step.rows:
            units = [unit for task in row.tasks for unit in self._units(task)]
            alone = (
                len(row.tasks) == 1
                and len(units) == 1
                and units[0]["unit"] == row.tasks[0].name
                and row.name == row.tasks[0].name
            )
            branches.append(
                {
                    # A branch that is one plain task is listed as that task;
                    # a source or a module is a label over its tasks.
                    "label": None if alone else row.name,
                    "state": _combine([self.state(task) for task in row.tasks]) or "waiting",
                    "units": units,
                }
            )
        units = [unit for branch in branches for unit in branch["units"]]
        state = _combine([branch["state"] for branch in branches]) or "waiting"
        lone = not step.group and len(units) == 1 and units[0]["unit"] == step.name
        return {
            "key": step.key,
            "name": step.name,
            "ingest": step.ingest,
            "state": state,
            "warned": any(unit["warned"] for unit in units),
            # A lone task is one line in the rail; anything with tasks
            # beneath it is a heading over them.
            "unit": units[0] if lone else None,
            "units": " ".join(unit["unit"] for unit in units),
            "count": len(units),
            "branches": [] if lone else branches,
        }

    def views(self) -> list[dict[str, Any]]:
        return [self.view(step) for step in self.steps]

    def filter_counts(self) -> dict[str, int]:
        """The rail's filters, which double as the run's tallies: every task
        it lists, those that ran, those cached, and those that warned.
        ``app.js`` recounts the same four off the rail as a live run moves."""
        units = [unit for step in self.steps for task in step.tasks() for unit in self._units(task)]
        return {
            "all": len(units),
            "ran": sum(1 for unit in units if unit["state"] not in _NOT_RAN),
            "cached": sum(1 for unit in units if unit["state"] == "cached"),
            "warned": sum(1 for unit in units if unit["warned"]),
        }

    def selected(self) -> str:
        """The task a finished run opens on: its first failure, if it had one."""
        if not self.terminal:
            return ""
        for step in self.steps:
            for task in step.tasks():
                for unit in self._units(task):
                    if unit["state"] == "failed":
                        return unit["unit"]
        return ""


def _combine(states: list[str]) -> str:
    """One state for several: the most pressing, or what they all share."""
    if not states:
        return ""
    for urgent in ("failed", "interrupted", "running"):
        if urgent in states:
            return urgent
    unique = set(states)
    if len(unique) == 1:
        return states[0]
    finished = {"succeeded", "cached", "bypassed"}
    if unique <= finished:
        return "succeeded"
    if unique & finished:
        # Some done, some still to come.
        return "not_run" if "not_run" in unique else "running"
    return "not_run" if "not_run" in unique else "waiting"


def run_timing(run: Any) -> str:
    """The line under the run bar's title: when the run started, and how long
    it took once it is over. In the server's own time zone, as the log's
    clock is -- the UI is served from the developer's own machine."""
    started: datetime | None = getattr(run, "started_at", None)
    if started is None:
        return ""
    local = started.astimezone()
    clock = local.strftime("%I:%M:%S %p").lstrip("0")
    if local.date() != datetime.now().astimezone().date():
        clock = f"{local:%b} {local.day}, {clock}"
    parts = [f"started {clock}"]
    finished: datetime | None = getattr(run, "finished_at", None)
    if finished is not None:
        parts.append(f"took {format_duration((finished - started).total_seconds())}")
    return " · ".join(parts)


def build_outline(
    entries: Iterable[_Entry], events: Iterable[StoredEvent], *, terminal: bool = False
) -> RunOutline:
    """An outline with every one of *events* applied, as the page renders it."""
    outline = RunOutline(entries)
    for event in events:
        outline.apply(event)
    outline.terminal = outline.terminal or terminal
    return outline


def prime_outline(
    entries: Iterable[_Entry], events: Iterable[StoredEvent], cursor: int
) -> RunOutline:
    """An outline that has applied every event up to *cursor*, for a resumed stream."""
    outline = RunOutline(entries)
    for event in events:
        if event.sequence > cursor:
            break
        outline.apply(event)
    return outline


def outline_frame(templates: Any, outline: RunOutline, step: OutlineStep) -> Mapping[str, Any]:
    """The part of an SSE frame that redraws *step*'s part of the rail."""
    module = templates.get_template("_outline.html").module
    return {
        "id": f"outline-{step.key}",
        "html": str(module.outline_step(outline.view(step))),
        "focus": outline.focus,
    }


__all__ = [
    "STATE_WORDS",
    "OutlineRow",
    "OutlineStep",
    "RunOutline",
    "build_outline",
    "format_duration",
    "outline_frame",
    "prime_outline",
    "run_timing",
]
