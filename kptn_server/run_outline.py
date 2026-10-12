"""The run page's sidebar: the pipeline as a column of steps, with the run on it.

A step is one task, or a stage (or nested pipeline) drawn as a card with a row
per branch. Every task shows as a square -- or, for a source or a mapped task,
a square per table or item, added as the run reports them. The sidebar is
built from :func:`kptn.inspection.pipeline_outline` so steps that have not run
yet are there, and then the run's events are applied to it in order, here on
the server: the page renders the result, and the stream re-renders a step's
card whenever an event changes it. The browser only swaps cards in.

Selecting a step or a row filters the console beside it to that step's tasks;
the ``data-tasks`` each one carries is what ``app.js`` filters by.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
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


def _duration(seconds: object) -> str:
    """A finished step's run time; nothing for one too quick to be worth a number."""
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or seconds < 0.05:
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
            self.duration = _duration(event.payload.get("duration_seconds"))


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

    def squares(self, task: _Task) -> list[dict[str, Any]]:
        if task.items:
            return [
                {
                    "label": name.split("[", 1)[1].rstrip("]"),
                    "state": (state := self._settle(item.state)),
                    "warned": item.warned,
                    "title": _title(name.split("[", 1)[1].rstrip("]"), state, item.duration, item.warned),
                }
                for name, item in task.items.items()
            ]
        state = self.state(task)
        return [
            {
                "label": task.name,
                "state": state,
                "warned": task.progress.warned,
                "title": _title(task.name, state, task.progress.duration, task.progress.warned),
            }
        ]

    def view(self, step: OutlineStep) -> dict[str, Any]:
        """Everything ``_outline.html`` needs to draw *step*'s card."""
        rows = []
        for row in step.rows:
            states = [self.state(task) for task in row.tasks]
            rows.append(
                {
                    "key": row.key,
                    "name": row.name,
                    "state": _combine(states) or "waiting",
                    "tasks": " ".join(task.name for task in row.tasks),
                    "squares": [sq for task in row.tasks for sq in self.squares(task)],
                }
            )
        states = [row["state"] for row in rows]
        state = _combine(states) or "waiting"
        if step.group:
            done = sum(1 for s in states if s in ("succeeded", "cached"))
            status = f"{done} of {len(rows)}" if state == "running" else STATE_WORDS.get(state, state)
            duration = ""
        else:
            task = step.rows[0].tasks[0]
            status = STATE_WORDS.get(state, state)
            duration = task.progress.duration if state in ("succeeded", "failed") else ""
        return {
            "key": step.key,
            "name": step.name,
            "group": step.group,
            "ingest": step.ingest,
            "state": state,
            "status": status,
            "duration": duration,
            "tasks": " ".join(task.name for task in step.tasks()),
            "rows": rows,
            # A lone task shows squares only when it has items to show.
            "squares": rows[0]["squares"] if not step.group and step.rows[0].tasks[0].items else [],
        }

    def views(self) -> list[dict[str, Any]]:
        return [self.view(step) for step in self.steps]

    def selected(self) -> str:
        """What the page opens on: the running step, a failure, or everything."""
        if not self.terminal and self.focus:
            return self.focus
        for step in self.steps:
            for row in step.rows:
                if any(self.state(task) == "failed" for task in row.tasks):
                    return row.key
        return "all"


def _title(name: str, state: str, duration: str, warned: bool = False) -> str:
    word = STATE_WORDS.get(state, state)
    title = f"{name}: {word} in {duration}" if duration else f"{name}: {word}"
    return f"{title}, with warnings" if warned else title


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
    """The part of an SSE frame that redraws *step*'s card."""
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
    "outline_frame",
    "prime_outline",
]
