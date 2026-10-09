"""Where each console event goes: which fold it opens, and which it lands in.

The console groups a run's flat event stream into folds -- one per pipeline
or stage a task sits in, and one per task -- so twelve thousand lines of
output read as an outline rather than a wall. The grouping is decided here,
once, and both surfaces use it: the run page feeds a run's whole history
through :class:`ConsoleLayout` to build the tree it renders, and the stream
feeds the same layout one event at a time and tells ``app.js`` exactly where
to put each row. The browser never works out structure for itself, so a
reload and a live page cannot disagree about it.

The rules rely on one fact about the runner: tasks run one at a time, so a
task's output is the unbroken run of events between its ``task_started`` and
its ``task_finished``.

* ``task_started`` and ``task_skipped`` carry the task's enclosing groups in
  ``payload["groups"]``, outermost first (the run's own pipeline is never
  among them). The groups already open are reused as far as they agree with
  that path; the rest are opened. A started task then opens a fold of its
  own; a skipped one -- which prints nothing -- is a plain row in its group.
* Any other event belongs to the task fold at the end of the open chain when
  it names that task, or names no task while that task is still running.
* An event naming a task that has no fold open (output from a task whose
  start was never recorded) opens a bare fold for it where the last task was.
* Anything else with no task goes in the innermost open group; the run's own
  ``run_started`` and ``run_finished`` go at the top, and close every group.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from kptn.runner.events import EventKind
from kptn_server.run_store import StoredEvent

#: The id of the console's top-level list: the parent of anything that sits
#: in no fold. Shared with ``_console.html``.
ROOT_ID = "console-events"

FOLD_GROUP = "group"
FOLD_TASK = "task"

_RUN_KINDS = frozenset({EventKind.RUN_STARTED.value, EventKind.RUN_FINISHED.value})

#: Task folds left open behind the newest one. "Two tasks back" is when a
#: task's output stops being the thing the reader is following.
OPEN_TASKS = 2


@dataclass(eq=False)
class Fold:
    """One collapsible section of the console: a group, or a task."""

    id: str
    kind: str
    key: str
    parent: Fold | None
    #: What the fold holds, in order: folds and prepared event rows. Filled
    #: only when a whole history is built into a tree for the page.
    children: list[Any] = field(default_factory=list)
    #: The task's outcome, from its ``task_finished``; empty while it runs.
    status: str = ""
    #: The prepared ``task_finished`` row, for the fold's heading.
    outcome: dict[str, Any] | None = None
    lines: int = 0
    warnings: int = 0
    failed: bool = False
    open: bool = True

    @property
    def body_id(self) -> str:
        return f"{self.id}-body"

    @property
    def parent_id(self) -> str:
        return self.parent.body_id if self.parent is not None else ROOT_ID

    def ancestors(self) -> list[Fold]:
        chain: list[Fold] = []
        fold: Fold | None = self
        while fold is not None:
            chain.append(fold)
            fold = fold.parent
        return chain


@dataclass(frozen=True)
class Placement:
    """Where one event goes."""

    #: The fold the event's row is appended to; ``None`` for the top level.
    container: Fold | None
    #: Folds this event opens, outermost first. Each is appended to its
    #: parent's body before the row is placed.
    opens: tuple[Fold, ...] = ()
    #: The task fold this event finishes, when it is a ``task_finished``.
    finishes: Fold | None = None

    @property
    def parent_id(self) -> str:
        return self.container.body_id if self.container is not None else ROOT_ID

    @property
    def in_task(self) -> bool:
        """Is the row inside its task's own fold? Then it need not name it."""
        return self.container is not None and self.container.kind == FOLD_TASK


class ConsoleLayout:
    """Places a run's events, in sequence order, into folds."""

    def __init__(self) -> None:
        # The open chain: groups outermost first, then the newest task fold.
        self._chain: list[Fold] = []

    def place(self, event: StoredEvent) -> Placement:
        if event.kind in _RUN_KINDS:
            self._chain = []
            return Placement(container=None)

        if _opens_task_row(event):
            return self._place_task_row(event)

        task = self._task()
        if task is not None and (
            event.task_name == task.key or (event.task_name is None and not task.status)
        ):
            return self._land(event, task)

        groups = self._groups()
        if event.task_name is None:
            return Placement(container=groups[-1] if groups else None)

        # Output from a task with no fold open. It still gets one, next to
        # the last task, rather than spilling its lines among the groups.
        fold = Fold(
            id=f"fold-{event.sequence}-task",
            kind=FOLD_TASK,
            key=event.task_name,
            parent=groups[-1] if groups else None,
        )
        self._chain = [*groups, fold]
        placement = self._land(event, fold)
        return Placement(
            container=placement.container, opens=(fold,), finishes=placement.finishes
        )

    def _task(self) -> Fold | None:
        if self._chain and self._chain[-1].kind == FOLD_TASK:
            return self._chain[-1]
        return None

    def _groups(self) -> list[Fold]:
        return [fold for fold in self._chain if fold.kind == FOLD_GROUP]

    def _place_task_row(self, event: StoredEvent) -> Placement:
        path = _group_path(event)
        current = self._groups()
        kept = 0
        while kept < min(len(path), len(current)) and current[kept].key == path[kept]:
            kept += 1
        chain = current[:kept]
        opens: list[Fold] = []
        for depth in range(kept, len(path)):
            fold = Fold(
                id=f"fold-{event.sequence}-{depth}",
                kind=FOLD_GROUP,
                key=path[depth],
                parent=chain[-1] if chain else None,
            )
            chain.append(fold)
            opens.append(fold)

        container = chain[-1] if chain else None
        if _opens_task_fold(event):
            task = Fold(
                id=f"fold-{event.sequence}-task",
                kind=FOLD_TASK,
                key=str(event.task_name),
                parent=container,
            )
            chain.append(task)
            opens.append(task)
            container = task
        self._chain = chain
        return Placement(container=container, opens=tuple(opens))

    def _land(self, event: StoredEvent, task: Fold) -> Placement:
        if event.kind != EventKind.TASK_FINISHED.value:
            return Placement(container=task)
        task.status = str(event.payload.get("status") or "")
        if task.status == "failed":
            for fold in task.ancestors():
                fold.failed = True
        return Placement(container=task, finishes=task)


def _opens_task_row(event: StoredEvent) -> bool:
    """Is this the event that puts a task in its group?"""
    return event.kind in (EventKind.TASK_STARTED.value, EventKind.TASK_SKIPPED.value)


def _opens_task_fold(event: StoredEvent) -> bool:
    """Does this task get a fold, or only a row in its group?

    A started task gets a fold. A skipped one prints nothing, and the marker a
    mapped task emits before its items has nothing of its own to hold -- each
    item is a task with its own ``task_started``, and its own fold.
    """
    return (
        event.kind == EventKind.TASK_STARTED.value
        and bool(event.task_name)
        and event.payload.get("mode") != "map"
    )


def _group_path(event: StoredEvent) -> list[str]:
    groups = event.payload.get("groups")
    if not isinstance(groups, list):
        return []
    return [str(name) for name in groups if isinstance(name, str) and name]


__all__ = [
    "FOLD_GROUP",
    "FOLD_TASK",
    "OPEN_TASKS",
    "ROOT_ID",
    "ConsoleLayout",
    "Fold",
    "Placement",
]
