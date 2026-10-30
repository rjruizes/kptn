"""The sidebar's Runs pane: a profile's runs, newest first, under the day
they were made.

One row per run -- how it went, as the rail's icon; its number, which is
what the UI calls a run (:func:`run_label`); and the time it was made.
Built here from run rows of either kind -- the owner's
:class:`~kptn_server.run_store.RunRecord` or a published
:class:`~kptn_server.run_files.IndexedRun` -- so the owner's pane and a
colleague's read-only one cannot drift.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from kptn_server.run_outline import run_timing

#: How many characters of a run's id name a run that has no number.
SHORT_ID = 7

#: A run's status as the icon the rail draws for a task (``_status.html``).
_ICONS = {
    "succeeded": "succeeded",
    "failed": "failed",
    "errored": "failed",
    "running": "running",
    "queued": "running",
    "stop_requested": "running",
    "stopped": "stopped",
    "interrupted": "interrupted",
}

_WORDS = {
    "succeeded": "Succeeded",
    "failed": "Failed",
    "errored": "Errored",
    "running": "Running",
    "queued": "Queued",
    "stop_requested": "Stopping",
    "stopped": "Stopped",
    "interrupted": "Interrupted",
}


def run_label(run: Any) -> str:
    """What the UI calls *run*: ``#12``, its number in the project's history.

    A colleague's index published before runs were numbered has none, and
    there the run goes by the start of its id instead. The full id is never
    far: it is the run's URL, and the label's tooltip.
    """
    number = getattr(run, "number", None)
    return f"#{number}" if number is not None else run.run_id[:SHORT_ID]


@dataclass(frozen=True)
class RunRow:
    run_id: str
    label: str
    #: The icon's state, and whether a succeeded run warned.
    icon: str
    warned: bool
    #: When it was made, as a clock time.
    time: str
    #: How it went in words, and when -- the row's tooltip.
    title: str
    profile: str | None = None
    #: A colleague's run that has stopped publishing (see ``view.py``).
    not_reporting: bool = False


@dataclass(frozen=True)
class RunDay:
    label: str
    rows: list[RunRow] = field(default_factory=list)


def _day_label(day: date, today: date) -> str:
    if day == today:
        return "Today"
    if day == today - timedelta(days=1):
        return "Yesterday"
    if day.year == today.year:
        return f"{day:%b} {day.day}"
    return f"{day:%b} {day.day}, {day.year}"


def run_days(
    runs: Iterable[Any],
    warnings: Callable[[Any], int],
    *,
    not_reporting: Callable[[Any], bool] | None = None,
    now: datetime | None = None,
) -> list[RunDay]:
    """*runs*, newest first as given, grouped under the day each was made.

    *warnings* says how many warnings a run raised; it is asked only of a
    run that succeeded, the one case where they change the icon.
    """
    today = (now or datetime.now()).astimezone().date()
    days: list[RunDay] = []
    for run in runs:
        made: datetime | None = run.created_at or run.started_at
        local = made.astimezone() if made is not None else None
        label = _day_label(local.date(), today) if local is not None else "Undated"
        if not days or days[-1].label != label:
            days.append(RunDay(label))
        warned = run.status == "succeeded" and warnings(run) > 0
        word = _WORDS.get(run.status, run.status.replace("_", " ").capitalize())
        timing = run_timing(run)
        days[-1].rows.append(
            RunRow(
                run_id=run.run_id,
                label=run_label(run),
                icon=_ICONS.get(run.status, "waiting"),
                warned=warned,
                time=local.strftime("%I:%M %p").lstrip("0") if local is not None else "",
                title=f"{word}{' with warnings' if warned else ''}" + (f" · {timing}" if timing else ""),
                profile=run.profile,
                not_reporting=bool(not_reporting and not_reporting(run)),
            )
        )
    return days


__all__ = ["SHORT_ID", "RunDay", "RunRow", "run_days", "run_label"]
