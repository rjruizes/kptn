"""Read-only pages for somebody else's project, mounted at ``/view/{slug}``.

Everything here is read from the files the owner's runs publish -- the
project's ``index.json`` and each run's ``.jsonl`` (see
:mod:`kptn_server.run_files`). Nothing here opens the owner's ``ui.db``,
imports their pipeline, or touches a process: their server runs in another
pod, where SQLite cannot be shared and their PIDs mean nothing.

That is enforced by construction rather than by care. These routes are given
a :class:`RequestView`, which has no store, no supervisor and no slot, and
``request.state.ui`` is never set for them -- so a handler from the owner's
routers that somehow ended up here would fail on its first line instead of
quietly opening somebody else's database.

What a reader can and cannot know follows from that:

* Status comes from ``index.json``, which the owner's processes republish on
  every state change and every few seconds while a run is going.
* Liveness cannot be checked. A run whose heartbeat is old is shown as "not
  reporting" and left at that: only the owner's own UI may decide it is dead,
  because only it can look for the process.
* There is nothing to act on, so there are no controls -- no Run, Stop,
  Retry, Plan, or lineage, all of which would need the owner's database or
  code.

The files are somebody else's, so they are untrusted: a run id from the URL
must look like one *and* be listed in the index before it names a file, the
file path is built here and never read out of a file, and every read refuses
symlinks (see :func:`~kptn_server.run_files.read_untrusted`).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Sequence

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates

from kptn.runner.events import EventKind
from kptn_server.console_layout import ConsoleLayout
from kptn_server.log_render import render_events
from kptn_server.registry import ProjectEntry
from kptn_server.routes.runs import (
    HEARTBEAT_INTERVAL_SECONDS,
    _cursor,
    _event_frame,
    _status_frame,
    build_console,
    render_event,
)
from kptn_server.routes.support import error_response
from kptn_server.run_files import (
    RUN_FILE_SUFFIX,
    RUN_ID_RE,
    IndexedRun,
    RunFileError,
    counters,
    published_runs_dir,
    read_index,
    read_untrusted,
)
from kptn_server.run_store import (
    COUNTED_EVENT_KINDS,
    TERMINAL_STATUSES,
    StoredEvent,
    read_run_file,
)

router = APIRouter()

#: How often an open view stream re-reads the run file and the index. Slower
#: than the owner's own stream: every pass is two reads over NFS, and the
#: index it watches for the ending is itself republished every few seconds.
VIEW_POLL_INTERVAL_SECONDS = 1.0

#: A run whose last heartbeat is older than this is shown as not reporting.
#: Far longer than the owner's own grace window, because the two clocks being
#: compared belong to different pods.
STALE_HEARTBEAT_SECONDS = 60.0


@dataclass(frozen=True)
class RequestView:
    """What a read-only page knows about the project it shows."""

    entry: ProjectEntry
    base: str
    project_base: str
    templates: Jinja2Templates


def view(request: Request) -> RequestView:
    return request.state.view


# -- reading what the owner published -------------------------------------


def _runs_dir(entry: ProjectEntry) -> Path:
    return published_runs_dir(entry.root)


def _load_index(entry: ProjectEntry) -> tuple[list[IndexedRun], str | None]:
    """The published runs, and why there are none when that is a problem.

    No index at all is not a problem: the owner has not run anything with a
    kptn that publishes one, and the page says so in its own words.
    """
    try:
        return read_index(_runs_dir(entry)), None
    except FileNotFoundError:
        return [], None
    except RunFileError as exc:
        return [], str(exc)
    except OSError as exc:
        return [], f"cannot read the run index: {exc.strerror or exc}"


def _listed_run(entry: ProjectEntry, run_id: str) -> IndexedRun | None:
    """The run *run_id* names, if it looks like a run id and the index lists it."""
    if not RUN_ID_RE.match(run_id):
        return None
    runs, _ = _load_index(entry)
    return next((run for run in runs if run.run_id == run_id), None)


def _read_run(
    entry: ProjectEntry, run: IndexedRun
) -> tuple[list[StoredEvent], Path, str | None]:
    """The run's events, the file they came from, and any problem reading it.

    A run recorded before run files existed has a ``.log`` of raw output and
    no events anywhere a reader can reach. Its output is shown whole, as a
    single block, rather than not at all.
    """
    runs_dir = _runs_dir(entry)
    path = runs_dir / f"{run.run_id}{RUN_FILE_SUFFIX}"
    try:
        events, _ = read_run_file(path, run.run_id)
        return events, path, None
    except FileNotFoundError:
        pass
    except (RunFileError, OSError) as exc:
        return [], path, str(exc)

    legacy = runs_dir / f"{run.run_id}.log"
    try:
        raw = read_untrusted(legacy)
    except FileNotFoundError:
        return [], path, None
    except (RunFileError, OSError) as exc:
        return [], legacy, str(exc)
    block = StoredEvent(
        run_id=run.run_id,
        sequence=1,
        timestamp=run.created_at or datetime.now(timezone.utc),
        kind=EventKind.LOG.value,
        task_name=None,
        payload={"stream": "stdout", "severity": "output"},
        log_start=None,
        log_end=None,
        text=raw.decode("utf-8", errors="replace"),
    )
    return [block], legacy, None


def _tallies(events: Sequence[StoredEvent]) -> dict[tuple[str, str | None], int]:
    """What :meth:`RunStore.event_counts` would have counted, from the events."""
    tallies: dict[tuple[str, str | None], int] = {}
    for event in events:
        if event.kind not in COUNTED_EVENT_KINDS:
            continue
        status = event.payload.get("status")
        key = (event.kind, status if isinstance(status, str) else None)
        tallies[key] = tallies.get(key, 0) + 1
    return tallies


def not_reporting(run: IndexedRun, *, now: datetime | None = None) -> bool:
    """Has a run that says it is going stopped heartbeating, as far as we can tell?"""
    if run.status in TERMINAL_STATUSES or run.heartbeat_at is None:
        return False
    current = now or datetime.now(timezone.utc)
    return (current - run.heartbeat_at).total_seconds() > STALE_HEARTBEAT_SECONDS


def _no_such_run(request: Request, run_id: str) -> HTMLResponse:
    return error_response(
        request,
        status_code=404,
        title="No such run",
        detail=f"There is no run {run_id!r} in this project's published history.",
    )


# -- pages -------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
def view_history(request: Request) -> HTMLResponse:
    """The owner's run history, as their last publish described it."""
    current = view(request)
    runs, problem = _load_index(current.entry)
    return current.templates.TemplateResponse(
        request,
        "view_runs.html",
        {
            "project": current.entry,
            "project_base": current.project_base,
            "nav_active": "runs",
            "runs": [
                {"run": run, "counters": run.counters, "not_reporting": not_reporting(run)}
                for run in runs
            ],
            "problem": problem,
        },
    )


@router.get("/runs/{run_id}", response_class=HTMLResponse)
def view_run(request: Request, run_id: str) -> HTMLResponse:
    """One run's console, rendered from its file with the owner's own templates."""
    current = view(request)
    run = _listed_run(current.entry, run_id)
    if run is None:
        return _no_such_run(request, run_id)

    stored, path, problem = _read_run(current.entry, run)
    is_terminal = run.status in TERMINAL_STATUSES
    return current.templates.TemplateResponse(
        request,
        "view_run.html",
        {
            "project": current.entry,
            "project_base": current.project_base,
            "nav_active": "run",
            "run": run,
            "items": build_console(stored, path),
            # From the file rather than the index: the file is never behind
            # the index, and the console's own counters are recounted from
            # these same events as the stream appends to them.
            "counters": counters(_tallies(stored)),
            "is_terminal": is_terminal,
            "not_reporting": not_reporting(run),
            "last_sequence": stored[-1].sequence if stored else 0,
            "problem": problem,
        },
    )


@router.get("/runs/{run_id}/log")
def view_run_log(request: Request, run_id: str):
    """The run's log as the CLI printed it, rendered from its file."""
    current = view(request)
    run = _listed_run(current.entry, run_id)
    if run is None:
        return _no_such_run(request, run_id)
    stored, _, problem = _read_run(current.entry, run)
    if problem is not None or not stored:
        return error_response(
            request,
            status_code=404,
            title="This run has no log to download",
            detail=problem or f"Run {run_id} has published no output.",
        )
    return Response(
        content=render_events(stored),
        media_type="text/plain; charset=utf-8",
        headers={"content-disposition": f'attachment; filename="kptn-{run_id}.log"'},
    )


# -- the live stream -----------------------------------------------------------


async def view_frames(
    entry: ProjectEntry,
    templates: Jinja2Templates,
    run_id: str,
    *,
    after: int = 0,
) -> AsyncIterator[str]:
    """SSE frames for a colleague's run, tailing its file from byte to byte.

    The same frames the owner's stream sends, so ``app.js`` cannot tell the
    two apart -- minus the closing region frames, which re-render the
    owner's header from their store.

    The status is read before the file on every pass, as in the owner's
    stream: the index is republished only after the run's last line was
    appended, so a pass that sees a terminal status then reads a file that
    already holds everything.
    """
    path = _runs_dir(entry) / f"{run_id}{RUN_FILE_SUFFIX}"
    offset = 0
    cursor = after
    last_output_at = time.monotonic()
    # The file is read from its start, so the history before the cursor
    # passes through the loop below and primes this as it goes.
    layout = ConsoleLayout()

    while True:
        run = await asyncio.to_thread(_listed_run, entry, run_id)
        if run is None:
            return
        is_terminal = run.status in TERMINAL_STATUSES

        try:
            events, offset = await asyncio.to_thread(
                read_run_file, path, run_id, offset=offset
            )
        except FileNotFoundError:
            events = []
        except (RunFileError, OSError):
            return

        sent = False
        for event in events:
            if event.sequence <= cursor:
                # Already on the page, and placed there: the layout follows
                # it so what comes next lands where the page expects.
                layout.place(event)
                continue
            yield _event_frame(
                render_event(templates, event, path, layout.place(event)),
                event.kind,
                event.sequence,
            )
            cursor = event.sequence
            sent = True

        if is_terminal:
            # Duck-typed: the status fragment reads only fields IndexedRun shares.
            yield _status_frame(templates, run)  # ty: ignore[invalid-argument-type]
            return

        if sent:
            last_output_at = time.monotonic()
        elif time.monotonic() - last_output_at >= HEARTBEAT_INTERVAL_SECONDS:
            yield ": heartbeat\n\n"
            last_output_at = time.monotonic()

        await asyncio.sleep(VIEW_POLL_INTERVAL_SECONDS)


@router.get("/runs/{run_id}/events")
def view_run_events(request: Request, run_id: str, after: str | None = None):
    current = view(request)
    if _listed_run(current.entry, run_id) is None:
        return _no_such_run(request, run_id)
    return StreamingResponse(
        view_frames(
            current.entry, current.templates, run_id, after=_cursor(request, after)
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


__all__ = [
    "RequestView",
    "STALE_HEARTBEAT_SECONDS",
    "VIEW_POLL_INTERVAL_SECONDS",
    "not_reporting",
    "router",
    "view",
    "view_frames",
]
