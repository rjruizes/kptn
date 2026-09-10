"""Starting runs, the run page, and the resumable event stream.

Three endpoints, one idea: **nothing about a run lives in this process.**

``POST /runs``
    Validates the profile, creates the run row and the project lock, and only
    then asks the supervisor for a worker. The lock is what makes "one active
    run per project" true, and it is taken by
    :meth:`~kptn_server.run_store.RunStore.create_run` inside the same
    transaction as the row, so a second request is *rejected* (409) rather
    than queued. A launch that fails finishes the run immediately -- nothing
    else ever will, and an unfinished run holds the project's lock forever.

``GET /runs/{run_id}``
    Renders the run's whole event history from the store, with
    ``data-sequence`` / ``id="event-N"`` anchors so the browser knows where it
    is and can resume from there.

``GET /runs/{run_id}/events``
    Server-sent events, read from persisted cursors only. The cursor comes
    from the client on every connection -- ``Last-Event-ID`` on a reconnect,
    ``?after=`` on a first connection -- and the events come from
    :meth:`~kptn_server.run_store.RunStore.events_after`. A worker therefore
    survives the browser closing, VS Code quitting, and this server
    restarting: the new connection just resumes from the number the page
    already has.

Two decisions worth stating outright.

**Terminal state is read from the run's status, never inferred from events.**
:meth:`~kptn_server.processes.RunProcessManager.reconcile` settles a run whose
worker a reboot killed by writing ``interrupted`` and appending *no* event. A
stream that closed on ``run_finished`` would hold that connection open
forever, and a page that derived its state from the event log would show the
run as still going. So every stream reads the status, and closes with a
``run_status`` frame carrying the rendered status fragment.

**Log text is read server-side and rendered escaped.** A ``log`` event carries
``stream``, ``severity``, and byte offsets into the run's log file -- no
inline text (see :mod:`kptn_server.capture`). The text is therefore sliced out
of the log file here and rendered through Jinja, whose autoescaping is on. The
browser only ever appends the fragment it is given; pipeline output is
untrusted text and must never reach the DOM as live markup.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Mapping

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from kptn.runner.events import EventKind
from kptn_server.run_store import (
    STATUS_FAILED,
    TERMINAL_STATUSES,
    ActiveRunError,
    RunRequest,
    RunStore,
    StoredEvent,
)

_LOGGER = logging.getLogger(__name__)

router = APIRouter()

#: How often an open stream asks the store for new events. Short enough that
#: the console feels live, long enough that an idle connection is not a busy
#: loop against SQLite.
POLL_INTERVAL_SECONDS = 0.1

#: How often an idle stream sends an SSE comment. Proxies and sleeping laptops
#: drop connections that say nothing, and a dropped connection costs a
#: reconnect the client has to resume by hand.
HEARTBEAT_INTERVAL_SECONDS = 15.0

#: Ceiling on the log text rendered for a single ``log`` event. A task that
#: prints a megabyte in one write should not put a megabyte into one DOM node
#: (or one SSE frame).
MAX_LOG_SLICE_BYTES = 64 * 1024

_TRUNCATION_NOTE = "\n[... truncated by the console ...]"

#: The SSE event name used for the run's status. Deliberately not an
#: ``EventKind``: no such event exists in the store, because status changes
#: are not always events (``reconcile()`` writes one without appending
#: anything). Sent with no ``id:`` field, so it never becomes the client's
#: resume cursor.
STATUS_EVENT_NAME = "run_status"


# -- injectable clock ------------------------------------------------------


async def _sleep(delay: float) -> None:
    """Indirection so tests can drive the stream's cadence.

    A test replaces this with a fake that advances a fake clock, so a
    16-second idle stream is observed without waiting 16 seconds.
    """
    await asyncio.sleep(delay)


def _monotonic() -> float:
    """Clock the heartbeat measures idleness against. Replaced in tests."""
    return time.monotonic()


# -- log text --------------------------------------------------------------


def log_text(log_path: Path, event: StoredEvent) -> str:
    """The text for one ``log`` event.

    Two shapes exist in the wild and both are handled here:

    * the capture layer's, which writes bytes to the run's log file and stores
      only ``log_start``/``log_end``; and
    * the executor's, which emits a subprocess's captured ``stdout``/``stderr``
      inline as ``payload["message"]``.

    Returns ``""`` when there is nothing to show -- a missing or truncated log
    file is a degraded console, never a 500 on the run page.
    """
    inline = event.payload.get("message")
    if isinstance(inline, str) and inline:
        return _truncate(inline)

    start, end = event.log_start, event.log_end
    if start is None or end is None or end <= start:
        return ""

    length = min(end - start, MAX_LOG_SLICE_BYTES)
    try:
        with open(log_path, "rb") as handle:
            handle.seek(start)
            data = handle.read(length)
    except OSError:
        # The log file is the worker's, not ours: it can be missing, rotated,
        # or on a volume that just went away.
        _LOGGER.debug("could not read log slice for %s", log_path, exc_info=True)
        return ""

    text = data.decode("utf-8", errors="replace")
    if end - start > length:
        text += _TRUNCATION_NOTE
    return text


def _truncate(text: str) -> str:
    if len(text) <= MAX_LOG_SLICE_BYTES:
        return text
    return text[:MAX_LOG_SLICE_BYTES] + _TRUNCATION_NOTE


# -- one event, prepared for a template -----------------------------------


def _summary(event: StoredEvent) -> str:
    """A one-line human summary of a non-log event, from its payload."""
    payload: Mapping[str, Any] = event.payload
    if event.kind == EventKind.WARNING.value:
        return str(payload.get("message", ""))
    if event.kind == EventKind.TASK_SKIPPED.value:
        return "cached"
    if event.kind in (EventKind.TASK_FINISHED.value, EventKind.RUN_FINISHED.value):
        parts = [str(payload[key]) for key in ("status", "error") if payload.get(key)]
        duration = payload.get("duration_seconds")
        if isinstance(duration, (int, float)):
            parts.append(f"{duration:.2f}s")
        return " ".join(parts)
    if event.kind == EventKind.TASK_STARTED.value and payload.get("mode"):
        return str(payload["mode"])
    return ""


def console_event(event: StoredEvent, log_path: Path) -> dict[str, Any]:
    """Everything the event template needs, and nothing it has to compute.

    Text is resolved here rather than in the template so that the run page and
    the SSE stream render byte-identical fragments -- the browser appends what
    a reload would have rendered.
    """
    is_log = event.kind == EventKind.LOG.value
    return {
        "sequence": event.sequence,
        "kind": event.kind,
        "label": event.kind.replace("_", " "),
        "task": event.task_name,
        "timestamp": _isoformat(event.timestamp),
        "clock": event.timestamp.strftime("%H:%M:%S"),
        "severity": str(event.payload.get("severity") or "") if is_log else "",
        "text": log_text(log_path, event) if is_log else _summary(event),
    }


def _isoformat(value: datetime) -> str:
    return value.isoformat()


def render_event(
    templates: Jinja2Templates, event: StoredEvent, log_path: Path
) -> dict[str, Any]:
    """The JSON payload of one SSE frame.

    ``html`` is the same fragment the run page renders for this event, already
    escaped by Jinja. The client appends it verbatim and formats nothing
    itself, which is what keeps every rendering decision on the server -- and
    keeps untrusted log text out of the browser's markup parser as anything
    but text.
    """
    prepared = console_event(event, log_path)
    macro = templates.get_template("_event.html").module.event_row
    return {
        "sequence": prepared["sequence"],
        "kind": prepared["kind"],
        "task": prepared["task"],
        "html": str(macro(prepared)),
    }


def _event_frame(payload: Mapping[str, Any], kind: str, sequence: int) -> str:
    # json.dumps escapes newlines, which matters: SSE has no escaping of its
    # own, so a raw newline inside a data field would split the frame.
    return f"id: {sequence}\nevent: {kind}\ndata: {json.dumps(payload)}\n\n"


def _status_payload(templates: Jinja2Templates, record) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "status": record.status,
        "terminal": record.status in TERMINAL_STATUSES,
        "exit_code": record.exit_code,
        "current_task": record.current_task,
        "html": templates.get_template("_run_status.html").render(
            run=record, is_terminal=record.status in TERMINAL_STATUSES
        ),
    }


def _status_frame(templates: Jinja2Templates, record) -> str:
    # No ``id:`` field on purpose: this frame is not a stored event, and
    # letting it become the client's ``Last-Event-ID`` would corrupt the
    # resume cursor.
    return (
        f"event: {STATUS_EVENT_NAME}\n"
        f"data: {json.dumps(_status_payload(templates, record))}\n\n"
    )


# -- POST /runs ------------------------------------------------------------


@router.post("/runs")
async def start_run(request: Request):
    """Create a run, take the project lock, and launch its worker."""
    project = request.app.state.project
    store: RunStore = request.app.state.store
    manager = request.app.state.processes

    profile = await _submitted_profile(request) or None

    if profile is not None and profile not in project.profiles:
        # Rejected before anything is created: an invalid request must not
        # leave a run row behind, and above all must not take the lock.
        return _error_response(
            request,
            status_code=400,
            title="Unknown profile",
            detail=(
                f"{profile!r} is not a profile of this project. "
                f"Declared profiles: {', '.join(project.profiles) or 'none'}."
            ),
        )

    try:
        record = store.create_run(
            RunRequest(
                project_root=project.root,
                pipeline=project.pipeline_name,
                profile=profile,
            )
        )
    except ActiveRunError:
        active = store.active_run(project.root)
        if active is None:
            # The holder finished between the rejection and this read. Ask
            # again rather than reporting a conflict that no longer exists.
            return _error_response(
                request,
                status_code=409,
                title="Another run just finished",
                detail="The project was busy a moment ago. Try again.",
            )
        return HTMLResponse(
            request.app.state.templates.get_template("_run_status.html").render(
                run=active,
                is_terminal=active.status in TERMINAL_STATUSES,
                message=(
                    f"This project already has an active run ({active.run_id}). "
                    "Stop it before starting another."
                ),
            ),
            status_code=409,
        )

    try:
        manager.start(record.run_id)
    except Exception as exc:  # noqa: BLE001 - every launch failure lands here
        # Nothing else will ever finish this run: there is no worker to
        # report, and reconcile() only settles runs it can prove are gone. So
        # finish it here, which is also what releases the project lock.
        _LOGGER.exception("could not launch a worker for run %s", record.run_id)
        _finish_quietly(store, record.run_id)
        return _error_response(
            request,
            status_code=500,
            title="The run could not be started",
            detail=f"{type(exc).__name__}: {exc}",
            run_id=record.run_id,
        )

    return RedirectResponse(url=f"/runs/{record.run_id}", status_code=303)


async def _submitted_profile(request: Request) -> str:
    """The submitted ``profile`` field, parsed from the urlencoded body.

    Parsed here rather than through ``request.form()`` deliberately: Starlette
    routes *all* form parsing through ``python-multipart``, which is not a
    dependency of the ``web`` extra and does not need to become one. This UI
    posts one ``application/x-www-form-urlencoded`` field from a plain HTML
    form -- there are no file uploads anywhere in it, and there will not be.

    A repeated field takes its last value, matching how a browser resolves a
    duplicate control name.
    """
    body = (await request.body()).decode("utf-8", errors="replace")
    values = urllib.parse.parse_qs(body, keep_blank_values=True).get("profile")
    return values[-1].strip() if values else ""


def _finish_quietly(store: RunStore, run_id: str) -> None:
    try:
        store.finish_run(run_id, STATUS_FAILED)
    except Exception:  # noqa: BLE001 - the launch error is the one that matters
        _LOGGER.exception("could not finish run %s after a failed launch", run_id)


def _error_response(
    request: Request,
    *,
    status_code: int,
    title: str,
    detail: str,
    run_id: str | None = None,
) -> HTMLResponse:
    """Render an error as a page, or as a fragment for an htmx request."""
    template = "_error.html" if _is_fragment_request(request) else "error.html"
    return request.app.state.templates.TemplateResponse(
        request,
        template,
        {
            "nav_active": "run",
            "error_title": title,
            "error_detail": detail,
            "error_run_id": run_id,
        },
        status_code=status_code,
    )


def _is_fragment_request(request: Request) -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"


# -- GET /runs/{run_id} ----------------------------------------------------


@router.get("/runs/{run_id}", response_class=HTMLResponse)
def run_page(request: Request, run_id: str) -> HTMLResponse:
    """The run console for one run, with its whole history already rendered."""
    store: RunStore = request.app.state.store
    record = store.get_run(run_id)
    if record is None:
        return _error_response(
            request,
            status_code=404,
            title="No such run",
            detail=f"There is no run {run_id!r} in this project's history.",
        )

    log_path = Path(record.log_path)
    events = [console_event(event, log_path) for event in store.events_after(run_id, 0)]
    return request.app.state.templates.TemplateResponse(
        request,
        "run.html",
        {
            "nav_active": "run",
            "run": record,
            "events": events,
            "counters": _counters(events),
            "is_terminal": record.status in TERMINAL_STATUSES,
            "last_sequence": events[-1]["sequence"] if events else 0,
        },
    )


def _counters(events: list[dict[str, Any]]) -> dict[str, int]:
    kinds = [event["kind"] for event in events]
    return {
        "tasks": kinds.count(EventKind.TASK_STARTED.value),
        "skipped": kinds.count(EventKind.TASK_SKIPPED.value),
        "warnings": kinds.count(EventKind.WARNING.value),
    }


# -- GET /runs/{run_id}/events --------------------------------------------


async def event_frames(
    store: RunStore,
    templates: Jinja2Templates,
    run_id: str,
    *,
    after: int = 0,
) -> AsyncIterator[str]:
    """Yield SSE frames for *run_id*, starting after sequence *after*.

    The status is read *before* the events on every pass, which is the
    ordering that makes closing safe: any event appended before the run went
    terminal is either already sent or is in the batch read after that status.
    Reading them the other way round could see "running", then a batch, then
    miss the last event of a run that finished in between.
    """
    cursor = after
    last_output_at = _monotonic()

    while True:
        record = await asyncio.to_thread(store.get_run, run_id)
        if record is None:
            # Deleted mid-stream. There is nothing left to say and nothing to
            # resume from, so end the response rather than poll a ghost.
            return
        is_terminal = record.status in TERMINAL_STATUSES

        events = await asyncio.to_thread(store.events_after, run_id, cursor)
        log_path = Path(record.log_path)
        for event in events:
            yield _event_frame(
                render_event(templates, event, log_path), event.kind, event.sequence
            )
            cursor = event.sequence

        if is_terminal:
            # Status, not ``run_finished``: an interrupted run has no finish
            # event at all, and waiting for one would hang this connection
            # for as long as the browser keeps it open.
            yield _status_frame(templates, record)
            return

        if events:
            last_output_at = _monotonic()
        elif _monotonic() - last_output_at >= HEARTBEAT_INTERVAL_SECONDS:
            yield ": heartbeat\n\n"
            last_output_at = _monotonic()

        await _sleep(POLL_INTERVAL_SECONDS)


@router.get("/runs/{run_id}/events")
def run_events(request: Request, run_id: str, after: int = 0):
    """Stream this run's events, resuming from a cursor the client supplies."""
    store: RunStore = request.app.state.store
    if store.get_run(run_id) is None:
        return _error_response(
            request,
            status_code=404,
            title="No such run",
            detail=f"There is no run {run_id!r} in this project's history.",
        )

    return StreamingResponse(
        event_frames(
            store,
            request.app.state.templates,
            run_id,
            after=_cursor(request, after),
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            # nginx and friends buffer by default, which turns a live stream
            # into a batch delivered at close.
            "X-Accel-Buffering": "no",
        },
    )


def _cursor(request: Request, after: int) -> int:
    """The resume point: ``Last-Event-ID`` if the browser sent one, else ``after``.

    The header wins. A reconnecting ``EventSource`` reuses the URL it was
    created with -- ``?after=`` and all -- and adds the header, so honouring
    the query would replay the whole run into the console on every reconnect.
    """
    header = request.headers.get("Last-Event-ID")
    if header:
        try:
            return max(int(header.strip()), 0)
        except ValueError:
            _LOGGER.debug("ignoring unparseable Last-Event-ID %r", header)
    return max(after, 0)


__all__ = [
    "HEARTBEAT_INTERVAL_SECONDS",
    "MAX_LOG_SLICE_BYTES",
    "POLL_INTERVAL_SECONDS",
    "STATUS_EVENT_NAME",
    "console_event",
    "event_frames",
    "log_text",
    "render_event",
    "router",
]
