"""Starting, watching, summarizing, and stopping pipeline runs.

Every endpoint here rests on one idea: **nothing about a run lives in this
process.** The run row, its event log, its captured output, and its stop
intent are all on disk, so the browser, VS Code, and this server can each
restart mid-run without losing a thing.

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

Four more endpoints complete the surface.

``GET /runs``
    The project's run history, newest first, straight out of the store.

``GET /runs/{run_id}/log``
    The raw captured log. **The path comes from the run row and from nowhere
    else** -- no query parameter, header, or path segment can choose a file.
    That is the whole security posture of this endpoint: it hands a file back
    to a UI with no authentication, so the set of files it can hand back is
    exactly one per recorded run.

``POST /runs/{run_id}/stop`` and ``POST /runs/{run_id}/force-finish``
    Stop records intent and signals; it never writes an outcome, because only
    the worker knows whether cleanup finished. Force-finish is the escape
    hatch for the one run reconciliation cannot settle -- see
    :data:`FORCE_FINISH_CONFIRMATION`.

**Counts are computed from lifecycle events, never from console text.** A task
that prints the words ``task_started`` is output, not a task; see
:func:`counters`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Mapping, Sequence

from fastapi import APIRouter, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates

from kptn.runner.events import EventKind
from kptn_server.run_store import (
    STATUS_FAILED,
    STATUS_INTERRUPTED,
    TERMINAL_STATUSES,
    ActiveRunError,
    RunNotFoundError,
    RunRecord,
    RunRequest,
    RunStateError,
    RunStore,
    StoredEvent,
    WarningGroup,
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

#: How many runs ``GET /runs`` renders. The history is unbounded on disk and
#: deliberately not paginated in the UI: a developer wants the last few runs,
#: and a bounded page keeps the per-run summary queries bounded with it.
HISTORY_LIMIT = 50

#: The word a developer must type to force-finish a run.
#:
#: Force-finish exists because of one gap the supervisor cannot close. A
#: worker whose liveness the OS will not let us determine is deliberately
#: treated as *possibly alive* (see
#: :meth:`~kptn_server.processes.RunProcessManager._liveness`) -- burying it
#: would release the project's active-run lock while a process might still be
#: writing. But a permanently un-inspectable PID is then never reconcilable:
#: the run stays ``running``/``stop_requested`` forever, the project's lock is
#: never released, no new run can start, and ``stop()`` can only record
#: intent it has no way to deliver.
#:
#: So force-finish abandons that worker: it writes ``interrupted``, which is
#: exactly the status meaning "this run's real fate is unknown", and releases
#: the lock in the same transaction. It does not signal anything -- the PID
#: may have been recycled onto an innocent process, and a live-but-unreachable
#: worker would not receive it anyway.
#:
#: That makes it categorically different from Stop, which is why it is a
#: separate route behind a typed confirmation rather than a second button. A
#: checkbox or a bare POST would make abandoning a possibly-live process one
#: click away from asking it politely to stop.
FORCE_FINISH_CONFIRMATION = "abandon"


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


def _status_payload(templates: Jinja2Templates, record: RunRecord) -> dict[str, Any]:
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


def _status_frame(templates: Jinja2Templates, record: RunRecord) -> str:
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

    if not _is_form_encoded(request):
        # No body, or a body this route cannot parse. Falling through would
        # read "no profile" out of nothing and *start a pipeline* for a
        # request that never asked for one.
        return _error_response(
            request,
            status_code=415,
            title="Unsupported request body",
            detail=(
                "POST /runs takes an application/x-www-form-urlencoded body "
                f"with a 'profile' field; got {_media_type(request)!r}."
            ),
        )

    profile = await _submitted_field(request, "profile") or None

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
        # Through _error_response like every other error, so a plain form
        # post -- which is what every form in this UI is -- gets the page
        # shell rather than an orphan <div>. Nothing here is wired to htmx.
        return _error_response(
            request,
            status_code=409,
            title="This project already has an active run",
            detail=(
                "Only one run per project at a time. Stop the active run "
                "before starting another."
            ),
            run=active,
        )

    # Nothing else will ever finish a run whose launch did not take: there
    # is no worker to report, and reconcile() only settles runs it can prove
    # are gone -- which needs a recorded pid this run never got. The cleanup
    # is in a ``finally`` rather than the ``except`` so that a BaseException
    # (a KeyboardInterrupt arriving in this exact window, say) also releases
    # the project lock instead of wedging the project until someone edits the
    # database by hand.
    launched = False
    try:
        manager.start(record.run_id)
        launched = True
    except Exception as exc:  # noqa: BLE001 - every launch failure lands here
        _LOGGER.exception("could not launch a worker for run %s", record.run_id)
        return _error_response(
            request,
            status_code=500,
            title="The run could not be started",
            detail=f"{type(exc).__name__}: {exc}",
            run_id=record.run_id,
        )
    finally:
        if not launched:
            _finish_quietly(store, record.run_id)

    return RedirectResponse(url=f"/runs/{record.run_id}", status_code=303)


#: The one body type ``POST /runs`` accepts: what a plain HTML form sends.
FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"


def _media_type(request: Request) -> str:
    """The request's content type with any parameters (charset) stripped."""
    return request.headers.get("content-type", "").split(";")[0].strip().lower()


def _is_form_encoded(request: Request) -> bool:
    return _media_type(request) == FORM_CONTENT_TYPE


async def _submitted_field(request: Request, name: str) -> str:
    """One field of a urlencoded body, stripped, or ``""`` if absent.

    Parsed here rather than through ``request.form()`` deliberately: Starlette
    routes *all* form parsing through ``python-multipart``, which is not a
    dependency of the ``web`` extra and does not need to become one. This UI
    posts ``application/x-www-form-urlencoded`` fields from plain HTML forms
    -- there are no file uploads anywhere in it, and there will not be.

    Callers must gate on :func:`_is_form_encoded` first: hand-parsing means
    there is no framework layer to reject a JSON body or no body at all, and
    an unparseable body silently reads as "field absent". For ``profile`` that
    is a *valid* request that starts a pipeline; for ``confirm`` it is the
    difference between a refusal and an unconfirmed abandon.

    A repeated field takes its last value, matching how a browser resolves a
    duplicate control name.
    """
    body = (await request.body()).decode("utf-8", errors="replace")
    values = urllib.parse.parse_qs(body, keep_blank_values=True).get(name)
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
    run: RunRecord | None = None,
) -> HTMLResponse:
    """Render an error as a page, or as a fragment for an htmx request.

    *run_id* names a run to link to; *run* embeds that run's status fragment,
    which is what the conflict response needs -- the run holding the lock is
    the thing the reader has to act on.

    The page shell is the default and the fragment is the exception, because
    every form in this UI is a plain ``<form method="post">``: there is no
    ``hx-`` attribute anywhere in ``templates/``. A fragment served to a
    browser navigation is a page with no stylesheet and no nav.
    """
    template = "_error.html" if _is_fragment_request(request) else "error.html"
    return request.app.state.templates.TemplateResponse(
        request,
        template,
        {
            "nav_active": "run",
            "error_title": title,
            "error_detail": detail,
            "error_run_id": run_id,
            "run": run,
            "is_terminal": (
                run.status in TERMINAL_STATUSES if run is not None else False
            ),
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
    stored = store.events_after(run_id, 0)
    events = [console_event(event, log_path) for event in stored]
    return request.app.state.templates.TemplateResponse(
        request,
        "run.html",
        {
            "nav_active": "run",
            "run": record,
            "events": events,
            "counters": counters(stored),
            "warnings": warning_summary(store.warning_groups(run_id)),
            "force_finish_confirmation": FORCE_FINISH_CONFIRMATION,
            "is_terminal": record.status in TERMINAL_STATUSES,
            "last_sequence": events[-1]["sequence"] if events else 0,
        },
    )


# -- counts and the warning summary ---------------------------------------


#: The ``task_finished`` statuses the executor emits (see
#: ``kptn.runner.executor._emit_task_finished``).
_TASK_OUTCOMES = ("succeeded", "failed")


def counters(events: Sequence[StoredEvent]) -> dict[str, int]:
    """Task and warning counts for one run, from its lifecycle events.

    Counted from the stored events -- ``task_started``, ``task_skipped``, and
    the ``status`` on each ``task_finished`` -- and never from the console
    text. A task that prints the words ``task_started`` is output, not a task,
    and a summary that scraped the log would say otherwise.

    ``unfinished`` is the interesting one: tasks that started and never
    reported an outcome. That is the shape a stopped or interrupted run leaves
    behind, and it is the number that tells a reader where the run stopped
    being trustworthy. It is floored at zero rather than trusted to be
    non-negative: the event log is written by a worker that can be killed
    mid-sequence, so "more finishes than starts" is a corrupt log, not a
    negative count to render.

    ``tasks``, ``skipped`` and ``warnings`` are also the console header's
    counters, which ``app.js`` recounts off the DOM as events stream in. One
    function so the two can never disagree about what a task is.
    """
    tallies = {
        "tasks": 0,
        "skipped": 0,
        "warnings": 0,
        "succeeded": 0,
        "failed": 0,
    }
    for event in events:
        if event.kind == EventKind.TASK_STARTED.value:
            tallies["tasks"] += 1
        elif event.kind == EventKind.TASK_SKIPPED.value:
            tallies["skipped"] += 1
        elif event.kind == EventKind.WARNING.value:
            tallies["warnings"] += 1
        elif event.kind == EventKind.TASK_FINISHED.value:
            # Only the two outcomes the executor emits are counted. An
            # unrecognized status is left out of both rather than guessed at,
            # where it shows up as ``unfinished`` -- the honest answer for a
            # task whose outcome this UI does not understand.
            status = str(event.payload.get("status") or "")
            if status in _TASK_OUTCOMES:
                tallies[status] += 1
    tallies["unfinished"] = max(
        tallies["tasks"] - tallies["succeeded"] - tallies["failed"], 0
    )
    return tallies


def warning_summary(groups: Sequence[WarningGroup]) -> dict[str, Any]:
    """A run's warnings, grouped for display and linked to every occurrence.

    Grouping is *presentation only*: the store keeps every warning event, and
    every one of their sequences is turned into an anchor here. A summary that
    linked only the first occurrence would hide the repeat its own count is
    advertising.

    ``task_count`` counts the distinct tasks that warned. Warnings raised
    outside any task (at pipeline import time, say) have no task to attribute
    and are counted in ``total`` but not in ``task_count`` -- hence the
    separate headline wording rather than a claim of "0 tasks".

    Wording is decided here rather than in the template so that pluralization
    lives in one testable place, and so the run page and the history row read
    identically.
    """
    total = sum(group.count for group in groups)
    tasks = {group.task_name for group in groups if group.task_name}
    return {
        "total": total,
        "task_count": len(tasks),
        "headline": _warning_headline(total, len(tasks)),
        "groups": [_warning_group(group) for group in groups],
    }


def _warning_headline(total: int, task_count: int) -> str:
    if not total:
        return "No warnings"
    warnings = _plural(total, "warning")
    if not task_count:
        return f"{warnings} outside any task"
    return f"{warnings} in {_plural(task_count, 'task')}"


def _warning_group(group: WarningGroup) -> dict[str, Any]:
    # ``occurrence_sequences`` is the authoritative list. The fallback covers
    # a group whose sequences were not recorded: one anchor to the occurrence
    # the group does know about beats no way back to the console at all, and
    # nothing here invents a sequence that was never stored.
    sequences = group.occurrence_sequences or (group.first_sequence,)
    return {
        "task": group.task_name,
        "category": group.category,
        "sample_message": group.sample_message,
        "count": group.count,
        "occurrence_label": _plural(group.count, "occurrence"),
        "occurrences": [
            {"sequence": sequence, "anchor": f"#event-{sequence}"}
            for sequence in sequences
        ],
    }


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


# -- GET /runs -------------------------------------------------------------


@router.get("/runs", response_class=HTMLResponse)
def run_history(request: Request) -> HTMLResponse:
    """This project's run history, newest first.

    Read out of the store on every request, which is what makes the page
    durable rather than remembered: a run recorded by a server that has since
    restarted, or by a detached worker while no server was up at all, is
    still here.

    The order is the store's (``created_at DESC, run_id DESC``) and is passed
    straight through -- neither this function nor the template re-sorts, so
    there is exactly one place the ordering can be wrong.

    One summary query per listed run, bounded by :data:`HISTORY_LIMIT`. The
    alternative -- one aggregate query in the store -- would put the "what
    counts as a task" rule in a second place, which is the mistake
    :func:`counters` exists to prevent.
    """
    project = request.app.state.project
    store: RunStore = request.app.state.store
    runs = [
        {
            "run": record,
            "counters": counters(store.events_after(record.run_id, 0)),
            "warnings": warning_summary(store.warning_groups(record.run_id)),
        }
        for record in store.list_runs(project.root, limit=HISTORY_LIMIT)
    ]
    return request.app.state.templates.TemplateResponse(
        request, "runs.html", {"nav_active": "runs", "runs": runs}
    )


# -- GET /runs/{run_id}/log ------------------------------------------------


@router.get("/runs/{run_id}/log")
def run_log(request: Request, run_id: str):
    """The raw captured log for one run, as a download.

    **The path is resolved from the stored run and from nothing else.** Not
    from a query parameter, not from a header, not by joining the run id onto
    the project's log directory. This UI has no authentication and runs as the
    developer, so a route that let a request name a file would read any file
    that developer can read; the set of files this endpoint can serve is
    exactly one per recorded run, and an unknown run id is a 404 before any
    filesystem access happens at all.

    A missing file is also a 404, not a 500: the log belongs to the worker and
    can be deleted, rotated, or sitting on a volume that went away. That
    degrades the download; it does not break the server.
    """
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
    if not log_path.is_file():
        return _error_response(
            request,
            status_code=404,
            title="This run has no log file",
            detail=(
                f"Run {run_id} recorded its log at {log_path}, and there is no "
                "file there now. A worker's log can be deleted or rotated "
                "after the run ends."
            ),
            run_id=run_id,
        )

    return FileResponse(
        log_path,
        media_type="text/plain; charset=utf-8",
        # Named after the run, not after the file on disk: the stored path is
        # an internal detail, and a browser download called
        # "3f2c...e91.log" tells the developer nothing.
        filename=f"kptn-{run_id}.log",
    )


# -- POST /runs/{run_id}/stop ---------------------------------------------


@router.post("/runs/{run_id}/stop")
def stop_run(request: Request, run_id: str):
    """Ask a run to stop. Records the intent, then signals the worker.

    ``request_stop`` is the gate rather than a status read of our own: it
    tests and writes the transition in one transaction, so a run that went
    terminal a microsecond ago is refused instead of being "stopped" on the
    strength of a stale read. It is idempotent, so the supervisor recording
    the same intent again a moment later changes nothing.

    **A ``stop()`` of ``False`` is a success.** It means no live worker
    matched -- the process is gone, or its PID cannot be inspected -- and the
    intent is durably recorded for a worker that is mid-startup or for the
    next reconciliation pass. Reporting that as a failure would tell the
    developer their stop did not land when it did exactly what it promises.

    Nothing here writes an outcome. ``stop_requested`` is not terminal: the
    project keeps its lock until the worker records ``stopped`` or
    reconciliation records ``interrupted``, because finishing a run whose
    worker may still be writing is how two runs end up on one project.

    No body is read, so there is no content-type gate: the gate on
    ``POST /runs`` exists because an unparseable body there reads as a valid
    request, and this route has no field to misread.
    """
    store: RunStore = request.app.state.store
    manager = request.app.state.processes

    try:
        store.request_stop(run_id)
    except RunNotFoundError:
        return _error_response(
            request,
            status_code=404,
            title="No such run",
            detail=f"There is no run {run_id!r} in this project's history.",
        )
    except RunStateError:
        record = store.get_run(run_id)
        return _error_response(
            request,
            status_code=409,
            title="This run has already finished",
            detail=(
                "A finished run cannot be stopped. Its outcome is already "
                "recorded and it holds nothing."
            ),
            run=record,
            run_id=run_id,
        )

    try:
        manager.stop(run_id)
    except Exception:  # noqa: BLE001 - the intent is already durable
        # The stop is recorded either way, and reconciliation settles a worker
        # that never hears about it. A supervisor that could not signal must
        # not turn a landed stop request into a 500.
        _LOGGER.exception("could not signal the worker for run %s", run_id)

    return RedirectResponse(url=f"/runs/{run_id}", status_code=303)


# -- POST /runs/{run_id}/force-finish -------------------------------------


@router.post("/runs/{run_id}/force-finish")
async def force_finish_run(request: Request, run_id: str):
    """Abandon a wedged run's worker and release the project's lock.

    The escape hatch described on :data:`FORCE_FINISH_CONFIRMATION`: the one
    situation reconciliation cannot settle by design. It records
    ``interrupted`` -- "this run's real fate is unknown", which is precisely
    true here -- and ``finish_run`` drops the project's active-run lock in the
    same transaction.

    It signals nothing, on purpose. The recorded PID may have been recycled
    onto an unrelated process, and a worker that is alive but un-inspectable
    would not be reachable anyway. Abandoning is the honest description of
    what this does, and the template says so.

    The typed confirmation is what keeps this from being a one-click twin of
    Stop, so the body is gated and parsed rather than trusted: an unparseable
    body must be refused, never read as an absent field on a route where an
    absent field is the only thing standing between a click and a possibly
    live process being written off.
    """
    store: RunStore = request.app.state.store

    if not _is_form_encoded(request):
        return _error_response(
            request,
            status_code=415,
            title="Unsupported request body",
            detail=(
                "Force-finishing takes an application/x-www-form-urlencoded "
                f"body with a 'confirm' field; got {_media_type(request)!r}."
            ),
            run_id=run_id,
        )

    confirmation = await _submitted_field(request, "confirm")
    if confirmation != FORCE_FINISH_CONFIRMATION:
        return _error_response(
            request,
            status_code=400,
            title="Force-finish was not confirmed",
            detail=(
                f"Type {FORCE_FINISH_CONFIRMATION!r} to force-finish this run. "
                "It abandons a worker that may still be running, so it is not "
                "a one-click action and nothing has been changed."
            ),
            run_id=run_id,
        )

    try:
        store.finish_run(run_id, STATUS_INTERRUPTED)
    except RunNotFoundError:
        return _error_response(
            request,
            status_code=404,
            title="No such run",
            detail=f"There is no run {run_id!r} in this project's history.",
        )
    except RunStateError:
        return _error_response(
            request,
            status_code=409,
            title="This run has already finished",
            detail=("A finished run holds no lock and has no worker to abandon."),
            run=store.get_run(run_id),
            run_id=run_id,
        )

    _LOGGER.warning(
        "run %s was force-finished: its worker was abandoned, not stopped",
        run_id,
    )
    return RedirectResponse(url=f"/runs/{run_id}", status_code=303)


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
def run_events(request: Request, run_id: str, after: str | None = None):
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


def _cursor(request: Request, after: str | None) -> int:
    """The resume point: ``Last-Event-ID`` if the browser sent one, else ``after``.

    The header wins. A reconnecting ``EventSource`` reuses the URL it was
    created with -- ``?after=`` and all -- and adds the header, so honouring
    the query would replay the whole run into the console on every reconnect.

    Both are parsed by the same tolerant rule, which is why ``after`` is typed
    as text: a declared ``int`` would hand a garbled query string FastAPI's
    422 JSON while the identically garbled header fell back to 0. A cursor is
    a resume hint, and the worst case of not understanding one is replaying
    events the client already has.
    """
    header = _parse_sequence(request.headers.get("Last-Event-ID"), name="Last-Event-ID")
    if header is not None:
        return header
    return _parse_sequence(after, name="after") or 0


def _parse_sequence(value: str | None, *, name: str) -> int | None:
    """A non-negative sequence number, or ``None`` if *value* is not one."""
    if value is None or not value.strip():
        return None
    try:
        # Negatives clamp rather than reject: they are as meaningless as a
        # letter, and events_after() would treat one as "everything" anyway.
        return max(int(value.strip()), 0)
    except ValueError:
        _LOGGER.debug("ignoring unparseable %s %r", name, value)
        return None


__all__ = [
    "FORCE_FINISH_CONFIRMATION",
    "FORM_CONTENT_TYPE",
    "HEARTBEAT_INTERVAL_SECONDS",
    "HISTORY_LIMIT",
    "MAX_LOG_SLICE_BYTES",
    "POLL_INTERVAL_SECONDS",
    "STATUS_EVENT_NAME",
    "console_event",
    "counters",
    "event_frames",
    "log_text",
    "render_event",
    "router",
    "warning_summary",
]
