"""Routers for the shared pipeline UI.

This package is the single place the app factory looks for pages, so adding a
surface means adding a router here and listing it in :func:`register_routers`
-- the factory itself never needs to change.

The router in this module holds the health endpoint the standalone launcher
polls before it opens a browser, and the active-run probe the app bar polls
to know whether this project is busy. ``/`` is the run history and is
served, with the rest of the run surface, by :mod:`kptn_server.routes.runs`; the plan
view and the pipeline walkthrough live in
:mod:`kptn_server.routes.inspect`; the retained lineage and table-preview
surfaces live in :mod:`kptn_server.routes.lineage`.
"""

from __future__ import annotations

from fastapi import APIRouter, FastAPI, Request

from kptn_server.context import ui
from kptn_server.routes import inspect, lineage, runs

router = APIRouter()


@router.get("/healthz")
def health() -> dict[str, str]:
    """Liveness probe.

    The ``kptn ui`` launcher polls this and opens the browser only once it
    answers, so a developer never lands on a connection-refused page.
    """
    return {"status": "ok"}


@router.get("/active-run")
def active_run(request: Request) -> dict[str, object]:
    """Whether this project has a run in progress, and which one.

    The app bar renders its controls disabled while a run holds the project
    lock, but nothing server-rendered can know the run finished a second
    after the page was drawn -- and only the run console has a stream of its
    own. So the bar asks, on a timer, from every page.

    Deliberately JSON and deliberately tiny: the answer toggles ``disabled``
    on three controls that are already on the page. A fragment to swap in
    would replace the profile ``<select>`` on every poll, discarding a
    selection the reader had not submitted yet.

    Not under ``/runs/``: that prefix is run-id territory
    (``/runs/{run_id}``), and a sibling literal there is one route-ordering
    change away from meaning "the run whose id is 'active'".
    """
    current = ui(request)
    record = current.store.active_run(current.entry.root)
    return {"active": record is not None, "run_id": record.run_id if record else None}


def register_routers(app: FastAPI) -> None:
    """Attach every UI router to *app*."""
    app.include_router(router)
    app.include_router(runs.router)
    app.include_router(inspect.router)
    app.include_router(lineage.router)


__all__ = ["register_routers", "router"]
