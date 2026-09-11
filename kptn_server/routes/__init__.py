"""Routers for the shared pipeline UI.

This package is the single place the app factory looks for pages, so adding a
surface means adding a router here and listing it in :func:`register_routers`
-- the factory itself never needs to change.

The router in this module holds only the health endpoint the standalone
launcher polls before it opens a browser. ``/`` is the run history and is
served, with the rest of the run surface, by :mod:`kptn_server.routes.runs`; the plan
view and the pipeline walkthrough live in
:mod:`kptn_server.routes.inspect`; the retained lineage and table-preview
surfaces live in :mod:`kptn_server.routes.lineage`.
"""

from __future__ import annotations

from fastapi import APIRouter, FastAPI

from kptn_server.routes import inspect, lineage, runs

router = APIRouter()


@router.get("/healthz")
def health() -> dict[str, str]:
    """Liveness probe.

    The ``kptn ui`` launcher polls this and opens the browser only once it
    answers, so a developer never lands on a connection-refused page.
    """
    return {"status": "ok"}


def register_routers(app: FastAPI) -> None:
    """Attach every UI router to *app*."""
    app.include_router(router)
    app.include_router(runs.router)
    app.include_router(inspect.router)
    app.include_router(lineage.router)


__all__ = ["register_routers", "router"]
