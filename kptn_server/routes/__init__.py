"""Routers for the shared pipeline UI.

This package is the single place the app factory looks for pages, so adding a
surface means adding a router here and listing it in :func:`register_routers`
-- the factory itself never needs to change.

The router in this module holds only the two things that exist before any
feature page does: the health endpoint the standalone launcher polls before it
opens a browser, and the shell of the run console at ``/``. The run console's
behaviour and the run history live in :mod:`kptn_server.routes.runs`; the plan
view and the pipeline walkthrough live in
:mod:`kptn_server.routes.inspect`; the retained lineage and table-preview
surfaces live in :mod:`kptn_server.routes.lineage`.
"""

from __future__ import annotations

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse

from kptn_server.project import ProjectContext
from kptn_server.routes import inspect, lineage, runs
from kptn_server.routes.support import requested_profile, unknown_profile

router = APIRouter()


@router.get("/healthz")
def health() -> dict[str, str]:
    """Liveness probe.

    The ``kptn ui`` launcher polls this and opens the browser only once it
    answers, so a developer never lands on a connection-refused page.
    """
    return {"status": "ok"}


@router.get("/", response_class=HTMLResponse)
def index(request: Request, profile: str | None = None) -> HTMLResponse:
    """The run console shell: which project, which profiles, and a Run form.

    It accepts ``?profile=`` for the same reason the plan and walkthrough
    pages do: the profile travels with the reader through the app bar's nav,
    and arriving back here it has to be *visible* -- a selector reading
    "(no profile)" while the URL says otherwise would start the next run on
    the wrong one.
    """
    project: ProjectContext = request.app.state.project
    if profile and profile not in project.profiles:
        return unknown_profile(request, profile, nav_active="run")
    return request.app.state.templates.TemplateResponse(
        request,
        "index.html",
        {"nav_active": "run", "selected_profile": requested_profile(profile)},
    )


def register_routers(app: FastAPI) -> None:
    """Attach every UI router to *app*."""
    app.include_router(router)
    app.include_router(runs.router)
    app.include_router(inspect.router)
    app.include_router(lineage.router)


__all__ = ["register_routers", "router"]
