"""Routers for the shared pipeline UI.

This package is the single place the app factory looks for pages, so adding a
surface means adding a router here and listing it in :func:`register_routers`
-- the factory itself never needs to change.

The router in this module holds only the two things that exist before any
feature page does: the health endpoint the standalone launcher polls before it
opens a browser, and the shell of the run console at ``/``. The run console's
behaviour, the run history, and the plan walkthrough arrive as their own
routers.
"""

from __future__ import annotations

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/healthz")
def health() -> dict[str, str]:
    """Liveness probe.

    The ``kptn ui`` launcher polls this and opens the browser only once it
    answers, so a developer never lands on a connection-refused page.
    """
    return {"status": "ok"}


@router.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    """The run console shell: which project, which profiles, and a Run form."""
    return request.app.state.templates.TemplateResponse(
        request, "index.html", {"nav_active": "run"}
    )


def register_routers(app: FastAPI) -> None:
    """Attach every UI router to *app*."""
    app.include_router(router)


__all__ = ["register_routers", "router"]
