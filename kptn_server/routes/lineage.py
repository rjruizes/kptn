"""The retained lineage and table-preview surfaces, on the shared app.

These pages predate the pipeline UI. They were served by a second FastAPI
application (``kptn_server.api_http``) built for the React frontend, and were
reached either from that frontend or from the now-retired VS Code extension's
JSON-RPC backend. Both of those are gone, and the plan for this UI is explicit
that there is one supported frontend: so the *services* are retained and the
*routes* move here, onto the same application, the same templates directory,
and the same vendored assets as every other page.

What is served, and why exactly this set:

``GET /lineage-page``, ``GET /lineage-fragment``
    The lineage graph as a full page and as an htmx-swappable fragment. This
    is the target of the walkthrough's per-output "Lineage" link.

``GET /table-preview-fragment``
    A few rows of a declared output. The walkthrough's "Preview" link.

``GET /table-columns``, ``POST /table-preview-query``
    Called by the lineage page's own JavaScript (see
    ``templates/lineage.html``) to expand a node and to run the
    reader's ad-hoc SQL against the preview connection. They are here because
    the lineage page does not work without them, not for their own sake.

The JSON-shaped ``/lineage`` and ``/table-preview`` endpoints the React
application consumed are deliberately *not* carried over. Their only consumer
was deleted; a route with no caller is a maintenance obligation with no
payoff.

**``configPath`` is validated against the served project.** The old
standalone app took an arbitrary filesystem path in a query parameter, which
made sense when it served a project picker and no project of its own. A
single-process ``kptn ui`` invocation still resolves one project per request
-- held by :class:`~kptn_server.slot.ProjectSlot` across the ``os.chdir``
described in :mod:`kptn_server.service`, even when ``kptn ui --projects-root``
serves several -- and
:func:`kptn_server.routes.inspect.output_links` builds every link from that
request's own ``kptn.yaml``. Accepting any other path would let a page loaded
from anywhere in the browser point a loopback, unauthenticated server at an
arbitrary file. The parameter is kept -- the lineage template's JavaScript
threads it through its own URLs -- but a value that is not the resolved
project's config is refused.

Errors render through :func:`kptn_server.routes.support.error_response` for
the HTML routes, so a failure looks like every other failure in this UI, and
as an ``HTTPException`` for the two JSON routes, whose callers are JavaScript
and want a status code.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from kptn_server.context import ui
from kptn_server.project import PROFILE_CONFIG_FILENAME, ProjectContext
from kptn_server.routes.support import error_response

router = APIRouter()

#: Shown when a request names a config file that is not the served project's.
FOREIGN_CONFIG_DETAIL = (
    "These views only read the resolved project's own {filename}."
).format(filename=PROFILE_CONFIG_FILENAME)


class TablePreviewQuery(BaseModel):
    """Body of ``POST /table-preview-query``, as the lineage page sends it."""

    configPath: str  # noqa: N815 - wire format, read by lineage.html
    sql: str
    table: Optional[str] = None
    limit: Optional[int] = None
    columns: Optional[list[str]] = None


def project_config_path(project: ProjectContext) -> Path:
    """The one config file these routes will read."""
    return project.root / PROFILE_CONFIG_FILENAME


def _accepted_config(project: ProjectContext, config_path: str) -> Path | None:
    """*config_path* if it is *project*'s config, else ``None``.

    Resolved before comparison so that a path spelled with ``..`` or through
    a symlink cannot dodge the check. ``ProjectContext.root`` is already
    canonical, which is what makes the comparison meaningful.

    Takes an already-resolved :class:`ProjectContext` rather than a
    ``Request``, because every caller is already inside its own
    ``ui(request).project()`` block: entering the slot a second time here
    would deadlock -- ``ProjectSlot``'s lock is not reentrant.
    """
    expected = project_config_path(project)
    try:
        candidate = Path(config_path).resolve()
    except OSError:  # pragma: no cover - defensive
        return None
    return expected if candidate == expected.resolve() else None


def _refuse_foreign_config(request: Request) -> HTMLResponse:
    return error_response(
        request,
        status_code=400,
        title="That is not this project's configuration",
        detail=FOREIGN_CONFIG_DETAIL,
        nav_active="docs",
    )


def _service() -> Any:
    """Import the retained service, or fail with a 500 that says why.

    ``sqlglot`` is a declared dependency of the ``web`` extra, so in any
    environment that can serve this router the import succeeds. It is still
    guarded, because an install that somehow lacks it should produce a legible
    error on *these two* pages rather than a traceback out of app startup.
    """
    try:
        from kptn_server import service  # noqa: PLC0415 - see docstring
    except ImportError as exc:  # pragma: no cover - requires a broken install
        raise HTTPException(
            status_code=500,
            detail=(
                "The lineage views need the 'web' extra's SQL parser: "
                f"pip install 'kptn[web]' ({exc})"
            ),
        ) from exc
    return service


# -- lineage ---------------------------------------------------------------


def _render_lineage(
    request: Request, configPath: str, graph: Optional[str], *, fragment: bool
) -> HTMLResponse:
    with ui(request).project() as project:
        config = _accepted_config(project, configPath)
        if config is None:
            return _refuse_foreign_config(request)
        try:
            html, _, _ = _service().render_lineage_page(
                config, graph, base_url="", fragment=fragment
            )
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001 - any analyzer failure is one page
            return error_response(
                request,
                status_code=500,
                title="Lineage could not be built",
                detail=f"{type(exc).__name__}: {exc}",
                nav_active="docs",
            )
    return HTMLResponse(content=html)


@router.get("/lineage-page", response_class=HTMLResponse)
def lineage_page(
    request: Request,
    configPath: str,  # noqa: N803 - query parameter name is user-facing
    graph: Optional[str] = None,
) -> HTMLResponse:
    """The lineage graph for this project, as a standalone page."""
    return _render_lineage(request, configPath, graph, fragment=False)


@router.get("/lineage-fragment", response_class=HTMLResponse)
def lineage_fragment(
    request: Request,
    configPath: str,  # noqa: N803 - query parameter name is user-facing
    graph: Optional[str] = None,
) -> HTMLResponse:
    """The same graph without the document shell, for an htmx swap."""
    return _render_lineage(request, configPath, graph, fragment=True)


# -- table preview ---------------------------------------------------------


@router.get("/table-preview-fragment", response_class=HTMLResponse)
def table_preview_fragment(
    request: Request,
    configPath: str,  # noqa: N803 - query parameter name is user-facing
    table: str,
) -> HTMLResponse:
    """A few rows of *table*, rendered for an htmx swap.

    A table the project does not declare, or one whose database has never
    been built, comes back as a *message* inside the fragment rather than as
    an error -- that is what ``get_duckdb_preview`` reports, and "not built
    yet" is an ordinary state for a project someone has not run.
    """
    with ui(request).project() as project:
        config = _accepted_config(project, configPath)
        if config is None:
            return _refuse_foreign_config(request)
        service = _service()
        try:
            payload = service.get_duckdb_preview(config, table)
        except FileNotFoundError as exc:
            return error_response(
                request,
                status_code=404,
                title="No such table to preview",
                detail=str(exc),
                nav_active="docs",
            )
        except Exception as exc:  # noqa: BLE001 - any preview failure is one panel
            return error_response(
                request,
                status_code=500,
                title="The preview could not be read",
                detail=f"{type(exc).__name__}: {exc}",
                nav_active="docs",
            )
        return HTMLResponse(content=service.render_table_preview_fragment(payload))


@router.get("/table-columns")
def table_columns(
    request: Request,
    configPath: str,  # noqa: N803 - query parameter name is user-facing
    table: str,
) -> dict[str, object]:
    """Column names for *table*. Called by the lineage page's JavaScript."""
    with ui(request).project() as project:
        config = _accepted_config(project, configPath)
        if config is None:
            raise HTTPException(status_code=400, detail=FOREIGN_CONFIG_DETAIL)
        try:
            return _service().get_duckdb_table_columns(config, table)
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/table-preview-query")
def table_preview_query(request: Request, body: TablePreviewQuery) -> dict[str, object]:
    """Run the reader's own SELECT against the preview connection.

    Single-statement enforcement and the injected row limit live in
    ``service._prepare_client_sql``; this route adds only the project check.
    """
    with ui(request).project() as project:
        config = _accepted_config(project, body.configPath)
        if config is None:
            raise HTTPException(status_code=400, detail=FOREIGN_CONFIG_DETAIL)
        try:
            return _service().get_duckdb_preview(
                config,
                body.table,
                sql=body.sql,
                limit=body.limit or 50,
                requested_columns=body.columns,
            )
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc


__all__ = [
    "FOREIGN_CONFIG_DETAIL",
    "project_config_path",
    "router",
]
