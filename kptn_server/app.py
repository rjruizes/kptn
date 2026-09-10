"""FastAPI application factory for the shared pipeline UI.

One factory call serves one project. ``create_app`` resolves the project,
opens its durable run store, builds the worker supervisor, mounts the vendored
assets, registers the page routers, and -- the part with real teeth -- drives
:meth:`RunProcessManager.reconcile` on a fixed cadence for as long as the
server is up.

That cadence lives here rather than in the supervisor on purpose.
:class:`~kptn_server.processes.RunProcessManager` owns no threads and no
timers, which is precisely why a worker it launched survives the manager being
garbage-collected, the browser closing, VS Code quitting, and this server
restarting on a file save. Something still has to notice a run whose worker a
reboot or an OOM kill took out, though, or that project stays wedged behind its
active-run lock forever. So the FastAPI lifespan runs one reconciliation pass
at startup and another every
:data:`~kptn_server.processes.RECONCILE_INTERVAL_SECONDS`, and cancels the loop
cleanly on shutdown.

Shutdown cancels *the loop*, never a worker. Nothing in this module signals,
waits on, or otherwise touches a running worker process.

Security posture: no authentication, no remote execution, and the launcher
binds loopback. This serves a single developer's own project on their own
machine. What loopback does *not* cover is a page on another origin posting a
form at this server -- ``POST /runs`` is form-encoded, so a browser sends it
with no preflight -- and that is what the ``Sec-Fetch-Site`` middleware
registered here refuses. See :mod:`kptn_server.origin`.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from kptn_server.origin import enforce_same_origin
from kptn_server.processes import RECONCILE_INTERVAL_SECONDS, RunProcessManager
from kptn_server.project import ProjectContext
from kptn_server.routes import register_routers
from kptn_server.run_store import RunStore

_LOGGER = logging.getLogger(__name__)

# Package-relative, never cwd-relative: an installed (non-editable) wheel is
# not served out of the developer's working directory.
_PACKAGE_DIR = Path(__file__).parent
TEMPLATES_DIR = _PACKAGE_DIR / "templates"
STATIC_DIR = _PACKAGE_DIR / "static"


async def _sleep(delay: float) -> None:
    """Indirection so tests can drive the reconciliation cadence.

    A test replaces this to observe a pass without waiting five real seconds;
    production behaviour is a plain ``asyncio.sleep``.
    """
    await asyncio.sleep(delay)


async def _reconcile_periodically(app: FastAPI) -> None:
    """Run one reconciliation pass immediately, then one per interval.

    The first pass is deliberately before the first sleep: the common reason
    this server just started is that it *restarted*, and any run whose worker
    died while it was down should be settled now rather than five seconds from
    now.

    ``reconcile`` is synchronous and touches SQLite, so it goes to a worker
    thread rather than blocking the event loop. A failing pass is logged and
    retried on the next tick -- a transient "database is locked" must not take
    reconciliation out for the rest of the session.
    """
    while True:
        manager = app.state.processes
        try:
            interrupted = await asyncio.to_thread(manager.reconcile)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the loop must outlive one bad pass
            _LOGGER.exception("run reconciliation pass failed; retrying next tick")
        else:
            for run_id in interrupted:
                _LOGGER.warning(
                    "run %s was marked interrupted: its worker is gone", run_id
                )
        await _sleep(RECONCILE_INTERVAL_SECONDS)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    task = asyncio.create_task(
        _reconcile_periodically(app), name="kptn-run-reconciliation"
    )
    app.state.reconcile_task = task
    try:
        yield
    finally:
        # Cancel and *await* the loop, so shutdown never leaves an orphaned
        # task writing to the run store. This stops the supervisor's clock,
        # not any worker it launched.
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def _build_templates() -> Jinja2Templates:
    """Jinja environment whose every render already knows the project.

    The base page needs the project name and the profile list on *every* page,
    so a context processor supplies them once here instead of each router
    having to remember.
    """

    def project_context(request: Request) -> dict[str, Any]:
        return {"project": request.app.state.project}

    return Jinja2Templates(
        directory=str(TEMPLATES_DIR), context_processors=[project_context]
    )


def create_app(project_root: Path) -> FastAPI:
    """Build the UI application for the project rooted at *project_root*.

    Raises :class:`~kptn_server.project.ProjectError` if the directory is not
    a servable kptn project -- failing at construction rather than serving a
    UI that 500s on every page.
    """
    project = ProjectContext.load(project_root)
    store = RunStore(project.database_path)
    processes = RunProcessManager(store)

    app = FastAPI(
        title=f"kptn - {project.pipeline_name}",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=_lifespan,
    )
    app.state.project = project
    app.state.store = store
    app.state.processes = processes
    app.state.templates = _build_templates()

    # Before any router, so that a state-changing route added later cannot
    # be added without it. See :mod:`kptn_server.origin` for why
    # ``Sec-Fetch-Site`` is the check and why safe methods are exempt.
    app.middleware("http")(enforce_same_origin)

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    register_routers(app)
    return app


__all__ = [
    "RECONCILE_INTERVAL_SECONDS",
    "STATIC_DIR",
    "TEMPLATES_DIR",
    "create_app",
]
