"""FastAPI application factory for the shared pipeline UI.

``create_app`` serves one project, chosen by the launcher from the working
directory. ``create_multi_app`` serves several -- the working directories one
person has under a shared release folder -- each at ``/p/<slug>/``, because
behind jupyter-server-proxy the process's working directory belongs to the
notebook server and names no project at all. Both build the same routers over
the same per-request context; the difference is only where the project comes
from.

``create_app`` resolves the project, opens its durable run store, builds the
worker supervisor, mounts the vendored
assets, registers the page routers, and -- the part with real teeth -- drives
:meth:`RunProcessManager.reconcile` on a fixed cadence for as long as the
server is up.

That cadence lives here rather than in the supervisor on purpose.
:class:`~kptn_server.processes.RunProcessManager` owns no threads and no
timers, which is precisely why a worker it launched survives the manager being
garbage-collected, the browser closing, VS Code quitting, and this server
being stopped and started again. Something still has to notice a run whose
worker a reboot or an OOM kill took out, though, or that project stays wedged
behind its active-run lock forever. So the FastAPI lifespan runs one reconciliation pass
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
import os
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from starlette.types import Scope

from kptn_server.context import RequestUI, RequestUIMiddleware
from kptn_server.origin import enforce_same_origin
from kptn_server.processes import RECONCILE_INTERVAL_SECONDS, RunProcessManager
from kptn_server.project import ProjectContext
from kptn_server.registry import ProjectEntry, ProjectRegistry
from kptn_server.routes import register_routers, router as health_router
from kptn_server.run_store import RunStore
from kptn_server.slot import ProjectSlot

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


def _supervisors(app: FastAPI) -> list[RunProcessManager]:
    """Every supervisor this server has opened, for one reconciliation pass.

    Each factory sets ``app.state.supervisors`` to a callable that answers
    this for the shape it built, and this function only calls it. Sniffing
    ``app.state`` for whichever attribute happens to be set would make "where
    do the supervisors live" a question with two answers -- and the mode that
    lost would lose *silently*, reconciling one project forever while the
    rest stayed wedged.

    It is a callable and not a list because both answers are live. Single-
    project mode reads ``app.state.processes`` on every pass, because tests
    -- and the launcher, in principle -- swap a supervisor in *after* the app
    is built by assigning it, and a captured value would keep handing the
    loop the original one. Multi-project mode has one supervisor per project,
    created the first time that project is served, so the set grows while the
    server runs. A project nobody has opened has no store connection and no
    runs to reconcile, so walking the opened ones is the whole job.

    Reconciliation needs no pipeline and never touches the slot.
    """
    return list(app.state.supervisors())


async def _reconcile_periodically(app: FastAPI) -> None:
    """Run one reconciliation pass immediately, then one per interval.

    The first pass is deliberately before the first sleep: the common reason
    this server just started is that it *restarted*, and any run whose worker
    died while it was down should be settled now rather than five seconds from
    now.

    ``reconcile`` is synchronous and touches SQLite, so it goes to a worker
    thread rather than blocking the event loop. A failing pass is logged and
    retried on the next tick -- a transient "database is locked" must not take
    reconciliation out for the rest of the session, and one project's bad
    pass must not stop the projects after it in the same tick.
    """
    while True:
        for manager in _supervisors(app):
            try:
                interrupted = await asyncio.to_thread(manager.reconcile)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the loop must outlive one bad pass
                _LOGGER.exception(
                    "run reconciliation pass failed; retrying next tick"
                )
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


def normalise_root_path(root_path: str) -> str:
    """A prefix that concatenates cleanly: leading slash, no trailing one.

    ``vscode.env.asExternalUri`` yields a trailing slash, and templates join
    with ``{{ base }}/static/...``, so an unnormalised value produces
    ``//static`` -- a protocol-relative URL the browser sends to a host named
    "static". Empty stays empty, which is the loopback default.
    """
    trimmed = root_path.strip().rstrip("/")
    if not trimmed:
        return ""
    return trimmed if trimmed.startswith("/") else f"/{trimmed}"


def _build_templates(base: str = "") -> Jinja2Templates:
    """Jinja environment whose every render already knows the project.

    The base page needs the project name and the profile list on *every* page,
    so a context processor supplies them once here instead of each router
    having to remember.

    ``active_run`` is here for the same reason and one more: the app bar
    disables Run, Plan and the profile selector while a run holds the
    project lock, and the bar is on every page -- including the error pages,
    which no router renders through a shared helper. A router that had to
    remember this would eventually forget on exactly one page, and that page
    would offer a Run button whose only outcome is the 409.

    One indexed lookup by project root per render, against the same lock
    table ``POST /runs`` consults before it creates anything.

    *base* is the path prefix every emitted URL must carry when the server is
    reached through a path-prefixing proxy (VS Code for the Web forwards ports
    at ``/.../proxy/<port>/``). It is an environment global, not a context
    processor, because two run fragments are rendered with
    ``get_template(...).render(...)`` and never see a ``Request`` -- so
    request-scoped lookup would render empty in exactly those fragments.
    Empty by default, which reproduces the root-absolute markup byte for byte.
    """

    def project_context(request: Request) -> dict[str, Any]:
        current = getattr(request.state, "ui", None)
        if current is None:
            # The project list renders through this same environment with no
            # project resolved -- it is the page you pick a project *from*.
            return {}
        return {
            "project": current.entry,
            "active_run": current.store.active_run(current.entry.root),
            "project_base": current.project_base,
        }

    templates = Jinja2Templates(
        directory=str(TEMPLATES_DIR), context_processors=[project_context]
    )
    templates.env.globals["base"] = base
    return templates


def create_app(project_root: Path, root_path: str = "") -> FastAPI:
    """Build the UI application for the project rooted at *project_root*.

    Raises :class:`~kptn_server.project.ProjectError` if the directory is not
    a servable kptn project -- failing at construction rather than serving a
    UI that 500s on every page.

    *root_path* is the path prefix a reverse proxy strips before the request
    arrives. Routes stay unprefixed -- the proxy already removed it -- but
    every URL the templates emit gains it, because a root-absolute URL in the
    served HTML resolves against the proxy's host and never reaches this
    server. Empty means "served at the root", the loopback default.

    Deliberately *not* passed to ``FastAPI(root_path=...)``. ASGI's
    ``root_path`` describes a proxy that forwards the original path intact and
    only names its prefix; jupyter-server-proxy, which is what serves this in
    VS Code for the Web, strips the prefix instead. Setting it makes the
    ``/static`` mount insist on a prefix that never arrives -- 404 on every
    asset, the very failure this prefix exists to fix.
    """
    base = normalise_root_path(root_path)
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
    slot = ProjectSlot(preloaded=project)
    entry = ProjectEntry(
        slug=project.root.name,
        root=project.root,
        release="",
        display_name=project.display_name,
        profiles=project.profiles,
        database_path=project.database_path,
        run_log_dir=project.run_log_dir,
    )

    app.state.project = project
    app.state.store = store
    app.state.processes = processes
    app.state.base = base
    app.state.templates = _build_templates(base)
    # The one supervisor, read fresh on every reconciliation pass rather than
    # captured: swapping ``app.state.processes`` after construction is how a
    # test -- and, in principle, the launcher -- substitutes one, and the loop
    # has to follow. This attribute is the *only* place the loop looks; there
    # is deliberately no second copy of it in a dictionary to go stale.
    app.state.supervisors = lambda: [app.state.processes]

    # Registered *before* the resolver below, and therefore inside it.
    #
    # Starlette builds its middleware stack in reverse registration order, so
    # the last thing registered here is the outermost thing at request time.
    # The origin check has to run before any *router*, which it still does --
    # that is the property :mod:`kptn_server.origin` is about, and why the
    # check is middleware rather than a decorator a new route can forget to
    # wear. What it must not run before is the request context, because its
    # refusal is an ordinary rendered page: ``error_response`` renders through
    # ``base.html``, and that shell names the project and lists its profiles.
    # Refuse first and there is no project to name, so the 403 raises
    # ``AttributeError`` out of the middleware and the caller gets a 500 --
    # a cross-origin POST turning the defence itself into the failure.
    #
    # Resolving the context is not "work" in the sense the defence cares
    # about. It constructs a frozen dataclass and, in multi-project mode,
    # looks a slug up in a dictionary. It reads no body, loads no pipeline,
    # touches no run store and starts nothing. Every effect the defence
    # exists to prevent still happens strictly after the check.
    #
    # See :mod:`kptn_server.origin` for why ``Sec-Fetch-Site`` is the check
    # and why safe methods are exempt.
    app.middleware("http")(enforce_same_origin)

    def resolve_ui(scope: Scope) -> RequestUI:
        # Single-project mode: one project for the process's lifetime, so the
        # per-request object is constant in everything but identity. It
        # exists anyway so that routes have exactly one way to ask, in both
        # modes.
        #
        # ``store``, ``processes`` and ``templates`` are read off
        # ``app.state`` on every request rather than closed over: tests swap
        # in a mock supervisor by assigning ``app.state.processes`` after the
        # app is built, and a captured value would keep handing routes the
        # original one.
        state = scope["app"].state
        return RequestUI(
            entry=entry,
            store=state.store,
            processes=state.processes,
            slot=slot,
            base=base,
            project_base=base,
            templates=state.templates,
        )

    app.add_middleware(RequestUIMiddleware, resolver=resolve_ui)

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    # ``/healthz`` is not among the project-scoped routers any more -- it has
    # to answer without a project in multi-project mode -- so single-project
    # mode includes it here, unprefixed, keeping both endpoints where they
    # have always been.
    app.include_router(health_router)
    register_routers(app)
    return app


def create_multi_app(
    projects_root: Path, user: str, root_path: str = ""
) -> FastAPI:
    """Build a UI serving the working directories *user* has under *projects_root*.

    The routers are the single-project ones, mounted under ``/p/{slug}``. A
    dependency turns the slug into the project, its store and its supervisor,
    and puts them on the request -- so no handler signature changes and no
    handler learns that more than one project exists.

    What is *not* here is a sub-application per project. Each would need its
    own loaded pipeline, and two loaded pipelines in one process is the
    silent-wrong-code bug :mod:`kptn_server.slot` exists to prevent.

    Unlike :func:`create_app` this does not fail when a project is unusable:
    there is no one project whose brokenness should stop the server, and the
    list page reports each one's error where it can be read.

    A slug from a URL is a key in the registry's dictionary and is never
    joined onto a path. That is the whole of the traversal defence, and it is
    why a miss is a 404 rather than an attempt to read somewhere.
    """
    base = normalise_root_path(root_path)
    registry = ProjectRegistry(projects_root, user)
    registry.scan()
    slot = ProjectSlot()

    app = FastAPI(
        title="kptn",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=_lifespan,
    )
    app.state.base = base
    app.state.registry = registry
    app.state.slot = slot
    app.state.templates = _build_templates(base)
    # One store and one supervisor per project, all live at once: they hold a
    # SQLite path and a process table, and neither imports project code. Only
    # the pipeline is limited to one, through the slot above.
    #
    # ``app.state.processes`` is deliberately *not* set here and must never
    # be: there is no single supervisor on this server, and an attribute
    # named as if there were is exactly the thing a later change would reach
    # for. The reconciliation loop asks ``supervisors()`` and nothing else,
    # so it walks every project that has been opened.
    app.state.stores = {}
    app.state.managers = {}
    app.state.supervisors = lambda: list(app.state.managers.values())

    def resolve_project(request: Request) -> None:
        """Turn the URL's ``{slug}`` into this request's project.

        A FastAPI dependency and deliberately not a ``BaseHTTPMiddleware``:
        a second one of those in this stack wedges any request whose endpoint
        raises a ``BaseException`` that is not an ``Exception``, which is a
        thing ``tests/test_ui_runs.py`` does on purpose. See
        :class:`~kptn_server.context.RequestUIMiddleware` for the mechanism.

        Writing ``request.state.ui`` here writes into the ASGI scope, which
        the handler's own ``Request`` reads back -- the same channel the
        single-project resolver uses.
        """
        slug = request.path_params["slug"]
        entry = registry.resolve(slug)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"No such project: {slug}")
        store = app.state.stores.get(entry.slug)
        if store is None:
            store = RunStore(entry.database_path)
            app.state.stores[entry.slug] = store
            app.state.managers[entry.slug] = RunProcessManager(store)
        request.state.ui = RequestUI(
            entry=entry,
            store=store,
            processes=app.state.managers[entry.slug],
            slot=slot,
            base=base,
            project_base=f"{base}/p/{entry.slug}",
            templates=app.state.templates,
        )

    # Same ordering argument as in ``create_app``: the origin check must run
    # before any router, and its refusal is a rendered page. Here the page it
    # renders is the project-less shell, because the dependency that would
    # have named a project runs inside the router the check refuses to reach.
    # See :func:`kptn_server.routes.support.error_response`.
    app.middleware("http")(enforce_same_origin)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", response_class=HTMLResponse)
    def project_list(request: Request) -> HTMLResponse:
        """Which working directory to open.

        Rescans on every view so a fresh checkout appears without a restart.
        Needs no pipeline, and so never takes the slot.
        """
        entries = registry.scan()
        return app.state.templates.TemplateResponse(
            request,
            "projects.html",
            {
                "entries": entries,
                "release": registry.release,
                "projects_root": str(projects_root),
                "user": user,
            },
        )

    app.include_router(health_router)  # project-independent, stays unprefixed
    register_routers(
        app, prefix="/p/{slug}", dependencies=[Depends(resolve_project)]
    )
    return app


def create_app_for_env() -> FastAPI:
    """Build the application the ``--reload`` subprocess should serve.

    The reloader re-imports the app from an import string in a fresh
    subprocess, and an import string carries no arguments. The environment is
    the only channel, and the subprocess inherits both it and the working
    directory.
    """
    projects_root = os.environ.get("KPTN_UI_PROJECTS_ROOT", "")
    root_path = os.environ.get("KPTN_UI_ROOT_PATH", "")
    if projects_root:
        from kptn_server.registry import default_user  # noqa: PLC0415

        user = os.environ.get("KPTN_UI_USER") or default_user()
        return create_multi_app(Path(projects_root), user, root_path)
    return create_app(Path.cwd(), root_path)


#: Historical name for the reload entry point, kept because it is the import
#: string older invocations pass to uvicorn -- and because reading
#: ``Path.cwd()`` is still exactly what it does when no projects root is set.
create_app_for_cwd = create_app_for_env


__all__ = [
    "RECONCILE_INTERVAL_SECONDS",
    "STATIC_DIR",
    "TEMPLATES_DIR",
    "create_app",
    "create_app_for_cwd",
    "create_app_for_env",
    "create_multi_app",
    "normalise_root_path",
]
