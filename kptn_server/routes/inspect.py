"""The plan view and the pipeline walkthrough: what runs, in what order, why.

These are the UI's read-only pages. Nothing here starts a run, writes a run
row, takes the project lock, or edits a line of documentation.

``GET /plan``
    The table ``kptn plan`` prints, as a page. The rows come from
    :func:`kptn.runner.plan.build_plan` -- the same function the CLI calls --
    so the page cannot develop its own opinion about which tasks are stale. A
    route that walked the graph itself would render something plausible and
    disagree with ``kptn run`` in exactly the cases that matter.

``GET /walkthrough``
    Every node of the profile-resolved graph in
    :func:`kptn.inspection.inspect_pipeline`'s order (``topo_sort``, the
    runner's order), with structural nodes as headings and executable tasks
    numbered. Bypassed tasks are *shown and marked* rather than hidden: the
    graph still contains them, and a reader comparing the page to the code has
    to be able to see why a number is missing.

``GET /walkthrough/task/{name}``
    One task's detail. htmx swaps this into the walkthrough's panel, but it is
    a whole page for any request that is not htmx -- the walkthrough's rows
    are ordinary ``<a href>`` links, and this UI has to work with scripting
    switched off. See :mod:`kptn_server.routes.support`.

Three decisions worth stating outright.

**The profile is validated before anything is resolved.** An unknown profile
is a 400 that names the declared profiles, not a traceback out of the resolver
and not an empty page.

**Reading a plan must not write to the project.** ``kptn``'s SQLite state
store creates its file *and* its table on construction, and its configured
path is usually relative -- so building one per page view would leave a
database behind, possibly outside the project, just because someone opened
``/plan``. The store is therefore opened only when the project's state
database already exists; otherwise a read-only stand-in answers "nothing is
cached", which is what an unrun project's plan says anyway. The exception is
a pipeline that declares ``kptn.config(duckdb=...)``: there the factory *is*
the state store, exactly as in :func:`kptn.runner.api.resolve_pipeline`, and
the page has to go through it or it reports every task RUN while ``kptn
plan`` reads real hashes out of the same project.

**A link is only offered when it resolves.** The lineage and table-preview
surfaces are retained in :mod:`kptn_server.service` and served by
:mod:`kptn_server.routes.lineage` on this same application. A link is still
rendered only when the service can resolve the declared output to a file *and*
this app actually serves the target path, so the reader is never handed a
guaranteed 404 -- the served-path check is what makes the two conditions
independent, and it is why an app assembled without the lineage router simply
shows no links. The service module is imported lazily for a narrower reason:
it pulls in the lineage analyzer and its ``sqlglot`` dependency, and a broken
install must cost this page two links rather than cost every page its ability
to start.
"""

from __future__ import annotations

import urllib.parse
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from kptn.exceptions import KptnError
from kptn.graph.requires import gate_disjunctive
from kptn.inspection import InspectionItem, PipelineInspection, inspect_pipeline
from kptn.profiles.resolved import ResolvedGraph
from kptn.profiles.resolver import ProfileResolver
from kptn.runner.api import find_duckdb_factory
from kptn.runner.plan import PlanAction, PlanEntry, build_plan
from kptn.state_store.factory import init_state_store
from kptn.state_store.protocol import StateStoreBackend
from kptn_server.markdown import DocumentationError, render_project_markdown
from kptn_server.project import PROFILE_CONFIG_FILENAME, ProjectContext
from kptn_server.routes.support import error_response, is_fragment_request

router = APIRouter()

#: Shown wherever a task declared nothing. One spelling, one definition: a
#: page that said "n/a" in one panel and "-" in another would read as though
#: the two meant different things.
NOT_DOCUMENTED = "Not documented."

#: Where ``kptn`` keeps task state when ``kptn.yaml`` names no ``db_path``.
#: Mirrors :func:`kptn.state_store.factory.init_state_store`'s own default.
DEFAULT_STATE_DB_PATH = ".kptn/kptn.db"

#: Node kinds that represent a unit of work and therefore have a detail page.
#: The rest -- pipeline, stage, parallel, noop, config -- are structure, and
#: render as headings rather than steps.
TASK_KINDS = frozenset({"python", "sql", "r", "map"})

#: The retained lineage and table-preview endpoints (see the module docstring).
LINEAGE_PATH = "/lineage-page"
TABLE_PREVIEW_PATH = "/table-preview-fragment"


# -- profiles --------------------------------------------------------------


def _requested_profile(profile: str | None) -> str | None:
    """Normalize the query parameter: a blank selection means no profile.

    The profile ``<select>`` on these pages submits ``profile=`` for its
    ``(no profile)`` option, and "no profile" is a legitimate, documented way
    to inspect a pipeline -- the raw graph, unpruned.
    """
    return profile or None


def _unknown_profile(
    request: Request, profile: str, *, nav_active: str
) -> HTMLResponse:
    project: ProjectContext = request.app.state.project
    return error_response(
        request,
        status_code=400,
        title="Unknown profile",
        detail=(
            f"{profile!r} is not a profile of this project. "
            f"Declared profiles: {', '.join(project.profiles) or 'none'}."
        ),
        nav_active=nav_active,
    )


# -- the resolved graph and its state store --------------------------------


def _resolved_graph(project: ProjectContext, profile: str | None) -> ResolvedGraph:
    """The graph the runner would execute for *profile*.

    Mirrors :func:`kptn.runner.api.resolve_pipeline`: compile through the
    profile resolver when a profile is named, take the pipeline as-is when it
    is not, and gate disjunctive ``any_of`` requirements either way. The
    ``storage_key`` stays the *configured* (usually relative) ``db_path``
    string, because that is the key the CLI wrote its hashes under -- only the
    location of the database file is made absolute, further down.
    """
    if profile is not None:
        resolved = ProfileResolver(project.config).compile(project.pipeline, profile)
    else:
        resolved = ResolvedGraph(
            graph=project.pipeline,
            pipeline=project.pipeline.name,
            storage_key=project.config.settings.db_path or DEFAULT_STATE_DB_PATH,
        )
    return ResolvedGraph(
        graph=gate_disjunctive(resolved.graph),
        pipeline=resolved.pipeline,
        storage_key=resolved.storage_key,
        bypassed_names=resolved.bypassed_names,
        profile_args=resolved.profile_args,
    )


class _NeverRunStateStore:
    """A state store for a project that has no state database yet.

    Answers every read with "nothing recorded", which is precisely the truth
    for a project that has never run, and refuses writes: no page in this UI
    has any business recording a task hash. Exists so that opening ``/plan``
    on a fresh project does not *create* the project's state store as a side
    effect of rendering a page.
    """

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None:
        return None

    def write_hash(self, storage_key: str, pipeline: str, task: str, hash: str) -> None:  # noqa: A002 - protocol parameter name
        raise RuntimeError("the plan view never writes task state")

    def delete(self, storage_key: str, pipeline: str, task: str) -> None:
        raise RuntimeError("the plan view never deletes task state")

    def list_tasks(self, storage_key: str, pipeline: str) -> list[str]:
        return []


def state_database_path(project: ProjectContext) -> Path:
    """Absolute location of the project's own task-state database."""
    configured = Path(project.config.settings.db_path or DEFAULT_STATE_DB_PATH)
    return configured if configured.is_absolute() else project.root / configured


@contextmanager
def _state_store(project: ProjectContext) -> Iterator[StateStoreBackend]:
    """The store ``kptn plan`` would read, for the length of one request.

    Built the way :func:`kptn.runner.api.resolve_pipeline` builds it, because
    the two have to answer the same question the same way. When the pipeline
    declares ``kptn.config(duckdb=...)`` the factory *is* the state store:
    hashes live in the pipeline's own analytical database, and the configured
    ``db_path`` may name a file that does not exist at all. A page that
    ignored the factory would find no file, fall back to "nothing cached",
    and report every task RUN while ``kptn plan``, one terminal away, read
    real hashes.

    Only the pathwise case keeps the stand-in: opening ``/plan`` on a project
    that has never run must not *create* a state database as a side effect of
    rendering a page, and "nothing recorded" is what an unrun project's plan
    says anyway.

    The connection is closed on the way out unless it came from the project's
    factory, whose connection this module borrows and does not own.
    """
    factory, _ = find_duckdb_factory(project.pipeline)
    if factory is not None:
        yield init_state_store(project.config.settings, duckdb_factory=factory)
        return

    path = state_database_path(project)
    if not path.exists():
        yield _NeverRunStateStore()
        return

    settings = project.config.settings.model_copy(update={"db_path": str(path)})
    store = init_state_store(settings)
    try:
        yield store
    finally:
        _release(store)


def _release(store: StateStoreBackend) -> None:
    """Close a state store opened for one request.

    ``DuckDbBackend`` exposes ``close``; ``SqliteBackend`` does not, and holds
    its connection on ``_conn``. A page view must not leak a database
    connection per request either way, so both shapes are handled here rather
    than left to the garbage collector.
    """
    closer = getattr(store, "close", None)
    if callable(closer):
        closer()
        return
    connection = getattr(store, "_conn", None)
    if connection is not None and callable(getattr(connection, "close", None)):
        connection.close()


# -- GET /plan -------------------------------------------------------------


def _plan_row(entry: PlanEntry) -> dict[str, Any]:
    """One plan entry as the template's row.

    ``PlanAction`` is a ``StrEnum`` whose values are lowercase; the terminal
    renders them uppercased in brackets and so does this page, so a developer
    reading both sees the same words. A ``MAP`` entry carries no reason and a
    provider instead -- the same sentence
    :func:`kptn.runner.plan._format_plan_status_line` prints.
    """
    reason = entry.reason
    if entry.action is PlanAction.MAP and entry.provider:
        reason = f"dynamic, expands after {entry.provider}"
    return {
        "task_name": entry.task_name,
        "action": entry.action.name,
        "reason": reason,
    }


@router.get("/plan", response_class=HTMLResponse)
def plan_view(request: Request, profile: str | None = None) -> HTMLResponse:
    """What ``kptn run`` would do next, without doing any of it."""
    project: ProjectContext = request.app.state.project
    if profile and profile not in project.profiles:
        return _unknown_profile(request, profile, nav_active="plan")
    selected = _requested_profile(profile)

    try:
        resolved = _resolved_graph(project, selected)
        with _state_store(project) as store:
            entries = build_plan(resolved, store)
    except (KptnError, ValueError, OSError) as exc:
        return error_response(
            request,
            status_code=500,
            title="The plan could not be built",
            detail=f"{type(exc).__name__}: {exc}",
            nav_active="plan",
        )

    return request.app.state.templates.TemplateResponse(
        request,
        "plan.html",
        {
            "nav_active": "plan",
            "profile_action": "/plan",
            "selected_profile": selected,
            "rows": [_plan_row(entry) for entry in entries],
        },
    )


# -- GET /walkthrough ------------------------------------------------------


def _inspection(project: ProjectContext, profile: str | None) -> PipelineInspection:
    return inspect_pipeline(project.pipeline, project.config, profile, project.root)


def _detail_url(name: str, profile: str | None) -> str:
    query = urllib.parse.urlencode({"profile": profile}) if profile else ""
    path = f"/walkthrough/task/{urllib.parse.quote(name, safe='')}"
    return f"{path}?{query}" if query else path


def _invalid_documentation(
    request: Request, exc: Exception, *, nav_active: str
) -> HTMLResponse:
    """A project whose ``docs`` reference escapes the project root.

    ``inspect_pipeline`` refuses the reference while building the read model,
    so this fires before any file is opened -- and it is a page, not a blank
    panel, because a project that cannot be inspected at all is not something
    to render half of.
    """
    return error_response(
        request,
        status_code=500,
        title="This pipeline's documentation cannot be read",
        detail=(
            f"{exc} Documentation must live inside the project, and "
            "the reference is declared on a task, Stage, or Pipeline in the "
            "project's own code."
        ),
        nav_active=nav_active,
    )


@router.get("/walkthrough", response_class=HTMLResponse)
def walkthrough(request: Request, profile: str | None = None) -> HTMLResponse:
    """The resolved pipeline, in order, as a reader would walk it."""
    project: ProjectContext = request.app.state.project
    if profile and profile not in project.profiles:
        return _unknown_profile(request, profile, nav_active="docs")
    selected = _requested_profile(profile)

    try:
        inspection = _inspection(project, selected)
    except ValueError as exc:
        return _invalid_documentation(request, exc, nav_active="docs")
    except KptnError as exc:
        return error_response(
            request,
            status_code=500,
            title="This pipeline could not be inspected",
            detail=f"{type(exc).__name__}: {exc}",
            nav_active="docs",
        )

    return request.app.state.templates.TemplateResponse(
        request,
        "walkthrough.html",
        {
            "nav_active": "docs",
            "profile_action": "/walkthrough",
            "selected_profile": selected,
            "inspection": inspection,
            "rows": [
                {
                    "item": item,
                    "detail_url": _detail_url(item.name, selected),
                    "linkable": item.kind in TASK_KINDS,
                }
                for item in inspection.items
            ],
            "not_documented": NOT_DOCUMENTED,
        },
    )


# -- GET /walkthrough/task/{name} ------------------------------------------


def _docs_reference(project: ProjectContext, item: InspectionItem) -> str | None:
    """The item's ``docs`` reference, back in project-relative form.

    ``InspectionItem`` carries the *resolved* path, already checked against
    the project root. Handing the renderer the relative form means the
    containment check runs a second time, on the value the page is about to
    read, rather than being trusted from one layer away.
    """
    if item.docs_path is None:
        return None
    try:
        relative = item.docs_path.relative_to(project.root)
    except ValueError:
        # Not reachable through inspect_pipeline, which refuses such a path
        # outright. Kept as a refusal rather than an absolute-path read.
        return str(item.docs_path)
    reference = relative.as_posix()
    return f"{reference}#{item.docs_anchor}" if item.docs_anchor else reference


def _rendered_docs(
    project: ProjectContext, item: InspectionItem
) -> tuple[Any | None, str | None]:
    """``(html, error)`` for this item's documentation file.

    Exactly one of the two is set when a ``docs`` reference exists. A missing
    or unreadable file becomes a visible message: a blank panel would be
    indistinguishable from a task that documented nothing.
    """
    reference = _docs_reference(project, item)
    if reference is None:
        return None, None
    try:
        return render_project_markdown(project.root, reference), None
    except DocumentationError as exc:
        return None, str(exc)


def _served_paths(request: Request) -> set[str]:
    return {
        route.path
        for route in request.app.router.routes
        if isinstance(getattr(route, "path", None), str)
    }


def _resolver() -> tuple[Any, Any] | None:
    """The retained service's table mapping and name normalizer, if available.

    Imported here rather than at module scope because
    :mod:`kptn_server.service` pulls in the lineage analyzer, which imports
    ``sqlglot``. The ``web`` extra declares ``sqlglot``, so this normally
    succeeds; the guard is for an install that lacks it, where a missing
    lineage stack must cost the walkthrough its two links rather than cost the
    whole UI its ability to start.

    Returns ``None`` when the service cannot be imported. It is also the seam
    the link tests replace, so that a test about *linking* does not depend on
    a real project having a built DuckDB file.
    """
    try:
        from kptn_server.service import (  # noqa: PLC0415 - see docstring
            _normalize_table_name,
            build_table_file_map,
        )
    except ImportError:
        return None
    return build_table_file_map, _normalize_table_name


def output_links(
    request: Request, project: ProjectContext, outputs: Sequence[str]
) -> dict[str, list[dict[str, str]]]:
    """Lineage and table-preview links, per declared output, when resolvable.

    Two conditions, both required. The *service* has to be able to resolve the
    output name to a file through ``kptn.yaml``'s ``tasks`` mapping -- that is
    what makes lineage and a preview meaningful for that name -- and this
    application has to actually serve the endpoint, or the link is a 404 with
    extra steps.
    """
    if not outputs:
        return {}
    config_path = project.root / PROFILE_CONFIG_FILENAME
    if not config_path.is_file():
        return {}

    resolution = _resolver()
    if resolution is None:
        return {}
    build_table_file_map, normalize = resolution

    table_map = build_table_file_map(config_path)
    if not table_map:
        return {}

    served = _served_paths(request)
    targets = [
        (LINEAGE_PATH, "Lineage", False),
        (TABLE_PREVIEW_PATH, "Preview", True),
    ]
    links: dict[str, list[dict[str, str]]] = {}
    for output in outputs:
        normalized = normalize(output)
        if not normalized or normalized not in table_map:
            continue
        for path, label, needs_table in targets:
            if path not in served:
                continue
            query: dict[str, str] = {"configPath": str(config_path)}
            if needs_table:
                query["table"] = output
            links.setdefault(output, []).append(
                {"href": f"{path}?{urllib.parse.urlencode(query)}", "label": label}
            )
    return links


def _source_reference(item: InspectionItem) -> str | None:
    """``path:line`` for the code, or just the path when there is no line.

    SQL and R tasks are a file, not a function, so they have no line to name;
    a Python task whose source cannot be located has neither, and the page
    says "Not documented." rather than rendering a bare colon.
    """
    if item.source_path is None:
        return None
    if item.source_line is None:
        return str(item.source_path)
    return f"{item.source_path}:{item.source_line}"


@router.get("/walkthrough/task/{name}", response_class=HTMLResponse)
def task_detail(
    request: Request, name: str, profile: str | None = None
) -> HTMLResponse:
    """One task, in full: metadata, declared data, source, documentation."""
    project: ProjectContext = request.app.state.project
    if profile and profile not in project.profiles:
        return _unknown_profile(request, profile, nav_active="docs")
    selected = _requested_profile(profile)

    try:
        inspection = _inspection(project, selected)
    except ValueError as exc:
        return _invalid_documentation(request, exc, nav_active="docs")
    except KptnError as exc:
        return error_response(
            request,
            status_code=500,
            title="This pipeline could not be inspected",
            detail=f"{type(exc).__name__}: {exc}",
            nav_active="docs",
        )

    item = _first_named(inspection.items, name)
    if item is None:
        return error_response(
            request,
            status_code=404,
            title="No such task",
            detail=(
                f"{name!r} is not in this pipeline's resolved graph"
                + (f" for profile {selected!r}." if selected else ".")
            ),
            nav_active="docs",
        )

    docs_html, docs_error = _rendered_docs(project, item)
    template = (
        "_task_detail.html" if is_fragment_request(request) else "task_detail.html"
    )
    return request.app.state.templates.TemplateResponse(
        request,
        template,
        {
            "nav_active": "docs",
            "profile_action": "/walkthrough",
            "selected_profile": selected,
            "item": item,
            "source_reference": _source_reference(item),
            "docs_html": docs_html,
            "docs_error": docs_error,
            "output_links": output_links(request, project, item.outputs),
            "not_documented": NOT_DOCUMENTED,
        },
    )


def _first_named(items: Iterable[InspectionItem], name: str) -> InspectionItem | None:
    return next((item for item in items if item.name == name), None)


__all__ = [
    "LINEAGE_PATH",
    "TASK_KINDS",
    "NOT_DOCUMENTED",
    "TABLE_PREVIEW_PATH",
    "output_links",
    "router",
    "state_database_path",
]
