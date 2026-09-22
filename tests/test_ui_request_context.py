"""The per-request view of "which project is this".

Routes used to read `request.app.state.project`, which can only ever name one
project. They now read a request-scoped object instead. Single-project mode
fills it from the app, so its behaviour is unchanged -- that is what the
first test here pins.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from kptn_server.app import create_app
from kptn_server.context import RequestUI, RequestUIMiddleware
from kptn_server.origin import enforce_same_origin

# Every test here loads the fixture project, which prepends its root to
# ``sys.path`` and evicts project modules from ``sys.modules``. See
# ``tests/conftest.py`` for why that has to be undone between tests.
pytestmark = pytest.mark.ui_hygiene


def test_single_project_mode_exposes_the_served_project(ui_project: Path) -> None:
    app = create_app(ui_project)
    captured: list[RequestUI] = []

    @app.get("/__probe")
    def probe(request: Request):  # type: ignore[no-untyped-def]
        captured.append(request.state.ui)
        return {}

    TestClient(app).get("/__probe")

    (found,) = captured
    assert found.entry.root == ui_project.resolve()
    assert found.project_base == ""
    assert found.base == ""


def test_the_slot_yields_the_served_project(ui_project: Path) -> None:
    app = create_app(ui_project)
    captured: list[str] = []

    @app.get("/__probe")
    def probe(request: Request):  # type: ignore[no-untyped-def]
        with request.state.ui.project() as project:
            captured.append(str(project.root))
        return {}

    TestClient(app).get("/__probe")

    assert captured == [str(ui_project.resolve())]


def test_display_name_is_the_pipeline_name_in_single_project_mode(
    ui_project: Path,
) -> None:
    """The app bar's title must not change on the desktop."""
    from kptn_server.project import ProjectContext

    project = ProjectContext.load(ui_project)

    assert project.display_name == project.pipeline_name


# -- how the resolver is wired into the stack ------------------------------
#
# Both assertions below are about the middleware stack rather than about a
# response, because both were found as *silent* failures: one turned the
# cross-origin defence into a 500, and the other turned a passing test into a
# hang with no traceback. A response-level test catches each of them only
# after something has already gone badly wrong.


def test_the_resolver_wraps_the_origin_guard(ui_project: Path) -> None:
    """The context is resolved before the same-origin check refuses anything.

    Starlette builds the stack in reverse registration order, so the *last*
    entry in ``user_middleware`` is the innermost. The origin guard's refusal
    is a rendered page, and the shell it renders through names the project --
    so refusing before the project is resolved raises out of the middleware
    and answers a cross-origin POST with a 500 instead of a 403.
    """
    app = create_app(ui_project)

    stack = [middleware.cls for middleware in app.user_middleware]

    assert stack[0] is RequestUIMiddleware
    assert stack.index(RequestUIMiddleware) < stack.index(BaseHTTPMiddleware)


def test_the_resolver_adds_no_base_http_middleware(ui_project: Path) -> None:
    """Resolving the context must stay raw ASGI.

    ``BaseHTTPMiddleware`` runs the rest of the application in an anyio task
    group and pumps the response back over a memory stream. A
    ``BaseException`` that is not an ``Exception`` -- the ``KeyboardInterrupt``
    that ``tests/test_ui_runs.py`` raises to pin that an interrupted launch
    releases the project lock -- escapes the inner task without completing
    that handshake. One such middleware unwinds; two wedge the request
    forever. There is exactly one, and it is the origin guard.
    """
    app = create_app(ui_project)

    wrapped = [m for m in app.user_middleware if m.cls is BaseHTTPMiddleware]

    assert len(wrapped) == 1
    assert wrapped[0].kwargs["dispatch"] is enforce_same_origin
