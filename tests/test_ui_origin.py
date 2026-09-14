"""Tests for the cross-origin defence on every state-changing route.

The attack this closes is not hypothetical arithmetic. ``POST /runs`` accepts
``application/x-www-form-urlencoded``, which makes it a *simple* request: a
browser sends it cross-origin with no preflight and no CORS check to fail. So
any page a developer visits while ``kptn ui`` is running could submit a hidden
form at ``http://127.0.0.1:8000/runs`` and execute their pipeline. The
attacker never sees the response; the run is the effect.

Two properties are pinned here.

1. **Every** state-changing route refuses a cross-origin request -- the check
   is middleware, not a per-route habit a new route can forget. The list below
   is derived from the app's own routing table, so a new POST that skips the
   defence fails this module rather than shipping.
2. A body with no ``profile`` key is refused. That is the payload the attack
   sends -- no fields at all -- and it used to read as "(no profile)" and
   start a run.

Reads are never refused, and neither is a request with no ``Sec-Fetch-Site``
header: an absent header cannot be a cross-site *browser* request, and
refusing it would break ``curl``, the VS Code extension, and every test in
this suite for no gain.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import create_app
from kptn_server.origin import REFUSAL_TITLE, SAFE_METHODS, is_cross_site
from kptn_server.processes import ProcessIdentity

pytestmark = pytest.mark.ui_hygiene

FORM = {"Content-Type": "application/x-www-form-urlencoded"}


@pytest.fixture
def app(ui_project: Path):
    application = create_app(ui_project)
    manager = MagicMock()
    manager.start.return_value = ProcessIdentity(pid=4321, started_at=1.0)
    application.state.processes = manager
    return application


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def state_changing_routes(app) -> list[tuple[str, str]]:
    """``(method, path)`` for every route on this app that can change state.

    Read off the router rather than hand-listed, so adding a POST without a
    defence is a failure here and not a discovery in review.
    """
    found: list[tuple[str, str]] = []
    for route in app.router.routes:
        methods = getattr(route, "methods", None) or set()
        path = getattr(route, "path", None)
        if not isinstance(path, str):
            continue
        for method in sorted(methods):
            if method.upper() not in SAFE_METHODS:
                found.append((method.upper(), path))
    return found


def _concrete(path: str) -> str:
    return path.replace("{run_id}", "does-not-exist")


# -- the decision itself ---------------------------------------------------


@pytest.mark.parametrize(
    ("method", "site", "refused"),
    [
        ("POST", "cross-site", True),
        ("POST", "same-site", True),
        ("POST", "Cross-Site", True),
        ("POST", "same-origin", False),
        ("POST", "none", False),
        ("POST", None, False),
        ("POST", "", False),
        ("GET", "cross-site", False),
        ("HEAD", "cross-site", False),
        ("OPTIONS", "cross-site", False),
    ],
)
def test_cross_site_decision(method: str, site: str | None, refused: bool) -> None:
    assert is_cross_site(method, site) is refused


# -- every state-changing route ---------------------------------------------


def test_the_app_has_state_changing_routes(app) -> None:
    """Guards the parametrization below against silently covering nothing."""
    routes = state_changing_routes(app)
    paths = {path for _, path in routes}
    assert {"/runs", "/runs/{run_id}/stop", "/runs/{run_id}/force-finish"} <= paths
    assert "/table-preview-query" in paths


def test_every_state_changing_route_refuses_a_cross_origin_request(
    app, client: TestClient
) -> None:
    routes = state_changing_routes(app)
    assert routes  # see the test above

    for method, path in routes:
        response = client.request(
            method,
            _concrete(path),
            headers={**FORM, "Sec-Fetch-Site": "cross-site"},
            content="profile=",
            follow_redirects=False,
        )
        assert response.status_code == 403, (method, path, response.status_code)
        assert REFUSAL_TITLE in response.text, (method, path)


def test_a_cross_origin_post_does_not_start_a_run(app, client: TestClient) -> None:
    """The whole point: the refusal happens before any run row exists."""
    response = client.post(
        "/runs",
        headers={**FORM, "Sec-Fetch-Site": "cross-site"},
        content="",
        follow_redirects=False,
    )

    assert response.status_code == 403
    app.state.processes.start.assert_not_called()
    assert app.state.store.active_run(app.state.project.root) is None


def test_a_same_origin_post_still_starts_a_run(app, client: TestClient) -> None:
    response = client.post(
        "/runs",
        headers={**FORM, "Sec-Fetch-Site": "same-origin"},
        content="profile=success",
        follow_redirects=False,
    )

    assert response.status_code == 303
    app.state.processes.start.assert_called_once()


def test_a_post_with_no_fetch_site_header_still_starts_a_run(
    app, client: TestClient
) -> None:
    """``curl``, the extension, and this suite send no ``Sec-Fetch-Site``."""
    response = client.post(
        "/runs",
        headers=FORM,
        content="profile=success",
        follow_redirects=False,
    )

    assert response.status_code == 303
    app.state.processes.start.assert_called_once()


def test_reads_are_never_refused(client: TestClient) -> None:
    response = client.get("/plan", headers={"Sec-Fetch-Site": "cross-site"})
    assert response.status_code == 200


# -- the payload the attack sends ------------------------------------------


def test_a_form_body_without_a_profile_key_is_refused(app, client: TestClient) -> None:
    """No fields at all is not this UI's form, and must not start a run."""
    response = client.post(
        "/runs",
        headers=FORM,
        content="",
        follow_redirects=False,
    )

    assert response.status_code == 400
    assert "profile" in response.text
    app.state.processes.start.assert_not_called()
    assert app.state.store.active_run(app.state.project.root) is None


def test_an_empty_profile_value_is_still_accepted(app, client: TestClient) -> None:
    """``profile=`` is the ``(no profile)`` option, and stays legal."""
    response = client.post(
        "/runs",
        headers=FORM,
        content="profile=",
        follow_redirects=False,
    )

    assert response.status_code == 303
    app.state.processes.start.assert_called_once()
