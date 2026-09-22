"""Serving the UI behind a path-prefixing reverse proxy.

VS Code for the Web forwards a loopback port at a *path*, not a host: the
extension's webview frames

    https://<host>/notebook/user/<user>/vscode/proxy/37657/

and the proxy strips that prefix before the request reaches uvicorn. Routes
therefore stay unprefixed -- but every URL the server *emits* must carry the
prefix, or the browser resolves it against the host root and escapes the proxy
entirely. That is what made the UI load as unstyled HTML in production: the
page arrived, and ``/static/app.css`` 404ed one directory structure away.

The prefix is one value for the life of the process, so it is a Jinja
environment global rather than per-request state. Two of the run fragments are
rendered with ``get_template(...).render(...)`` and never see a ``Request``, so
anything request-scoped would render empty in exactly those fragments.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import create_app

PREFIX = "/notebook/user/rruizesparza/vscode/proxy/37657"


@pytest.fixture
def prefixed_client(ui_project: Path) -> TestClient:
    return TestClient(create_app(ui_project, root_path=PREFIX))


@pytest.fixture
def bare_client(ui_project: Path) -> TestClient:
    return TestClient(create_app(ui_project))


def test_asset_urls_carry_the_prefix(prefixed_client: TestClient) -> None:
    """The stylesheet and scripts are what break first, and most visibly."""
    body = prefixed_client.get("/").text

    assert f'href="{PREFIX}/static/app.css"' in body
    assert f'src="{PREFIX}/static/htmx.min.js"' in body
    assert f'src="{PREFIX}/static/app.js"' in body


def test_no_unprefixed_absolute_urls_remain(prefixed_client: TestClient) -> None:
    """A single missed URL is a broken link, so assert the absence directly.

    Root-absolute URLs are exactly the ones that escape the proxy. Anything
    starting with the prefix is fine, as is anything relative.
    """
    body = prefixed_client.get("/").text

    offenders = [
        fragment
        for attribute in ("href", "src", "action", "hx-get", "hx-post")
        for fragment in _absolute_urls(body, attribute)
        if not fragment.startswith(PREFIX)
    ]
    assert offenders == []


def test_form_actions_carry_the_prefix(prefixed_client: TestClient) -> None:
    """A form posting to the host root reaches the proxy, not the server."""
    body = prefixed_client.get("/").text

    assert 'action="/runs"' not in body
    assert f'action="{PREFIX}/runs"' in body


def test_default_serving_is_unchanged(bare_client: TestClient) -> None:
    """With no prefix the markup must be exactly what it always was.

    The overwhelmingly common case is a developer on loopback. This is the
    regression guard for them.
    """
    body = bare_client.get("/").text

    assert 'href="/static/app.css"' in body
    assert 'src="/static/htmx.min.js"' in body
    assert 'action="/runs"' in body
    assert "//static" not in body


def test_static_files_are_still_served_unprefixed(prefixed_client: TestClient) -> None:
    """The proxy strips the prefix, so the ASGI app must not expect it."""
    assert prefixed_client.get("/static/app.css").status_code == 200


def test_fragments_rendered_without_a_request_are_prefixed(
    prefixed_client: TestClient,
) -> None:
    """``_run_header`` and ``_error`` render through ``get_template``.

    They never receive a ``Request``, which is why the prefix is an
    environment global. Rendering one directly is the only way to catch a
    regression that reintroduces request-scoped lookup.
    """
    templates = prefixed_client.app.state.templates  # ty: ignore[unresolved-attribute]

    rendered = templates.get_template("_error.html").render(
        message="boom", error_run_id="run-1"
    )

    assert f'href="{PREFIX}/runs/run-1"' in rendered


def test_the_prefix_is_not_given_to_asgi_root_path(ui_project: Path) -> None:
    """The prefix must stay out of ``FastAPI(root_path=...)``.

    ASGI's ``root_path`` describes a proxy that forwards the original path and
    merely names its prefix. jupyter-server-proxy strips the prefix instead,
    so with ``root_path`` set Starlette's ``/static`` mount requires a prefix
    that never arrives and 404s every asset -- verified against starlette
    0.49.1. This asserts the mistake is not reintroduced.
    """
    app = create_app(ui_project, root_path=PREFIX)

    assert app.root_path == ""
    assert app.state.base == PREFIX


def test_trailing_slash_is_normalised(ui_project: Path) -> None:
    """``asExternalUri`` yields a trailing slash; joining it blindly doubles it."""
    app = create_app(ui_project, root_path=f"{PREFIX}/")

    with TestClient(app) as client:
        body = client.get("/").text

    assert f'href="{PREFIX}/static/app.css"' in body
    assert "//static" not in body


def _absolute_urls(body: str, attribute: str) -> list[str]:
    """Every ``attribute="/..."`` value in *body*."""
    import re

    return re.findall(rf'{attribute}="(/[^"]*)"', body)


# ---------------------------------------------------------------------------
# Redirects
# ---------------------------------------------------------------------------
#
# The prefix must NOT appear in a ``Location`` header. jupyter-server-proxy
# re-prefixes any root-absolute Location itself, so a prefixed one arrives at
# the browser doubled -- observed in production as
#
#   /notebook/hub/proxy/45555/notebook/user/me/vscode/proxy/45555/runs/<id>
#
# which 404s. A relative reference has no absolute path to rewrite, and the
# browser resolves it against the request URL, which is already correct.
# These tests resolve the header the way a browser would, with ``urljoin``.

BROWSER_ROOT = f"https://nph-rs-1.example.org{PREFIX}"


def test_create_run_redirect_is_relative(prefixed_client: TestClient) -> None:
    response = prefixed_client.post(
        "/runs", data={"profile": "success"}, follow_redirects=False
    )

    assert response.status_code == 303
    location = response.headers["location"]
    assert not location.startswith("/"), (
        f"root-absolute Location gets re-prefixed: {location}"
    )
    assert PREFIX not in location


def test_create_run_redirect_resolves_to_the_run_page(
    prefixed_client: TestClient,
) -> None:
    """What the browser actually does with the header we send."""
    from urllib.parse import urljoin

    response = prefixed_client.post(
        "/runs", data={"profile": "success"}, follow_redirects=False
    )
    run_id = response.headers["location"].rsplit("/", 1)[-1]

    resolved = urljoin(f"{BROWSER_ROOT}/runs", response.headers["location"])

    assert resolved == f"{BROWSER_ROOT}/runs/{run_id}"


def test_stop_redirect_resolves_to_the_run_page(prefixed_client: TestClient) -> None:
    """Stop posts one level deeper, so its relative target differs."""
    from urllib.parse import urljoin

    created = prefixed_client.post(
        "/runs", data={"profile": "slow"}, follow_redirects=False
    )
    run_id = created.headers["location"].rsplit("/", 1)[-1]

    response = prefixed_client.post(f"/runs/{run_id}/stop", follow_redirects=False)

    assert response.status_code == 303
    location = response.headers["location"]
    assert not location.startswith("/"), location
    resolved = urljoin(f"{BROWSER_ROOT}/runs/{run_id}/stop", location)
    assert resolved == f"{BROWSER_ROOT}/runs/{run_id}"


def test_redirects_are_prefix_independent(bare_client: TestClient) -> None:
    """The same relative header has to work with no proxy in front, too."""
    from urllib.parse import urljoin

    response = bare_client.post(
        "/runs", data={"profile": "success"}, follow_redirects=False
    )
    run_id = response.headers["location"].rsplit("/", 1)[-1]

    resolved = urljoin("http://127.0.0.1:8000/runs", response.headers["location"])

    assert resolved == f"http://127.0.0.1:8000/runs/{run_id}"
