"""The retained lineage and table-preview surfaces, on the shared app.

Task 10 could only assert that the walkthrough *would* link to these if
something served them: ``sqlglot`` was in no extra, so
``kptn_server.service`` was unimportable and the endpoints lived on a second
FastAPI application built for the deleted React frontend. Both halves are now
fixed -- the ``web`` extra declares the parser, and the routes are registered
by :func:`kptn_server.routes.register_routers` -- so this module follows the
links the way a reader would and checks that something renders at the far end.

Three properties.

1. **The link the walkthrough renders resolves.** The test does not construct
   a URL; it scrapes the ``href`` out of the task-detail page and requests
   *that*, so a mismatch between the link and the route is a failure here.

2. **``configPath`` is confined to the served project.** These routes inherited
   an arbitrary-path query parameter from an application that served a project
   picker. This one serves a single project, binds loopback, and has no
   authentication, so any other path is refused rather than read.

3. **The reader is never handed a guaranteed 404.** A preview of a table whose
   database was never built comes back as a *message* in the fragment, not an
   error: an unrun project is an ordinary state.

Nothing here starts a run.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import create_app, create_multi_app
from kptn_server.assets import asset_version
from kptn_server.routes import inspect as inspect_routes
from kptn_server.routes import lineage as lineage_routes

pytestmark = pytest.mark.ui_hygiene


PYPROJECT = """\
[project]
name = "lineage-fixture"
version = "0.0.0"

[tool.kptn]
pipeline = "lineage_pipeline"
"""

#: A project whose one task declares an output that ``kptn.yaml`` maps to a
#: real SQL file -- which is exactly the condition ``output_links`` requires.
PIPELINE = """\
import kptn


@kptn.task(outputs=["main.widgets"], description="Builds the widgets table.")
def build_widgets() -> None:
    return None


pipeline = kptn.Pipeline("lineage_fixture", build_widgets)
"""

KPTN_YAML = """\
settings:
  db: sqlite
  db_path: .kptn/kptn.db

tasks:
  build_widgets:
    file: src/build_widgets.sql
    outputs:
      - duckdb://main.widgets
"""

BUILD_WIDGETS_SQL = """\
create or replace table main.widgets as
select 1 as id, 'widget' as name;
"""


@pytest.fixture
def lineage_project(tmp_path: Path) -> Path:
    root = tmp_path / "lineage_project"
    (root / "src").mkdir(parents=True)
    (root / "pyproject.toml").write_text(PYPROJECT, encoding="utf-8")
    (root / "lineage_pipeline.py").write_text(PIPELINE, encoding="utf-8")
    (root / "kptn.yaml").write_text(KPTN_YAML, encoding="utf-8")
    (root / "src" / "build_widgets.sql").write_text(BUILD_WIDGETS_SQL, encoding="utf-8")
    return root


@pytest.fixture
def client(lineage_project: Path) -> TestClient:
    return TestClient(create_app(lineage_project))


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    """Pin the ``pytestmark`` opt-in; losing it silently disarms the fixtures."""
    assert request.node.get_closest_marker("ui_hygiene") is not None


def _links(
    client: TestClient, path: str = "/walkthrough/task/build_widgets"
) -> dict[str, str]:
    """``{label: href}`` for the output links on the task-detail page."""
    body = client.get(path).text
    return {
        match.group(2): match.group(1).replace("&amp;", "&")
        for match in re.finditer(
            r'<a[^>]+href="([^"]*(?:lineage-page|table-preview-fragment)[^"]*)"[^>]*>'
            r"\s*([A-Za-z]+)\s*</a>",
            body,
        )
    }


def test_the_walkthrough_offers_both_links_for_a_mapped_output(
    client: TestClient,
) -> None:
    """No stub resolver: the real service resolves this project's output."""
    links = _links(client)

    assert set(links) == {"Lineage", "Preview"}
    assert links["Lineage"].startswith(inspect_routes.LINEAGE_PATH)
    assert links["Preview"].startswith(inspect_routes.TABLE_PREVIEW_PATH)


def test_the_lineage_link_renders_a_lineage_page(client: TestClient) -> None:
    """Follow the rendered href and get the graph, not a 404 or a traceback."""
    href = _links(client)["Lineage"]

    response = client.get(href)

    assert response.status_code == 200, response.text
    body = response.text
    assert "<title>kptn Lineage</title>" in body
    # Vendored assets only -- the page pulls Alpine from this server's own
    # /static mount, never from a CDN.
    assert f'src="/static/alpine.min.js?v={asset_version("alpine.min.js")}"' in body
    # The analyzer really parsed the project's SQL: the table it creates is
    # named in the rendered graph.
    assert "widgets" in body
    assert "could not be built" not in body


def test_the_lineage_fragment_renders_without_the_document_shell(
    client: TestClient, lineage_project: Path
) -> None:
    """The htmx form of the same page: the graph inside a swappable wrapper.

    The wrapper is what an ``hx-target`` replaces. (The retained graph
    renderer emits its own document inside it -- a pre-existing quirk of
    ``kptn.lineage.html_renderer``, not something this route decides -- so the
    assertion is on the wrapper, which is the part the route owns.)
    """
    config = str(lineage_project / "kptn.yaml")

    response = client.get(f"/lineage-fragment?configPath={config}")

    assert response.status_code == 200, response.text
    assert response.text.lstrip().startswith('<div id="kptn-lineage-fragment">')
    assert response.text.rstrip().endswith("</div>")
    # The route's own shell is absent: no <html> wrapper from this template.
    assert "<title>kptn Lineage</title>" not in response.text
    assert "widgets" in response.text


def test_the_preview_link_renders_a_fragment_for_an_unbuilt_table(
    client: TestClient,
) -> None:
    """A project nobody has run yet gets a message, not an error page.

    The link is offered because the *output* resolves to a file. Whether the
    database exists is a separate question, and the honest answer is a
    sentence in the panel.
    """
    href = _links(client)["Preview"]

    response = client.get(href)

    assert response.status_code == 200, response.text
    assert "table-preview" in response.text


def test_a_foreign_config_path_is_refused_by_every_lineage_route(
    client: TestClient, tmp_path: Path
) -> None:
    """These routes read this project's kptn.yaml and no other file.

    Loopback and unauthenticated is only a defensible posture while the server
    cannot be pointed at arbitrary paths on the machine.
    """
    intruder = tmp_path / "elsewhere" / "kptn.yaml"
    intruder.parent.mkdir(parents=True)
    intruder.write_text(KPTN_YAML, encoding="utf-8")
    foreign = str(intruder)

    html_routes = [
        f"{lineage_routes.router.routes[0].path}?configPath={foreign}",
        f"/lineage-fragment?configPath={foreign}",
        f"/table-preview-fragment?configPath={foreign}&table=main.widgets",
    ]
    for url in html_routes:
        response = client.get(url)
        assert response.status_code == 400, url
        # The title renders HTML-escaped, so assert on the detail sentence.
        assert "only read the resolved project" in response.text, url

    columns = client.get(f"/table-columns?configPath={foreign}&table=main.widgets")
    assert columns.status_code == 400
    assert columns.json()["detail"] == lineage_routes.FOREIGN_CONFIG_DETAIL

    query = client.post(
        "/table-preview-query",
        json={"configPath": foreign, "sql": "select 1"},
    )
    assert query.status_code == 400
    assert query.json()["detail"] == lineage_routes.FOREIGN_CONFIG_DETAIL


def test_the_project_config_path_is_accepted_by_the_json_routes(
    client: TestClient, lineage_project: Path
) -> None:
    """The check admits the served project -- it is a filter, not a wall.

    ``/table-columns`` against an unbuilt database answers with a message
    rather than a 400, which is how this test tells "accepted then found
    nothing" apart from "refused".
    """
    own = str(lineage_project / "kptn.yaml")

    response = client.get(f"/table-columns?configPath={own}&table=main.widgets")

    assert response.status_code == 200, response.text
    assert response.json() != {"detail": lineage_routes.FOREIGN_CONFIG_DETAIL}


def test_a_config_path_spelled_with_dot_dot_still_resolves_to_the_project(
    client: TestClient, lineage_project: Path
) -> None:
    """The comparison is on resolved paths, so an equivalent spelling passes.

    The mirror of the refusal test: a check that compared raw strings would
    reject this, and one that did not resolve at all would accept
    ``<project>/../<elsewhere>/kptn.yaml``.
    """
    equivalent = str(lineage_project / "src" / ".." / "kptn.yaml")

    response = client.get(f"/table-columns?configPath={equivalent}&table=main.widgets")

    assert response.status_code == 200, response.text


# ---------------------------------------------------------------------------
# The two links behind a prefix, and behind a prefix plus a project slug
# ---------------------------------------------------------------------------
#
# These hrefs are built in Python, not in a template, so nothing prefixes
# them for free -- the same trap ``_detail_url`` is documented for. Two
# regressions lived here at once: the href was emitted root-absolute (so it
# escaped the proxy under ``--root-path``), and the "does this app serve the
# target" check compared the bare route path against the mounted one, so
# under ``--projects-root`` -- where every project router is mounted at
# ``/p/{slug}`` -- both links vanished from every task-detail page and the
# lineage surface became reachable only by typing a URL.

PREFIX = "/notebook/user/rruizesparza/kptn"
USER = "rruizesparza"


def test_the_output_links_carry_the_proxy_prefix(lineage_project: Path) -> None:
    client = TestClient(create_app(lineage_project, root_path=PREFIX))

    links = _links(client)

    assert set(links) == {"Lineage", "Preview"}
    assert links["Lineage"].startswith(f"{PREFIX}{inspect_routes.LINEAGE_PATH}?")
    assert links["Preview"].startswith(f"{PREFIX}{inspect_routes.TABLE_PREVIEW_PATH}?")


@pytest.fixture
def lineage_projects_root(lineage_project: Path, tmp_path: Path) -> Path:
    """A releases folder holding one of this person's working directories."""
    import shutil

    root = tmp_path / "shared"
    (root / "r1").mkdir(parents=True)
    shutil.copytree(lineage_project, root / "r1" / f"{USER}_main")
    return root


def test_the_output_links_are_offered_under_projects_root(
    lineage_projects_root: Path,
) -> None:
    """The links have to survive the ``/p/<slug>`` mount, prefix and all."""
    client = TestClient(create_multi_app(lineage_projects_root, USER, PREFIX))
    project_base = f"{PREFIX}/p/{USER}_main"

    links = _links(client, f"/p/{USER}_main/walkthrough/task/build_widgets")

    assert set(links) == {"Lineage", "Preview"}
    assert links["Lineage"].startswith(f"{project_base}{inspect_routes.LINEAGE_PATH}?")
    assert links["Preview"].startswith(
        f"{project_base}{inspect_routes.TABLE_PREVIEW_PATH}?"
    )


def test_a_projects_root_lineage_link_resolves(lineage_projects_root: Path) -> None:
    """Follow it the way a reader would: the proxy strips ``PREFIX`` first."""
    client = TestClient(create_multi_app(lineage_projects_root, USER, PREFIX))

    href = _links(client, f"/p/{USER}_main/walkthrough/task/build_widgets")["Lineage"]
    response = client.get(href[len(PREFIX) :])

    assert response.status_code == 200, response.text
    assert "<title>kptn Lineage</title>" in response.text
