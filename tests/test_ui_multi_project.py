"""One server, several of the person's working directories.

The layout is the deployment's: a shared folder of release directories, each
holding one working directory per person per branch. The UI offers the
person's own directories from the latest release that has one, each at
`/p/<slug>/`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import kptn
from kptn_server.app import create_multi_app
from kptn_server.origin import REFUSAL_TITLE
from kptn_server.processes import ProcessIdentity
from tests.conftest import BROKEN_FIXTURE_PROJECT, FIXTURE_PROJECT, copy_fixture_project

# This module builds real apps and loads real pipelines, so it opts into the
# fixtures that undo what that does to the process -- ``sys.modules``,
# ``sys.path``, the working directory and ``kptn.project``'s record of the
# roots it has loaded. Without it a stale ``ui_pipeline`` survives into the
# next module and the slot's own tests fail, blaming the slot.
pytestmark = pytest.mark.ui_hygiene

USER = "rruizesparza"
PREFIX = "/notebook/user/rruizesparza/kptn"

FORM = {"Content-Type": "application/x-www-form-urlencoded"}


@pytest.fixture
def projects_root(tmp_path: Path) -> Path:
    root = tmp_path / "shared"
    (root / "r1").mkdir(parents=True)
    release = root / "r2"
    release.mkdir(parents=True)
    copy_fixture_project(FIXTURE_PROJECT, release, f"{USER}_main")
    copy_fixture_project(FIXTURE_PROJECT, release, f"{USER}_featureA")
    copy_fixture_project(FIXTURE_PROJECT, root / "r1", f"{USER}_old")
    copy_fixture_project(FIXTURE_PROJECT, release, "someoneelse_main")
    return root


@pytest.fixture
def client(projects_root: Path) -> TestClient:
    return TestClient(create_multi_app(projects_root, USER))


def test_the_landing_page_lists_my_projects(client: TestClient) -> None:
    body = client.get("/").text

    assert f"{USER}_main" in body
    assert f"{USER}_featureA" in body


def test_the_landing_page_omits_other_releases_and_other_people(
    client: TestClient,
) -> None:
    body = client.get("/").text

    assert f"{USER}_old" not in body
    assert "someoneelse_main" not in body


def test_the_landing_page_names_the_release(client: TestClient) -> None:
    assert "r2" in client.get("/").text


def test_the_landing_page_names_the_kptn_version(client: TestClient) -> None:
    assert f"kptn {kptn.__version__}" in client.get("/").text


def test_a_project_serves_its_run_history(client: TestClient) -> None:
    response = client.get(f"/p/{USER}_main/")

    assert response.status_code == 200
    assert f"{USER}_main" in response.text


def test_an_unknown_slug_is_a_404(client: TestClient) -> None:
    assert client.get("/p/nobody_here/").status_code == 404


def test_links_inside_a_project_carry_its_prefix(client: TestClient) -> None:
    body = client.get(f"/p/{USER}_main/").text

    assert f'action="/p/{USER}_main/runs"' in body
    assert f'action="/p/{USER}_main/plan"' in body


def test_static_assets_are_not_project_scoped(client: TestClient) -> None:
    body = client.get(f"/p/{USER}_main/").text

    assert 'href="/static/app.css"' in body


def test_the_proxy_prefix_and_the_project_prefix_compose(
    projects_root: Path,
) -> None:
    client = TestClient(create_multi_app(projects_root, USER, root_path=PREFIX))

    body = client.get(f"/p/{USER}_main/").text

    assert f'href="{PREFIX}/static/app.css"' in body
    assert f'action="{PREFIX}/p/{USER}_main/runs"' in body


def test_healthz_answers_without_a_project(client: TestClient) -> None:
    """server-proxy's readiness probe cannot know a slug."""
    assert client.get("/healthz").json() == {"status": "ok"}


def test_the_plan_page_serves_each_project_from_its_own_checkout(
    client: TestClient,
) -> None:
    """The slot's reason for existing, end to end through HTTP.

    The two fixtures are identical copies, so "the pages differ" would be a
    vacuous assertion: what distinguishes them is which checkout each page
    *names*. Asserting each page names its own root is what fails if the
    slot hands the second request the first project.
    """
    first = client.get(f"/p/{USER}_main/plan")
    second = client.get(f"/p/{USER}_featureA/plan")
    third = client.get(f"/p/{USER}_main/plan")

    assert first.status_code == 200
    assert second.status_code == 200
    assert f"{USER}_featureA" in second.text
    assert f"{USER}_featureA" not in first.text
    assert third.text == first.text


def test_run_history_is_per_project(projects_root: Path) -> None:
    """Each project has its own run store, and neither sees the other's rows."""
    app = create_multi_app(projects_root, USER)
    client = TestClient(app)
    # One request opens this project's store and supervisor; the supervisor
    # is then swapped for a stub, because what is under test is the store
    # each project reads, not a real worker process.
    client.get(f"/p/{USER}_main/")
    manager = MagicMock()
    manager.start.return_value = ProcessIdentity(pid=4321, started_at=1.0)
    app.state.managers[f"{USER}_main"] = manager

    # ``profile=`` is the "(no profile)" option; a body with no ``profile``
    # key at all is the drive-by payload and is refused with a 400.
    started = client.post(
        f"/p/{USER}_main/runs", data={"profile": ""}, follow_redirects=False
    )
    assert started.status_code in (302, 303)

    other = client.get(f"/p/{USER}_featureA/").text

    # A run row is a link to that run. The other project has no runs, so it
    # must have none of these -- "the word 'runs' appears" would be true of
    # nearly any page in this UI and would pass against a shared store.
    assert "run-history__link" not in other
    assert "run-history__link" in client.get(f"/p/{USER}_main/").text
    assert started.headers["location"].startswith("runs/")


def test_an_empty_root_explains_itself_rather_than_failing_to_start(
    tmp_path: Path,
) -> None:
    client = TestClient(create_multi_app(tmp_path, USER))

    response = client.get("/")

    assert response.status_code == 200
    assert str(tmp_path) in response.text
    assert USER in response.text


def test_a_project_created_after_startup_is_found(
    projects_root: Path, client: TestClient
) -> None:
    client.get("/")
    copy_fixture_project(FIXTURE_PROJECT, projects_root / "r2", f"{USER}_featureB")

    assert client.get(f"/p/{USER}_featureB/").status_code == 200


# -- the cross-origin defence, on a server with no single project ----------
#
# ``enforce_same_origin`` refuses in middleware, ahead of every router and so
# ahead of the dependency that resolves a project. Its refusal is a rendered
# page, and the page it used to render read ``project.display_name`` off the
# app bar -- so on the project list, where there is no project at all, the
# 403 raised and the caller got a 500: the defence becoming the failure.


def test_a_cross_origin_post_to_the_project_list_is_refused_not_a_500(
    client: TestClient,
) -> None:
    response = client.post(
        "/",
        headers={**FORM, "Sec-Fetch-Site": "cross-site"},
        content="",
        follow_redirects=False,
    )

    assert response.status_code == 403, response.text
    assert REFUSAL_TITLE in response.text
    assert f"kptn {kptn.__version__}" in response.text


def test_a_cross_origin_post_to_a_project_is_still_refused(
    client: TestClient,
) -> None:
    """The defence still covers the routes that can actually start a run."""
    response = client.post(
        f"/p/{USER}_main/runs",
        headers={**FORM, "Sec-Fetch-Site": "cross-site"},
        content="profile=",
        follow_redirects=False,
    )

    assert response.status_code == 403, response.text
    assert REFUSAL_TITLE in response.text


# -- the lineage routes chdir; the slot has to be held across that ---------
#
# ``kptn_server.service`` chdirs into the served project because
# ``read_config`` and the DuckDB connection factory resolve relative paths
# against the working directory. That used to be safe because the app served
# one project for its lifetime; under ``--projects-root`` it is safe only
# because ``kptn_server/routes/lineage.py`` holds the slot across the whole
# call into that module, not merely across the project lookup.


def _lineage_url(slug: str, config: Path) -> str:
    return f"/p/{slug}/lineage-page?configPath={config}"


def test_the_lineage_page_serves_each_project_from_its_own_checkout(
    client: TestClient, projects_root: Path
) -> None:
    """Each project's lineage page reads that project's own ``kptn.yaml``."""
    main_config = projects_root / "r2" / f"{USER}_main" / "kptn.yaml"
    feature_config = projects_root / "r2" / f"{USER}_featureA" / "kptn.yaml"

    first = client.get(_lineage_url(f"{USER}_main", main_config))
    second = client.get(_lineage_url(f"{USER}_featureA", feature_config))

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text


def test_the_working_directory_is_restored_after_a_lineage_request(
    client: TestClient, projects_root: Path
) -> None:
    """A leaked chdir would break every later request, and the test process."""
    import os

    config = projects_root / "r2" / f"{USER}_main" / "kptn.yaml"
    before = os.getcwd()

    response = client.get(_lineage_url(f"{USER}_main", config))

    assert response.status_code == 200, response.text
    assert os.getcwd() == before


def test_concurrent_lineage_requests_do_not_interleave_directories(
    client: TestClient, projects_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The slot must be held across the chdir, not just the project lookup.

    ``generate_lineage_html`` chdirs into the project and then calls
    ``read_config``. Pausing the *first* project's request there, mid-call,
    and only then firing the *second* project's request tells us, by whether
    the second request can complete while the first is still paused, whether
    the slot is held for the whole call. An implementation that releases the
    slot after the project lookup (the bug this test exists to catch) lets
    the second request run straight through -- chdir included -- while the
    first sits paused with the working directory set to its own project;
    that would make ``second_completed_while_first_was_paused`` true, and
    this test would fail. Holding the slot across the call blocks the second
    request at ``ProjectSlot.use`` until the first is entirely done, so it
    cannot even attempt its own chdir while the first is paused -- the only
    way this test passes.
    """
    import threading

    from kptn.read_config import read_config as real_read_config
    from kptn_server import service

    reached_pause = threading.Event()
    release_pause = threading.Event()
    calls: list[None] = []

    def paced_read_config() -> object:
        calls.append(None)
        if len(calls) == 1:
            reached_pause.set()
            release_pause.wait(timeout=5)
        return real_read_config()

    monkeypatch.setattr(service, "read_config", paced_read_config)

    main_config = projects_root / "r2" / f"{USER}_main" / "kptn.yaml"
    feature_config = projects_root / "r2" / f"{USER}_featureA" / "kptn.yaml"
    results: dict[str, Any] = {}

    def call_main() -> None:
        results["main"] = client.get(_lineage_url(f"{USER}_main", main_config))

    def call_feature() -> None:
        results["feature"] = client.get(
            _lineage_url(f"{USER}_featureA", feature_config)
        )

    first = threading.Thread(target=call_main)
    first.start()
    assert reached_pause.wait(timeout=5), "first request never reached the pause"

    second = threading.Thread(target=call_feature)
    second.start()
    second.join(timeout=0.5)
    second_completed_while_first_was_paused = not second.is_alive()

    release_pause.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not second_completed_while_first_was_paused, (
        "the second project's request completed while the first was still "
        "paused inside its own chdir -- the slot was not held across the "
        "service call"
    )
    assert results["main"].status_code == 200, results["main"].text
    assert results["feature"].status_code == 200, results["feature"].text


@pytest.mark.parametrize(
    "page",
    [
        "plan",
        "walkthrough",
        "walkthrough/task/anything",
        # configPath is a required query parameter; without it FastAPI
        # answers 422 before the handler -- and so before the slot -- runs.
        "lineage-page?configPath=kptn.yaml",
    ],
)
def test_a_project_whose_pipeline_will_not_import_says_why(
    tmp_path: Path, page: str
) -> None:
    """A broken pipeline is reported on the page, not as a bare 500.

    The registry reads pyproject.toml as text and cannot know a pipeline is
    broken until something imports it -- and in multi-project mode nothing
    does until the first page that needs the graph. Single-project mode
    loaded at startup, so a broken pipeline stopped the launcher and no
    handler ever saw one. Here the load happens inside a request, so the
    failure has to come back as a page that names it: otherwise the reader
    gets "Internal Server Error" and nothing to act on.
    """
    root = tmp_path / "shared"
    copy_fixture_project(BROKEN_FIXTURE_PROJECT, root / "r1", f"{USER}_broken")
    client = TestClient(create_multi_app(root, USER))

    response = client.get(f"/p/{USER}_broken/{page}")

    assert response.status_code == 500
    assert response.text.strip() != "Internal Server Error"
    assert "Could not load the pipeline" in response.text
