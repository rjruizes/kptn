"""Tests for the FastAPI application factory, project discovery, and base page.

The UI is a thin, durable read/write surface over the run store. Three things
in this layer have to hold before any page or router is worth writing:

1. ``ProjectContext.load`` must refuse anything that is not a kptn project,
   and it must place all UI state under ``.kptn/`` -- nowhere else.
2. ``create_app`` must drive :meth:`RunProcessManager.reconcile` on a cadence,
   because the supervisor deliberately owns no threads of its own. A run whose
   worker a reboot took out stays wedged forever if nobody reconciles it.
3. That same loop must start and stop cleanly *without ever* disturbing a live
   worker -- the server restarts whenever a developer saves a file, and a run
   has to survive that.

Nothing here synchronizes on ``sleep``: the reconciliation loop's sleep is
replaced with a fake that signals an event and then parks, and the stale-worker
clock is injected, so a full reconciliation pass is observed without waiting
five real seconds for it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psutil
import pytest
from fastapi.testclient import TestClient

from kptn_server import app as app_module
from kptn_server import processes as processes_module
from kptn_server.app import STATIC_DIR, TEMPLATES_DIR, create_app
from kptn_server.assets import asset_version
from kptn_server.processes import RECONCILE_INTERVAL_SECONDS, RunProcessManager
from kptn_server.project import ProjectContext, ProjectError
from kptn_server.run_store import (
    STATUS_INTERRUPTED,
    STATUS_QUEUED,
    STATUS_SUCCEEDED,
    RunRequest,
    RunStore,
)

EVENT_TIMEOUT_SECONDS = 30.0

# Opts this module into restore_process_state and reap_spawned_workers; the
# ui_project / broken_ui_project fixtures come from tests/conftest.py too.
pytestmark = pytest.mark.ui_hygiene


# -- fixtures --------------------------------------------------------------


class ParkingSleep:
    """A stand-in for the reconciliation loop's sleep.

    The first call records its delay and signals ``reached``, then parks
    forever on an event nobody sets. The loop therefore makes exactly one
    reconciliation pass and then waits to be cancelled at shutdown, which
    gives a test a precise, sleep-free place to observe "one pass is done".
    """

    def __init__(self) -> None:
        self.delays: list[float] = []
        self.reached = threading.Event()

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        self.reached.set()
        await asyncio.Event().wait()

    def wait_for_one_pass(self) -> None:
        assert self.reached.wait(EVENT_TIMEOUT_SECONDS), (
            "the reconciliation loop never completed a pass"
        )


@pytest.fixture
def parking_sleep(monkeypatch: pytest.MonkeyPatch) -> ParkingSleep:
    sleeper = ParkingSleep()
    monkeypatch.setattr(app_module, "_sleep", sleeper)
    return sleeper


def _queued_run(store: RunStore, project_root: Path, *, profile: str = "success"):
    return store.create_run(
        RunRequest(project_root=project_root, pipeline="fixture", profile=profile)
    )


# -- project discovery -----------------------------------------------------


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    """Pin the ``pytestmark`` opt-in.

    ``restore_process_state`` and ``reap_spawned_workers`` are gated on the
    marker, so deleting the module's ``pytestmark`` line would silently strip
    this module of both -- no error, no failure, just a module that can leak a
    detached worker and pollute ``sys.path`` for everything after it. This
    turns that silent loss into a failure.
    """
    assert request.node.get_closest_marker("ui_hygiene") is not None


def test_index_lists_profiles(ui_project: Path) -> None:
    client = TestClient(create_app(ui_project))
    response = client.get("/")
    assert response.status_code == 200
    assert "success" in response.text
    assert "Run" in response.text


def test_app_rejects_non_project(tmp_path: Path) -> None:
    with pytest.raises(ProjectError, match="pyproject.toml"):
        create_app(tmp_path)


def test_project_context_reports_every_declared_profile(ui_project: Path) -> None:
    context = ProjectContext.load(ui_project)

    assert context.pipeline_name == "fixture"
    # Declaration order from kptn.yaml, not sorted: the UI's selector should
    # read the way the author wrote the file.
    assert context.profiles == ("success", "slow", "failure", "db_error")


def test_project_context_keeps_ui_state_under_dot_kptn(ui_project: Path) -> None:
    context = ProjectContext.load(ui_project)

    assert context.database_path == ui_project / ".kptn" / "ui.db"
    assert context.run_log_dir == ui_project / ".kptn" / "runs"


def test_project_context_canonicalizes_the_root(ui_project: Path) -> None:
    """One active run per *canonical* path, so the root must be resolved.

    The run store locks on ``Path(project_root).resolve()``. A context holding
    an unresolved root would let ``project`` and ``project/../project`` look
    like two different projects to everything built on top of it.
    """
    detour = ui_project / ".." / ui_project.name

    context = ProjectContext.load(detour)

    assert context.root == ui_project.resolve()
    assert ".." not in context.root.parts


def test_project_context_rejects_a_project_without_a_pipeline_entry(
    tmp_path: Path,
) -> None:
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "no-kptn"\n')

    with pytest.raises(ProjectError, match="pipeline"):
        ProjectContext.load(tmp_path)


def test_project_context_rejects_an_invalid_profile_file(ui_project: Path) -> None:
    (ui_project / "kptn.yaml").write_text("profiles: [not, a, mapping]\n")

    with pytest.raises(ProjectError, match="profiles"):
        ProjectContext.load(ui_project)


def test_project_context_cannot_be_built_without_a_pipeline() -> None:
    """No defaults on ``pipeline``/``config``.

    A default of ``None`` would permit a silently-invalid context that Task
    10's inspection call hits as an ``AttributeError`` far from the cause.
    Every field is required, so the invalid shape is unconstructable.
    """
    with pytest.raises(TypeError, match="pipeline"):
        ProjectContext(  # type: ignore[call-arg]
            root=Path("/tmp/x"),
            pipeline_name="p",
            profiles=(),
            database_path=Path("/tmp/x/.kptn/ui.db"),
            run_log_dir=Path("/tmp/x/.kptn/runs"),
        )


def test_project_context_rejects_a_pipeline_module_that_raises_on_import(
    broken_ui_project: Path,
) -> None:
    """The most common project-authoring mistake must still be a ProjectError.

    ``load_pipeline`` wraps a missing file, bad TOML, a missing
    ``[tool.kptn] pipeline``, an ImportError, and a wrong attribute type -- but
    an error raised in the project module's *body* is none of those and
    travels straight out of ``importlib.import_module``. A ``ValueError`` is
    not an ``ImportError``, so nothing below this line would convert it.

    ``create_app`` documents ``ProjectError`` and ``kptn ui`` catches only
    ``ProjectError``, so anything else reaches the developer as a raw
    traceback -- and Tasks 8-11 would inherit the hole.
    """
    with pytest.raises(ProjectError) as excinfo:
        ProjectContext.load(broken_ui_project)

    # The underlying message has to survive: it is the only thing telling the
    # developer what is actually wrong with their file.
    assert "broken fixture project" in str(excinfo.value)
    assert "ValueError" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_create_app_rejects_a_pipeline_module_that_raises_on_import(
    broken_ui_project: Path,
) -> None:
    """The factory's own documented contract, not just ProjectContext's."""
    with pytest.raises(ProjectError, match="broken fixture project"):
        create_app(broken_ui_project)


@pytest.mark.parametrize(
    ("body", "expected_type"),
    [
        ("def broken(:\n    pass\n", "SyntaxError"),
        ("pipeline = undefined_name\n", "NameError"),
        ("raise KeyError('missing step')\n", "KeyError"),
    ],
    ids=["syntax-error", "name-error", "key-error"],
)
def test_project_context_wraps_any_import_time_failure(
    tmp_path: Path, body: str, expected_type: str
) -> None:
    """Not just one exception type: the wrap has to be unconditional.

    A SyntaxError is the sharpest case -- it is not an ImportError, and it is
    what a half-saved file in an editor produces.
    """
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "wip"\nversion = "0.0.0"\n\n'
        '[tool.kptn]\npipeline = "wip_pipeline"\n'
    )
    (tmp_path / "wip_pipeline.py").write_text(body)

    with pytest.raises(ProjectError) as excinfo:
        ProjectContext.load(tmp_path)

    assert expected_type in str(excinfo.value)


def test_project_context_allows_a_project_with_no_profile_file(
    ui_project: Path,
) -> None:
    (ui_project / "kptn.yaml").unlink()

    context = ProjectContext.load(ui_project)

    assert context.profiles == ()


# -- the app factory's wiring ---------------------------------------------


def test_app_exposes_its_store_and_supervisor(ui_project: Path) -> None:
    app = create_app(ui_project)

    assert app.state.store.path == ui_project / ".kptn" / "ui.db"
    assert app.state.project.root == ui_project.resolve()
    assert isinstance(app.state.processes, RunProcessManager)


def test_health_endpoint_responds(ui_project: Path) -> None:
    """The launcher polls this before opening a browser, so it must exist."""
    client = TestClient(create_app(ui_project))

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


# -- reconciliation cadence -----------------------------------------------


def test_reconcile_loop_uses_the_supervisor_interval(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The interval must be the supervisor's constant, not a second copy of 5.0.

    The app's name for it is asserted to *be* the supervisor's value, and then
    monkeypatched to something unmistakable. A loop that slept on a literal
    would record 5.0 here and fail.
    """
    assert app_module.RECONCILE_INTERVAL_SECONDS == RECONCILE_INTERVAL_SECONDS
    assert app_module.RECONCILE_INTERVAL_SECONDS is (
        processes_module.RECONCILE_INTERVAL_SECONDS
    )

    sentinel_interval = 61.5
    monkeypatch.setattr(app_module, "RECONCILE_INTERVAL_SECONDS", sentinel_interval)
    sleeper = ParkingSleep()
    monkeypatch.setattr(app_module, "_sleep", sleeper)

    with TestClient(create_app(ui_project)):
        sleeper.wait_for_one_pass()

    assert sleeper.delays == [sentinel_interval]


def test_lifespan_reconciles_a_run_whose_worker_is_gone(
    ui_project: Path, parking_sleep: ParkingSleep, caplog: pytest.LogCaptureFixture
) -> None:
    """The whole point of the loop: release a project wedged by a dead worker.

    ``reconcile()`` writes status only and appends no event, so the run's fate
    is observed through its status -- never inferred from the event stream.
    """
    app = create_app(ui_project)
    store = app.state.store
    record = _queued_run(store, ui_project)
    assert store.active_run(ui_project) is not None

    # A clock an hour ahead puts the run well past STALE_WORKER_GRACE_SECONDS
    # with no worker ever registered, so reconcile must call it interrupted.
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    app.state.processes = RunProcessManager(store, now=lambda: future)

    with caplog.at_level(logging.DEBUG, logger=app_module._LOGGER.name):
        with TestClient(app):
            parking_sleep.wait_for_one_pass()

    assert store.get_run(record.run_id).status == STATUS_INTERRUPTED
    # finish_run drops the project lock in the same transaction, so the
    # project must be runnable again.
    assert store.active_run(ui_project) is None
    # reconcile() appends no event, so nothing in the run's event stream will
    # ever mention this. The warning is the only trace that the supervisor,
    # rather than the pipeline, decided this run's fate.
    warnings_logged = [
        r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert any(
        record.run_id in message and "interrupted" in message
        for message in warnings_logged
    ), f"the interrupted run was not logged; saw {warnings_logged}"


def test_lifespan_never_disturbs_a_live_worker(
    ui_project: Path, parking_sleep: ParkingSleep
) -> None:
    """A reconciliation pass, and shutdown, must leave a live run alone.

    This process stands in for the worker: registering its real pid and real
    OS creation time makes the run's recorded identity genuinely live, so any
    regression that reconciles on weaker evidence than "definitely gone" marks
    this run terminal and fails here.
    """
    app = create_app(ui_project)
    store = app.state.store
    record = _queued_run(store, ui_project, profile="slow")
    store.record_worker_start(
        record.run_id,
        pid=os.getpid(),
        started_at=psutil.Process(os.getpid()).create_time(),
    )

    with TestClient(app):
        parking_sleep.wait_for_one_pass()

    assert store.get_run(record.run_id).status == STATUS_QUEUED
    assert store.active_run(ui_project).run_id == record.run_id


def test_lifespan_cancels_the_reconcile_loop_before_shutdown_returns(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Shutdown must cancel *and await* the loop, not orphan it.

    Driven through the lifespan context manager directly rather than through
    ``TestClient``, and asserted while the event loop is still alive. That
    distinction is the whole test: tearing an event loop down cancels every
    pending task anyway, so a check made after the loop is gone passes even
    for a lifespan that never cancels anything.
    """
    app = create_app(ui_project)

    async def scenario() -> None:
        reached = asyncio.Event()

        async def fake_sleep(_delay: float) -> None:
            reached.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(app_module, "_sleep", fake_sleep)

        async with app_module._lifespan(app):
            await asyncio.wait_for(reached.wait(), EVENT_TIMEOUT_SECONDS)
            task = app.state.reconcile_task
            assert not task.done(), "the loop should still be running mid-lifespan"

        assert task.done(), (
            "shutdown returned while the reconciliation loop was still running"
        )
        assert task.cancelled()

    asyncio.run(scenario())


def test_reconcile_loop_survives_a_failing_pass(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A store hiccup must not silently take the loop out for the whole session."""
    app = create_app(ui_project)
    calls: list[str] = []
    released = threading.Event()

    class ExplodingThenFine:
        def reconcile(self) -> list[str]:
            calls.append("reconcile")
            if len(calls) == 1:
                raise RuntimeError("database is locked")
            released.set()
            return []

    app.state.processes = ExplodingThenFine()

    async def immediate_sleep(delay: float) -> None:
        if released.is_set():
            await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "_sleep", immediate_sleep)

    with caplog.at_level(logging.DEBUG, logger=app_module._LOGGER.name):
        with TestClient(app):
            assert released.wait(EVENT_TIMEOUT_SECONDS)

    assert len(calls) >= 2
    # The log is the operator's only window into a loop that is limping: the
    # pass failed, nothing was written, and no page will ever say so.
    failures = [
        record
        for record in caplog.records
        if record.levelno == logging.ERROR and record.exc_info is not None
    ]
    assert failures, "a failing reconciliation pass was swallowed silently"
    assert "reconciliation" in failures[0].getMessage()
    assert failures[0].exc_info[0] is RuntimeError


# -- assets and the base page ---------------------------------------------


@pytest.mark.parametrize(
    "asset", ["htmx.min.js", "app.css", "app.js", "auto-scroll.js"]
)
def test_vendored_assets_are_served(ui_project: Path, asset: str) -> None:
    client = TestClient(create_app(ui_project))

    response = client.get(f"/static/{asset}")

    assert response.status_code == 200, f"/static/{asset} is not served"
    assert response.content


def test_every_asset_link_carries_its_content_hash() -> None:
    """Every vendored stylesheet and script link is cache-busted by contents.

    The query is ``asset_version`` of the very file the link names, so a
    link that versions itself by some other file -- copied from the line
    above and only half edited -- would keep a stale copy alive and fails
    here. This scans the template sources, so a new page that forgets the
    query fails here too. The lineage page's ``static_prefix`` links count.
    """
    link = re.compile(
        r'(?:href|src)="[^"]*(?:/static/|\{\{ static_prefix \}\}/)([^"?]+)([^"]*)"'
    )
    links = [
        (template.name, asset, query)
        for template in sorted(TEMPLATES_DIR.iterdir())
        if template.is_file()
        for asset, query in link.findall(template.read_text())
    ]

    assert links, "no asset links found -- has the template layout changed?"
    assert any(asset == "alpine.min.js" for _, asset, _ in links), (
        "the lineage page's static_prefix links were not scanned"
    )
    stale = [
        (name, asset, query)
        for name, asset, query in links
        if query != f"?v={{{{ asset_version('{asset}') }}}}"
    ]
    assert not stale, f"asset links not versioned by their own contents: {stale}"


@pytest.mark.parametrize("module", ["app.js", "auto-scroll.js"])
def test_modules_are_served_as_javascript(ui_project: Path, module: str) -> None:
    """A module script is refused unless it arrives with a JavaScript MIME
    type -- unlike a classic script, which a browser runs regardless."""
    response = TestClient(create_app(ui_project)).get(f"/static/{module}")

    assert "javascript" in response.headers["content-type"]


def _import_map(body: str) -> dict[str, str]:
    match = re.search(r'<script type="importmap">(.*?)</script>', body, re.S)
    assert match, "the page has no import map"
    return json.loads(match.group(1))["imports"]


def test_every_module_import_is_mapped_and_versioned(ui_project: Path) -> None:
    """Each bare specifier a module imports resolves to a versioned URL.

    An unmapped specifier fails the importing module outright -- app.js and
    everything it sets up -- and a relative import could not carry the
    ``?v=`` query, so it is not allowed either.
    """
    imported = {
        (path.name, specifier)
        for path in STATIC_DIR.glob("*.js")
        if not path.name.endswith(".min.js")
        for specifier in re.findall(
            r'^\s*(?:import\s+|(?:import|export)\b[^;"\']*\bfrom\s+)["\']([^"\']+)["\']',
            path.read_text(),
            re.M,
        )
    }
    assert imported, "no module imports found -- has app.js stopped importing?"
    relative = [item for item in imported if item[1].startswith((".", "/"))]
    assert not relative, f"relative imports drop the version query: {relative}"

    imports = _import_map(TestClient(create_app(ui_project)).get("/").text)

    unmapped = [item for item in imported if item[1] not in imports]
    assert not unmapped, f"imports missing from _modules.html: {unmapped}"
    for specifier, url in imports.items():
        name = url.split("/static/", 1)[1].split("?")[0]
        assert (STATIC_DIR / name).is_file()
        assert url.endswith(f"?v={asset_version(name)}"), f"{specifier} is not versioned"


def test_app_js_is_loaded_only_through_the_modules_partial() -> None:
    """The import map must precede the first module script, so a shell that
    loaded app.js itself could put it ahead of the map, or leave the map out."""
    loaders = sorted(
        template.name
        for template in TEMPLATES_DIR.iterdir()
        if template.is_file() and "/static/app.js" in template.read_text()
    )

    assert loaders == ["_modules.html"]


def test_static_files_are_mounted_package_relative(
    ui_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An installed wheel is not served from the developer's cwd.

    Building and serving the app from an unrelated working directory fails
    immediately if the static mount was resolved relative to the cwd.
    """
    app = create_app(ui_project)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    response = TestClient(app).get("/static/app.css")

    assert response.status_code == 200


def test_base_page_loads_only_vendored_assets(ui_project: Path) -> None:
    """No CDN assets whatsoever -- the UI must work with no network."""
    body = TestClient(create_app(ui_project)).get("/").text

    assert "/static/htmx.min.js" in body
    assert "/static/app.css" in body
    assert "/static/app.js" in body
    for forbidden in ("https://", "http://", "cdn.", "unpkg", "jsdelivr"):
        assert forbidden not in body.lower(), f"base page references {forbidden}"


def test_app_bar_is_a_path_to_the_profile(ui_project: Path) -> None:
    """kptn, the project, the profile -- and nothing to act on.

    Run and Plan live in the sidebar's Next run pane now, and there is no
    nav: the run page lists the runs itself.
    """
    bar = _app_bar(TestClient(create_app(ui_project)).get("/").text)

    assert 'class="app-bar__brand"' in bar
    assert 'class="app-bar__project">fixture<' in bar
    assert bar.index("app-bar__project") < bar.index('id="profile-select"')
    assert "app-bar__nav" not in bar
    assert "Run</button>" not in bar and "Plan</button>" not in bar


def test_base_page_shows_the_project_and_a_profile_selector(ui_project: Path) -> None:
    body = TestClient(create_app(ui_project)).get("/").text

    assert "fixture" in body
    assert 'name="profile"' in body
    for profile in ("success", "slow", "failure", "db_error"):
        assert f'value="{profile}"' in body, f"{profile} is not selectable"


# -- the profile is the one every page shows ---------------------------------
#
# Choosing a profile goes to it: the landing page, which opens the profile's
# newest run with its runs in the sidebar -- or, on a page about a profile,
# that page for the new one. The selector says where in
# ``data-profile-nav``; app.js does the going.


def _profile_nav(body: str) -> str:
    match = re.search(r'<select[^>]*id="profile-select"[^>]*data-profile-nav="([^"]*)"', body, re.S)
    assert match, "the selector says nowhere to go"
    return match.group(1)


def test_choosing_a_profile_goes_to_its_newest_run(ui_project: Path) -> None:
    assert _profile_nav(TestClient(create_app(ui_project)).get("/").text) == "/"


@pytest.mark.parametrize("page", ["/plan", "/walkthrough"])
def test_choosing_a_profile_on_a_profile_page_stays_on_it(ui_project: Path, page: str) -> None:
    assert _profile_nav(TestClient(create_app(ui_project)).get(page).text) == page


def test_app_js_navigates_when_a_profile_is_chosen() -> None:
    """The one piece that needs script: the templates render where to go."""
    source = (STATIC_DIR / "app.js").read_text()

    assert "data-profile-nav" in source, "app.js does not read where to go"
    assert "profile-select" in source, "app.js does not read the selector"
    assert "window.location.assign" in source


def test_index_accepts_a_profile_and_marks_it_selected(ui_project: Path) -> None:
    """A profile with no runs yet lands on the page to start one -- showing
    the profile it was handed, which is the one Run will use."""
    body = TestClient(create_app(ui_project)).get("/?profile=slow").text

    assert '<option value="slow" selected>slow</option>' in body
    assert "No runs with this profile yet" in body


def test_index_refuses_an_unknown_profile(ui_project: Path) -> None:
    """Same answer the plan and walkthrough pages already give."""
    response = TestClient(create_app(ui_project)).get("/?profile=nope")

    assert response.status_code == 400
    assert "nope" in response.text
    assert "success" in response.text, "the error does not name the real profiles"


def test_run_page_selector_shows_that_runs_profile(ui_project: Path) -> None:
    """A run knows its profile without one being in the URL, and Run and
    Plan beside it use it."""
    app = create_app(ui_project)
    store = app.state.store
    record = store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    bar = _app_bar(TestClient(app).get(f"/runs/{record.run_id}").text)

    assert '<option value="slow" selected>' in bar, "the selector does not show it"


def test_unknown_profile_error_page_offers_a_way_out(ui_project: Path) -> None:
    """The error page's selector must not hold the profile that caused it:
    the next Run from it would be another 400."""
    body = TestClient(create_app(ui_project)).get("/?profile=nope").text

    assert '<option value="" selected>' in _app_bar(body)
    assert 'value="nope"' not in body


def test_run_history_refuses_an_unknown_profile(ui_project: Path) -> None:
    """Same answer every other profile-aware page gives."""
    response = TestClient(create_app(ui_project)).get("/runs?profile=nope")

    assert response.status_code == 400
    assert "success" in response.text, "the error does not name the real profiles"


# -- the landing page is the newest run ------------------------------------
#
# There is no run history page: the run page lists the profile's runs in
# its sidebar, so "/" opens the newest one.


def _app_bar(body: str) -> str:
    match = re.search(r'<header class="app-bar">(.*?)</header>', body, re.S)
    assert match, "the app bar is gone"
    return match.group(1)


def _next_pane(body: str) -> str:
    match = re.search(r'<section class="pane pane--next".*?</section>', body, re.S)
    assert match, "the sidebar has no Next run pane"
    return match.group(0)


def _made(store, root: Path, profile: str | None, when: datetime):
    record = store.create_run(RunRequest(project_root=root, pipeline="fixture", profile=profile))
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
    conn = sqlite3.connect(store.path)  # only the clock is staged
    try:
        conn.execute(
            "UPDATE runs SET created_at = ? WHERE run_id = ?",
            (when.astimezone(timezone.utc).isoformat(), record.run_id),
        )
        conn.commit()
    finally:
        conn.close()
    return record


def test_root_opens_the_newest_run(ui_project: Path) -> None:
    app = create_app(ui_project)
    now = datetime.now(timezone.utc)
    _made(app.state.store, ui_project, "slow", now - timedelta(minutes=5))
    newest = _made(app.state.store, ui_project, "success", now)

    response = TestClient(app).get("/", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == f"runs/{newest.run_id}"


def test_root_opens_the_chosen_profiles_newest_run(ui_project: Path) -> None:
    """``?profile=`` is a profile's runs; blank is the runs with none."""
    app = create_app(ui_project)
    now = datetime.now(timezone.utc)
    slow = _made(app.state.store, ui_project, "slow", now - timedelta(minutes=5))
    bare = _made(app.state.store, ui_project, None, now - timedelta(minutes=9))
    _made(app.state.store, ui_project, "success", now)
    client = TestClient(app)

    assert client.get("/?profile=slow", follow_redirects=False).headers["location"] == f"runs/{slow.run_id}"
    assert client.get("/?profile=", follow_redirects=False).headers["location"] == f"runs/{bare.run_id}"


def test_the_old_runs_url_still_reaches_the_newest_run(ui_project: Path) -> None:
    """Bookmarks and links from the proxied notebook environment must not 404."""
    app = create_app(ui_project)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    response = TestClient(app).get("/runs")

    assert response.status_code == 200
    assert str(response.url).endswith(f"/runs/{record.run_id}")


def test_with_no_runs_the_landing_page_offers_the_first(ui_project: Path) -> None:
    body = TestClient(create_app(ui_project)).get("/").text

    assert "Start a run from the sidebar" in body
    assert "No runs with this profile yet" in body
    assert '<form id="run-form"' in _next_pane(body)


# -- Run and Plan are in the sidebar's Next run pane -----------------------


def test_run_posts_the_selected_profile(ui_project: Path) -> None:
    """The app bar's select carries ``form="run-form"``, so the pane's form
    takes it: one selector driving a form it is not nested in."""
    body = TestClient(create_app(ui_project)).get("/").text
    pane = _next_pane(body)

    assert '<form id="run-form"' in pane
    assert 'method="post"' in pane and 'action="/runs"' in pane
    assert 'form="run-form"' in _app_bar(body), "the selector is not bound to the run form"
    assert "btn--run" in pane, "Run is not the primary action"


def test_plan_gets_the_plan_for_the_selected_profile(ui_project: Path) -> None:
    """Plan is a second submit button of the Run form that GETs the plan,
    so it sends the same selector -- with no JavaScript."""
    pane = _next_pane(TestClient(create_app(ui_project)).get("/").text)

    plan = re.search(r"<button(?:(?!<button).)*?>\s*(?:<svg.*?</svg>)?Plan</button>", pane, re.S)
    assert plan, "the pane has no Plan button"
    assert 'formaction="/plan"' in plan.group(0) and 'formmethod="get"' in plan.group(0)


@pytest.mark.parametrize("page", ["/", "/plan"])
def test_every_workspace_page_can_start_a_run(ui_project: Path, page: str) -> None:
    body = TestClient(create_app(ui_project)).get(page).text

    assert '<form id="run-form"' in _next_pane(body)
    assert 'form="run-form"' in _app_bar(body), f"{page}'s selector is not bound to the run form"


def test_a_page_without_the_run_form_leaves_the_selector_unbound(ui_project: Path) -> None:
    """An orphaned ``form=`` would take the select out of every form."""
    body = TestClient(create_app(ui_project)).get("/walkthrough").text

    assert 'id="run-form"' not in body
    assert 'form="run-form"' not in body


# -- Run and Plan go quiet while a run is in progress ----------------------
#
# One run per project is the store's rule, enforced by the lock row that
# ``create_run`` takes and ``finish_run`` releases -- POST /runs answers 409
# to anyone who asks anyway. The pane is where that rule becomes visible:
# offering an enabled Run button whose only possible outcome is an error page
# is an invitation to be told no.
#
# The disabling is an affordance, never the guard. The 409 stays exactly
# where it is; these tests are about what the reader is offered. The
# selector stays: it chooses which runs to show, which a run never blocks.


def _control_tag(html: str, pattern: str) -> str:
    match = re.search(pattern, html, re.S)
    assert match, f"no control matching {pattern!r}"
    return match.group(0)


def _pane_controls(pane: str) -> dict[str, str]:
    """Run and Plan, each the whole button and never across another one."""
    return {
        "run": _control_tag(pane, r"<button(?:(?!<button).)*?>\s*(?:<svg.*?</svg>)?Run</button>"),
        "plan": _control_tag(pane, r"<button(?:(?!<button).)*?>\s*(?:<svg.*?</svg>)?Plan</button>"),
    }


def test_run_and_plan_are_disabled_while_a_run_holds_the_lock(
    ui_project: Path,
) -> None:
    app = create_app(ui_project)
    app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    body = TestClient(app).get("/").text

    for name, tag in _pane_controls(_next_pane(body)).items():
        assert "disabled" in tag, f"{name} is still offered"
    select = _control_tag(_app_bar(body), r'<select[^>]*id="profile-select"[^>]*>')
    assert "disabled" not in select, "the selector is refused, so the runs cannot be browsed"


def test_run_and_plan_are_offered_when_the_project_is_idle(
    ui_project: Path,
) -> None:
    """The other half: a finished run must not leave the pane wedged shut."""
    app = create_app(ui_project)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )
    app.state.store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    pane = _next_pane(TestClient(app).get("/").text)

    for name, tag in _pane_controls(pane).items():
        assert "disabled" not in tag, f"{name} is refused for no reason"


def test_the_pane_says_why_run_is_disabled_and_links_to_the_run(
    ui_project: Path,
) -> None:
    """A control refused without a reason is a bug report waiting to happen.

    The link is the way out, too: the active run's page is where Stop is.
    """
    app = create_app(ui_project)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    pane = _next_pane(TestClient(app).get("/").text)

    hint = re.search(r'<a[^>]*class="next__busy"[^>]*>.*?</a>', pane, re.S)
    assert hint, "the pane does not say a run is in progress"
    assert f'href="/runs/{record.run_id}"' in hint.group(0)
    assert "hidden" not in hint.group(0), "the reason is on the page but invisible"


def test_the_busy_hint_is_hidden_when_the_project_is_idle(ui_project: Path) -> None:
    """No run, no notice -- but the element stays, hidden.

    The poller toggles it rather than building it, so the hint's markup
    lives in the template alone.
    """
    pane = _next_pane(TestClient(create_app(ui_project)).get("/").text)

    hint = re.search(r"<a[^>]*class=\"next__busy\"[^>]*>", pane, re.S)
    assert hint, "the hint element is gone, so the poller has nothing to reveal"
    assert "hidden" in hint.group(0), "the pane claims a run is in progress"


@pytest.mark.parametrize("page", ["/", "/plan"])
def test_every_workspace_page_disables_run_during_a_run(
    ui_project: Path, page: str
) -> None:
    """The run page is deliberately included: "run this again" is exactly
    the mis-click this prevents."""
    app = create_app(ui_project)
    app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    pane = _next_pane(TestClient(app).get(page).text)

    assert all("disabled" in tag for tag in _pane_controls(pane).values())


# -- ... and come back without a reload ------------------------------------
#
# Nothing server-rendered can know that the run finished a second after the
# page was drawn. The history and plan pages have no stream of their own, so
# the bar asks: one small endpoint, polled, toggling `disabled` and nothing
# else. It deliberately does not re-render the controls -- a swap would wipe
# an unsubmitted profile choice out from under the reader every few seconds.


def test_active_run_endpoint_names_the_run_holding_the_lock(
    ui_project: Path,
) -> None:
    app = create_app(ui_project)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    payload = TestClient(app).get("/active-run").json()

    assert payload == {"active": True, "run_id": record.run_id}


def test_active_run_endpoint_reports_an_idle_project(ui_project: Path) -> None:
    payload = TestClient(create_app(ui_project)).get("/active-run").json()

    assert payload == {"active": False, "run_id": None}


def test_active_run_endpoint_follows_the_lock_being_released(
    ui_project: Path,
) -> None:
    """The transition the poller exists to see."""
    app = create_app(ui_project)
    client = TestClient(app)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )
    assert client.get("/active-run").json()["active"] is True

    app.state.store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    assert client.get("/active-run").json()["active"] is False


def test_app_js_polls_the_active_run_endpoint() -> None:
    """The one piece of this that no server render can cover.

    Asserted against the source for the same reason the nav's sync is: the
    behaviour needs a browser, but the wiring -- which URL, which controls --
    is a contract the templates and the route share.
    """
    source = (STATIC_DIR / "app.js").read_text()

    assert "/active-run" in source, "app.js does not ask whether a run is active"
    assert "data-run-control" in source, "app.js does not target the bar's controls"


# -- the factory the reloader imports --------------------------------------


def test_create_app_for_cwd_serves_the_working_directory(
    ui_project_cwd: Path,
) -> None:
    """``kptn ui --reload`` needs a factory an import string can name.

    ``create_app`` takes the project root, which is why the launcher normally
    hands uvicorn the object it built. The reloader re-imports instead, in a
    subprocess that inherits the working directory -- so the name it imports
    has to be the one that reads it.
    """
    application = app_module.create_app_for_cwd()

    assert application.state.project.root == ui_project_cwd.resolve()
