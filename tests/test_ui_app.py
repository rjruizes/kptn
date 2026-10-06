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
import logging
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psutil
import pytest
from fastapi.testclient import TestClient

from kptn_server import app as app_module
from kptn_server import processes as processes_module
from kptn_server.app import STATIC_DIR, TEMPLATES_DIR, create_app
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


@pytest.mark.parametrize("asset", ["htmx.min.js", "app.css", "app.js"])
def test_vendored_assets_are_served(ui_project: Path, asset: str) -> None:
    client = TestClient(create_app(ui_project))

    response = client.get(f"/static/{asset}")

    assert response.status_code == 200, f"/static/{asset} is not served"
    assert response.content


def test_every_asset_link_carries_the_version_query() -> None:
    """Every vendored stylesheet and script link is cache-busted by version.

    ``StaticFiles`` sends no ``Cache-Control``, so a browser heuristically
    caches ``app.js`` and keeps running the old one after kptn is upgraded.
    ``?v={{ kptn_version }}`` changes the URL on every release. This scans the
    template sources, so a new page that forgets the query fails here.
    """
    link = re.compile(r'(?:href|src)="[^"]*/static/[^"]*"')
    links = [
        (template.name, match)
        for template in sorted(TEMPLATES_DIR.iterdir())
        if template.is_file()
        for match in link.findall(template.read_text())
    ]

    assert links, "no asset links found -- has the template layout changed?"
    stale = [
        (name, match)
        for name, match in links
        if not match.endswith('?v={{ kptn_version }}"')
    ]
    assert not stale, f"asset links without the version query: {stale}"


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


def test_base_page_offers_the_shared_navigation(ui_project: Path) -> None:
    """One nav destination, and the two actions that live in the bar.

    Asserted against the nav and the bar separately, because "Run" and
    "Plan" appear in both as words: checking the whole document for the
    string would pass no matter which side they were on.
    """
    body = TestClient(create_app(ui_project)).get("/").text
    nav = re.search(r'<nav class="app-bar__nav".*?</nav>', body, re.S).group(0)
    bar = _app_bar(body)

    assert ">History<" in nav
    assert ">Run<" in bar and ">Plan<" in bar


def test_base_page_shows_the_project_and_a_profile_selector(ui_project: Path) -> None:
    body = TestClient(create_app(ui_project)).get("/").text

    assert "fixture" in body
    assert 'name="profile"' in body
    for profile in ("success", "slow", "failure", "db_error"):
        assert f'value="{profile}"' in body, f"{profile} is not selectable"


# -- the profile follows the reader across pages ---------------------------
#
# The selector is in the app bar of every page, but the nav used to be four
# bare links: choosing a profile and then clicking Plan landed on a plan for
# no profile at all. The profile now travels in the nav's own hrefs, which is
# what makes it survive a navigation with no JavaScript involved.


def _nav_href(body: str, base: str) -> str:
    """The nav's href for the link whose base path is *base*."""
    match = re.search(
        r'<a href="([^"]*)"[^>]*data-profile-link="' + re.escape(base) + r'"', body
    )
    assert match, f"no nav link for {base!r} in the page"
    return match.group(1)


@pytest.mark.parametrize("base", ["/"])
def test_nav_carries_the_selected_profile_between_pages(
    ui_project: Path, base: str
) -> None:
    """Reached with a profile, every profile-aware nav link keeps it."""
    body = TestClient(create_app(ui_project)).get("/plan?profile=slow").text

    assert _nav_href(body, base) == f"{base}?profile=slow"


@pytest.mark.parametrize("base", ["/"])
def test_nav_omits_the_profile_when_none_is_selected(
    ui_project: Path, base: str
) -> None:
    """No profile must mean a clean URL, not ``?profile=``.

    A bare ``?profile=`` is the "(no profile)" selection spelled the long
    way, and it would turn every link in the app bar into one.
    """
    body = TestClient(create_app(ui_project)).get("/").text

    assert _nav_href(body, base) == base


def test_nav_link_to_the_history_carries_the_profile(ui_project: Path) -> None:
    """Every nav link carries it, the history's included.

    The history does not *filter* by profile, but it does carry one through
    to the next page -- so the link that gets you there has to hand it over.
    Leaving this one bare is what made "Run -> Runs" drop the profile even
    after the history learned to pass it on: the chain broke at the click,
    one step before anything server-side could help.
    """
    body = TestClient(create_app(ui_project)).get("/plan?profile=slow").text

    assert _nav_href(body, "/") == "/?profile=slow"


def test_index_accepts_a_profile_and_marks_it_selected(ui_project: Path) -> None:
    """The run console has to be able to *show* a profile it is handed.

    Without this the profile could travel to Plan and back and arrive home
    invisible -- the selector would say "(no profile)" while the URL said
    otherwise, and the next Run would use the wrong one.
    """
    body = TestClient(create_app(ui_project)).get("/?profile=slow").text

    assert '<option value="slow" selected>slow</option>' in body


def test_index_refuses_an_unknown_profile(ui_project: Path) -> None:
    """Same answer the plan and walkthrough pages already give."""
    response = TestClient(create_app(ui_project)).get("/?profile=nope")

    assert response.status_code == 400
    assert "nope" in response.text
    assert "success" in response.text, "the error does not name the real profiles"


def test_run_page_nav_carries_that_runs_profile(ui_project: Path) -> None:
    """A run knows its profile without one being in the URL.

    From a finished run, "show me the plan for this" is the obvious next
    move, so the nav offers it rather than making the reader re-pick.
    """
    app = create_app(ui_project)
    store = app.state.store
    record = store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    bar = _app_bar(TestClient(app).get(f"/runs/{record.run_id}").text)

    assert re.search(r'<input[^>]*name="profile"[^>]*value="slow"', bar), (
        "the Plan button does not offer this run's profile"
    )
    assert '<option value="slow" selected>' in bar, "the selector does not show it"


def test_app_js_syncs_the_nav_with_the_live_selector() -> None:
    """The unsubmitted selection is the case HTML cannot express.

    On the run console the selector is an input for ``run-form``, so its
    value reaches the server only on submit. Nothing server-rendered can know
    it, which is exactly the bug: pick a profile, click Plan, get no profile.
    This is the one piece that needs script, so assert it is wired to the
    same ``data-profile-link`` contract the templates render.
    """
    source = (STATIC_DIR / "app.js").read_text()

    assert "data-profile-link" in source, "app.js does not target the nav links"
    assert "profile-select" in source, "app.js does not read the selector"


def test_unknown_profile_error_page_offers_a_way_out(ui_project: Path) -> None:
    """The error page's nav must not carry the profile that caused it.

    The whole point of naming the declared profiles is that the reader can
    get somewhere useful. A nav that propagated the bad profile would make
    every link on the page another 400 -- the dead end this error exists to
    replace.
    """
    body = TestClient(create_app(ui_project)).get("/?profile=nope").text

    for base in ("/",):
        assert _nav_href(body, base) == base, (
            f"the {base!r} link carries the profile that was just refused"
        )
    hidden = re.search(r'<input[^>]*name="profile"[^>]*>', _app_bar(body))
    assert hidden and "disabled" in hidden.group(0), (
        "the Plan button still carries the profile that was just refused"
    )


# -- the run history keeps the profile without claiming to filter by it ----


def test_run_history_refuses_an_unknown_profile(ui_project: Path) -> None:
    """Same answer every other profile-aware page gives."""
    response = TestClient(create_app(ui_project)).get("/runs?profile=nope")

    assert response.status_code == 400
    assert "success" in response.text, "the error does not name the real profiles"


# -- the app bar is the one place a run starts -----------------------------
#
# There is no separate "Run" page any more: "/" is the run history, and the
# controls that used to live on that page -- the profile, the Run button --
# are in the app bar, on every page. Plan joins them there and leaves the
# nav, because it acts on the selected profile exactly as Run does.


def _app_bar(body: str) -> str:
    match = re.search(r'<header class="app-bar">(.*?)</header>', body, re.S)
    assert match, "the app bar is gone"
    return match.group(1)


def test_root_serves_the_run_history(ui_project: Path) -> None:
    """ "/" is the history now, not a console shell."""
    app = create_app(ui_project)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    body = TestClient(app).get("/").text

    assert record.run_id in body


def test_app_bar_orders_profile_then_run_then_plan(ui_project: Path) -> None:
    """The order is the request: profile, then Run, then Plan."""
    bar = _app_bar(TestClient(create_app(ui_project)).get("/").text)

    profile_at = bar.find('id="profile-select"')
    run_at = bar.find('id="run-form"')
    plan_at = bar.find('action="/plan"')

    assert -1 not in (profile_at, run_at, plan_at), "a control is missing from the bar"
    assert profile_at < run_at < plan_at


def test_app_bar_run_button_posts_the_selected_profile(ui_project: Path) -> None:
    """The select carries ``form="run-form"``, so the bar's form takes it.

    That association is the whole reason the selector can sit outside the
    form it drives, and it is what makes Run work from any page.
    """
    bar = _app_bar(TestClient(create_app(ui_project)).get("/").text)

    assert '<form id="run-form"' in bar
    assert 'method="post"' in bar and 'action="/runs"' in bar
    assert 'form="run-form"' in bar, "the selector is not bound to the run form"
    assert "button--primary" in bar, "Run is not the primary action"


def test_app_bar_plan_button_carries_the_selected_profile(ui_project: Path) -> None:
    """Plan is a GET to the plan page for whatever profile is selected."""
    bar = _app_bar(TestClient(create_app(ui_project)).get("/?profile=slow").text)

    assert 'action="/plan"' in bar
    assert re.search(r'<input[^>]*name="profile"[^>]*value="slow"', bar), (
        "the Plan button does not carry the profile"
    )


def test_app_bar_plan_button_sends_nothing_when_no_profile_is_selected(
    ui_project: Path,
) -> None:
    """A disabled input is not submitted, so Plan stays a clean ``/plan``."""
    bar = _app_bar(TestClient(create_app(ui_project)).get("/").text)

    hidden = re.search(r'<input[^>]*name="profile"[^>]*>', bar)
    assert hidden, "the Plan form has no profile input"
    assert "disabled" in hidden.group(0)


def test_nav_no_longer_offers_a_plan_link(ui_project: Path) -> None:
    """Plan is a button in the bar; a second one in the nav is the duplicate
    the restructure removes."""
    body = TestClient(create_app(ui_project)).get("/").text
    nav = re.search(r'<nav class="app-bar__nav".*?</nav>', body, re.S)
    assert nav, "the nav is gone"

    assert ">Plan<" not in nav.group(0)


def test_nav_offers_the_history_alone(ui_project: Path) -> None:
    """One destination left, and no "Run" page to link to.

    The walkthrough is off the bar: ``/walkthrough`` still serves, and the
    plan page still links into it, but the nav does not offer it.
    """
    body = TestClient(create_app(ui_project)).get("/").text
    nav = re.search(r'<nav class="app-bar__nav".*?</nav>', body, re.S).group(0)

    assert _nav_href(nav, "/") == "/"
    assert "/walkthrough" not in nav, "the nav still offers the walkthrough"
    assert ">Run<" not in nav, "the nav still links to a run page that is gone"


def test_the_old_runs_url_still_reaches_the_history(ui_project: Path) -> None:
    """Bookmarks and links from the proxied notebook environment must not 404."""
    app = create_app(ui_project)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    response = TestClient(app).get("/runs")

    assert response.status_code == 200
    assert record.run_id in response.text


@pytest.mark.parametrize("page", ["/", "/plan", "/walkthrough"])
def test_every_page_can_start_a_run(ui_project: Path, page: str) -> None:
    """The point of moving Run into the bar.

    The read-only pages used to replace the selector with a GET form of
    their own, which meant the bar's select belonged to a different form --
    a Run button beside it would have posted no profile at all.
    """
    bar = _app_bar(TestClient(create_app(ui_project)).get(page).text)

    assert '<form id="run-form"' in bar
    assert 'form="run-form"' in bar, f"{page}'s selector is not bound to the run form"


# -- the bar's controls go quiet while a run is in progress ----------------
#
# One run per project is the store's rule, enforced by the lock row that
# ``create_run`` takes and ``finish_run`` releases -- POST /runs answers 409
# to anyone who asks anyway. The bar is where that rule becomes visible:
# offering an enabled Run button whose only possible outcome is an error page
# is an invitation to be told no.
#
# The disabling is an affordance, never the guard. The 409 stays exactly
# where it is; these tests are about what the reader is offered.


def _control_tag(bar: str, pattern: str) -> str:
    """The opening tag of one app-bar control, by a pattern matching it."""
    match = re.search(pattern, bar, re.S)
    assert match, f"no control matching {pattern!r} in the app bar"
    return match.group(0)


def _bar_controls(bar: str) -> dict[str, str]:
    """The three controls that act on the selected profile."""
    return {
        "select": _control_tag(bar, r"<select[^>]*id=\"profile-select\"[^>]*>"),
        "run": _control_tag(bar, r"<button[^>]*>\s*Run\s*</button>"),
        "plan": _control_tag(bar, r"<button[^>]*>\s*Plan\s*</button>"),
    }


def test_app_bar_controls_are_disabled_while_a_run_holds_the_lock(
    ui_project: Path,
) -> None:
    """The three controls that would start or re-aim a run.

    Without this the bar offers a Run button whose only outcome is the 409
    page, and a selector whose choice that page throws away.
    """
    app = create_app(ui_project)
    app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    bar = _app_bar(TestClient(app).get("/").text)

    for name, tag in _bar_controls(bar).items():
        assert "disabled" in tag, f"the {name} control is still offered"


def test_app_bar_controls_are_offered_when_the_project_is_idle(
    ui_project: Path,
) -> None:
    """The other half: a finished run must not leave the bar wedged shut."""
    app = create_app(ui_project)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )
    app.state.store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    bar = _app_bar(TestClient(app).get("/").text)

    for name, tag in _bar_controls(bar).items():
        assert "disabled" not in tag, f"the {name} control is refused for no reason"


def test_app_bar_says_why_it_is_disabled_and_links_to_the_run(
    ui_project: Path,
) -> None:
    """A control refused without a reason is a bug report waiting to happen.

    The link is the way out, too: the active run's page is where Stop is.
    """
    app = create_app(ui_project)
    record = app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    bar = _app_bar(TestClient(app).get("/").text)

    hint = re.search(r'<a[^>]*class="app-bar__busy"[^>]*>.*?</a>', bar, re.S)
    assert hint, "the bar does not say a run is in progress"
    assert f'href="/runs/{record.run_id}"' in hint.group(0)
    assert "hidden" not in hint.group(0), "the reason is on the page but invisible"


def test_app_bar_hint_is_hidden_when_the_project_is_idle(ui_project: Path) -> None:
    """No run, no notice -- but the element stays, hidden.

    The poller toggles it rather than building it, so the hint's markup
    lives in the template alone. A version assembled in JavaScript would be
    a second copy of it, free to drift from this one.
    """
    bar = _app_bar(TestClient(create_app(ui_project)).get("/").text)

    hint = re.search(r"<a[^>]*class=\"app-bar__busy\"[^>]*>", bar)
    assert hint, "the hint element is gone, so the poller has nothing to reveal"
    assert "hidden" in hint.group(0), "the bar claims a run is in progress"


@pytest.mark.parametrize("page", ["/", "/plan", "/walkthrough"])
def test_every_page_disables_the_bar_during_a_run(
    ui_project: Path, page: str
) -> None:
    """The bar is on every page, so the rule has to be on every page.

    The run console is deliberately included: its own Run button is the app
    bar's, and "run this again" is exactly the mis-click this prevents.
    """
    app = create_app(ui_project)
    app.state.store.create_run(
        RunRequest(project_root=ui_project, pipeline="fixture", profile="slow")
    )

    bar = _app_bar(TestClient(app).get(page).text)

    assert all("disabled" in tag for tag in _bar_controls(bar).values())


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
