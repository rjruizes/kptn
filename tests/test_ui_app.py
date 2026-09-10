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
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psutil
import pytest
from fastapi.testclient import TestClient

from kptn_server import app as app_module
from kptn_server import processes as processes_module
from kptn_server.app import create_app
from kptn_server.processes import RECONCILE_INTERVAL_SECONDS, RunProcessManager
from kptn_server.project import ProjectContext, ProjectError
from kptn_server.run_store import (
    STATUS_INTERRUPTED,
    STATUS_QUEUED,
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
    body = TestClient(create_app(ui_project)).get("/").text

    for label in ("Run", "Runs", "Plan", "How it works"):
        assert label in body, f"the base page is missing the {label!r} link"
    for href in ("/runs", "/plan", "/how-it-works"):
        assert href in body, f"the base page is missing a link to {href}"


def test_base_page_shows_the_project_and_a_profile_selector(ui_project: Path) -> None:
    body = TestClient(create_app(ui_project)).get("/").text

    assert "fixture" in body
    assert 'name="profile"' in body
    for profile in ("success", "slow", "failure", "db_error"):
        assert f'value="{profile}"' in body, f"{profile} is not selectable"
