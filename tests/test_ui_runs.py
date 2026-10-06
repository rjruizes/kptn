"""Tests for starting a run and rendering the run page.

Three things in this layer carry weight:

1. **The lock is the contract.** One active run per canonical project path, and
   a second request is *rejected*, never queued. The rejection has to name the
   run that holds the lock, because that is the only way the page can offer a
   link to it.
2. **Durability before launch.** The run row and the project lock are written
   before a worker is ever spawned. A worker that starts before its row exists
   is a worker nothing can find after a restart.
3. **Pipeline output is untrusted text.** Captured log bytes reach the page
   through Jinja's autoescaping and must never arrive as live markup.

``manager`` is a mock here on purpose: these tests exercise the route, not the
supervisor, and spawning a real worker for a redirect assertion would trade a
millisecond test for a detached process. ``tests/test_run_processes.py`` covers
the real launch path.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import create_app
from kptn_server.processes import ProcessIdentity, ProcessLaunchError
from kptn_server.routes.runs import LAUNCH_FAILURE_PREFIX
from kptn_server.run_store import (
    STATUS_FAILED,
    STATUS_INTERRUPTED,
    STATUS_SUCCEEDED,
    RunRecord,
    RunRequest,
    RunStore,
)

# Opts this module into restore_process_state and reap_spawned_workers; the
# ui_project fixture comes from tests/conftest.py too.
pytestmark = pytest.mark.ui_hygiene


# -- fixtures --------------------------------------------------------------


@pytest.fixture
def app(ui_project: Path):
    """The UI app for a private copy of the fixture project.

    Built without entering ``TestClient`` as a context manager anywhere below,
    so the reconciliation lifespan never runs: these tests are about one
    request each.
    """
    return create_app(ui_project)


@pytest.fixture
def store(app) -> RunStore:
    return app.state.store


@pytest.fixture
def manager(app) -> MagicMock:
    """A stand-in supervisor, so no test here spawns a real worker."""
    mock = MagicMock()
    mock.start.return_value = ProcessIdentity(pid=4321, started_at=1.0)
    app.state.processes = mock
    return mock


@pytest.fixture
def client(app, manager: MagicMock) -> TestClient:
    return TestClient(app)


@pytest.fixture
def seeded_active_run(app, store: RunStore) -> RunRecord:
    """A run holding the project's lock, exactly as a live run would."""
    return store.create_run(
        RunRequest(
            project_root=app.state.project.root,
            pipeline=app.state.project.pipeline_name,
            profile="slow",
        )
    )


def _seed_run(store: RunStore, project_root: Path, *, profile: str = "success"):
    record = store.create_run(
        RunRequest(project_root=project_root, pipeline="fixture", profile=profile)
    )
    store.append_event(record.run_id, "run_started")
    return record


def _seed_log_event(store: RunStore, record: RunRecord, text: str) -> None:
    """Append a ``log`` event whose text lives in the run's file.

    The shape the capture layer writes: the row holds only the span of the
    text's line in the run file, so the console has to read the file to have
    anything to show.
    """
    store.append_event(
        record.run_id,
        "log",
        task_name="alpha",
        payload={"stream": "stdout", "severity": "output"},
        text=text,
    )


# -- the opt-in itself -----------------------------------------------------


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    """Pin the ``pytestmark`` opt-in.

    Deleting the module's ``pytestmark`` line would silently strip both
    hygiene fixtures with no error and no failure. This turns that loss into a
    failure.
    """
    assert request.node.get_closest_marker("ui_hygiene") is not None


# -- starting a run --------------------------------------------------------


def test_post_run_starts_profile_and_redirects(
    client: TestClient, manager: MagicMock
) -> None:
    response = client.post("/runs", data={"profile": "success"}, follow_redirects=False)

    assert response.status_code == 303
    run_id = response.headers["location"].rsplit("/", 1)[-1]
    manager.start.assert_called_once_with(run_id)


def test_post_run_records_the_project_pipeline_and_profile(
    client: TestClient, app, store: RunStore
) -> None:
    response = client.post("/runs", data={"profile": "success"}, follow_redirects=False)

    run_id = response.headers["location"].rsplit("/", 1)[-1]
    record = store.get_run(run_id)
    assert record is not None
    assert record.profile == "success"
    assert record.pipeline == app.state.project.pipeline_name
    assert record.project_root == app.state.project.root


def test_post_run_accepts_no_profile(client: TestClient, store: RunStore) -> None:
    """``(no profile)`` is a legitimate choice, not a validation failure."""
    response = client.post("/runs", data={"profile": ""}, follow_redirects=False)

    assert response.status_code == 303
    run_id = response.headers["location"].rsplit("/", 1)[-1]
    assert store.get_run(run_id).profile is None


def test_post_run_persists_the_run_and_lock_before_launching_a_worker(
    client: TestClient, app, manager: MagicMock, store: RunStore
) -> None:
    """Durability first: the worker must never outrun its own row.

    ``manager.start`` is where a detached process comes into existence. If the
    row or the project lock were written after that call, a crash in between
    would leave a live worker no restart could ever find, and the project
    unlocked for a second concurrent run. Asserted from *inside* the launch,
    which is the only moment the ordering is observable.
    """
    observed: list[tuple[str, bool, str | None]] = []

    def inspect(run_id: str) -> ProcessIdentity:
        observed.append(
            (
                run_id,
                store.get_run(run_id) is not None,
                getattr(store.active_run(app.state.project.root), "run_id", None),
            )
        )
        return ProcessIdentity(pid=1, started_at=1.0)

    manager.start.side_effect = inspect

    response = client.post("/runs", data={"profile": "success"}, follow_redirects=False)

    # Every launch, not just the last: a route that launched something before
    # creating the run and then launched again afterwards would look correct
    # if only the final call were inspected.
    run_id = response.headers["location"].rsplit("/", 1)[-1]
    assert observed == [(run_id, True, run_id)]


def test_post_run_returns_active_fragment_when_locked(
    client: TestClient, seeded_active_run: RunRecord
) -> None:
    response = client.post(
        "/runs",
        data={"profile": "success"},
        headers={"HX-Request": "true"},
    )

    assert response.status_code == 409
    assert seeded_active_run.run_id in response.text


def test_post_run_conflict_renders_the_whole_page_for_a_plain_form_post(
    client: TestClient, seeded_active_run: RunRecord
) -> None:
    """The lock conflict is the one error a real user actually hits.

    Every form in this UI is a plain ``<form method="post">`` -- there is no
    ``hx-`` attribute anywhere in ``templates/`` -- so pressing Run while the
    project is busy is a browser *navigation*. Answering it with a bare
    fragment lands the developer on an unstyled orphan ``<div>`` with no nav
    and no stylesheet, while the same module builds a proper page for the 400,
    404 and 500 paths.
    """
    response = client.post("/runs", data={"profile": "success"})

    assert response.status_code == 409
    assert seeded_active_run.run_id in response.text
    # The page shell, not a fragment.
    assert "<!DOCTYPE html>" in response.text
    assert "/static/app.css" in response.text
    assert 'aria-label="Main"' in response.text


def test_post_run_conflict_stays_a_fragment_for_an_htmx_request(
    client: TestClient, seeded_active_run: RunRecord
) -> None:
    """The fragment path must stay a fragment, or a swap injects a whole page."""
    response = client.post(
        "/runs", data={"profile": "success"}, headers={"HX-Request": "true"}
    )

    assert response.status_code == 409
    assert seeded_active_run.run_id in response.text
    assert "<!DOCTYPE html>" not in response.text


def test_post_run_does_not_start_a_second_worker_when_locked(
    client: TestClient, manager: MagicMock, seeded_active_run: RunRecord
) -> None:
    """The lock has to be checked before the supervisor is asked for anything.

    A route that launched first and reported the conflict afterwards would
    still return 409 and still satisfy the assertion above, while having
    spawned a worker for a run that was never created.
    """
    client.post("/runs", data={"profile": "success"})

    manager.start.assert_not_called()


def test_post_run_rejects_an_unknown_profile(
    client: TestClient, manager: MagicMock, app, store: RunStore
) -> None:
    response = client.post("/runs", data={"profile": "not-a-profile"})

    assert response.status_code == 400
    assert "not-a-profile" in response.text
    manager.start.assert_not_called()
    # Nothing may be created, and above all the project must not be left
    # locked by a run that will never execute.
    assert store.list_runs(app.state.project.root) == []
    assert store.active_run(app.state.project.root) is None


def test_post_run_finishes_the_run_as_failed_when_the_launch_fails(
    client: TestClient, manager: MagicMock, app, store: RunStore
) -> None:
    """A launch that never produced a worker must not wedge the project.

    Nothing else will ever finish this run -- there is no worker to report,
    and ``reconcile()`` only settles runs it can prove are gone. So the route
    finishes it here, which is also what releases the lock.
    """
    manager.start.side_effect = ProcessLaunchError("worker vanished before exec")

    response = client.post("/runs", data={"profile": "success"})

    assert response.status_code == 500
    assert "worker vanished before exec" in response.text

    runs = store.list_runs(app.state.project.root)
    assert len(runs) == 1
    assert runs[0].status == STATUS_FAILED
    assert store.active_run(app.state.project.root) is None


@pytest.mark.parametrize(
    ("kwargs", "label"),
    [
        ({}, "no body at all"),
        ({"content": b"{}", "headers": {"content-type": "application/json"}}, "json"),
        (
            {"content": b"profile=success", "headers": {"content-type": "text/plain"}},
            "text/plain",
        ),
    ],
    ids=["no-body", "json-body", "text-body"],
)
def test_post_run_rejects_a_body_it_cannot_parse(
    client: TestClient, manager: MagicMock, app, store: RunStore, kwargs, label: str
) -> None:
    """A body this route cannot read must not be read as "no profile".

    The form field is hand-parsed (Starlette routes ``request.form()`` through
    ``python-multipart``, which is not a dependency here), so there is no
    framework layer rejecting a JSON body or an empty one. Without this gate,
    ``parse_qs`` finds no ``profile`` key, that reads as the perfectly legal
    "(no profile)" choice, and a typo'd client *starts a pipeline* instead of
    getting an error.
    """
    response = client.post("/runs", **kwargs)

    assert response.status_code == 415, f"{label} was accepted"
    manager.start.assert_not_called()
    assert store.list_runs(app.state.project.root) == []
    assert store.active_run(app.state.project.root) is None


def test_post_run_accepts_a_form_body_with_a_charset(
    client: TestClient, store: RunStore
) -> None:
    """The gate matches on the media type, not the raw header.

    A browser is entitled to send ``; charset=UTF-8``, and rejecting that
    would break the only client this UI has.
    """
    response = client.post(
        "/runs",
        content=b"profile=success",
        headers={"content-type": "application/x-www-form-urlencoded; charset=UTF-8"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    run_id = response.headers["location"].rsplit("/", 1)[-1]
    assert store.get_run(run_id).profile == "success"


def test_post_run_persists_the_launch_error_for_a_later_reload(
    client: TestClient, manager: MagicMock, app, store: RunStore
) -> None:
    """The reason a run never started has to survive a page refresh.

    It used to exist only in the 500 body: reload the run page and you got a
    failed run with no explanation anywhere. The design says a failure to
    spawn the worker marks the run failed *with the launch error*, so the
    error is appended as a console event before the run is finished.
    """
    manager.start.side_effect = ProcessLaunchError("worker vanished before exec")

    first = client.post("/runs", data={"profile": "success"})
    assert first.status_code == 500

    run_id = store.list_runs(app.state.project.root)[0].run_id
    reloaded = client.get(f"/runs/{run_id}")

    assert reloaded.status_code == 200
    assert LAUNCH_FAILURE_PREFIX in reloaded.text
    assert "worker vanished before exec" in reloaded.text
    assert f'data-status="{STATUS_FAILED}"' in reloaded.text


def test_an_unidentifiable_worker_fails_the_run_like_any_other_launch_failure(
    app, store: RunStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One event, one status.

    ``RunProcessManager.start`` used to write ``interrupted`` when it could
    not identify the process it had just spawned, while the route wrote
    ``failed`` for every other launch failure -- and then the route's
    ``finally`` called ``finish_run`` on that already-terminal run and logged
    a full traceback for a case it had handled. ``start`` now finishes
    nothing and the route is the single writer.
    """
    import psutil

    from kptn_server import processes as processes_module

    class _FakePopen:
        pid = 999999

        def poll(self):
            return 1

    monkeypatch.setattr(
        processes_module.subprocess, "Popen", lambda *a, **k: _FakePopen()
    )

    def _no_such_process(pid):
        raise psutil.NoSuchProcess(pid)

    monkeypatch.setattr(processes_module.psutil, "Process", _no_such_process)

    app.state.processes = processes_module.RunProcessManager(store)
    client = TestClient(app)

    response = client.post("/runs", data={"profile": "success"})

    assert response.status_code == 500
    runs = store.list_runs(app.state.project.root)
    assert len(runs) == 1
    assert runs[0].status == STATUS_FAILED
    assert store.active_run(app.state.project.root) is None

    body = client.get(f"/runs/{runs[0].run_id}").text
    assert LAUNCH_FAILURE_PREFIX in body
    assert "vanished before it could be identified" in body


def test_post_run_releases_the_lock_when_the_launch_is_interrupted(
    client: TestClient, manager: MagicMock, app, store: RunStore
) -> None:
    """A BaseException in the launch window must not wedge the project.

    ``reconcile()`` can only settle a run it can prove is gone, and proof is a
    recorded pid -- which a run whose launch never returned does not have. So
    a ``KeyboardInterrupt`` arriving between ``create_run`` and a successful
    ``start`` would leave the project locked by an unfinishable run until
    someone edited the database by hand. Cleanup therefore belongs in a
    ``finally``, not in an ``except Exception``.
    """
    manager.start.side_effect = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        client.post("/runs", data={"profile": "success"})

    runs = store.list_runs(app.state.project.root)
    assert len(runs) == 1
    assert runs[0].status == STATUS_FAILED
    assert store.active_run(app.state.project.root) is None


# -- the run page ----------------------------------------------------------


def test_run_page_renders_existing_events_with_sequence_anchors(
    client: TestClient, app, store: RunStore
) -> None:
    record = _seed_run(store, app.state.project.root)
    store.append_event(record.run_id, "task_started", task_name="alpha")

    body = client.get(f"/runs/{record.run_id}").text

    for sequence in (1, 2):
        assert f'data-sequence="{sequence}"' in body
        assert f'id="event-{sequence}"' in body


def test_run_page_shows_captured_log_text(
    client: TestClient, app, store: RunStore
) -> None:
    """The log text is not in the event -- the server has to read the file.

    Task 5's ``log`` payloads carry byte offsets and no message at all, so a
    console that only rendered payloads would show an empty run.
    """
    record = _seed_run(store, app.state.project.root)
    _seed_log_event(store, record, "hello from the pipeline\n")

    body = client.get(f"/runs/{record.run_id}").text

    assert "hello from the pipeline" in body


def test_run_page_escapes_captured_log_text(
    client: TestClient, app, store: RunStore
) -> None:
    """Pipeline output must never become live markup.

    Log bytes are attacker-adjacent: a dependency's banner, a filename, a SQL
    error echoing a value. One ``| safe`` on the console's text node turns any
    of those into script execution in the developer's browser.
    """
    record = _seed_run(store, app.state.project.root)
    _seed_log_event(store, record, "<script>alert('xss')</script>\n")

    body = client.get(f"/runs/{record.run_id}").text

    assert "<script>alert('xss')</script>" not in body
    assert "&lt;script&gt;" in body


def test_run_page_renders_a_terminal_status_with_no_run_finished_event(
    client: TestClient, app, store: RunStore
) -> None:
    """``reconcile()`` writes status only and appends no event.

    So an interrupted run's fate exists *nowhere* in its event stream. A page
    that derived its state from the events would show this run as still
    running forever.
    """
    record = _seed_run(store, app.state.project.root, profile="slow")
    store.finish_run(record.run_id, STATUS_INTERRUPTED)

    body = client.get(f"/runs/{record.run_id}").text

    assert STATUS_INTERRUPTED in body
    assert f'data-status="{STATUS_INTERRUPTED}"' in body


def test_run_page_marks_the_runs_profile_selected(
    client: TestClient, app, store: RunStore
) -> None:
    """The base page's selector should show the profile actually in play."""
    record = _seed_run(store, app.state.project.root, profile="failure")

    body = client.get(f"/runs/{record.run_id}").text

    assert 'value="failure" selected' in body


def test_run_page_links_its_event_stream(
    client: TestClient, app, store: RunStore
) -> None:
    record = _seed_run(store, app.state.project.root)

    body = client.get(f"/runs/{record.run_id}").text

    assert f"/runs/{record.run_id}/events" in body


def _run_heading(body: str) -> str:
    """The run panel's heading, which is a breadcrumb rather than a title."""
    match = re.search(r"<h2[^>]*>.*?</h2>", body, re.S)
    assert match, "the run panel has no heading"
    return match.group(0)


def test_run_heading_leads_back_to_the_history(
    client: TestClient, app, store: RunStore
) -> None:
    """"Run" named the page; "Runs" gets you off it.

    A single run's page is reached from the history and has nowhere else to
    go, so the heading is the way back rather than a label for where you
    already know you are.
    """
    record = _seed_run(store, app.state.project.root, profile="slow")

    heading = _run_heading(client.get(f"/runs/{record.run_id}").text)

    assert ">Runs<" in heading
    assert 'href="/?profile=slow"' in heading, "the way back drops the profile"


def test_run_heading_names_the_runs_profile(
    client: TestClient, app, store: RunStore
) -> None:
    """The second crumb is which run this is: its profile."""
    record = _seed_run(store, app.state.project.root, profile="slow")

    heading = _run_heading(client.get(f"/runs/{record.run_id}").text)

    assert "slow" in heading


def test_run_heading_says_so_when_the_run_had_no_profile(
    client: TestClient, app, store: RunStore
) -> None:
    """Running with no profile is a real, documented way to run a pipeline.

    A crumb that just stopped after "Runs" would read as a missing value
    rather than the deliberate one it is.
    """
    record = _seed_run(store, app.state.project.root, profile=None)

    heading = _run_heading(client.get(f"/runs/{record.run_id}").text)

    assert "no profile" in heading
    assert "?profile=" not in heading, "the way back carries an empty profile"


def _retry_form(body: str) -> str:
    match = re.search(r'<form class="run-header__retry".*?</form>', body, re.S)
    assert match, "the run header offers no retry"
    return match.group(0)


def test_run_page_offers_a_retry_for_the_runs_own_profile(
    client: TestClient, app, store: RunStore
) -> None:
    """"Run this again" without going back to the bar and re-picking.

    It posts the profile explicitly rather than borrowing the app bar's
    selector, which the reader may have changed since the page loaded.
    """
    record = _seed_run(store, app.state.project.root, profile="slow")
    store.finish_run(record.run_id, STATUS_FAILED, exit_code=1)

    form = _retry_form(client.get(f"/runs/{record.run_id}").text)

    assert 'action="/runs"' in form and 'method="post"' in form
    assert re.search(r'<input[^>]*name="profile"[^>]*value="slow"', form)


def test_retry_submits_an_empty_profile_for_a_run_that_had_none(
    client: TestClient, app, store: RunStore
) -> None:
    """``POST /runs`` rejects a body with no ``profile`` field at all.

    An omitted field is what a drive-by form post looks like, so the route
    refuses it; "(no profile)" is the field sent empty. A retry that dropped
    the input would be a 400 rather than a run.
    """
    record = _seed_run(store, app.state.project.root, profile=None)
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    form = _retry_form(client.get(f"/runs/{record.run_id}").text)

    assert re.search(r'<input[^>]*name="profile"[^>]*value=""', form)


def test_retry_is_not_a_second_run_form(
    client: TestClient, app, store: RunStore
) -> None:
    """The app bar's form owns ``id="run-form"``, and an id is unique.

    A second one would capture the bar's own selector, which binds by form
    id -- the bar's Run would then post whatever this form carried.
    """
    record = _seed_run(store, app.state.project.root, profile="slow")
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    body = client.get(f"/runs/{record.run_id}").text

    assert body.count('id="run-form"') == 1


def test_retry_is_disabled_while_a_run_holds_the_lock(
    client: TestClient, app, store: RunStore
) -> None:
    """Same rule as the app bar's Run: one run per project.

    ``data-run-control`` is how it joins the group the active-run poll
    re-enables, so a retry offered on a page that was loaded mid-run comes
    back by itself when that run ends.
    """
    record = _seed_run(store, app.state.project.root, profile="slow")

    form = _retry_form(client.get(f"/runs/{record.run_id}").text)

    assert "disabled" in form
    assert "data-run-control" in form


def test_run_page_is_one_panel(client: TestClient, app, store: RunStore) -> None:
    """The heading, the controls and the console are one surface, not two.

    They were two bordered cards with a gap between them, which read as two
    unrelated things -- and the top one was four lines tall.
    """
    record = _seed_run(store, app.state.project.root)

    body = client.get(f"/runs/{record.run_id}").text
    panels = re.findall(r'<section class="panel[^"]*"', body)

    assert len(panels) == 1, f"the run page renders {len(panels)} panels"
    panel = body[body.index('<section class="panel') :]
    assert panel.index('id="run-header"') < panel.index('id="console"'), (
        "the console renders before the run's own header"
    )


def test_run_header_stays_a_swappable_region(
    client: TestClient, app, store: RunStore
) -> None:
    """Merging the panels must not cost the stream its target.

    ``app.js`` replaces ``#run-header`` by id when a run goes terminal under
    an open page; an id that moved into the panel's wrapper would be replaced
    along with the console.
    """
    record = _seed_run(store, app.state.project.root)

    body = client.get(f"/runs/{record.run_id}").text

    header_at = body.index('id="run-header"')
    console_at = body.index('id="console"')
    assert body.index("</div>", header_at) < console_at, (
        "the run header region is not closed before the console starts"
    )


def test_run_heading_carries_the_status_after_the_profile(
    client: TestClient, app, store: RunStore
) -> None:
    """One line: which runs, which profile, how it is going.

    The status was a row of its own under the heading, which spent a line on
    a single word.
    """
    record = _seed_run(store, app.state.project.root, profile="slow")

    heading = _run_heading(client.get(f"/runs/{record.run_id}").text)

    assert 'id="run-status"' in heading, "the status is not in the heading"
    assert heading.index("slow") < heading.index('id="run-status"'), (
        "the status renders before the profile it belongs to"
    )


def test_run_page_does_not_report_an_exit_code(
    client: TestClient, app, store: RunStore
) -> None:
    """The status already says how the run ended.

    A number beside it is the shell's vocabulary, not the reader's, and the
    history row still carries it for anyone who wants it.
    """
    record = _seed_run(store, app.state.project.root)
    store.finish_run(record.run_id, STATUS_FAILED, exit_code=2)

    body = client.get(f"/runs/{record.run_id}").text

    assert "run-status__exit" not in body


def test_status_line_no_longer_repeats_the_profile(
    client: TestClient, app, store: RunStore
) -> None:
    """It is in the heading above, one line up."""
    record = _seed_run(store, app.state.project.root, profile="slow")

    body = client.get(f"/runs/{record.run_id}").text
    status = re.search(r'<span class="run-status".*?</span>\s*</h2>', body, re.S)
    assert status, "the status line is gone"

    assert "run-status__profile" not in status.group(0)


def test_status_line_does_not_repeat_the_run_id(
    client: TestClient, app, store: RunStore
) -> None:
    """The status line says what the run is doing, and nothing else.

    It used to end in a link whose text was the run's 32-character hash. The
    id is in the URL, in the history row, and in ``data-run-id`` on this very
    element for anything that needs it -- and the app bar's "a run is in
    progress" notice is what links to the active run from elsewhere, which is
    the one place that link was the way out.
    """
    record = _seed_run(store, app.state.project.root)

    body = client.get(f"/runs/{record.run_id}").text
    status = re.search(r'<span class="run-status".*?</span>\s*</h2>', body, re.S)
    assert status, "the status line is gone"

    assert "run-status__link" not in status.group(0)
    assert f">{record.run_id}<" not in status.group(0)


def test_run_page_404s_for_an_unknown_run(client: TestClient) -> None:
    response = client.get("/runs/deadbeef")

    assert response.status_code == 404
