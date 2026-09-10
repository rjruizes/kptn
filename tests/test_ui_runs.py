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

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import create_app
from kptn_server.processes import ProcessIdentity, ProcessLaunchError
from kptn_server.run_store import (
    STATUS_FAILED,
    STATUS_INTERRUPTED,
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
    """Append a ``log`` event whose text lives in the run's log file.

    This is the shape Task 5 actually writes: byte offsets, no inline text.
    The console has to read the file to have anything to show.
    """
    data = text.encode("utf-8")
    log_path = Path(record.log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "ab") as handle:
        start = handle.tell()
        handle.write(data)
    store.append_event(
        record.run_id,
        "log",
        task_name="alpha",
        payload={"stream": "stdout", "severity": "output"},
        log_start=start,
        log_end=start + len(data),
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


def test_run_page_404s_for_an_unknown_run(client: TestClient) -> None:
    response = client.get("/runs/deadbeef")

    assert response.status_code == 404
