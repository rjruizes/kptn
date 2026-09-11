"""Tests for run history, the completion summary, log download, and stop.

Four surfaces meet here, and each one has a way of going quietly wrong:

1. **History is durable, not remembered.** ``GET /runs`` reads the project's
   run store, so a run recorded by a server that has since been restarted (or
   by a detached worker while no server was up at all) still shows. The
   guard for that is ``test_history_survives_app_recreation``, which throws
   the whole application away between the write and the read.

2. **The summary is computed from lifecycle events and warning groups, never
   from console text.** A task that *prints* the words ``task_started`` must
   not add to the task count, and a warning that occurred five times must
   still be five linkable occurrences after the UI groups it.

3. **The raw log is served from the stored run's path and nothing else.** No
   query parameter, header, or path segment may pick the file. The download
   is the one place in this UI that hands a file back, so the tests here
   both point the *stored* path at a decoy (proving the store is the source)
   and pass a hostile ``?path=`` (proving the request is not).

4. **Stop records intent; only a worker records ``stopped``.** A stop that
   cannot signal anything is a success, because the intent is durable and
   reconciliation will settle the run. The one case reconciliation *cannot*
   settle -- a worker whose liveness can never be determined -- is what the
   force-finish escape hatch exists for, and it must not be reachable by the
   same click as an ordinary Stop.

Nothing here synchronizes on ``sleep``. Where a test needs the supervisor's
grace window to have elapsed, it injects a clock.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import create_app
from kptn_server.processes import STALE_WORKER_GRACE_SECONDS, RunProcessManager
from kptn_server.routes.runs import (
    FORCE_FINISH_CONFIRMATION,
    HISTORY_LIMIT,
    render_region,
)
from kptn_server.run_store import (
    STATUS_FAILED,
    STATUS_INTERRUPTED,
    STATUS_RUNNING,
    STATUS_STOP_REQUESTED,
    STATUS_SUCCEEDED,
    TERMINAL_STATUSES,
    ActiveRunError,
    RunRecord,
    RunRequest,
    RunStore,
)

# Opts this module into restore_process_state and reap_spawned_workers; the
# ui_project fixture comes from tests/conftest.py too.
pytestmark = pytest.mark.ui_hygiene

#: A PID no process can hold, so ``psutil`` answers "definitely gone" rather
#: than "cannot tell". Used to make a run reconcilable on purpose.
DEAD_PID = 2_147_483_646

#: Hostile strings, one per field that reaches the page, so an assertion about
#: one field cannot be satisfied by another field being escaped. Each must
#: arrive as text on every surface that shows it.
HOSTILE_MESSAGE = '<img src=x onerror="alert(1)">'
HOSTILE_TASK = '<svg onload="task()">'
HOSTILE_CATEGORY = '<iframe src="javascript:0">'
HOSTILE_PROFILE = '<object data="x">'


def _escaped(value: str) -> str:
    """*value* as Jinja's autoescaping renders it."""
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&#34;")
        .replace("'", "&#39;")
    )


# -- fixtures --------------------------------------------------------------


@pytest.fixture
def app(ui_project: Path):
    """The UI app for a private copy of the fixture project.

    Never entered as a context manager, so the reconciliation lifespan does
    not run: the tests that want a reconciliation pass drive one themselves,
    with a clock they control.
    """
    return create_app(ui_project)


@pytest.fixture
def store(app) -> RunStore:
    return app.state.store


@pytest.fixture
def manager(app) -> MagicMock:
    """A stand-in supervisor, so no test here spawns a real worker."""
    mock = MagicMock()
    mock.stop.return_value = True
    app.state.processes = mock
    return mock


@pytest.fixture
def client(app, manager: MagicMock) -> TestClient:
    return TestClient(app)


def _seed_run(store: RunStore, project_root: Path, *, profile: str = "success"):
    record = store.create_run(
        RunRequest(project_root=project_root, pipeline="fixture", profile=profile)
    )
    store.append_event(record.run_id, "run_started")
    return record


@pytest.fixture
def completed_run(app, store: RunStore) -> RunRecord:
    """A finished run with three warnings across two tasks.

    The sequences matter, because the summary links to them:

    ==  ===========================================
    1   run_started
    2   task_started alpha
    3   warning alpha "deprecated call"
    4   warning alpha "deprecated call"  <- same group as 3
    5   task_finished alpha succeeded
    6   task_started beta
    7   warning beta "missing column"
    8   task_finished beta succeeded
    9   run_finished
    ==  ===========================================

    So: three warnings in two tasks, and the alpha group has two occurrences
    whose anchors are ``#event-3`` and ``#event-4``.
    """
    record = _seed_run(store, app.state.project.root)
    store.append_event(record.run_id, "task_started", task_name="alpha")
    store.append_event(
        record.run_id,
        "warning",
        task_name="alpha",
        payload={"message": "deprecated call"},
    )
    store.append_event(
        record.run_id,
        "warning",
        task_name="alpha",
        payload={"message": "deprecated call"},
    )
    store.append_event(
        record.run_id,
        "task_finished",
        task_name="alpha",
        payload={"status": "succeeded", "duration_seconds": 0.5},
    )
    store.append_event(record.run_id, "task_started", task_name="beta")
    store.append_event(
        record.run_id,
        "warning",
        task_name="beta",
        payload={"message": "missing column"},
    )
    store.append_event(
        record.run_id,
        "task_finished",
        task_name="beta",
        payload={"status": "succeeded", "duration_seconds": 0.5},
    )
    store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
    return store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)


@pytest.fixture
def seeded_store(ui_project: Path) -> RunRecord:
    """A run written straight into the project's store, with no app involved.

    This is what makes the durability guarantee testable: the run exists on
    disk before any application object does, and survives every one of them.
    """
    store = RunStore(ui_project / ".kptn" / "ui.db")
    return _seed_run(store, ui_project)


@pytest.fixture
def active_run(app, store: RunStore) -> RunRecord:
    """A running run holding the project's active-run lock."""
    return _seed_run(store, app.state.project.root, profile="slow")


# -- helpers ---------------------------------------------------------------


def _write_column(store: RunStore, run_id: str, column: str, value: object) -> None:
    """Set one ``runs`` column directly.

    Used only to make time and worker identity deterministic. Going through
    SQLite rather than the store's API is deliberate: the store has no
    "pretend this run was created yesterday" method and should not grow one
    for a test's benefit.
    """
    conn = sqlite3.connect(store.path)
    try:
        conn.execute(f"UPDATE runs SET {column} = ? WHERE run_id = ?", (value, run_id))
        conn.commit()
    finally:
        conn.close()


def _run_ids_in_order(body: str) -> list[str]:
    """The run ids of the history list, in the order the page renders them."""
    return re.findall(r'data-run-id="([0-9a-f]{32})"', body)


def _seed_finished_run(
    store: RunStore, project_root: Path, *, created_at: str, profile: str
) -> RunRecord:
    """A finished run with an explicit creation time.

    Finished, because only one run may hold a project's lock at a time -- a
    history of three runs is a history of three runs that ended.
    """
    record = _seed_run(store, project_root, profile=profile)
    store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
    _write_column(store, record.run_id, "created_at", created_at)
    return record


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


# -- the completion summary ------------------------------------------------


def test_completed_run_groups_warning_links(client, completed_run) -> None:
    response = client.get(f"/runs/{completed_run.run_id}")
    assert "3 warnings in 2 tasks" in response.text
    assert 'href="#event-4"' in response.text
    assert "2 occurrences" in response.text


def test_warning_group_links_every_occurrence(client, completed_run) -> None:
    """Grouping is presentation; no occurrence may be dropped.

    The store keeps all three warning events and the group carries all of
    their sequences, so the summary has to offer a link to each. A summary
    that linked only the first (or only the last) would hide the repeat the
    count is advertising.
    """
    body = client.get(f"/runs/{completed_run.run_id}").text
    for sequence in (3, 4, 7):
        assert f'href="#event-{sequence}"' in body
    # And the anchors exist to jump to: the raw occurrences are still in the
    # console, not collapsed away.
    for sequence in (3, 4, 7):
        assert f'id="event-{sequence}"' in body


def test_single_warning_reads_as_one_occurrence(client, completed_run) -> None:
    """Pluralization, pinned. The beta group has exactly one warning."""
    body = client.get(f"/runs/{completed_run.run_id}").text
    assert "1 occurrence" in body
    assert "1 occurrences" not in body


def test_summary_counts_tasks_from_lifecycle_events(
    client, store: RunStore, app
) -> None:
    """Task counts come from events, not from console text.

    The log line here says ``task_started`` three times. A summary that
    scraped console output would report five tasks; one that reads the
    lifecycle events reports two.
    """
    record = _seed_run(store, app.state.project.root)
    store.append_event(record.run_id, "task_started", task_name="alpha")
    _seed_log_event(store, record, "task_started task_started task_started\n")
    store.append_event(
        record.run_id,
        "task_finished",
        task_name="alpha",
        payload={"status": "succeeded"},
    )
    store.append_event(record.run_id, "task_started", task_name="beta")
    store.append_event(
        record.run_id, "task_skipped", task_name="gamma", payload={"cached": True}
    )
    store.append_event(
        record.run_id,
        "task_finished",
        task_name="beta",
        payload={"status": "failed", "error": "boom"},
    )
    store.append_event(record.run_id, "run_finished", payload={"status": "failed"})
    store.finish_run(record.run_id, STATUS_FAILED, exit_code=1)

    body = client.get(f"/runs/{record.run_id}").text
    summary = _summary_section(body)
    assert 'data-summary="tasks" data-count="2"' in summary
    assert 'data-summary="skipped" data-count="1"' in summary
    assert 'data-summary="succeeded" data-count="1"' in summary
    assert 'data-summary="failed" data-count="1"' in summary


def test_summary_counts_an_unfinished_task(client, store: RunStore, app) -> None:
    """A task that started and never finished is reported, not silently lost.

    This is the shape an interrupted run leaves behind, and the number a
    reader needs in order to know where to look.
    """
    record = _seed_run(store, app.state.project.root)
    store.append_event(record.run_id, "task_started", task_name="alpha")
    store.append_event(
        record.run_id,
        "task_finished",
        task_name="alpha",
        payload={"status": "succeeded"},
    )
    store.append_event(record.run_id, "task_started", task_name="beta")
    store.finish_run(record.run_id, STATUS_INTERRUPTED)

    summary = _summary_section(client.get(f"/runs/{record.run_id}").text)
    assert 'data-summary="tasks" data-count="2"' in summary
    assert 'data-summary="unfinished" data-count="1"' in summary


def test_summary_reports_no_warnings_when_there_are_none(
    client, store: RunStore, app
) -> None:
    record = _seed_run(store, app.state.project.root)
    store.append_event(record.run_id, "task_started", task_name="alpha")
    store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    summary = _summary_section(client.get(f"/runs/{record.run_id}").text)
    assert "No warnings" in summary
    assert "warnings in" not in summary


def _summary_section(body: str) -> str:
    """Just the completion-summary panel, so a match cannot come from elsewhere.

    Without this, an assertion about the summary could be satisfied by the
    console below it -- which renders every warning individually and would
    make several of these tests pass against a summary that renders nothing.
    """
    start = body.index('id="run-summary"')
    return body[start : body.index("</section>", start)]


def _seed_log_event(store: RunStore, record: RunRecord, text: str) -> None:
    """Append a ``log`` event whose text lives in the run's log file."""
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


# -- history ---------------------------------------------------------------


def test_history_survives_app_recreation(ui_project, seeded_store) -> None:
    first = TestClient(create_app(ui_project))
    first.close()
    second = TestClient(create_app(ui_project))
    assert seeded_store.run_id in second.get("/runs").text


def test_history_lists_newest_first(client, store: RunStore, app) -> None:
    """Newest first, asserted rather than assumed.

    Creation times are written explicitly so the order is a property of the
    query and the route, not of how fast three inserts happened to run.
    """
    root = app.state.project.root
    oldest = _seed_finished_run(
        store, root, created_at="2026-01-01T00:00:00+00:00", profile="success"
    )
    middle = _seed_finished_run(
        store, root, created_at="2026-02-01T00:00:00+00:00", profile="failure"
    )
    newest = _seed_finished_run(
        store, root, created_at="2026-03-01T00:00:00+00:00", profile="success"
    )

    body = client.get("/runs").text
    assert _run_ids_in_order(body) == [newest.run_id, middle.run_id, oldest.run_id]


def test_history_shows_each_run_status_and_profile(
    client, store: RunStore, app
) -> None:
    record = _seed_finished_run(
        store,
        app.state.project.root,
        created_at="2026-01-01T00:00:00+00:00",
        profile="failure",
    )
    body = client.get("/runs").text
    row = _history_row(body, record.run_id)
    assert STATUS_SUCCEEDED in row
    assert "failure" in row
    assert f'href="/runs/{record.run_id}"' in row
    assert f'href="/runs/{record.run_id}/log"' in row


def test_history_summarizes_each_run_warnings(client, completed_run) -> None:
    """The history row carries the run's warning count, from its groups."""
    row = _history_row(client.get("/runs").text, completed_run.run_id)
    assert "3 warnings in 2 tasks" in row
    assert "deprecated call" in row


def test_history_is_empty_without_runs(client) -> None:
    response = client.get("/runs")
    assert response.status_code == 200
    assert _run_ids_in_order(response.text) == []
    assert "No runs recorded yet" in response.text


def test_history_only_lists_this_project(client, store: RunStore, tmp_path) -> None:
    """A store shared by two projects must not leak one into the other's page.

    ``.kptn/ui.db`` is per-project today, but the store is keyed by project
    root and nothing stops a future launcher from pointing two contexts at one
    database. The page filters, and this is the guard that it does.
    """
    other_root = tmp_path / "elsewhere"
    other_root.mkdir()
    stranger = _seed_run(store, other_root)

    body = client.get("/runs").text
    assert stranger.run_id not in _run_ids_in_order(body)


def _history_row(body: str, run_id: str) -> str:
    start = body.index(f'data-run-id="{run_id}"')
    return body[start : body.index("</li>", start)]


# -- raw log download ------------------------------------------------------


def test_log_download_sends_attachment_headers(client, store, app) -> None:
    record = _seed_run(store, app.state.project.root)
    _seed_log_event(store, record, "hello from the pipeline\n")

    response = client.get(f"/runs/{record.run_id}/log")
    assert response.status_code == 200
    assert response.text == "hello from the pipeline\n"
    disposition = response.headers["content-disposition"]
    assert "attachment" in disposition
    assert f'filename="kptn-{record.run_id}.log"' in disposition
    assert response.headers["content-type"].startswith("text/plain")


def test_log_download_resolves_the_path_from_the_stored_run(
    client, store, app, tmp_path
) -> None:
    """The served file is whatever the *run row* says, and nothing else.

    The run's stored ``log_path`` is repointed at a decoy outside the project.
    A route that derived the path from the run id, the project's run-log
    directory, or anything else in the request would still serve the original
    file and fail here.
    """
    record = _seed_run(store, app.state.project.root)
    _seed_log_event(store, record, "the original log\n")
    decoy = tmp_path / "decoy.log"
    decoy.write_text("the stored path won\n")
    _write_column(store, record.run_id, "log_path", str(decoy))

    response = client.get(f"/runs/{record.run_id}/log")
    assert response.text == "the stored path won\n"
    assert (
        f'filename="kptn-{record.run_id}.log"'
        in (response.headers["content-disposition"])
    )


@pytest.mark.parametrize("parameter", ["path", "log_path", "file", "filename"])
def test_log_download_ignores_a_request_supplied_path(
    client, store, app, tmp_path, parameter: str
) -> None:
    """No query parameter may choose the file.

    The secret here stands in for anything readable by the developer's own
    account -- which is everything, since this UI has no authentication.
    """
    record = _seed_run(store, app.state.project.root)
    _seed_log_event(store, record, "the run's own log\n")
    secret = tmp_path / "secret.txt"
    secret.write_text("root:x:0:0:\n")

    response = client.get(f"/runs/{record.run_id}/log", params={parameter: str(secret)})
    assert response.status_code == 200
    assert response.text == "the run's own log\n"
    assert "root:x:0:0" not in response.text


@pytest.mark.parametrize(
    "run_id",
    [
        "..%2f..%2f..%2f..%2fetc%2fpasswd",
        "%2e%2e%2f%2e%2e%2fetc%2fpasswd",
        "not-a-run-id",
    ],
)
def test_log_download_rejects_an_unknown_run(client, run_id: str) -> None:
    """A run id that is not in the store is a 404, never a file read."""
    response = client.get(f"/runs/{run_id}/log")
    assert response.status_code == 404
    assert "root:x:0:0" not in response.text


def test_log_download_is_a_404_when_the_file_is_gone(client, store, app) -> None:
    """A missing log file is a 404 page, not a 500.

    The log belongs to the worker: it can be deleted, rotated, or sit on a
    volume that went away. That degrades the download, it does not break the
    server.
    """
    record = _seed_run(store, app.state.project.root)
    response = client.get(f"/runs/{record.run_id}/log")
    assert response.status_code == 404
    assert record.run_id in response.text


def test_run_page_offers_the_log_download(client, completed_run) -> None:
    """A real GET form, so the control is a ``<button>`` the UI already styles
    rather than a link wearing a class no stylesheet defines."""
    body = client.get(f"/runs/{completed_run.run_id}").text
    assert f'action="/runs/{completed_run.run_id}/log"' in body
    assert 'method="get"' in body


# -- stop ------------------------------------------------------------------


def test_stop_requests_a_stop_and_signals_the_worker(
    client, store: RunStore, manager: MagicMock, active_run: RunRecord
) -> None:
    response = client.post(f"/runs/{active_run.run_id}/stop", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == f"/runs/{active_run.run_id}"
    manager.stop.assert_called_once_with(active_run.run_id)
    assert store.get_run(active_run.run_id).status == STATUS_STOP_REQUESTED


def test_stop_succeeds_when_no_worker_could_be_signalled(
    client, store: RunStore, manager: MagicMock, active_run: RunRecord
) -> None:
    """``stop()`` returning ``False`` is a success, not a server error.

    It means no live worker matched, the intent is recorded, and
    reconciliation will finish the run. Surfacing that as a 500 would tell the
    developer their stop failed when it did exactly what it promises.
    """
    manager.stop.return_value = False
    response = client.post(f"/runs/{active_run.run_id}/stop", follow_redirects=False)
    assert response.status_code == 303
    assert store.get_run(active_run.run_id).status == STATUS_STOP_REQUESTED


def test_stop_is_rejected_for_a_terminal_run(
    client, store: RunStore, manager: MagicMock, completed_run: RunRecord
) -> None:
    response = client.post(f"/runs/{completed_run.run_id}/stop")
    assert response.status_code == 409
    manager.stop.assert_not_called()
    assert store.get_run(completed_run.run_id).status == STATUS_SUCCEEDED


def test_stop_is_a_404_for_an_unknown_run(client, manager: MagicMock) -> None:
    response = client.post("/runs/nope/stop")
    assert response.status_code == 404
    manager.stop.assert_not_called()


def test_stop_does_not_finish_the_run_or_release_the_lock(
    client, store: RunStore, manager: MagicMock, active_run: RunRecord
) -> None:
    """Only a worker records ``stopped``.

    ``stop_requested`` is not terminal: the project's lock stays held and the
    run stays out of the terminal statuses until the worker (or
    reconciliation) says otherwise. A stop that finished the run itself would
    let a second run start on top of a worker that is still writing.
    """
    manager.stop.return_value = False
    client.post(f"/runs/{active_run.run_id}/stop")

    record = store.get_run(active_run.run_id)
    assert record.status == STATUS_STOP_REQUESTED
    assert record.finished_at is None
    assert store.active_run(active_run.project_root).run_id == active_run.run_id


def test_run_page_offers_stop_only_while_a_run_is_live(
    client, completed_run: RunRecord, active_run: RunRecord
) -> None:
    """``completed_run`` is requested first on purpose: only one run may hold
    the project's lock, so the finished one has to exist before the live one
    takes it."""
    live = client.get(f"/runs/{active_run.run_id}").text
    assert f'action="/runs/{active_run.run_id}/stop"' in live

    done = client.get(f"/runs/{completed_run.run_id}").text
    assert f'action="/runs/{completed_run.run_id}/stop"' not in done


# -- the force-finish escape hatch -----------------------------------------


def test_force_finish_requires_the_typed_confirmation(
    client, store: RunStore, manager: MagicMock, active_run: RunRecord
) -> None:
    """Abandoning a possibly-live worker is not a one-click action.

    Without the typed word the request is refused and *nothing* changes: the
    run keeps its status and the project keeps its lock.
    """
    response = client.post(
        f"/runs/{active_run.run_id}/force-finish", data={"unrelated": "1"}
    )
    assert response.status_code == 400

    record = store.get_run(active_run.run_id)
    assert record.status == STATUS_RUNNING
    assert store.active_run(active_run.project_root).run_id == active_run.run_id


@pytest.mark.parametrize(
    "confirm", ["", "yes", "ok", "Abandon this run", "ABANDON", "abandoned"]
)
def test_force_finish_rejects_a_wrong_confirmation(
    client, store: RunStore, active_run: RunRecord, confirm: str
) -> None:
    """Only the exact word counts -- not a prefix, not another case, not a
    sentence containing it. Surrounding whitespace is stripped, which is the
    only latitude the comparison allows."""
    response = client.post(
        f"/runs/{active_run.run_id}/force-finish", data={"confirm": confirm}
    )
    assert response.status_code == 400
    assert store.get_run(active_run.run_id).status == STATUS_RUNNING


def test_force_finish_accepts_the_confirmation_with_stray_whitespace(
    client, store: RunStore, active_run: RunRecord
) -> None:
    """A trailing space from a paste is not a reason to refuse."""
    response = client.post(
        f"/runs/{active_run.run_id}/force-finish",
        data={"confirm": f"  {FORCE_FINISH_CONFIRMATION} "},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert store.get_run(active_run.run_id).status == STATUS_INTERRUPTED


def test_force_finish_abandons_the_worker_and_releases_the_lock(
    client, store: RunStore, manager: MagicMock, active_run: RunRecord
) -> None:
    """The escape hatch for a run reconciliation can never settle.

    A worker whose liveness is permanently un-inspectable keeps its run out of
    every terminal status forever, so the project's active-run lock is never
    released and the project cannot be run again. Force-finish records
    ``interrupted`` -- the status that already means "this run's real fate is
    unknown" -- which releases the lock in the same transaction.
    """
    response = client.post(
        f"/runs/{active_run.run_id}/force-finish",
        data={"confirm": FORCE_FINISH_CONFIRMATION},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == f"/runs/{active_run.run_id}"

    record = store.get_run(active_run.run_id)
    assert record.status == STATUS_INTERRUPTED
    assert record.finished_at is not None
    assert store.active_run(active_run.project_root) is None

    # The lock really is gone: a new run can be created for the project.
    replacement = store.create_run(
        RunRequest(project_root=active_run.project_root, pipeline="fixture")
    )
    assert replacement.run_id != active_run.run_id


def test_force_finish_does_not_signal_anything(
    client, manager: MagicMock, active_run: RunRecord
) -> None:
    """It abandons the worker; it does not pretend to have stopped it.

    Signalling here would be a lie in both directions: the PID may have been
    recycled (so the signal could hit an innocent process) and the worker may
    be alive and unreachable (so the signal would not arrive).
    """
    client.post(
        f"/runs/{active_run.run_id}/force-finish",
        data={"confirm": FORCE_FINISH_CONFIRMATION},
    )
    manager.stop.assert_not_called()


def test_stop_is_not_a_force_finish(
    client, store: RunStore, manager: MagicMock, active_run: RunRecord
) -> None:
    """An ordinary Stop must never take the escape hatch's shortcut.

    The two live on different routes for exactly this reason. Even when the
    worker cannot be signalled -- the situation the escape hatch is for --
    Stop leaves the run unfinished and the project locked, because a
    possibly-live worker must stay buried only by explicit choice.
    """
    manager.stop.return_value = False
    client.post(f"/runs/{active_run.run_id}/stop")

    record = store.get_run(active_run.run_id)
    assert record.status not in TERMINAL_STATUSES
    assert record.finished_at is None
    with pytest.raises(ActiveRunError):
        store.create_run(
            RunRequest(project_root=active_run.project_root, pipeline="fixture")
        )


def test_force_finish_is_rejected_for_a_terminal_run(
    client, store: RunStore, completed_run: RunRecord
) -> None:
    response = client.post(
        f"/runs/{completed_run.run_id}/force-finish",
        data={"confirm": FORCE_FINISH_CONFIRMATION},
    )
    assert response.status_code == 409
    assert store.get_run(completed_run.run_id).status == STATUS_SUCCEEDED


def test_force_finish_rejects_a_body_it_cannot_parse(
    client, store: RunStore, active_run: RunRecord
) -> None:
    """A JSON body is not a confirmation.

    The confirmation is read out of a urlencoded body by hand, so a body of
    another type has to be refused rather than read as "no confirmation" --
    and certainly rather than read as a match.
    """
    response = client.post(
        f"/runs/{active_run.run_id}/force-finish",
        json={"confirm": FORCE_FINISH_CONFIRMATION},
    )
    assert response.status_code == 415
    assert store.get_run(active_run.run_id).status == STATUS_RUNNING


def _offers_force_finish(client, run_id: str) -> bool:
    return f'action="/runs/{run_id}/force-finish"' in client.get(f"/runs/{run_id}").text


def test_force_finish_is_not_offered_for_a_healthy_run(
    client, active_run: RunRecord
) -> None:
    """A visibly fine run must not be invited to abandon its own worker.

    ``active_run`` heartbeat lands when ``run_started`` is appended, so this
    run is running and reporting in. That is precisely the case where
    force-finishing does the damage the page warns about, so the hatch is out
    of sight -- the typed confirmation makes showing it harmless, not useful.
    """
    assert not _offers_force_finish(client, active_run.run_id)


def test_force_finish_is_offered_for_a_run_that_will_not_stop(
    client, store: RunStore, manager: MagicMock, active_run: RunRecord
) -> None:
    """Asked to stop and still going is the first wedged shape."""
    manager.stop.return_value = False
    client.post(f"/runs/{active_run.run_id}/stop")
    assert store.get_run(active_run.run_id).status == STATUS_STOP_REQUESTED
    assert _offers_force_finish(client, active_run.run_id)


def test_force_finish_is_offered_for_a_run_with_a_stale_heartbeat(
    client, store: RunStore, active_run: RunRecord
) -> None:
    """No heartbeat for longer than the supervisor's grace window is the other.

    The heartbeat is written back into the row rather than waited for, so
    nothing here sleeps.
    """
    stale = datetime.now(timezone.utc) - timedelta(
        seconds=STALE_WORKER_GRACE_SECONDS * 4
    )
    _write_column(store, active_run.run_id, "heartbeat_at", stale.isoformat())
    assert _offers_force_finish(client, active_run.run_id)


def test_force_finish_is_not_offered_for_a_terminal_run(
    client, completed_run: RunRecord
) -> None:
    assert not _offers_force_finish(client, completed_run.run_id)


def test_the_confirmation_control_is_never_prefilled(
    client, store: RunStore, active_run: RunRecord
) -> None:
    """The whole point of a typed word is that it is typed.

    A ``value`` on the input would make the hatch a one-click twin of Stop
    again, which is the thing its shape exists to prevent.
    """
    stale = datetime.now(timezone.utc) - timedelta(
        seconds=STALE_WORKER_GRACE_SECONDS * 4
    )
    _write_column(store, active_run.run_id, "heartbeat_at", stale.isoformat())
    body = client.get(f"/runs/{active_run.run_id}").text
    assert 'name="confirm"' in body
    assert f'value="{FORCE_FINISH_CONFIRMATION}"' not in body


def test_force_finish_still_accepts_a_healthy_run(
    client, store: RunStore, active_run: RunRecord
) -> None:
    """The gate is on the offer, not on the route.

    A run can go from wedged to healthy between the render and the POST.
    Refusing there would turn that race into the dead end this hatch exists
    to escape, so the route stays open to any non-terminal run.
    """
    response = client.post(
        f"/runs/{active_run.run_id}/force-finish",
        data={"confirm": FORCE_FINISH_CONFIRMATION},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert store.get_run(active_run.run_id).status == STATUS_INTERRUPTED


# -- interrupted runs ------------------------------------------------------


def test_interrupted_run_renders_from_status_not_events(
    client, store: RunStore, app, active_run: RunRecord
) -> None:
    """Reconciliation writes a status and appends no event.

    So a page (or summary) that derived terminal state from the event stream
    would show an interrupted run as still going, forever. This drives a real
    reconciliation pass and then asserts both halves: the page says
    ``interrupted``, and the event log still has no finish event to have said
    it.
    """
    store.record_worker_start(active_run.run_id, pid=DEAD_PID, started_at=1.0)
    # Past the supervisor's grace window, without waiting for it.
    future = datetime.now(timezone.utc) + timedelta(seconds=600)
    manager = RunProcessManager(store, now=lambda: future)
    assert manager.reconcile() == [active_run.run_id]

    kinds = [event.kind for event in store.events_after(active_run.run_id)]
    assert "run_finished" not in kinds

    body = client.get(f"/runs/{active_run.run_id}").text
    assert STATUS_INTERRUPTED in body
    # Terminal, so no Stop control and no live stream.
    assert f'action="/runs/{active_run.run_id}/stop"' not in body
    assert 'data-terminal="true"' in body


def test_history_shows_an_interrupted_run_as_interrupted(
    client, store: RunStore, active_run: RunRecord
) -> None:
    store.finish_run(active_run.run_id, STATUS_INTERRUPTED)
    row = _history_row(client.get("/runs").text, active_run.run_id)
    assert STATUS_INTERRUPTED in row


# -- escaping --------------------------------------------------------------


def _hostile_run(store: RunStore, project_root: Path) -> RunRecord:
    """A finished run whose task name, warning message, and warning category
    are three *distinct* hostile strings.

    Distinct on purpose: with one shared string, a test asserting "the task
    name is escaped" passes on the strength of the message being escaped, and
    the test's name stops being true.
    """
    record = _seed_run(store, project_root)
    store.append_event(record.run_id, "task_started", task_name=HOSTILE_TASK)
    store.append_event(
        record.run_id,
        "warning",
        task_name=HOSTILE_TASK,
        payload={"message": HOSTILE_MESSAGE, "category": HOSTILE_CATEGORY},
    )
    store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
    return store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)


def test_summary_escapes_a_hostile_warning_message(
    client, store: RunStore, app
) -> None:
    record = _hostile_run(store, app.state.project.root)
    summary = _summary_section(client.get(f"/runs/{record.run_id}").text)
    assert HOSTILE_MESSAGE not in summary
    assert _escaped(HOSTILE_MESSAGE) in summary


def test_summary_escapes_a_hostile_task_name(client, store: RunStore, app) -> None:
    record = _hostile_run(store, app.state.project.root)
    summary = _summary_section(client.get(f"/runs/{record.run_id}").text)
    assert HOSTILE_TASK not in summary
    assert _escaped(HOSTILE_TASK) in summary


def test_summary_escapes_a_hostile_warning_category(
    client, store: RunStore, app
) -> None:
    record = _hostile_run(store, app.state.project.root)
    summary = _summary_section(client.get(f"/runs/{record.run_id}").text)
    assert HOSTILE_CATEGORY not in summary
    assert _escaped(HOSTILE_CATEGORY) in summary


def test_history_escapes_a_hostile_warning_message(
    client, store: RunStore, app
) -> None:
    record = _hostile_run(store, app.state.project.root)
    row = _history_row(client.get("/runs").text, record.run_id)
    assert HOSTILE_MESSAGE not in row
    assert _escaped(HOSTILE_MESSAGE) in row


def test_history_escapes_a_hostile_task_name(client, store: RunStore, app) -> None:
    record = _hostile_run(store, app.state.project.root)
    row = _history_row(client.get("/runs").text, record.run_id)
    assert HOSTILE_TASK not in row
    assert _escaped(HOSTILE_TASK) in row


def test_history_escapes_a_hostile_profile_name(client, store: RunStore, app) -> None:
    """A profile name comes from the project's own ``kptn.yaml``.

    It is not attacker-controlled in any meaningful sense, but it is text the
    server did not author, and it reaches the page through the same include.
    """
    record = store.create_run(
        RunRequest(
            project_root=app.state.project.root,
            pipeline="fixture",
            profile=HOSTILE_PROFILE,
        )
    )
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
    row = _history_row(client.get("/runs").text, record.run_id)
    assert HOSTILE_PROFILE not in row
    assert _escaped(HOSTILE_PROFILE) in row


# -- bounded work on the page a developer lands on ------------------------


def test_history_does_not_hydrate_a_run_s_events(
    client, store: RunStore, app, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The history page must not read a run's events row by row.

    A ``log`` event is one row per captured output span, so hydrating them
    costs a dataclass construction and a ``json.loads`` per line of pipeline
    output the project has ever produced -- on the page a developer lands on.
    ``HISTORY_LIMIT`` bounds the run count, not the event count, so the counts
    have to come from an aggregate.

    The guard is structural: ``events_after`` is replaced with a spy that
    fails on sight. It cannot pass against an implementation that hydrates,
    however few events a test happens to seed.
    """
    record = _seed_run(store, app.state.project.root)
    store.append_event(record.run_id, "task_started", task_name="alpha")
    for index in range(300):
        _seed_log_event(store, record, f"line {index}\n")
    store.append_event(
        record.run_id, "warning", task_name="alpha", payload={"message": "careful"}
    )
    store.append_event(
        record.run_id,
        "task_finished",
        task_name="alpha",
        payload={"status": "succeeded"},
    )
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    def refuse(*args: object, **kwargs: object):
        raise AssertionError(
            "GET /runs hydrated a run's events; it must use the SQL aggregate"
        )

    monkeypatch.setattr(store, "events_after", refuse)

    row = _history_row(client.get("/runs").text, record.run_id)
    # And the counts are still right, read out of the aggregate.
    assert "tasks <b>1</b>" in row
    assert "1 warning in 1 task" in row


def test_history_counts_ignore_log_events(client, store: RunStore, app) -> None:
    """Output volume must not move a single count.

    The same run twice over, once with 200 log events and once with none: the
    rendered counts have to be identical. This is the behavioural half of the
    guard above -- it stays true even if the aggregate is one day replaced.
    """

    def seed(*, noisy: bool) -> RunRecord:
        record = _seed_run(store, app.state.project.root)
        store.append_event(record.run_id, "task_started", task_name="alpha")
        if noisy:
            for index in range(200):
                _seed_log_event(store, record, f"line {index}\n")
        store.append_event(
            record.run_id,
            "task_finished",
            task_name="alpha",
            payload={"status": "succeeded"},
        )
        store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
        return record

    quiet = seed(noisy=False)
    noisy = seed(noisy=True)

    body = client.get("/runs").text
    counts = _history_row(body, quiet.run_id)
    assert "tasks <b>1</b>" in counts
    assert "tasks <b>1</b>" in _history_row(body, noisy.run_id)

    # And on the run page, where the events *are* hydrated for the console.
    summary = _summary_section(client.get(f"/runs/{noisy.run_id}").text)
    assert 'data-summary="tasks" data-count="1"' in summary
    assert 'data-summary="succeeded" data-count="1"' in summary


def test_history_truncates_to_the_limit_dropping_the_oldest(
    client, store: RunStore, app
) -> None:
    """``HISTORY_LIMIT`` truncates, and truncates from the *old* end.

    A limit that silently reordered -- or that kept the oldest runs -- would
    hide the run the developer just finished, which is the only one they are
    reliably looking for.
    """
    root = app.state.project.root
    seeded = [
        _seed_finished_run(
            store,
            root,
            # Zero-padded so the text ordering matches the chronology.
            created_at=f"2026-01-01T00:{index:02d}:00+00:00",
            profile="success",
        )
        for index in range(HISTORY_LIMIT + 3)
    ]

    rendered = _run_ids_in_order(client.get("/runs").text)
    assert len(rendered) == HISTORY_LIMIT
    newest_first = [record.run_id for record in reversed(seeded)]
    assert rendered == newest_first[:HISTORY_LIMIT]
    # The three oldest are the ones dropped.
    for record in seeded[:3]:
        assert record.run_id not in rendered


# -- project scoping ------------------------------------------------------


@pytest.fixture
def foreign_run(store: RunStore, tmp_path: Path) -> RunRecord:
    """A run recorded in the same store for a *different* project root.

    ``.kptn/ui.db`` holds one project today, but the store is keyed by project
    root throughout -- ``create_run`` locks on it, ``list_runs`` filters on it
    -- because nothing structurally stops two contexts from sharing a
    database. ``get_run`` is keyed by run id alone, so every run-scoped route
    has to filter for itself.
    """
    other_root = tmp_path / "elsewhere"
    other_root.mkdir()
    record = _seed_run(store, other_root)
    _seed_log_event(store, record, "another project's output\n")
    return record


@pytest.mark.parametrize(
    ("method", "suffix"),
    [
        ("get", ""),
        ("get", "/log"),
        ("get", "/events"),
        ("post", "/stop"),
        ("post", "/force-finish"),
    ],
)
def test_a_foreign_project_s_run_is_a_404_everywhere(
    client,
    store: RunStore,
    manager: MagicMock,
    foreign_run: RunRecord,
    method: str,
    suffix: str,
) -> None:
    """Every run-scoped route answers the same way, through one helper."""
    url = f"/runs/{foreign_run.run_id}{suffix}"
    response = (
        client.get(url)
        if method == "get"
        else client.post(url, data={"confirm": FORCE_FINISH_CONFIRMATION})
    )
    assert response.status_code == 404
    # Nothing about the other project's run leaked, and nothing acted on it.
    assert "another project's output" not in response.text
    manager.stop.assert_not_called()
    assert store.get_run(foreign_run.run_id).status == STATUS_RUNNING


def test_a_foreign_project_s_log_is_never_served(
    client, foreign_run: RunRecord
) -> None:
    """The download in particular: a 404, not that project's captured output."""
    response = client.get(f"/runs/{foreign_run.run_id}/log")
    assert response.status_code == 404
    assert "content-disposition" not in response.headers


# -- warning attribution --------------------------------------------------


def test_headline_counts_only_attributed_warnings_against_tasks(
    client, store: RunStore, app
) -> None:
    """A mixed run must not claim outside-task warnings are "in" tasks.

    Two warnings in two tasks plus one raised outside any task is four facts,
    not "3 warnings in 2 tasks": the total would be counted against a task
    list that does not account for all of it.
    """
    record = _seed_run(store, app.state.project.root)
    store.append_event(
        record.run_id, "warning", task_name=None, payload={"message": "at import time"}
    )
    store.append_event(record.run_id, "task_started", task_name="alpha")
    store.append_event(
        record.run_id, "warning", task_name="alpha", payload={"message": "in alpha"}
    )
    store.append_event(record.run_id, "task_started", task_name="beta")
    store.append_event(
        record.run_id, "warning", task_name="beta", payload={"message": "in beta"}
    )
    store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    summary = _summary_section(client.get(f"/runs/{record.run_id}").text)
    assert "2 warnings in 2 tasks, 1 outside any task" in summary
    assert "3 warnings in 2 tasks" not in summary
    # The total is still all three, and still linked.
    assert 'data-warning-total="3"' in summary


def test_headline_says_so_when_every_warning_is_outside_a_task(
    client, store: RunStore, app
) -> None:
    record = _seed_run(store, app.state.project.root)
    for _ in range(2):
        store.append_event(
            record.run_id,
            "warning",
            task_name=None,
            payload={"message": "at import time"},
        )
    store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    summary = _summary_section(client.get(f"/runs/{record.run_id}").text)
    assert "2 warnings outside any task" in summary
    assert "in 0 tasks" not in summary


def test_headline_omits_the_outside_clause_when_there_is_nothing_outside(
    client, completed_run: RunRecord
) -> None:
    """The all-attributed wording is the brief's, and stays exact."""
    summary = _summary_section(client.get(f"/runs/{completed_run.run_id}").text)
    assert "3 warnings in 2 tasks" in summary
    assert "outside any task" not in summary


# -- the selector is never orphaned ---------------------------------------


def test_every_page_pairs_the_selector_with_its_run_form(
    client, completed_run: RunRecord
) -> None:
    """``<select form="run-form">`` must never outlive the form it names.

    A selector with no matching form still renders and still takes input,
    driving nothing -- worse than not being there. The old answer was to drop
    the selector on pages that owned no form; now the app bar carries both
    together, so the pairing holds everywhere, error shells included.
    """
    for path in (
        "/",
        "/runs",
        f"/runs/{completed_run.run_id}",
        "/runs/nope",
        "/runs/nope/log",
    ):
        body = client.get(path).text
        has_select = 'id="profile-select"' in body
        has_form = 'id="run-form"' in body
        assert has_select == has_form, (
            f"{path} renders one of the selector/run-form pair without the other"
        )


def test_the_error_shell_still_offers_the_bar(client) -> None:
    """An error page is a place you need a way out of, not a dead end."""
    body = client.get("/runs/nope").text

    assert 'id="profile-select"' in body
    assert 'id="run-form"' in body


def test_warning_payloads_with_a_status_are_still_counted(
    client, store: RunStore, app
) -> None:
    """A grouped tally must be summed across statuses, not read at ``None``.

    The aggregate groups by ``(kind, json_extract(payload, '$.status'))``.
    Only ``task_finished`` carries a status today, but nothing stops another
    kind's payload from growing one -- and the moment it does, reading a
    kind's count at ``(kind, None)`` splits it across two rows and silently
    undercounts. The console header is where that would show.
    """
    record = _seed_run(store, app.state.project.root)
    store.append_event(
        record.run_id,
        "warning",
        task_name="alpha",
        payload={"message": "careful", "status": "advisory"},
    )
    store.append_event(
        record.run_id, "warning", task_name="alpha", payload={"message": "also this"}
    )
    store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    body = client.get(f"/runs/{record.run_id}").text
    assert '<b data-counter="warnings">2</b>' in body


# -- the page and the stream render the same regions -----------------------


def test_run_page_and_stream_render_one_run_header(
    client, store: RunStore, completed_run: RunRecord, app
) -> None:
    """The header the stream settles must be the header the page renders.

    Two copies of this markup is the failure mode worth guarding: the page
    would keep a Stop control the stream's fragment had already learned to
    drop, and nobody would notice until a run went terminal under an open
    stream -- which is the bug this pair exists to fix. So both render the
    same partial, and this asserts they agree on the header's whole body.
    """
    page = client.get(f"/runs/{completed_run.run_id}").text
    fragment = render_region(
        app.state.templates, store, "run_header", store.get_run(completed_run.run_id)
    )

    assert fragment.strip() in page


def test_run_page_and_stream_render_one_run_summary(
    client, store: RunStore, completed_run: RunRecord, app
) -> None:
    """Same contract for the summary panel: one partial, both surfaces."""
    page = client.get(f"/runs/{completed_run.run_id}").text
    fragment = render_region(
        app.state.templates, store, "run_summary", store.get_run(completed_run.run_id)
    )

    assert fragment.strip() in page


def test_history_carrying_a_profile_still_lists_every_run(
    client, store: RunStore, ui_project: Path
) -> None:
    """Carrying a profile must not quietly filter the list.

    The profile rides through this page so the *next* one keeps it. The
    history itself is still every run of every profile, which is the whole
    reason it shows each row's profile individually.
    """
    # Finished one at a time: the store allows a project only one active run.
    mine = _seed_run(store, ui_project, profile="success")
    store.finish_run(mine.run_id, STATUS_SUCCEEDED, exit_code=0)
    other = _seed_run(store, ui_project, profile="failure")
    store.finish_run(other.run_id, STATUS_SUCCEEDED, exit_code=0)

    body = client.get("/runs?profile=success").text

    assert mine.run_id in body
    assert other.run_id in body, "the history filtered itself by the carried profile"


def test_log_download_sits_in_the_console_bar_after_follow_output(
    client, completed_run: RunRecord
) -> None:
    """The two console controls belong together, in that order.

    "Follow output" and "Download raw log" both act on the console's output,
    so the download lives in the console's own bar to the right of the
    toggle -- not down in the summary panel, a section away from the thing
    it downloads.
    """
    body = client.get(f"/runs/{completed_run.run_id}").text

    bar = re.search(r'<header class="console__bar">(.*?)</header>', body, re.S)
    assert bar, "the console bar is gone"
    inside = bar.group(1)

    follow_at = inside.find('id="follow-output"')
    download_at = inside.find(f'action="/runs/{completed_run.run_id}/log"')

    assert follow_at != -1, "the follow toggle left the console bar"
    assert download_at != -1, "the log download is not in the console bar"
    assert follow_at < download_at, (
        "the log download renders before the follow toggle, not to its right"
    )


def test_run_summary_no_longer_carries_the_log_download(
    client, completed_run: RunRecord
) -> None:
    """Moved, not duplicated -- two download buttons would be a regression."""
    body = client.get(f"/runs/{completed_run.run_id}").text

    assert body.count(f'action="/runs/{completed_run.run_id}/log"') == 1
    summary = re.search(
        r'<section class="panel" id="run-summary">(.*?)</section>', body, re.S
    )
    assert summary, "the summary panel is gone"
    assert "/log" not in summary.group(1)
