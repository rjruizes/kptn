"""The plan's central durability claim, end to end, against a real server.

Every other UI test module drives ``create_app`` in-process through
``TestClient``. That cannot express the one property this whole UI was built
around: **a run is a detached process, so it survives the server**. In-process
there is no server to kill.

So this module runs the real thing. It starts ``python -m kptn ui --no-open``
as a subprocess against a private copy of the fixture project, drives it over
HTTP, and then kills *only the server* while a run is in flight -- the same
thing that happens when a developer stops ``kptn ui``, saves a file and
triggers a reload, or the notebook server restarts. A second server is started on the same
port, and the assertions after that point are the evidence: the worker is
still alive, the run is still ``running``, releasing the fixture's sentinel
still finishes it, and the history, console, warning anchors, plan and
walkthrough are all correct on the far side.

Determinism, not sleeping. The fixture project's ``slow`` profile blocks on a
sentinel file (``KPTN_UI_FIXTURE_SENTINEL``) and prints
``STARTED_MARKER`` first, so the test waits for observable conditions --
``/healthz`` answering, the marker appearing in the console, a status becoming
terminal -- and never for a duration. :func:`_until` is the only waiting
primitive here and every use of it names the condition it is waiting on, so a
failure says what never happened rather than timing out anonymously.

Reaping. Both servers are terminated in a ``finally``, and the conftest
``reap_spawned_workers`` fixture asserts nothing leaked. The sentinel is
released before the assertions that need a finished run, so no worker is ever
left blocking on a file that never appears.
"""

from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable, Iterator, TypeVar

from unittest.mock import MagicMock

import httpx
import psutil
import pytest
from fastapi.testclient import TestClient

from kptn_server.processes import ProcessIdentity
from kptn_server.project import UI_DATABASE_RELATIVE_PATH
from kptn_server.run_store import RunStore
from tests.test_ui_multi_project import USER, projects_root

pytestmark = pytest.mark.ui_hygiene

REPO_ROOT = Path(__file__).resolve().parents[1]

#: The port the task brief names for the acceptance flow.
PORT = 8123
BASE_URL = f"http://127.0.0.1:{PORT}"

#: What ``ui_pipeline.noisy_task`` prints before it blocks on the sentinel.
STARTED_MARKER = "slow task waiting for sentinel"

#: Longest this test will wait for any single condition. Generous, because it
#: bounds a real interpreter start and a real pipeline run on a loaded CI box;
#: it is a failure deadline, never a synchronization mechanism.
DEADLINE_SECONDS = 90.0
POLL_SECONDS = 0.05

T = TypeVar("T")


def _until(what: str, condition: Callable[[], T | None]) -> T:
    """Poll *condition* until it returns something truthy, or fail naming *what*."""
    deadline = time.monotonic() + DEADLINE_SECONDS
    last: object = None
    while time.monotonic() < deadline:
        last = condition()
        if last:
            return last  # type: ignore[return-value]
        time.sleep(POLL_SECONDS)
    pytest.fail(f"timed out waiting for {what} (last value: {last!r})")


def _port_is_free() -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", PORT))
        except OSError:
            return False
    return True


class Server:
    """One ``kptn ui`` process, started and stopped explicitly."""

    def __init__(self, project: Path, sentinel: Path) -> None:
        env = dict(os.environ)
        env["KPTN_UI_FIXTURE_SENTINEL"] = str(sentinel)
        # The worker inherits this environment through the server, which is
        # the only way the 'slow' profile learns where its sentinel is.
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "kptn",
                "ui",
                "--no-open",
                "--host",
                "127.0.0.1",
                "--port",
                str(PORT),
            ],
            cwd=project,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            # Its own process group, for two reasons. It lets `stop()` signal
            # the *group* -- which is what a terminal's Ctrl-C does to the
            # foreground job, and therefore the case a detached worker has to
            # survive -- and it keeps that signal away from the pytest process
            # this server was spawned from.
            start_new_session=True,
        )

    def wait_until_ready(self) -> None:
        def answered() -> bool:
            if self.process.poll() is not None:
                pytest.fail(
                    f"the server exited before answering /healthz:\n{self._drain()}"
                )
            try:
                return httpx.get(f"{BASE_URL}/healthz", timeout=1.0).status_code == 200
            except httpx.HTTPError:
                return False

        _until("the server to answer /healthz", answered)

    def _drain(self) -> str:
        if self.process.stdout is None:  # pragma: no cover - defensive
            return "<no output captured>"
        try:
            return self.process.stdout.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001 - diagnostics only
            return "<output unavailable>"

    def stop(self) -> None:
        """Signal the server's whole process group and wait for it to exit.

        The group, not the process. A plain ``SIGTERM`` to the server's pid
        would leave a worker alive whether or not the supervisor detached it
        -- the signal simply never reaches a child -- so a test that killed
        only the pid would pass against a supervisor that had stopped
        detaching workers at all. Signalling the group is what a terminal does
        to its foreground job on Ctrl-C, and it reaches every process still in
        the server's session. A worker survives it only because
        ``RunProcessManager.start`` puts each worker in a session of its own.
        """
        if self.process.poll() is None:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):  # pragma: no cover
                self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:  # pragma: no cover - defensive
                self.process.kill()
                self.process.wait(timeout=30)
        if self.process.stdout is not None:
            self.process.stdout.close()
        _until(
            "the port to be released after the server stopped",
            lambda: _port_is_free() or None,
        )


@pytest.fixture
def sentinel(tmp_path: Path) -> Path:
    return tmp_path / "release-the-slow-task"


@pytest.fixture
def servers(ui_project: Path, sentinel: Path) -> Iterator[Callable[[], Server]]:
    """A factory for servers, all of which are stopped at teardown."""
    if not _port_is_free():
        pytest.skip(f"port {PORT} is already in use on this machine")
    started: list[Server] = []

    def start() -> Server:
        server = Server(ui_project, sentinel)
        started.append(server)
        server.wait_until_ready()
        return server

    try:
        yield start
    finally:
        # Release any blocked worker *before* stopping the servers, so a
        # failing assertion cannot strand a task waiting on a file forever.
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.touch(exist_ok=True)
        for server in reversed(started):
            server.stop()


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    """Pin the ``pytestmark`` opt-in.

    Without the marker, ``reap_spawned_workers`` no-ops and this module -- the
    one that deliberately detaches a worker from its server -- would be the
    one able to strand a process on the machine.
    """
    assert request.node.get_closest_marker("ui_hygiene") is not None


# -- helpers reading the served pages --------------------------------------


def _start_run(client: httpx.Client, profile: str) -> str:
    """POST the Run form and return the created run's id."""
    response = client.post(
        "/runs",
        data={"profile": profile},
        headers={"content-type": "application/x-www-form-urlencoded"},
        follow_redirects=False,
    )
    assert response.status_code == 303, response.text
    location = response.headers["location"]
    # Relative by design: a root-absolute Location is re-prefixed by
    # jupyter-server-proxy, arriving doubled.
    # See tests/test_ui_root_path.py.
    assert location.startswith("runs/"), location
    return location.rsplit("/", 1)[-1]


def _status(client: httpx.Client, run_id: str) -> tuple[str, bool, str]:
    """``(status, is_terminal, page)`` for a run, straight off its page."""
    page = client.get(f"/runs/{run_id}").text
    status = re.search(r'id="run-status"[^>]*data-status="([^"]+)"', page)
    terminal = re.search(r'id="run-status"[^>]*data-terminal="([^"]+)"', page)
    assert status and terminal, page[:2000]
    return status.group(1), terminal.group(1) == "true", page


def _await_terminal(client: httpx.Client, run_id: str, what: str) -> tuple[str, str]:
    def finished() -> tuple[str, str] | None:
        status, is_terminal, page = _status(client, run_id)
        return (status, page) if is_terminal else None

    return _until(what, finished)


def _warning_rows(page: str) -> set[str]:
    """The console rows a run's warnings rendered as.

    The run page's grouped warning summary is gone -- a warning is a console
    row beside the output that produced it -- so this is where a warning has
    to show up now.
    """
    return set(
        re.findall(r'<li class="event[^"]*"\s+id="(event-\d+)"[^>]*data-kind="warning"', page)
    )


def _event_ids(page: str) -> set[str]:
    return set(re.findall(r'<li class="event[^"]*"\s+id="(event-\d+)"', page))


def _recorded_worker(project: Path, run_id: str) -> psutil.Process:
    """The live worker process for *run_id*, read out of the run store."""
    record = RunStore(project / UI_DATABASE_RELATIVE_PATH).get_run(run_id)
    assert record is not None and record.worker_pid is not None, record
    return psutil.Process(record.worker_pid)


# -- the acceptance flow ---------------------------------------------------


def test_a_run_survives_the_server_that_started_it(
    servers: Callable[[], Server], ui_project: Path, sentinel: Path
) -> None:
    """The whole flow, in one test, because the ordering *is* the claim."""
    first = servers()

    with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
        # 1. The console shell renders, and offers the fixture's profiles.
        console = client.get("/")
        assert console.status_code == 200, console.text
        for profile in ("success", "slow", "failure", "db_error"):
            assert f'<option value="{profile}"' in console.text

        # 2. A complete run, start to finish, on the first server.
        success_id = _start_run(client, "success")
        status, page = _await_terminal(client, success_id, "the success run to finish")
        assert status == "succeeded", page[:4000]
        assert "ordinary output" in page
        assert "raw stderr output" in page
        warnings = _warning_rows(page)
        assert warnings, "the completed run rendered no warning rows"
        assert warnings <= _event_ids(page)

        # 3. Re-requesting the page (a closed and reopened tab) replays the
        #    same console from the store rather than losing it with the stream.
        _, reopened = _await_terminal(client, success_id, "the run page to re-render")
        assert _event_ids(reopened) == _event_ids(page)
        assert _warning_rows(reopened) == warnings

        # 4. Clear kptn's *task-state* cache so the next run really executes.
        #    Without this, `noisy_task` is skipped as cached -- its hash does
        #    not depend on the profile's args -- and the "slow" run would
        #    finish instantly having printed nothing, which would make the
        #    restart below prove nothing at all. The UI's own history lives in
        #    `.kptn/ui.db` and is deliberately *not* touched here; the history
        #    assertions further down are what show the two are independent.
        task_state_db = ui_project / ".kptn" / "kptn.db"
        assert task_state_db.is_file(), "the finished run recorded no task state"
        task_state_db.unlink()

        # 5. Start the blocking run and wait for proof its task is executing.
        slow_id = _start_run(client, "slow")
        _until(
            "the slow task to report that it is blocked on the sentinel",
            lambda: STARTED_MARKER in client.get(f"/runs/{slow_id}").text or None,
        )
        worker = _recorded_worker(ui_project, slow_id)
        worker_identity = (worker.pid, worker.create_time())
        assert worker.is_running()

    # 6. Signal the server's process group. Nothing signals the worker
    #    itself; it is in a session of its own, which is the whole point.
    first.stop()
    assert worker.is_running(), (
        "signalling the server's process group killed the run's worker: "
        "the worker was not launched into a session of its own"
    )
    assert (worker.pid, worker.create_time()) == worker_identity

    # 7. A second server, same port, same project, same run store.
    servers()

    with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
        status, _, page = _status(client, slow_id)
        assert status == "running", (
            f"the restarted server did not find the run still running: {page[:4000]}"
        )
        assert STARTED_MARKER in page, "the console did not survive the restart"

        # 8. Release the sentinel. The worker -- which the new server never
        #    launched and does not own -- finishes, and the new server sees it.
        sentinel.touch()
        status, page = _await_terminal(
            client, slow_id, "the slow run to finish after the restart"
        )
        assert status == "succeeded", page[:4000]

        # -- everything the brief asks to be correct afterwards ------------

        # console
        assert "ordinary output" in page
        assert STARTED_MARKER in page

        # warnings, as console rows
        slow_warnings = _warning_rows(page)
        assert slow_warnings, "the restarted run rendered no warning rows"
        assert slow_warnings <= _event_ids(page)

        # history: both runs, both succeeded, newest first
        history = client.get("/runs")
        assert history.status_code == 200, history.text
        listed = re.findall(
            r'class="run-history__link" href="/runs/([0-9a-zA-Z_-]+)"', history.text
        )
        assert slow_id in listed and success_id in listed, listed
        assert listed.index(slow_id) < listed.index(success_id)
        assert history.text.count('class="status status--succeeded"') >= 2

        # log download
        log = client.get(f"/runs/{slow_id}/log")
        assert log.status_code == 200
        assert "ordinary output" in log.text
        assert STARTED_MARKER in log.text

        # plan
        plan = client.get("/plan?profile=slow")
        assert plan.status_code == 200, plan.text
        assert "setup_task" in plan.text and "noisy_task" in plan.text

        # walkthrough, and one task's detail with its project-relative docs
        walkthrough = client.get("/walkthrough?profile=slow")
        assert walkthrough.status_code == 200, walkthrough.text
        assert "setup_task" in walkthrough.text
        assert "noisy_task" in walkthrough.text

        detail = client.get("/walkthrough/task/noisy_task?profile=slow")
        assert detail.status_code == 200, detail.text
        assert "Emit output and warnings." in detail.text
        # A sentence that appears only in the docs file, so this cannot pass
        # on the task name the page prints anyway.
        assert "The task prints one ordinary line" in detail.text, (
            "the task's docs file was not rendered"
        )
        # And the docs file's raw markup arrived as text, not as markup.
        assert "&lt;script&gt;" in detail.text

        # and the project is unlocked again: a third run is accepted.
        third = _start_run(client, "success")
        _await_terminal(client, third, "a run started after the restart to finish")


# -- the project switch, walked as a reader would ---------------------------
#
# ``kptn ui --projects-root`` serves several of a person's working
# directories behind one server, each at ``/p/<slug>/``. Everything above
# this section proves the single-project server; these two prove the switch
# itself -- reached in-process through ``create_multi_app`` rather than a
# subprocess, because what is under test is routing and isolation between
# projects, not the durability claim the rest of this module exists for.


@pytest.fixture
def multi_client(projects_root: Path) -> TestClient:
    from kptn_server.app import create_multi_app

    return TestClient(create_multi_app(projects_root, USER))


def _stub_supervisor(client: TestClient, slug: str) -> None:
    """Open *slug*'s project, then swap its supervisor for a stub.

    A request has to reach the project once for ``app.state.managers`` to
    hold an entry for it at all (it is populated lazily, per slug, on first
    resolution). What is under test here is routing and per-project
    isolation, not process supervision, so the real ``RunProcessManager`` is
    replaced before any run is started -- without this a "started" run
    spawns a real detached worker for a pipeline this test never finishes
    waiting on, which the ``reap_spawned_workers`` hygiene fixture flags as
    a leak.
    """
    client.get(f"/p/{slug}/")
    manager = MagicMock()
    manager.start.return_value = ProcessIdentity(pid=4321, started_at=1.0)
    client.app.state.managers[slug] = manager  # type: ignore[attr-defined]


def test_a_reader_goes_from_the_list_to_a_project_and_runs_it(
    multi_client: TestClient,
) -> None:
    """The whole path a person takes on their first visit.

    List -> a project's history -> start a run there. The redirect's
    ``Location`` must be relative: a root-absolute one is re-prefixed by
    jupyter-server-proxy, arriving doubled -- see
    ``tests/test_ui_root_path.py``.
    """
    listing = multi_client.get("/")
    assert f'href="/p/{USER}_main/"' in listing.text

    _stub_supervisor(multi_client, f"{USER}_main")

    history = multi_client.get(f"/p/{USER}_main/")
    assert history.status_code == 200

    started = multi_client.post(
        f"/p/{USER}_main/runs", data={"profile": ""}, follow_redirects=False
    )
    assert started.status_code in (302, 303)
    assert started.headers["location"].startswith("runs/"), started.headers["location"]

    # The run just started shows up in the project it was started in --
    # without this, the isolation test below would pass vacuously against an
    # implementation that never renders a run-history link anywhere at all.
    assert "run-history__link" in multi_client.get(f"/p/{USER}_main/").text


def test_a_run_in_one_project_is_absent_from_the_other(
    multi_client: TestClient,
) -> None:
    """Starting a run in one project must not leak into another's history.

    Asserted on ``run-history__link``, not on the word "runs" -- "runs"
    appears on nearly any page in this UI (the nav link, the form action,
    the redirect target) and would pass even against a shared store.
    """
    _stub_supervisor(multi_client, f"{USER}_main")

    multi_client.post(
        f"/p/{USER}_main/runs", data={"profile": ""}, follow_redirects=False
    )

    other = multi_client.get(f"/p/{USER}_featureA/")

    assert "run-history__link" not in other.text
