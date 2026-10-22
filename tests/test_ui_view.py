"""Read-only pages for somebody else's project, at ``/view/<slug>/``.

Each test publishes a run the way the colleague's own pod would -- their own
``RunStore`` writing their own ``.kptn`` -- and then reads it through *this*
person's server, which may use only the files that run published.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kptn.runner.events import EventKind
from kptn_server.app import create_multi_app
from kptn_server.routes import view as view_routes
from kptn_server.run_files import INDEX_FILENAME, encode_event
from kptn_server.run_store import STATUS_SUCCEEDED, RunRecord, RunRequest, RunStore
from tests.conftest import FIXTURE_PROJECT, copy_fixture_project

pytestmark = pytest.mark.ui_hygiene

USER = "rruizesparza"
OTHER = "someoneelse_main"


@pytest.fixture
def projects_root(tmp_path: Path) -> Path:
    release = tmp_path / "shared" / "r1"
    release.mkdir(parents=True)
    copy_fixture_project(FIXTURE_PROJECT, release, f"{USER}_main")
    copy_fixture_project(FIXTURE_PROJECT, release, OTHER)
    return tmp_path / "shared"


@pytest.fixture
def theirs(projects_root: Path) -> Path:
    return (projects_root / "r1" / OTHER).resolve()


@pytest.fixture
def client(projects_root: Path) -> TestClient:
    return TestClient(create_multi_app(projects_root, USER))


def publish_run(
    project: Path, *, lines: tuple[str, ...] = ("hello <b>world</b>\n",), finish: bool = True
) -> RunRecord:
    """Record a run the way its owner's pod would."""
    store = RunStore(project / ".kptn" / "state" / "ui.db")
    record = store.create_run(
        RunRequest(project_root=project, pipeline="fixture", profile="success")
    )
    store.append_event(record.run_id, EventKind.RUN_STARTED.value)
    store.append_event(record.run_id, EventKind.TASK_STARTED.value, task_name="alpha")
    for line in lines:
        store.append_event(
            record.run_id,
            EventKind.LOG.value,
            task_name="alpha",
            payload={"stream": "stdout", "severity": "output"},
            text=line,
        )
    store.append_event(
        record.run_id,
        EventKind.TASK_FINISHED.value,
        task_name="alpha",
        payload={"status": "succeeded"},
    )
    if finish:
        store.append_event(
            record.run_id, EventKind.RUN_FINISHED.value, payload={"status": "succeeded"}
        )
        store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
    store.close()
    return record


# -- what a reader sees --------------------------------------------------------


def test_their_history_lists_their_published_runs(client: TestClient, theirs: Path) -> None:
    record = publish_run(theirs)

    response = client.get(f"/view/{OTHER}/")

    assert response.status_code == 200
    assert f'data-run-id="{record.run_id}"' in response.text
    assert 'data-status="succeeded"' in response.text
    assert "read-only" in response.text


def test_their_landing_page_opens_their_newest_run(client: TestClient, theirs: Path) -> None:
    publish_run(theirs)
    newest = publish_run(theirs)

    response = client.get(f"/view/{OTHER}/", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == f"runs/{newest.run_id}"


def test_their_workspace_lists_every_profile_and_runs_nothing(
    client: TestClient, theirs: Path
) -> None:
    """No profile selector here, so the Runs pane is every run, each naming
    its profile -- and no Next run pane: there is nothing to run."""
    record = publish_run(theirs)

    body = client.get(f"/view/{OTHER}/runs/{record.run_id}").text

    assert 'class="runrow__profile">success<' in body
    assert re.search(r'<a class="runrow"[^>]*aria-current="page"', body)
    assert "pane--next" not in body and 'id="profile-select"' not in body
    assert 'data-url="/view/' + OTHER + "/run-list?current=" + record.run_id + '"' in body


def test_their_runs_pane_refreshes_from_the_index(client: TestClient, theirs: Path) -> None:
    record = publish_run(theirs)

    response = client.get(f"/view/{OTHER}/run-list")

    assert response.status_code == 200
    assert f'data-run-id="{record.run_id}"' in response.text


def test_their_run_page_renders_their_output_escaped(
    client: TestClient, theirs: Path
) -> None:
    record = publish_run(theirs)

    body = client.get(f"/view/{OTHER}/runs/{record.run_id}").text

    assert "hello &lt;b&gt;world&lt;/b&gt;" in body
    assert "<b>world</b>" not in body
    assert 'data-counter="ran">1<' in body
    assert f'data-stream-url="/view/{OTHER}/runs/{record.run_id}/events"' in body


def test_their_pages_offer_nothing_that_acts_on_a_run(
    client: TestClient, theirs: Path
) -> None:
    record = publish_run(theirs, finish=False)

    for path in (f"/view/{OTHER}/", f"/view/{OTHER}/runs/{record.run_id}"):
        body = client.get(path).text
        assert 'method="post"' not in body
        assert "data-run-control" not in body
        assert f"/p/{OTHER}" not in body


def test_their_download_is_the_cli_transcript(client: TestClient, theirs: Path) -> None:
    record = publish_run(theirs)

    response = client.get(f"/view/{OTHER}/runs/{record.run_id}/log")

    assert response.status_code == 200
    assert f'filename="kptn-{record.run_id}.log"' in response.headers["content-disposition"]
    assert "[RUN]" in response.text
    assert "hello <b>world</b>\n" in response.text


def test_their_pages_never_need_their_run_store(client: TestClient, theirs: Path) -> None:
    """The point of the run files: their ui.db is in another pod's hands."""
    record = publish_run(theirs)
    for name in ("ui.db", "ui.db-wal", "ui.db-shm"):
        (theirs / ".kptn" / "state" / name).unlink(missing_ok=True)

    for path in (
        f"/view/{OTHER}/",
        f"/view/{OTHER}/runs/{record.run_id}",
        f"/view/{OTHER}/runs/{record.run_id}/log",
        f"/view/{OTHER}/runs/{record.run_id}/events",
    ):
        assert client.get(path).status_code == 200, path

    assert not (theirs / ".kptn" / "state" / "ui.db").exists()


def test_a_project_with_nothing_published_says_so(client: TestClient) -> None:
    body = client.get(f"/view/{OTHER}/").text
    assert "No runs published yet" in body


# -- one URL per project ---------------------------------------------------------


def test_their_project_is_not_served_as_mine(client: TestClient, theirs: Path) -> None:
    publish_run(theirs)
    assert client.get(f"/p/{OTHER}/").status_code == 404


def test_my_project_is_not_served_read_only(client: TestClient) -> None:
    assert client.get(f"/view/{USER}_main/").status_code == 404


@pytest.mark.parametrize(
    "run_id",
    ["a" * 32, "..%2F..%2Fui.db", "index", "A" * 32],
)
def test_a_run_id_must_be_listed_to_name_a_file(
    client: TestClient, theirs: Path, run_id: str
) -> None:
    publish_run(theirs)
    for suffix in ("", "/log", "/events"):
        assert client.get(f"/view/{OTHER}/runs/{run_id}{suffix}").status_code == 404


# -- files that cannot be trusted ------------------------------------------------


def test_a_symlinked_run_file_is_refused(
    client: TestClient, theirs: Path, tmp_path: Path
) -> None:
    record = publish_run(theirs)
    decoy = tmp_path / "decoy.jsonl"
    decoy.write_bytes(
        encode_event(
            sequence=1, timestamp="2026-10-06T00:00:00+00:00", kind="log",
            task_name=None, payload={}, text="a reader's private file\n",
        )
    )
    record.log_path.unlink()
    record.log_path.symlink_to(decoy)

    page = client.get(f"/view/{OTHER}/runs/{record.run_id}")
    download = client.get(f"/view/{OTHER}/runs/{record.run_id}/log")

    assert "a reader&#39;s private file" not in page.text
    assert "a reader's private file" not in page.text
    assert "cannot be read" in page.text
    assert download.status_code == 404


def test_an_unreadable_index_is_reported_not_raised(client: TestClient, theirs: Path) -> None:
    publish_run(theirs)
    (theirs / ".kptn" / "state" / "runs" / INDEX_FILENAME).write_text(
        json.dumps({"schema": 99, "runs": []})
    )

    response = client.get(f"/view/{OTHER}/")

    assert response.status_code == 200
    assert "cannot be read" in response.text
    assert "Upgrade kptn" in response.text


def test_a_run_with_an_old_heartbeat_is_shown_as_not_reporting(
    client: TestClient, theirs: Path
) -> None:
    record = publish_run(theirs, finish=False)
    db = theirs / ".kptn" / "state" / "ui.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE runs SET heartbeat_at = ? WHERE run_id = ?",
        ("2020-01-01T00:00:00+00:00", record.run_id),
    )
    conn.commit()
    conn.close()
    RunStore(db).publish_index(record.project_root)

    assert "data-not-reporting" in client.get(f"/view/{OTHER}/").text
    assert "data-not-reporting" in client.get(f"/view/{OTHER}/runs/{record.run_id}").text


def test_a_run_from_before_run_files_shows_its_raw_log(
    client: TestClient, theirs: Path
) -> None:
    run_id = "c" * 32
    runs_dir = theirs / ".kptn" / "state" / "runs"
    runs_dir.mkdir(parents=True)
    (runs_dir / INDEX_FILENAME).write_text(
        json.dumps({"schema": 1, "runs": [{"run_id": run_id, "status": "succeeded"}]})
    )
    (runs_dir / f"{run_id}.log").write_text("from an older kptn\n")

    assert "from an older kptn" in client.get(f"/view/{OTHER}/runs/{run_id}").text
    assert "from an older kptn" in client.get(f"/view/{OTHER}/runs/{run_id}/log").text


# -- the live stream ---------------------------------------------------------------


def test_a_finished_runs_stream_sends_its_events_then_its_status(
    client: TestClient, theirs: Path
) -> None:
    record = publish_run(theirs)

    body = client.get(f"/view/{OTHER}/runs/{record.run_id}/events").text

    assert "event: log" in body
    assert "hello &lt;b&gt;world" in body
    assert body.rstrip().splitlines()[-2] == "event: run_status"


def test_a_live_runs_stream_tails_the_file_until_the_run_ends(
    projects_root: Path, theirs: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(view_routes, "VIEW_POLL_INTERVAL_SECONDS", 0.01)
    record = publish_run(theirs, finish=False)
    app = create_multi_app(projects_root, USER)
    entry = app.state.registry.resolve(OTHER, servable_only=False)

    async def collect() -> list[str]:
        frames: list[str] = []
        stream = view_routes.view_frames(entry, app.state.templates, record.run_id)
        async for frame in stream:
            frames.append(frame)
            if len(frames) == 4:
                # Everything published so far has been sent; the run goes on.
                store = RunStore(theirs / ".kptn" / "state" / "ui.db")
                store.append_event(
                    record.run_id, "log", task_name="alpha",
                    payload={"stream": "stdout", "severity": "output"},
                    text="written later\n",
                )
                store.append_event(
                    record.run_id, EventKind.RUN_FINISHED.value,
                    payload={"status": "succeeded"},
                )
                store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
        return frames

    frames = asyncio.run(asyncio.wait_for(collect(), timeout=10))

    assert any("written later" in frame for frame in frames)
    assert frames[-1].startswith("event: run_status")
    sequences = [
        int(line.split(": ")[1])
        for frame in frames
        for line in frame.splitlines()
        if line.startswith("id: ")
    ]
    assert sequences == sorted(set(sequences))


# -- settings.shared_logs ------------------------------------------------------


def _share_logs(project: Path, setting: str = "../_kptn_logs") -> None:
    config = project / "kptn.yaml"
    config.write_text(config.read_text().replace("settings:\n", f"settings:\n  shared_logs: {setting}\n", 1))
    (project / setting).mkdir(exist_ok=True)  # set up, as on the server


def test_their_runs_are_read_from_their_shared_logs(projects_root: Path, theirs: Path) -> None:
    _share_logs(theirs)
    record = publish_run(theirs)
    client = TestClient(create_multi_app(projects_root, USER))  # scans the edited kptn.yaml
    assert record.log_path.parent == theirs.parent / "_kptn_logs" / OTHER

    history = client.get(f"/view/{OTHER}/")
    page = client.get(f"/view/{OTHER}/runs/{record.run_id}")

    assert f'data-run-id="{record.run_id}"' in history.text
    assert "hello &lt;b&gt;world&lt;/b&gt;" in page.text


def test_shared_logs_outside_the_projects_root_are_not_read(
    projects_root: Path, theirs: Path, tmp_path: Path
) -> None:
    _share_logs(theirs, str(tmp_path / "elsewhere"))
    record = publish_run(theirs)
    client = TestClient(create_multi_app(projects_root, USER))

    history = client.get(f"/view/{OTHER}/")

    assert history.status_code == 200
    assert record.run_id not in history.text
    assert "cannot be read" in history.text
    assert "outside" in history.text


# -- removed working directories -----------------------------------------------


@pytest.fixture
def removed(projects_root: Path, theirs: Path) -> tuple[TestClient, RunRecord]:
    """Their directory deleted after one run; mine still names the logs root."""
    _share_logs(projects_root / "r1" / f"{USER}_main")
    _share_logs(theirs)
    record = publish_run(theirs)
    shutil.rmtree(theirs)
    return TestClient(create_multi_app(projects_root, USER)), record


def test_a_removed_directory_is_listed_under_removed(removed) -> None:
    client, _record = removed
    body = client.get("/").text
    assert 'id="removed-projects"' in body
    removed_list = body[body.index('id="removed-projects"'):]
    assert f"/view/{OTHER}/" in removed_list


def test_a_removed_directorys_runs_still_open(removed) -> None:
    client, record = removed

    history = client.get(f"/view/{OTHER}/")
    page = client.get(f"/view/{OTHER}/runs/{record.run_id}")
    download = client.get(f"/view/{OTHER}/runs/{record.run_id}/log")

    assert f'data-run-id="{record.run_id}"' in history.text
    assert ">removed<" in history.text
    assert "hello &lt;b&gt;world&lt;/b&gt;" in page.text
    assert download.status_code == 200 and b"hello <b>world</b>" in download.content


def test_my_removed_directory_is_viewed_not_served(projects_root: Path) -> None:
    mine = projects_root / "r1" / f"{USER}_main"
    _share_logs(mine)
    copy_fixture_project(FIXTURE_PROJECT, projects_root / "r1", f"{USER}_old")
    old = (projects_root / "r1" / f"{USER}_old").resolve()
    _share_logs(old)
    record = publish_run(old)
    shutil.rmtree(old)
    client = TestClient(create_multi_app(projects_root, USER))

    assert client.get(f"/p/{USER}_old/").status_code == 404
    assert f'data-run-id="{record.run_id}"' in client.get(f"/view/{USER}_old/").text
