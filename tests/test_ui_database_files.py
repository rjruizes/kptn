"""The app bar's database chips: what the next run starts from.

A run reports the DuckDB file it opened in a ``database`` event; the app bar
lists that file and the checkpoints beside it, and deletes one by name.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import STATIC_DIR, create_app
from kptn_server.database_files import database_files
from kptn_server.run_store import STATUS_SUCCEEDED, RunRequest, RunStore

pytestmark = pytest.mark.ui_hygiene


@pytest.fixture
def app(ui_project: Path):
    return create_app(ui_project)


@pytest.fixture
def store(app) -> RunStore:
    return app.state.store


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def _run(store: RunStore, root: Path, database: Path, *, checkpoint: Path | None = None, finish=True):
    record = store.create_run(RunRequest(project_root=root, pipeline="fixture", profile="success"))
    store.append_event(record.run_id, "run_started")
    store.append_event(
        record.run_id,
        "database",
        payload={"path": str(database), "checkpoint": str(checkpoint) if checkpoint else None},
    )
    if finish:
        store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
        store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
    return record


def _files(tmp_path: Path) -> tuple[Path, Path, Path]:
    data = tmp_path / "data"
    data.mkdir()
    database = data / "nph.db"
    database.write_bytes(b"x" * 2048)
    first = data / "nph.load_redcap.backup.db"
    second = data / "nph.qc_detail.backup.db"
    first.write_bytes(b"x")
    second.write_bytes(b"x")
    (data / "other.db").write_bytes(b"x")  # not this database's
    return database, first, second


def test_nothing_is_listed_until_a_run_reports_its_database(app, store) -> None:
    assert database_files(store, app.state.project.root) == []


def test_the_reported_database_and_its_checkpoints_are_listed(app, store, tmp_path) -> None:
    database, first, second = _files(tmp_path)
    _run(store, app.state.project.root, database)

    files = database_files(store, app.state.project.root)

    assert [(f.name, f.kind, f.task) for f in files] == [
        ("nph.db", "database", ""),
        ("nph.load_redcap.backup.db", "checkpoint", "load_redcap"),
        ("nph.qc_detail.backup.db", "checkpoint", "qc_detail"),
    ]
    assert files[0].size == "2.0 KB"


def test_a_deleted_database_leaves_its_checkpoints_listed(app, store, tmp_path) -> None:
    database, first, _second = _files(tmp_path)
    _run(store, app.state.project.root, database)
    database.unlink()

    assert [f.name for f in database_files(store, app.state.project.root)][0] == first.name


def test_the_app_bar_shows_a_chip_per_file(app, store, client, tmp_path) -> None:
    database, _first, _second = _files(tmp_path)
    _run(store, app.state.project.root, database)

    bar = client.get("/").text
    bar = bar[bar.index('<header class="app-bar">') : bar.index("</header>")]

    chips = re.findall(r'<span class="dbchip__name">([^<]+)</span>', bar)
    assert chips == ["nph.db", "nph.load_redcap.backup.db", "nph.qc_detail.backup.db"]
    assert 'data-confirm="Delete nph.db? This cannot be undone."' in bar
    assert 'action="/database/delete"' in bar


def test_delete_removes_the_named_file_and_returns_to_the_page(app, store, client, tmp_path) -> None:
    database, first, _second = _files(tmp_path)
    wal = Path(f"{database}.wal")
    wal.write_bytes(b"x")
    _run(store, app.state.project.root, database)

    response = client.post(
        "/database/delete",
        data={"name": "nph.db"},
        headers={"referer": "http://testserver/plan?profile=success"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/plan?profile=success"
    assert not database.exists() and not wal.exists()
    assert first.exists()


def test_delete_takes_only_a_listed_name(app, store, client, tmp_path) -> None:
    database, _first, _second = _files(tmp_path)
    _run(store, app.state.project.root, database)

    for name in ("other.db", "../data/other.db", str(database.parent / "other.db")):
        response = client.post("/database/delete", data={"name": name}, follow_redirects=False)
        assert response.status_code == 404, name
    assert (database.parent / "other.db").exists()


def test_delete_is_refused_while_a_run_is_in_progress(app, store, client, tmp_path) -> None:
    database, _first, _second = _files(tmp_path)
    _run(store, app.state.project.root, database, finish=False)

    response = client.post("/database/delete", data={"name": "nph.db"}, follow_redirects=False)

    assert response.status_code == 409
    assert database.exists()


def test_the_log_says_what_the_run_started_from(app, store, client, tmp_path) -> None:
    database, first, _second = _files(tmp_path)
    plain = _run(store, app.state.project.root, database)
    restored = _run(store, app.state.project.root, database, checkpoint=first)

    assert ">database nph.db<" in client.get(f"/runs/{plain.run_id}").text
    assert (
        ">restored nph.db from checkpoint nph.load_redcap.backup.db<"
        in client.get(f"/runs/{restored.run_id}").text
    )


def test_deleting_asks_first() -> None:
    script = (STATIC_DIR / "app.js").read_text()
    assert 'getAttribute("data-confirm")' in script and "window.confirm" in script
