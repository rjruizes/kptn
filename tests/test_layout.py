"""Moving an earlier kptn's files into ``.kptn/state/`` and ``.kptn/_bronze/``."""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

from kptn.layout import DEFAULT_DB_PATH, DEFAULT_STORAGE_KEY, migrate_layout
from kptn.state_store.sqlite import SqliteBackend
from kptn_server.run_files import local_runs_dir, runs_dir_of
from kptn_server.run_store import RunRequest, RunStore


def _old_layout(root: Path) -> None:
    (root / ".kptn" / "runs").mkdir(parents=True)
    (root / ".kptn" / "runs" / "index.json").write_text("{}")
    (root / ".kptn" / "ui.db").write_bytes(b"ui")
    (root / ".kptn" / "ui.db-wal").write_bytes(b"ui wal")
    (root / ".kptn" / "kptn.db").write_bytes(b"state")
    (root / "_bronze" / "sales").mkdir(parents=True)
    (root / "_bronze" / "sales" / "orders.parquet").write_bytes(b"parquet")


def test_an_old_layout_moves_into_kptn(tmp_path: Path) -> None:
    _old_layout(tmp_path)

    notices = migrate_layout(tmp_path)

    state = tmp_path / ".kptn" / "state"
    assert (state / "ui.db").read_bytes() == b"ui"
    # SQLite then drops this stand-in log when the run paths are repointed.
    assert "[LAYOUT] moved .kptn/ui.db-wal to .kptn/state/ui.db-wal" in notices
    assert (state / "kptn.db").read_bytes() == b"state"
    assert (state / "runs" / "index.json").exists()
    assert (tmp_path / ".kptn" / "_bronze" / "sales" / "orders.parquet").exists()
    assert sorted(p.name for p in (tmp_path / ".kptn").iterdir()) == ["_bronze", "state"]
    assert not (tmp_path / "_bronze").exists()
    assert "[LAYOUT] moved _bronze/ to .kptn/_bronze/" in notices
    assert "[LAYOUT] moved .kptn/ui.db to .kptn/state/ui.db" in notices


def test_a_moved_layout_is_left_alone(tmp_path: Path) -> None:
    _old_layout(tmp_path)
    migrate_layout(tmp_path)
    assert migrate_layout(tmp_path) == []


def test_a_new_project_gets_nothing(tmp_path: Path) -> None:
    assert migrate_layout(tmp_path) == []
    assert not (tmp_path / ".kptn").exists()


def test_task_state_keeps_its_cache_key(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    store = SqliteBackend(path=".kptn/kptn.db")
    store.write_hash(DEFAULT_STORAGE_KEY, "p", "t", "abc")
    store._conn.close()

    migrate_layout(tmp_path)

    assert SqliteBackend(path=DEFAULT_DB_PATH).read_hash(DEFAULT_STORAGE_KEY, "p", "t") == "abc"


def test_a_configured_db_path_stays_put(tmp_path: Path) -> None:
    _old_layout(tmp_path)
    (tmp_path / "kptn.yaml").write_text("settings:\n  db_path: .kptn/kptn.db\n")

    migrate_layout(tmp_path)

    assert (tmp_path / ".kptn" / "kptn.db").exists()
    assert not (tmp_path / ".kptn" / "state" / "kptn.db").exists()


def test_an_old_copy_beside_a_new_one_stays_with_its_sidecars(tmp_path: Path) -> None:
    _old_layout(tmp_path)
    (tmp_path / ".kptn" / "state").mkdir()
    (tmp_path / ".kptn" / "state" / "ui.db").write_bytes(b"newer")
    (tmp_path / ".kptn" / "_bronze").mkdir()

    notices = migrate_layout(tmp_path)

    assert (tmp_path / ".kptn" / "state" / "ui.db").read_bytes() == b"newer"
    # The old log would be replayed into the newer database.
    assert (tmp_path / ".kptn" / "ui.db-wal").exists()
    assert not (tmp_path / ".kptn" / "state" / "ui.db-wal").exists()
    assert (tmp_path / "_bronze").exists()
    assert (
        "[LAYOUT] .kptn/ui.db is no longer used: kptn uses .kptn/state/ui.db. Delete .kptn/ui.db."
        in notices
    )
    assert (
        "[LAYOUT] _bronze/ is no longer used: kptn uses .kptn/_bronze/. Delete _bronze/."
        in notices
    )


def test_recorded_runs_follow_their_files(tmp_path: Path) -> None:
    root = tmp_path.resolve()
    store = RunStore(root / ".kptn" / "ui.db")
    record = store.create_run(RunRequest(project_root=root, pipeline="p", profile=None))
    legacy = root / ".kptn" / "runs" / record.log_path.name
    legacy.parent.mkdir(parents=True)
    shutil.move(record.log_path, legacy)
    shutil.rmtree(root / ".kptn" / "state")
    store.close()
    with sqlite3.connect(root / ".kptn" / "ui.db") as conn:
        conn.execute("UPDATE runs SET log_path = ?", (str(legacy),))
    conn.close()

    migrate_layout(root)

    moved = RunStore(root / ".kptn" / "state" / "ui.db").get_run(record.run_id)
    assert moved is not None
    assert moved.log_path == root / ".kptn" / "state" / "runs" / legacy.name
    assert moved.log_path.exists()


def test_a_colleagues_old_layout_is_read_where_it_is(tmp_path: Path) -> None:
    (tmp_path / ".kptn" / "runs").mkdir(parents=True)
    assert local_runs_dir(tmp_path) == tmp_path / ".kptn" / "runs"
    assert runs_dir_of(tmp_path, None) == tmp_path / ".kptn" / "runs"
    assert not (tmp_path / ".kptn" / "state").exists()
