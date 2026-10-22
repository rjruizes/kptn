"""Run logs that outlive their working directory: ``settings.shared_logs``.

With the setting, a recorded run writes its ``.jsonl`` and the project's
``index.json`` to ``<shared_logs>/<working directory name>/`` from the start,
so deleting the working directory -- ``ui.db`` and all -- keeps its history.
"""

from __future__ import annotations

import shutil
import stat
from pathlib import Path

from kptn.runner.events import EventKind
from kptn_server.run_files import INDEX_FILENAME, LOG_FILE_MODE, read_index
from kptn_server.run_store import STATUS_SUCCEEDED, RunRecord, RunRequest, RunStore

SHARED_LOGS_YAML = "settings:\n  shared_logs: ../_kptn_logs\n"


def _project(tmp_path: Path, name: str = "alice_main", yaml: str | None = SHARED_LOGS_YAML) -> Path:
    project = tmp_path / "r7" / name
    project.mkdir(parents=True)
    if yaml is not None:
        (project / "kptn.yaml").write_text(yaml)
    if yaml == SHARED_LOGS_YAML:
        (tmp_path / "r7" / "_kptn_logs").mkdir(exist_ok=True)  # set up, as on the server
    return project


def _logs(tmp_path: Path, name: str = "alice_main") -> Path:
    return (tmp_path / "r7" / "_kptn_logs" / name).resolve()


def _store(project: Path) -> RunStore:
    return RunStore(project / ".kptn" / "state" / "ui.db")


def _finished_run(store: RunStore, project: Path) -> RunRecord:
    record = store.create_run(RunRequest(project_root=project, pipeline="p"))
    store.append_event(record.run_id, EventKind.RUN_STARTED.value)
    store.append_event(
        record.run_id,
        EventKind.LOG.value,
        task_name="t",
        payload={"stream": "stdout", "severity": "output"},
        text="hello\n",
    )
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
    return record


def test_run_file_and_index_go_to_shared_logs(tmp_path: Path) -> None:
    project = _project(tmp_path)
    record = _store(project).create_run(RunRequest(project_root=project, pipeline="p"))
    logs = _logs(tmp_path)
    assert record.log_path == logs / f"{record.run_id}.jsonl"
    assert stat.S_IMODE(record.log_path.stat().st_mode) == LOG_FILE_MODE
    assert stat.S_IMODE((logs / INDEX_FILENAME).stat().st_mode) == LOG_FILE_MODE
    assert [run.run_id for run in read_index(logs)] == [record.run_id]
    assert not (project / ".kptn" / "state" / "runs").exists()


def test_logs_survive_deleting_the_working_directory(tmp_path: Path) -> None:
    project = _project(tmp_path)
    store = _store(project)
    record = _finished_run(store, project)
    store.close()
    shutil.rmtree(project)

    logs = _logs(tmp_path)
    assert "hello" in (logs / f"{record.run_id}.jsonl").read_text()
    assert [run.status for run in read_index(logs)] == [STATUS_SUCCEEDED]


def test_without_the_setting_nothing_moves(tmp_path: Path) -> None:
    project = _project(tmp_path, yaml=None)
    record = _store(project).create_run(RunRequest(project_root=project, pipeline="p"))
    assert record.log_path.parent == project.resolve() / ".kptn" / "state" / "runs"
    assert not (tmp_path / "r7" / "_kptn_logs").exists()


def test_the_index_follows_the_newest_run_after_the_setting_changes(tmp_path: Path) -> None:
    project = _project(tmp_path, yaml=None)
    store = _store(project)
    before = _finished_run(store, project)
    (project / "kptn.yaml").write_text(SHARED_LOGS_YAML)
    (tmp_path / "r7" / "_kptn_logs").mkdir()
    after = _finished_run(store, project)

    assert before.log_path.parent == project.resolve() / ".kptn" / "state" / "runs"
    assert [run.run_id for run in read_index(_logs(tmp_path))] == [after.run_id, before.run_id]


def test_ensure_index_writes_beside_the_newest_run(tmp_path: Path) -> None:
    project = _project(tmp_path)
    store = _store(project)
    record = _finished_run(store, project)
    (_logs(tmp_path) / INDEX_FILENAME).unlink()

    store.ensure_index(project.resolve())

    assert [run.run_id for run in read_index(_logs(tmp_path))] == [record.run_id]


def test_ensure_index_with_no_runs_writes_nothing(tmp_path: Path) -> None:
    project = _project(tmp_path)
    _store(project).ensure_index(project.resolve())
    assert list((tmp_path / "r7" / "_kptn_logs").iterdir()) == []


# -- one history across a directory's lifetimes --------------------------------


def test_a_recreated_directory_keeps_the_earlier_runs(tmp_path: Path) -> None:
    project = _project(tmp_path)
    store = _store(project)
    first = _finished_run(store, project)
    store.close()
    shutil.rmtree(project)

    project = _project(tmp_path)  # the same name, and a fresh ui.db
    second = _finished_run(_store(project), project)

    runs = read_index(_logs(tmp_path))
    assert [run.run_id for run in runs] == [second.run_id, first.run_id]
    assert [run.number for run in runs] == [1, 1]
    assert runs[1].status == STATUS_SUCCEEDED
    assert runs[1].counters == runs[0].counters


def test_runs_this_database_knows_are_not_carried_twice(tmp_path: Path) -> None:
    project = _project(tmp_path)
    store = _store(project)
    first = _finished_run(store, project)
    second = _finished_run(store, project)
    assert [run.run_id for run in read_index(_logs(tmp_path))] == [second.run_id, first.run_id]


def test_an_untrustworthy_earlier_index_carries_nothing(tmp_path: Path) -> None:
    logs = tmp_path / "r7" / "_kptn_logs" / "alice_main"
    logs.mkdir(parents=True)
    (logs / INDEX_FILENAME).write_text("not json")
    project = _project(tmp_path)
    record = _finished_run(_store(project), project)
    assert [run.run_id for run in read_index(_logs(tmp_path))] == [record.run_id]
