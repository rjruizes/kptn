"""``kptn run`` records its run where ``kptn ui`` keeps run history.

A run started in a terminal lands in the same store, with the same log file
and events, as one started from the UI -- so the UI lists it and serves its
log -- while the terminal still shows everything it did before.
"""

from __future__ import annotations

import logging
import os
import sys
import warnings
from pathlib import Path

import psutil
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from kptn.cli.commands import app
from kptn_server.app import create_app
from kptn_server.log_render import render_run_log
from kptn_server.run_files import run_file_text
from kptn_server.run_store import (
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    RunRequest,
    RunStore,
)

# ui_project_cwd comes from tests/conftest.py.


@pytest.fixture(autouse=True)
def restore_process_state(monkeypatch: pytest.MonkeyPatch):
    """Undo what an in-process run leaves behind: cwd, imports, streams."""
    monkeypatch.delenv("KPTN_UI_FIXTURE_RUN_LEVEL_WARNING", raising=False)
    original_cwd = Path.cwd()
    original_path = sys.path.copy()
    original_modules = set(sys.modules)
    original_showwarning = warnings.showwarning
    root = logging.getLogger()
    original_handlers = root.handlers.copy()

    yield

    os.chdir(original_cwd)
    sys.path[:] = original_path
    for name in set(sys.modules) - original_modules:
        sys.modules.pop(name, None)
    warnings.showwarning = original_showwarning
    root.handlers[:] = original_handlers


def recorded_runs(project: Path):
    store = RunStore(project / ".kptn" / "ui.db")
    return store, store.list_runs(project.resolve(), limit=50)


def test_kptn_run_records_the_run_and_its_log(ui_project_cwd: Path) -> None:
    result = CliRunner().invoke(app, ["run"])

    assert result.exit_code == 0, result.output
    # The terminal is unchanged: progress lines and task output both reach it.
    assert "[RUN]" in result.stdout and "noisy_task" in result.stdout
    assert "ordinary output" in result.stdout

    store, runs = recorded_runs(ui_project_cwd)
    assert len(runs) == 1
    (run,) = runs
    assert run.status == STATUS_SUCCEEDED
    assert run.exit_code == 0
    assert run.profile is None
    assert run.worker_pid == os.getpid()
    assert "ordinary output\n" in run_file_text(run.log_path)

    download = render_run_log(store.events_after(run.run_id), run.log_path)
    assert b"[RUN]" in download and b"ordinary output" in download


def test_kptn_run_records_a_failure_with_its_traceback(ui_project_cwd: Path) -> None:
    result = CliRunner().invoke(app, ["run", "--profile", "failure"])

    assert result.exit_code == 1
    _, (run,) = recorded_runs(ui_project_cwd)
    assert run.status == STATUS_FAILED
    assert run.profile == "failure"
    log_text = run_file_text(run.log_path)
    assert "Traceback" in log_text
    assert "fixture failure" in log_text


def test_the_ui_lists_a_run_started_from_the_terminal(ui_project_cwd: Path) -> None:
    CliRunner().invoke(app, ["run"])
    _, (run,) = recorded_runs(ui_project_cwd)

    with TestClient(create_app(ui_project_cwd)) as client:
        history = client.get("/runs")
        log = client.get(f"/runs/{run.run_id}/log")

    assert history.status_code == 200
    assert run.run_id in history.text
    assert log.status_code == 200
    assert "ordinary output" in log.text


@pytest.mark.filterwarnings("ignore:sample warning")
def test_kptn_run_no_record_keeps_no_history(ui_project_cwd: Path) -> None:
    result = CliRunner().invoke(app, ["run", "--no-record"])

    assert result.exit_code == 0, result.output
    assert "ordinary output" in result.stdout
    assert not (ui_project_cwd / ".kptn" / "ui.db").exists()


def test_kptn_run_refuses_while_another_recorded_run_is_live(
    ui_project_cwd: Path,
) -> None:
    store = RunStore(ui_project_cwd / ".kptn" / "ui.db")
    active = store.create_run(
        RunRequest(project_root=ui_project_cwd, pipeline="fixture", profile="slow")
    )
    # Claimed by this very process, so it is unmistakably alive.
    store.record_worker_start(
        active.run_id,
        pid=os.getpid(),
        started_at=psutil.Process(os.getpid()).create_time(),
    )

    result = CliRunner().invoke(app, ["run"])

    assert result.exit_code == 1
    assert active.run_id in result.stderr
    assert "--no-record" in result.stderr
    assert [r.run_id for r in store.list_runs(ui_project_cwd.resolve(), limit=50)] == [
        active.run_id
    ]
