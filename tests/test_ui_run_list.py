"""The sidebar's Runs pane: a profile's runs by day (kptn_server.run_list),
the store's per-profile listing under it, and the panes around it."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import STATIC_DIR, create_app
from kptn_server.run_list import run_days
from kptn_server.run_store import STATUS_SUCCEEDED, RunRequest, RunStore

pytestmark = pytest.mark.ui_hygiene

NOW = datetime(2026, 10, 14, 15, 0, tzinfo=UTC).astimezone()


@dataclass
class Run:
    run_id: str
    status: str
    created_at: datetime
    profile: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


def _run(status: str, ago: timedelta, run_id: str = "a" * 32) -> Run:
    return Run(run_id=run_id, status=status, created_at=NOW - ago)


def test_runs_fall_under_the_day_they_were_made() -> None:
    days = run_days(
        [
            _run("succeeded", timedelta(minutes=1)),
            _run("succeeded", timedelta(days=1)),
            _run("succeeded", timedelta(days=3)),
            _run("succeeded", timedelta(days=400)),
        ],
        lambda run: 0,
        now=NOW,
    )

    labels = [day.label for day in days]
    assert labels[:2] == ["Today", "Yesterday"]
    three = (NOW - timedelta(days=3)).date()
    assert labels[2] == f"{three:%b} {three.day}"
    assert labels[3].endswith(f", {(NOW - timedelta(days=400)).year}")


def test_a_row_is_a_short_id_and_a_clock_time() -> None:
    (day,) = run_days([_run("failed", timedelta(0), run_id="0123456789" + "f" * 22)], lambda run: 0, now=NOW)
    (row,) = day.rows

    assert row.short_id == "0123456"
    assert row.time == NOW.strftime("%I:%M %p").lstrip("0")
    assert row.icon == "failed"


@pytest.mark.parametrize(
    ("status", "icon"),
    [
        ("succeeded", "succeeded"),
        ("failed", "failed"),
        ("errored", "failed"),
        ("running", "running"),
        ("queued", "running"),
        ("stop_requested", "running"),
        ("stopped", "stopped"),
        ("interrupted", "interrupted"),
    ],
)
def test_a_runs_status_is_the_rails_icon(status: str, icon: str) -> None:
    (day,) = run_days([_run(status, timedelta(0))], lambda run: 0, now=NOW)
    assert day.rows[0].icon == icon


def test_only_a_success_is_marked_for_its_warnings() -> None:
    """Warnings change a succeeded run's icon; a failure's cross says more.
    The count is asked only where it matters."""
    asked: list[str] = []

    def warnings(run: Run) -> int:
        asked.append(run.status)
        return 2

    days = run_days(
        [_run("succeeded", timedelta(0)), _run("failed", timedelta(0))], warnings, now=NOW
    )

    assert [row.warned for row in days[0].rows] == [True, False]
    assert asked == ["succeeded"]
    assert days[0].rows[0].title.startswith("Succeeded with warnings")


# -- the store lists one profile's runs ---------------------------------------


def test_the_store_lists_one_profiles_runs(tmp_path: Path) -> None:
    store = RunStore(tmp_path / "ui.db")
    made = {}
    for profile in ("slow", None, "slow"):
        record = store.create_run(RunRequest(project_root=tmp_path, pipeline="p", profile=profile))
        store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
        made.setdefault(profile, []).append(record.run_id)

    assert {run.run_id for run in store.list_runs(tmp_path, profile="slow")} == set(made["slow"])
    assert [run.run_id for run in store.list_runs(tmp_path, profile="")] == made[None]
    assert len(store.list_runs(tmp_path)) == 3


# -- the panes -------------------------------------------------------------------


def test_the_sidebar_is_next_run_then_runs_then_run(ui_project: Path) -> None:
    body = TestClient(create_app(ui_project)).get("/").text
    side = body[body.index('<aside class="side"') : body.index("</aside>")]

    panes = [side.index(f'data-pane="{name}"') for name in ("next", "runs", "run")]
    assert panes == sorted(panes)
    for name in ("next", "runs", "run"):
        assert f'aria-controls="pane-{name}" data-pane-toggle' in side


def test_the_plan_opens_beside_the_sidebar(ui_project: Path) -> None:
    body = TestClient(create_app(ui_project)).get("/plan?profile=slow").text

    assert '<aside class="side"' in body
    assert body.index('<aside class="side"') < body.index('id="plan"')
    assert "No runs with this profile yet" in body


def test_app_js_remembers_folded_panes_and_refreshes_the_list() -> None:
    source = (STATIC_DIR / "app.js").read_text()

    assert 'var PANES_STORAGE_KEY = "kptn.closedPanes";' in source
    assert "[data-pane-toggle]" in source
    assert 'document.getElementById("run-list")' in source
    assert "document.addEventListener(RUNS_CHANGED, load);" in source
