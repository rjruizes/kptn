"""The run page's pipeline sidebar: steps from the pipeline, progress from events."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import create_app
from kptn_server.run_outline import RunOutline, build_outline
from kptn_server.run_store import STATUS_FAILED, STATUS_SUCCEEDED, RunRequest, StoredEvent

pytestmark = pytest.mark.ui_hygiene


@dataclass(frozen=True)
class Entry:
    name: str
    kind: str = "python"
    groups: tuple[str, ...] = ()
    bypassed: bool = False


ENTRIES = [
    Entry("init_database"),
    Entry("asa24", "source", ("bronze",)),
    Entry("dhq3", "source", ("bronze",)),
    Entry("bronze_views", "views"),
    Entry("load_a", groups=("datasets", "module_a")),
    Entry("qc_a", groups=("datasets", "module_a")),
    Entry("load_b", groups=("datasets", "module_b")),
    Entry("report"),
]


def _event(sequence: int, kind: str, task: str | None = None, **payload: Any) -> StoredEvent:
    return StoredEvent(
        run_id="run",
        sequence=sequence,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        kind=kind,
        task_name=task,
        payload=payload,
        log_start=None,
        log_end=None,
        text=None,
    )


def _views(outline: RunOutline) -> dict[str, dict[str, Any]]:
    return {view["name"]: view for view in outline.views()}


# -- the shape ---------------------------------------------------------------


def test_steps_follow_the_pipeline_with_stages_as_cards_of_rows() -> None:
    views = _views(RunOutline(ENTRIES))

    assert list(views) == ["init_database", "bronze", "bronze_views", "datasets", "report"]
    assert [row["name"] for row in views["bronze"]["rows"]] == ["asa24", "dhq3"]
    assert [row["name"] for row in views["datasets"]["rows"]] == ["module_a", "module_b"]
    assert views["datasets"]["rows"][0]["tasks"] == "load_a qc_a"
    assert views["bronze"]["ingest"] and views["bronze_views"]["ingest"]
    assert not views["datasets"]["ingest"]
    # Nothing has run: every step is waiting, with a square per task.
    assert {view["state"] for view in views.values()} == {"waiting"}
    assert len(views["datasets"]["rows"][0]["squares"]) == 2


def test_a_sources_tables_become_its_squares_as_they_are_reported() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "asa24", mode="source", count=2, groups=["bronze"]))
    outline.apply(_event(2, "task_skipped", "asa24[items]", mode="source_table"))
    outline.apply(_event(3, "task_started", "asa24[totals]", mode="source_table"))
    bronze = _views(outline)["bronze"]

    assert [(s["label"], s["state"]) for s in bronze["rows"][0]["squares"]] == [
        ("items", "cached"),
        ("totals", "running"),
    ]
    assert bronze["state"] == "running" and bronze["status"] == "0 of 2"
    assert outline.focus == bronze["rows"][0]["key"]

    outline.apply(
        _event(4, "task_finished", "asa24[totals]", mode="source_table", status="succeeded", duration_seconds=0.25)
    )
    # Its tables are done, but the source itself has not finished.
    assert _views(outline)["bronze"]["rows"][0]["state"] == "running"
    outline.apply(_event(5, "task_finished", "asa24", mode="source", status="succeeded", cached=False))
    bronze = _views(outline)["bronze"]
    assert bronze["rows"][0]["state"] == "succeeded"
    assert bronze["rows"][0]["squares"][1]["title"] == "totals: done in 0.2s"


def test_a_fully_cached_source_reads_cached() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "dhq3", mode="source", count=1, groups=["bronze"]))
    outline.apply(_event(2, "task_skipped", "dhq3[codes]", mode="source_table"))
    outline.apply(_event(3, "task_finished", "dhq3", mode="source", status="succeeded", cached=True))
    assert _views(outline)["bronze"]["rows"][1]["state"] == "cached"


def test_a_finished_task_shows_its_duration() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "report", mode="python"))
    outline.apply(_event(2, "task_finished", "report", mode="python", status="succeeded", duration_seconds=75))
    report = _views(outline)["report"]
    assert (report["status"], report["duration"]) == ("done", "1m 15s")


def test_once_the_run_is_over_nothing_is_still_going() -> None:
    outline = build_outline(
        ENTRIES,
        [_event(1, "task_started", "load_a", mode="python", groups=["datasets", "module_a"])],
        terminal=True,
    )
    views = _views(outline)
    assert views["datasets"]["rows"][0]["squares"][0]["state"] == "interrupted"
    assert views["report"]["state"] == "not_run"
    assert views["report"]["status"] == "not run"


def test_a_task_the_pipeline_does_not_have_is_added_from_its_event() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "renamed", mode="python", groups=["datasets", "module_c"]))
    datasets = _views(outline)["datasets"]
    assert [row["name"] for row in datasets["rows"]] == ["module_a", "module_b", "module_c"]

    bare = RunOutline()
    bare.apply(_event(1, "task_started", "solo", mode="python"))
    assert [view["name"] for view in bare.views()] == ["solo"]


def test_a_mapped_task_takes_its_state_from_its_items() -> None:
    outline = RunOutline([Entry("per_state", "map")])
    outline.apply(_event(1, "task_started", "per_state", mode="map", count=2))
    outline.apply(_event(2, "task_started", "per_state[ny]", mode="map_item"))
    outline.apply(_event(3, "task_finished", "per_state[ny]", mode="map_item", status="succeeded"))
    outline.apply(_event(4, "task_skipped", "per_state[ca]", mode="map_item"))
    [view] = outline.views()
    assert view["state"] == "succeeded"
    assert [s["label"] for s in view["squares"]] == ["ny", "ca"]


def test_the_page_opens_on_the_running_step_or_a_failure() -> None:
    running = RunOutline(ENTRIES)
    running.apply(_event(1, "task_started", "qc_a", mode="python", groups=["datasets", "module_a"]))
    assert running.selected() == _views(running)["datasets"]["rows"][0]["key"]

    failed = build_outline(
        ENTRIES,
        [
            _event(1, "task_started", "load_b", mode="python"),
            _event(2, "task_finished", "load_b", mode="python", status="failed"),
            _event(3, "run_finished", status="failed"),
        ],
    )
    assert failed.selected() == _views(failed)["datasets"]["rows"][1]["key"]
    assert build_outline(ENTRIES, [], terminal=True).selected() == "all"


# -- the page and the stream -----------------------------------------------


@pytest.fixture
def app(ui_project):
    return create_app(ui_project)


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def _run(store, root, *, fail: bool = False) -> str:
    record = store.create_run(RunRequest(project_root=root, pipeline="fixture"))
    run_id = record.run_id
    store.append_event(run_id, "run_started")
    store.append_event(run_id, "task_started", task_name="s", payload={"mode": "source", "count": 1, "groups": ["bronze"]})
    store.append_event(run_id, "task_started", task_name="s[t]", payload={"mode": "source_table", "groups": ["bronze"]})
    for name, mode in (("s[t]", "source_table"), ("s", "source")):
        store.append_event(
            run_id,
            "task_finished",
            task_name=name,
            payload={"mode": mode, "status": "failed" if fail else "succeeded", "duration_seconds": 1.0},
        )
    status = STATUS_FAILED if fail else STATUS_SUCCEEDED
    store.append_event(run_id, "run_finished", payload={"status": status})
    store.finish_run(run_id, status, exit_code=1 if fail else 0)
    return run_id


def test_the_run_page_has_the_pipeline_beside_the_console(app, client) -> None:
    run_id = _run(app.state.store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text

    assert 'class="run-body run-body--outlined"' in html
    start = html.index('<nav class="outline"')
    nav = html[start : html.index("</nav>", start)]
    assert 'data-selected="all"' in nav
    # The fixture's own task, waiting, then the run's stage from its events.
    assert "noisy_task" in nav and "not run" in nav
    assert re.search(r'data-select="s\d+r0" data-tasks="s"', nav)
    assert 'title="t: done in 1.0s"' in nav
    # Rows and folds say which task they belong to, for the filter.
    assert 'data-task="s[t]"' in html and 'data-key="s[t]"' in html


def test_a_failed_run_opens_on_the_failure(app, client) -> None:
    run_id = _run(app.state.store, app.state.project.root, fail=True)
    html = client.get(f"/runs/{run_id}").text
    assert re.search(r'data-selected="s\d+r0"', html)


def test_the_stream_redraws_a_steps_card_when_its_tasks_change(app, client) -> None:
    run_id = _run(app.state.store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text
    text = client.get(f"/runs/{run_id}/events").text
    frames = [json.loads(line[len("data: "):]) for line in text.splitlines() if line.startswith("data: {")]

    task_frames = [f for f in frames if f.get("kind", "").startswith("task_")]
    assert task_frames and all("outline" in f for f in task_frames)
    last = task_frames[-1]["outline"]
    # The card it sends is the card the page has, by id and content.
    assert f'id="{last["id"]}"' in html
    assert 'title="t: done in 1.0s"' in last["html"]
    assert task_frames[0]["outline"]["focus"] is not None
    assert all("outline" not in f for f in frames if f.get("kind") in ("run_started", "run_finished"))


def test_a_warning_rings_its_square_and_redraws_its_card() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "dhq3", mode="source", count=1, groups=["bronze"]))
    outline.apply(_event(2, "task_started", "dhq3[responses]", mode="source_table"))
    step = outline.apply(_event(3, "warning", "dhq3[responses]", message="No checks"))
    assert step is not None and step.name == "bronze"
    [square] = _views(outline)["bronze"]["rows"][1]["squares"]
    assert square["warned"] and square["title"].endswith("with warnings")
    # A second warning changes nothing to redraw.
    assert outline.apply(_event(4, "warning", "dhq3[responses]", message="again")) is None


def test_a_blink_of_a_task_shows_no_duration() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_finished", "report", mode="python", status="succeeded", duration_seconds=0.01))
    assert _views(outline)["report"]["duration"] == ""
