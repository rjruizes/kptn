"""The run page's rail: tasks from the pipeline, progress from events."""

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


def _units(view: dict[str, Any]) -> list[tuple[str, str]]:
    """Every task the view lists, as (unit, state)."""
    if view["unit"]:
        return [(view["unit"]["unit"], view["unit"]["state"])]
    return [(u["unit"], u["state"]) for b in view["branches"] for u in b["units"]]


# -- the shape ---------------------------------------------------------------


def test_steps_follow_the_pipeline_with_stages_as_trees() -> None:
    views = _views(RunOutline(ENTRIES))

    assert list(views) == ["init_database", "bronze", "bronze_views", "datasets", "report"]
    # A lone task is one line in the rail.
    assert views["report"]["unit"]["unit"] == "report"
    assert views["report"]["branches"] == []
    # A stage is a heading over a branch per source or module.
    assert [b["label"] for b in views["bronze"]["branches"]] == ["asa24", "dhq3"]
    assert [b["label"] for b in views["datasets"]["branches"]] == ["module_a", "module_b"]
    assert [u["unit"] for u in views["datasets"]["branches"][0]["units"]] == ["load_a", "qc_a"]
    assert views["datasets"]["units"] == "load_a qc_a load_b"
    assert views["bronze"]["ingest"] and views["bronze_views"]["ingest"]
    assert not views["datasets"]["ingest"]
    # Nothing has run. A source's tables are not known yet, so it lists none.
    assert {view["state"] for view in views.values()} == {"waiting"}
    assert views["bronze"]["branches"][0]["units"] == []
    assert views["bronze"]["count"] == 0


def test_a_sources_tables_are_listed_as_they_are_reported() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "asa24", mode="source", count=2, groups=["bronze"]))
    outline.apply(_event(2, "task_skipped", "asa24[items]", mode="source_table"))
    outline.apply(_event(3, "task_started", "asa24[totals]", mode="source_table"))
    bronze = _views(outline)["bronze"]

    assert [(u["unit"], u["label"], u["state"]) for u in bronze["branches"][0]["units"]] == [
        ("asa24[items]", "items", "cached"),
        ("asa24[totals]", "totals", "running"),
    ]
    assert bronze["state"] == "running" and bronze["count"] == 2

    outline.apply(
        _event(4, "task_finished", "asa24[totals]", mode="source_table", status="succeeded", duration_seconds=0.25)
    )
    # Its tables are done, but the source itself has not finished.
    assert _views(outline)["bronze"]["branches"][0]["state"] == "running"
    outline.apply(_event(5, "task_finished", "asa24", mode="source", status="succeeded", cached=False))
    bronze = _views(outline)["bronze"]
    assert bronze["branches"][0]["state"] == "succeeded"
    assert bronze["branches"][0]["units"][1]["duration"] == "0.2s"


def test_a_fully_cached_source_reads_cached() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "dhq3", mode="source", count=1, groups=["bronze"]))
    outline.apply(_event(2, "task_skipped", "dhq3[codes]", mode="source_table"))
    outline.apply(_event(3, "task_finished", "dhq3", mode="source", status="succeeded", cached=True))
    assert _views(outline)["bronze"]["branches"][1]["state"] == "cached"


def test_a_finished_task_shows_its_duration_however_short() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "report", mode="python"))
    outline.apply(_event(2, "task_finished", "report", mode="python", status="succeeded", duration_seconds=75))
    assert _views(outline)["report"]["unit"]["duration"] == "1m 15s"
    outline.apply(_event(3, "task_finished", "init_database", mode="python", status="succeeded", duration_seconds=0.01))
    assert _views(outline)["init_database"]["unit"]["duration"] == "0.0s"


def test_once_the_run_is_over_nothing_is_still_going() -> None:
    outline = build_outline(
        ENTRIES,
        [_event(1, "task_started", "load_a", mode="python", groups=["datasets", "module_a"])],
        terminal=True,
    )
    views = _views(outline)
    assert _units(views["datasets"])[0] == ("load_a", "interrupted")
    assert views["report"]["state"] == "not_run"


def test_a_task_the_pipeline_does_not_have_is_added_from_its_event() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "renamed", mode="python", groups=["datasets", "module_c"]))
    datasets = _views(outline)["datasets"]
    assert [b["label"] for b in datasets["branches"]] == ["module_a", "module_b", "module_c"]

    bare = RunOutline()
    bare.apply(_event(1, "task_started", "solo", mode="python"))
    assert [view["name"] for view in bare.views()] == ["solo"]


def test_a_mapped_task_lists_its_items() -> None:
    outline = RunOutline([Entry("per_state", "map")])
    outline.apply(_event(1, "task_started", "per_state", mode="map", count=2))
    outline.apply(_event(2, "task_started", "per_state[ny]", mode="map_item"))
    outline.apply(_event(3, "task_finished", "per_state[ny]", mode="map_item", status="succeeded"))
    outline.apply(_event(4, "task_skipped", "per_state[ca]", mode="map_item"))
    [view] = outline.views()
    assert view["state"] == "succeeded"
    assert _units(view) == [("per_state[ny]", "succeeded"), ("per_state[ca]", "cached")]
    assert outline.filter_counts() == {"all": 2, "ran": 1, "cached": 1, "warned": 0}


def test_a_source_that_fails_before_any_table_is_listed_itself() -> None:
    outline = build_outline(
        ENTRIES,
        [
            _event(1, "task_started", "asa24", mode="source", count=0, groups=["bronze"]),
            _event(2, "task_finished", "asa24", mode="source", status="failed", error="no files"),
            _event(3, "run_finished", status="failed"),
        ],
    )
    assert _units(_views(outline)["bronze"]) == [("asa24", "failed")]
    assert outline.selected() == "asa24"


def test_a_finished_run_opens_on_its_first_failure() -> None:
    failed = build_outline(
        ENTRIES,
        [
            _event(1, "task_started", "load_b", mode="python"),
            _event(2, "task_finished", "load_b", mode="python", status="failed"),
            _event(3, "run_finished", status="failed"),
        ],
    )
    assert failed.selected() == "load_b"
    assert build_outline(ENTRIES, [], terminal=True).selected() == ""
    # A live run opens where it is: nothing is selected for it.
    running = RunOutline(ENTRIES)
    running.apply(_event(1, "task_started", "qc_a", mode="python", groups=["datasets", "module_a"]))
    assert running.selected() == ""


def test_a_warning_marks_its_task_and_redraws_its_step() -> None:
    outline = RunOutline(ENTRIES)
    outline.apply(_event(1, "task_started", "dhq3", mode="source", count=1, groups=["bronze"]))
    outline.apply(_event(2, "task_started", "dhq3[responses]", mode="source_table"))
    step = outline.apply(_event(3, "warning", "dhq3[responses]", message="No checks"))
    assert step is not None and step.name == "bronze"
    bronze = _views(outline)["bronze"]
    assert bronze["branches"][1]["units"][0]["warned"] and bronze["warned"]
    # A second warning changes nothing to redraw.
    assert outline.apply(_event(4, "warning", "dhq3[responses]", message="again")) is None


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


def test_the_run_page_has_the_rail_beside_the_log(app, client) -> None:
    run_id = _run(app.state.store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text

    assert 'class="page--run"' in html
    start = html.index('<nav class="rail"')
    rail = html[start : html.index("</nav>", start)]
    assert html.index('<nav class="rail"') < html.index('id="console"')
    assert 'data-selected=""' in rail
    # The fixture's own task, never run, then the run's stage from its events.
    assert 'data-unit="noisy_task"' in rail and 'aria-label="Not run"' in rail
    assert 'data-units="s[t]"' in rail
    assert re.search(r'<div class="rail-label">s\s*</div>', rail)
    assert 'data-unit="s[t]"' in rail and ">1.0s<" in rail
    # setup_task, noisy_task, and the table; only the table ran.
    assert '<b data-counter="all">3</b>' in rail
    assert '<b data-counter="ran">1</b>' in rail
    # The log's section for the table, under its path.
    assert 'data-unit="s[t]"' in html[html.index('id="console"') :]
    assert '<span class="sec-h__path">bronze / s /</span>' in html


def test_a_failed_run_opens_on_the_failure(app, client) -> None:
    run_id = _run(app.state.store, app.state.project.root, fail=True)
    html = client.get(f"/runs/{run_id}").text
    assert 'data-selected="s[t]"' in html


def test_the_stream_redraws_a_steps_part_of_the_rail(app, client) -> None:
    run_id = _run(app.state.store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text
    text = client.get(f"/runs/{run_id}/events").text
    frames = [json.loads(line[len("data: "):]) for line in text.splitlines() if line.startswith("data: {")]

    task_frames = [f for f in frames if f.get("kind", "").startswith("task_")]
    assert task_frames and all("outline" in f for f in task_frames)
    last = task_frames[-1]["outline"]
    # The part it sends is the part the page has, by id and content.
    assert f'id="{last["id"]}"' in html
    assert 'data-unit="s[t]"' in last["html"] and ">1.0s<" in last["html"]
    assert all("outline" not in f for f in frames if f.get("kind") in ("run_started", "run_finished"))
