"""Tests for the console's folds: tasks grouped under their pipelines.

The grouping is decided once, in :mod:`kptn_server.console_layout`, and both
the run page and the stream use it. So the tests come in three layers: the
placement rules on their own, the tree the page renders from them, and the
frames that tell ``app.js`` where each streamed row goes -- which must land
every row exactly where a reload would have put it.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import STATIC_DIR, create_app
from kptn_server.console_layout import ROOT_ID, ConsoleLayout, Fold
from kptn_server.routes.runs import build_console
from kptn_server.run_store import STATUS_FAILED, STATUS_SUCCEEDED, RunRequest, RunStore, StoredEvent

pytestmark = pytest.mark.ui_hygiene


# -- placement, on its own --------------------------------------------------


def _event(
    sequence: int,
    kind: str,
    task: str | None = None,
    **payload: Any,
) -> StoredEvent:
    return StoredEvent(
        run_id="run",
        sequence=sequence,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        kind=kind,
        task_name=task,
        payload=payload,
        log_start=None,
        log_end=None,
        text="line\n" if kind == "log" else None,
    )


def _place_all(events: list[StoredEvent]) -> list[tuple[str, list[str]]]:
    """Each event's container id and the keys of the folds it opened."""
    layout = ConsoleLayout()
    placed = []
    for event in events:
        placement = layout.place(event)
        placed.append((placement.parent_id, [fold.key for fold in placement.opens]))
    return placed


def test_a_task_opens_its_groups_and_a_fold_of_its_own() -> None:
    layout = ConsoleLayout()
    placement = layout.place(_event(2, "task_started", "a", groups=["datasets", "load_a"]))

    assert [(f.kind, f.key) for f in placement.opens] == [
        ("group", "datasets"),
        ("group", "load_a"),
        ("task", "a"),
    ]
    datasets, load_a, task = placement.opens
    assert datasets.parent is None and load_a.parent is datasets and task.parent is load_a
    assert placement.container is task
    assert placement.in_task


def test_the_next_task_reuses_the_groups_it_shares() -> None:
    placed = _place_all(
        [
            _event(1, "task_started", "a", groups=["datasets", "load_a"]),
            _event(2, "task_finished", "a", status="succeeded"),
            _event(3, "task_started", "b", groups=["datasets", "load_a"]),
            _event(4, "task_started", "c", groups=["datasets", "load_b"]),
            _event(5, "task_started", "d"),
        ]
    )

    assert [opened for _, opened in placed] == [
        ["datasets", "load_a", "a"],
        [],
        ["b"],
        ["load_b", "c"],
        ["d"],
    ]
    assert placed[1][0] == "fold-1-task-body"


def test_output_lands_in_the_running_task() -> None:
    placed = _place_all(
        [
            _event(1, "task_started", "a", groups=["ingest"]),
            _event(2, "log", "a"),
            _event(3, "warning", "a"),
            # Output with no task while one is running is that task's.
            _event(4, "log", None),
            _event(5, "task_finished", "a", status="succeeded"),
            # After it finishes, untasked output is the pipeline's.
            _event(6, "log", None),
        ]
    )

    assert [parent for parent, _ in placed[1:5]] == ["fold-1-task-body"] * 4
    assert placed[5][0] == "fold-1-0-body"


def test_skipped_tasks_and_map_markers_are_rows_in_their_group() -> None:
    placed = _place_all(
        [
            _event(1, "task_skipped", "a", groups=["ingest"]),
            _event(2, "task_started", "items", groups=["ingest"], mode="map", count=2),
            _event(3, "task_started", "items[x]", groups=["ingest"], mode="map_item"),
        ]
    )

    assert placed[0] == ("fold-1-0-body", ["ingest"])
    assert placed[1] == ("fold-1-0-body", [])
    # Each item is a task of its own, folded in the same group.
    assert placed[2] == ("fold-3-task-body", ["items[x]"])


def test_run_events_sit_at_the_top_and_close_every_group() -> None:
    placed = _place_all(
        [
            _event(1, "run_started"),
            _event(2, "task_started", "a", groups=["ingest"]),
            _event(3, "run_finished", status="succeeded"),
            _event(4, "task_started", "b", groups=["ingest"]),
        ]
    )

    assert placed[0][0] == ROOT_ID
    assert placed[2][0] == ROOT_ID
    # A new segment: the old ``ingest`` is closed behind the run event.
    assert placed[3][1] == ["ingest", "b"]


def test_output_for_a_task_with_no_fold_gets_one() -> None:
    placed = _place_all(
        [
            _event(1, "task_started", "a", groups=["ingest"]),
            _event(2, "log", "ghost"),
        ]
    )

    assert placed[1] == ("fold-2-task-body", ["ghost"])


def test_a_failure_marks_every_fold_around_it() -> None:
    layout = ConsoleLayout()
    opened = layout.place(_event(1, "task_started", "a", groups=["datasets", "load_a"])).opens
    layout.place(_event(2, "task_finished", "a", status="failed"))

    assert all(fold.failed for fold in opened)


# -- the page --------------------------------------------------------------


@pytest.fixture
def app(ui_project: Path):
    return create_app(ui_project)


@pytest.fixture
def store(app) -> RunStore:
    return app.state.store


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def _nested_run(store: RunStore, root: Path, *, fail_last: bool = False) -> str:
    """``ingest`` > fetch, clean; then ``datasets`` > ``load_a`` > load, qc."""
    record = store.create_run(RunRequest(project_root=root, pipeline="fixture"))
    run_id = record.run_id
    store.append_event(run_id, "run_started")
    tasks = [
        ("fetch", ["ingest"]),
        ("clean", ["ingest"]),
        ("load", ["datasets", "load_a"]),
        ("qc", ["datasets", "load_a"]),
    ]
    for index, (task, groups) in enumerate(tasks):
        store.append_event(
            run_id, "task_started", task_name=task, payload={"mode": "python", "groups": groups}
        )
        store.append_event(
            run_id,
            "log",
            task_name=task,
            payload={"stream": "stdout", "severity": "output"},
            text=f"{task} one\n{task} two\n",
        )
        failed = fail_last and index == len(tasks) - 1
        store.append_event(
            run_id,
            "task_finished",
            task_name=task,
            payload={
                "mode": "python",
                "status": "failed" if failed else "succeeded",
                "duration_seconds": 75.0,
            },
        )
    status = STATUS_FAILED if fail_last else STATUS_SUCCEEDED
    store.append_event(run_id, "run_finished", payload={"status": status})
    store.finish_run(run_id, status, exit_code=1 if fail_last else 0)
    return run_id


def _fold_states(html: str) -> dict[str, bool]:
    """Each fold's name and whether it renders open, in document order."""
    states = {}
    for match in re.finditer(
        r'<li class="fold [^"]*" id="[^"]*" data-fold="\w+">\s*<details( open)?>\s*'
        r'<summary class="fold__head">\s*<span class="fold__name">([^<]*)</span>',
        html,
    ):
        states[match.group(2)] = bool(match.group(1))
    return states


def test_the_page_nests_tasks_inside_their_pipelines(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text

    assert list(_fold_states(html)) == ["ingest", "fetch", "clean", "datasets", "load_a", "load", "qc"]
    load_a = html.index('<span class="fold__name">load_a</span>')
    datasets = html.index('<span class="fold__name">datasets</span>')
    assert datasets < load_a < html.index('<span class="fold__name">load</span>')


def test_a_finished_run_folds_its_pipelines_and_all_but_the_last_tasks(
    app, store, client
) -> None:
    run_id = _nested_run(store, app.state.project.root)

    assert _fold_states(client.get(f"/runs/{run_id}").text) == {
        "ingest": False,
        "fetch": False,
        "clean": False,
        "datasets": False,
        "load_a": False,
        "load": True,
        "qc": True,
    }


def test_a_failure_is_never_folded_away(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root, fail_last=True)
    states = _fold_states(client.get(f"/runs/{run_id}").text)

    assert states["datasets"] and states["load_a"] and states["qc"]
    assert not states["ingest"]


def test_a_task_heading_names_its_outcome_and_size(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text
    head = html[html.index('<span class="fold__name">fetch</span>') :]
    head = head[: head.index("</summary>")]

    assert 'fold__status--succeeded">succeeded</span>' in head
    assert "1m 15s" in head
    assert 'data-fold-lines="2">2 lines</span>' in head
    # A row inside its own task's fold does not repeat the task's name.
    body = html[html.index('id="fold-2-task-body"') :]
    assert 'class="event__task"' not in body[: body.index("</ol>")]


def test_build_console_puts_only_the_outer_groups_at_the_top(app, store) -> None:
    """``build_console`` is the page's tree: nested folds hang off their parents."""
    run_id = _nested_run(store, app.state.project.root)
    record = store.get_run(run_id)
    assert record is not None
    top = build_console(store.events_after(run_id, 0), Path(record.log_path))

    folds = [item for item in top if isinstance(item, Fold)]
    assert [fold.key for fold in folds] == ["ingest", "datasets"]


# -- the stream ------------------------------------------------------------


def _payloads(text: str) -> list[dict]:
    return [
        json.loads(line[len("data: ") :])
        for line in text.splitlines()
        if line.startswith("data: ") and '"sequence"' in line
    ]


def _parent_of(html: str, sequence: int) -> str:
    """The id of the ``<ol>`` the page rendered event *sequence* inside."""
    at = html.index(f'id="event-{sequence}"')
    depth = 0
    position = at
    while True:
        open_at = html.rindex("<ol", 0, position)
        close_at = html.rfind("</ol>", 0, position)
        if close_at > open_at:
            depth += 1
            position = close_at
            continue
        if depth == 0:
            tag = html[open_at : html.index(">", open_at)]
            found = re.search(r'id="([^"]+)"', tag)
            assert found is not None, tag
            return found.group(1)
        depth -= 1
        position = open_at


def test_streamed_rows_land_where_the_page_renders_them(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text
    frames = _payloads(client.get(f"/runs/{run_id}/events").text)

    assert frames, "the stream sent no events"
    for frame in frames:
        assert frame["parent"] == _parent_of(html, frame["sequence"]), frame["sequence"]


def test_a_resumed_stream_places_rows_from_the_history_before_it(
    app, store, client
) -> None:
    """The layout depends on everything before the cursor, so it is rebuilt."""
    run_id = _nested_run(store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text
    resumed = _payloads(
        client.get(f"/runs/{run_id}/events", headers={"Last-Event-ID": "9"}).text
    )

    first = resumed[0]
    assert first["sequence"] == 10
    # qc's task_finished: no folds left to open, and into qc's own fold.
    assert first["opens"] == []
    assert first["parent"] == _parent_of(html, 10)


def test_frames_open_folds_as_empty_shells_with_their_parent(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root)
    frames = _payloads(client.get(f"/runs/{run_id}/events").text)
    load = next(frame for frame in frames if frame["task"] == "load")

    assert [opened["parent"] for opened in load["opens"]] == [
        ROOT_ID,
        load["opens"][0]["id"] + "-body",
        load["opens"][1]["id"] + "-body",
    ]
    shell = load["opens"][2]["html"]
    assert "<details open>" in shell
    # Nothing in the body but its gutter: the rows arrive in frames of their own.
    assert re.search(
        r'<ol class="fold__body" id="[^"]+"><li class="fold__gutter"[^>]*></li></ol>', shell
    )


def test_a_finished_frame_replaces_its_task_outcome(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root)
    frames = _payloads(client.get(f"/runs/{run_id}/events").text)
    finished = next(frame for frame in frames if frame["kind"] == "task_finished")

    assert finished["outcome"]["id"] == finished["parent"].replace("-body", "-outcome")
    assert "succeeded" in finished["outcome"]["html"]


def test_app_js_follows_the_frames_placement() -> None:
    """The server says where; app.js must read every part of it."""
    script = (STATIC_DIR / "app.js").read_text()

    for field in ("frame.opens", "frame.parent", "frame.outcome"):
        assert field in script, field
    # The open-task count is the server's; the two must not drift.
    from kptn_server.console_layout import OPEN_TASKS

    assert f"var OPEN_TASKS = {OPEN_TASKS};" in script


def test_task_headings_end_with_a_jump_to_the_end_of_the_log(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text

    def head(name: str) -> str:
        start = html.index(f'<span class="fold__name">{name}</span>')
        return html[start : html.index("</summary>", start)]

    task = head("fetch")
    assert 'type="button" class="fold__jump" data-fold-jump' in task
    # Last in the line: after the outcome and the counts.
    assert task.index("data-fold-jump") > task.index("data-fold-warnings")
    assert "data-fold-jump" not in head("ingest")
    assert "[data-fold-jump]" in (STATIC_DIR / "app.js").read_text()


def test_every_fold_body_has_a_gutter_that_closes_it(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text

    bodies = re.findall(r'<ol class="fold__body" id="[^"]+">(<li class="fold__gutter"[^>]*>)?', html)
    assert bodies and all(bodies), "a fold body without a gutter"
    assert 'data-fold-gutter aria-hidden="true" title="Collapse ingest"' in html
    assert "[data-fold-gutter]" in (STATIC_DIR / "app.js").read_text()


def test_output_rows_carry_no_log_label_and_stderr_says_so(app, store, client) -> None:
    run_id = _nested_run(store, app.state.project.root)
    record = store.get_run(run_id)
    assert record is not None
    html = client.get(f"/runs/{run_id}").text
    start = html.index('data-kind="log"')
    row = html[start : html.index("</li>", start)]

    assert 'class="event__kind"' not in row
    assert "fetch one" in row

    from kptn_server.routes.runs import console_event

    stderr = _event(99, "log", "fetch", severity="stderr")
    assert console_event(stderr, Path(record.log_path))["label"] == "stderr"


def test_an_open_task_heading_sticks_to_the_top() -> None:
    css = (STATIC_DIR / "app.css").read_text()
    rule = css[css.index(".fold--task > details[open] > .fold__head {") :]
    rule = rule[: rule.index("}")]

    assert "position: sticky" in rule
    assert "top: 0" in rule
    assert "background:" in rule, "rows scroll beneath it, so it must be opaque"
