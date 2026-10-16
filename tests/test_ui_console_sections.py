"""Tests for the run log's sections: one per task, table or item.

Where each event goes is decided once, in :mod:`kptn_server.console_layout`,
and both the run page and the stream use it. So the tests come in layers:
the placement rules on their own, the page they render, and the frames that
tell ``app.js`` where each streamed row goes -- which must land every row
exactly where a reload would have put it.
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
from kptn_server.console_layout import ROOT_ID, ConsoleLayout, Section
from kptn_server.run_store import STATUS_SUCCEEDED, RunRequest, RunStore, StoredEvent

pytestmark = pytest.mark.ui_hygiene


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
        text="line\n" if kind == "log" else None,
    )


# -- placement, on its own --------------------------------------------------


def test_a_task_opens_its_section_and_its_rows_land_in_it() -> None:
    layout = ConsoleLayout()
    started = layout.place(_event(1, "task_started", "load", mode="python", groups=["datasets", "mod"]))
    [section] = started.opens
    assert (section.key, section.label, section.path, section.runner) == (
        "load",
        "load",
        ("datasets", "mod"),
        "python",
    )
    assert started.container is section and started.hidden

    log = layout.place(_event(2, "log", "load"))
    assert log.container is section and not log.hidden
    # Output naming no task belongs to the task still running.
    assert layout.place(_event(3, "log", None)).container is section

    finished = layout.place(_event(4, "task_finished", "load", mode="python", status="succeeded"))
    assert finished.finishes is section and finished.hidden
    assert section.state == "succeeded"
    # Once it is done, output naming no task is the run's own.
    assert layout.place(_event(5, "log", None)).container is None


def test_a_failure_is_a_line_of_its_own() -> None:
    layout = ConsoleLayout()
    layout.place(_event(1, "task_started", "qa", mode="python"))
    failed = layout.place(_event(2, "task_finished", "qa", mode="python", status="failed", error="no"))
    assert not failed.hidden and failed.finishes is not None
    assert failed.finishes.state == "failed"


def test_a_source_is_a_marker_and_its_tables_open_sections_under_it() -> None:
    layout = ConsoleLayout()
    marker = layout.place(_event(1, "task_started", "sales", mode="source", count=2, groups=["raw_ingest"]))
    assert marker.hidden and marker.container is None and marker.opens == ()

    skipped = layout.place(_event(2, "task_skipped", "sales[items]", mode="source_table", groups=["raw_ingest"]))
    built = layout.place(_event(3, "task_started", "sales[totals]", mode="source_table", groups=["raw_ingest"]))
    for placement, label, status in ((skipped, "items", "cached"), (built, "totals", "")):
        [section] = placement.opens
        assert (section.label, section.path, section.runner, section.status) == (
            label,
            ("raw_ingest", "sales"),
            "ingest",
            status,
        )
    layout.place(_event(4, "task_finished", "sales[totals]", mode="source_table", status="succeeded"))
    # The source's own outcome: its tables have said it already.
    outcome = layout.place(_event(5, "task_finished", "sales", mode="source", status="failed"))
    assert outcome.hidden and outcome.container is None and outcome.opens == ()


def test_a_source_failing_before_any_table_gets_a_section_of_its_own() -> None:
    layout = ConsoleLayout()
    layout.place(_event(1, "task_started", "survey", mode="source", count=0, groups=["raw_ingest"]))
    failed = layout.place(_event(2, "task_finished", "survey", mode="source", status="failed", error="no files"))
    [section] = failed.opens
    assert (section.key, section.label, section.path) == ("survey", "survey", ("raw_ingest",))
    assert not failed.hidden and section.state == "failed"


def test_warnings_are_counted_on_their_section() -> None:
    layout = ConsoleLayout()
    [section] = layout.place(_event(1, "task_started", "alpha", mode="python")).opens
    layout.place(_event(2, "warning", "alpha", message="careful"))
    layout.place(_event(3, "warning", None, message="also"))
    assert section.warnings == 2


def test_a_cached_section_says_why_it_was_skipped() -> None:
    layout = ConsoleLayout()
    layout.place(_event(1, "task_started", "sales", mode="source"))
    [table] = layout.place(_event(2, "task_skipped", "sales[items]", mode="source_table")).opens
    [task] = layout.place(_event(3, "task_skipped", "curate", mode="python")).opens
    assert (table.state, table.reason) == ("cached", "source unchanged")
    assert (task.state, task.reason) == ("cached", "inputs unchanged")


def test_run_events_sit_at_the_top() -> None:
    layout = ConsoleLayout()
    started = layout.place(_event(1, "run_started"))
    assert started.parent_id == ROOT_ID and not started.hidden


# -- the page and the stream -----------------------------------------------


@pytest.fixture
def app(ui_project):
    return create_app(ui_project)


@pytest.fixture
def store(app) -> RunStore:
    return app.state.store


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def _source_run(store: RunStore, root: Path) -> str:
    """A source with a cached table and a built one that prints and warns,
    then a python task."""
    record = store.create_run(RunRequest(project_root=root, pipeline="fixture"))
    run_id = record.run_id
    store.append_event(run_id, "run_started")
    groups = {"groups": ["raw_ingest"]}
    store.append_event(run_id, "task_started", task_name="s", payload={"mode": "source", "count": 2, **groups})
    store.append_event(run_id, "task_skipped", task_name="s[a]", payload={"mode": "source_table", **groups})
    store.append_event(run_id, "task_started", task_name="s[b]", payload={"mode": "source_table", **groups})
    store.append_event(
        run_id, "log", task_name="s[b]", payload={"stream": "stdout", "severity": "output"}, text="read b\n"
    )
    store.append_event(run_id, "warning", task_name="s[b]", payload={"message": "no checks"})
    for name, mode in (("s[b]", "source_table"), ("s", "source")):
        store.append_event(
            run_id,
            "task_finished",
            task_name=name,
            payload={"mode": mode, "status": "succeeded", "duration_seconds": 1.0},
        )
    store.append_event(run_id, "task_started", task_name="after", payload={"mode": "python"})
    store.append_event(
        run_id,
        "task_finished",
        task_name="after",
        payload={"mode": "python", "status": "succeeded", "duration_seconds": 0.5},
    )
    store.append_event(run_id, "run_finished", payload={"status": STATUS_SUCCEEDED})
    store.finish_run(run_id, STATUS_SUCCEEDED, exit_code=0)
    return run_id


def _payloads(text: str) -> list[dict]:
    return [
        json.loads(line[len("data: ") :])
        for line in text.splitlines()
        if line.startswith("data: ") and '"sequence"' in line
    ]


def _parent_of(html: str, sequence: int) -> str:
    """The id of the element the page rendered event *sequence* inside: a
    section's lines, or the log itself."""
    at = html.index(f'id="event-{sequence}"')
    lines = html.rfind('class="sec__lines" id="', 0, at)
    closed = html.rfind("</section>", 0, at)
    if lines == -1 or closed > lines:
        return ROOT_ID
    start = lines + len('class="sec__lines" id="')
    return html[start : html.index('"', start)]


def test_the_page_renders_a_section_per_table_and_task(app, store, client) -> None:
    run_id = _source_run(store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text

    units = re.findall(r'data-section data-unit="([^"]+)"', html)
    assert units == ["s[a]", "s[b]", "after"]
    assert '<span class="sec-h__path">raw_ingest / s /</span>' in html
    assert re.search(r'<span class="sec-h__name">b</span>', html)
    # The warned table's heading: how it ran, how much it printed, a pill.
    log = html[html.index('id="console"') :]
    b = log[log.index('data-unit="s[b]"') : log.index('data-unit="after"')]
    assert "ingest · 1.0s · " in b and ">1 line<" in b
    assert 'aria-label="Succeeded with 1 warning"' in b
    # Lines are numbered across the run: run started, read b, the warning, run succeeded.
    assert re.findall(r'<span class="ln__no">(\d*)</span>', html) == ["1", "2", "3", "4"]


def test_streamed_rows_land_where_the_page_renders_them(app, store, client) -> None:
    run_id = _source_run(store, app.state.project.root)
    html = client.get(f"/runs/{run_id}").text
    frames = _payloads(client.get(f"/runs/{run_id}/events").text)

    assert frames, "the stream sent no events"
    for frame in frames:
        assert frame["parent"] == _parent_of(html, frame["sequence"]), frame["sequence"]


def test_a_section_arrives_as_an_empty_shell_and_its_outcome_after(app, store, client) -> None:
    run_id = _source_run(store, app.state.project.root)
    frames = _payloads(client.get(f"/runs/{run_id}/events").text)

    opening = next(f for f in frames if f["task"] == "s[b]" and f["opens"])
    [shell] = opening["opens"]
    assert shell["parent"] == ROOT_ID
    assert re.search(r'<div class="sec__lines" id="[^"]+"></div>', shell["html"])
    assert 'aria-label="Running"' in shell["html"]

    finished = next(f for f in frames if f["task"] == "s[b]" and f["kind"] == "task_finished")
    assert finished["outcome"]["id"] == f"{shell['id']}-outcome"
    assert 'data-state="succeeded"' in finished["outcome"]["html"]
    # The warning reached the outcome's count even though the stream never
    # reads the log's text.
    assert 'aria-label="Succeeded with 1 warning"' in finished["outcome"]["html"]


def test_app_js_follows_the_frames_placement() -> None:
    """The server says where; app.js must read every part of it."""
    script = (STATIC_DIR / "app.js").read_text()

    for field in ("frame.opens", "frame.parent", "frame.outcome", "frame.outline"):
        assert field in script, field


def test_a_section_with_nothing_to_show_offers_no_fold(app, store, client) -> None:
    run_id = _source_run(store, app.state.project.root)
    log = client.get(f"/runs/{run_id}").text
    log = log[log.index('id="console"') :]

    def heading(unit: str) -> str:
        start = log.index(f'data-unit="{unit}"')
        start = log.rindex("<section", 0, start)
        return log[start : log.index('class="sec__lines"', start)]

    # The quiet task printed nothing: its heading says so, and is not a fold.
    quiet = heading("after")
    assert 'class="sec is-empty"' in quiet
    assert 'tabindex="-1"' in quiet and "aria-expanded" not in quiet
    assert ">no output<" in quiet
    # The cached table never ran: its heading says why, and is no button.
    cached = heading("s[a]")
    assert 'class="sec is-empty"' in cached
    assert "<button" not in cached and "aria-expanded" not in cached
    assert "cached · source unchanged" in cached
    # The table that printed is a fold.
    loud = heading("s[b]")
    assert 'class="sec"' in loud and 'aria-expanded="true"' in loud


def test_a_folded_section_stays_findable(app) -> None:
    """Folding hides a section's lines with ``hidden="until-found"``, not
    ``display: none``, so the browser's find still searches them; app.js
    opens the section when find reveals a match."""
    module = app.state.templates.get_template("_event.html").module
    folded = str(module.section_markup(Section(id="sec-1", key="a", label="a", shown=1), False, ""))
    opened = str(module.section_markup(Section(id="sec-2", key="b", label="b", shown=1), True, ""))
    assert 'class="sec__lines" id="sec-1-lines" hidden="until-found"' in folded
    assert "hidden" not in opened[opened.index('class="sec__lines"') :]

    script = (STATIC_DIR / "app.js").read_text()
    assert 'body.setAttribute("hidden", "until-found")' in script
    assert '"beforematch"' in script
    css = (STATIC_DIR / "app.css").read_text()
    assert ".sec.is-closed .sec__lines {\n  display: none;" not in css


def test_a_shared_table_says_so_in_its_heading(app, store, client) -> None:
    record = store.create_run(RunRequest(project_root=app.state.project.root, pipeline="fixture"))
    run_id = record.run_id
    store.append_event(run_id, "run_started")
    store.append_event(run_id, "task_started", task_name="s", payload={"mode": "source", "count": 2})
    for table, outcome in (("a", "restored"), ("b", "published")):
        name = f"s[{table}]"
        started = {"mode": "source_table", **({"shared": "restored"} if outcome == "restored" else {})}
        store.append_event(run_id, "task_started", task_name=name, payload=started)
        store.append_event(
            run_id,
            "task_finished",
            task_name=name,
            payload={"mode": "source_table", "status": "succeeded", "duration_seconds": 1.0, "shared": outcome},
        )
    store.append_event(
        run_id, "task_finished", task_name="s",
        payload={"mode": "source", "status": "succeeded", "duration_seconds": 2.0},
    )
    store.append_event(run_id, "run_finished", payload={"status": STATUS_SUCCEEDED})
    store.finish_run(run_id, STATUS_SUCCEEDED, exit_code=0)

    log = client.get(f"/runs/{run_id}").text
    log = log[log.index('id="console"') :]
    a = log[log.index('data-unit="s[a]"') : log.index('data-unit="s[b]"')]
    b = log[log.index('data-unit="s[b]"') :]
    assert "ingest · from shared cache · 1.0s · " in a
    assert "ingest · published · 1.0s · " in b

    frames = _payloads(client.get(f"/runs/{run_id}/events").text)
    finished = next(f for f in frames if f["task"] == "s[a]" and f["kind"] == "task_finished")
    assert "from shared cache" in finished["outcome"]["html"]
