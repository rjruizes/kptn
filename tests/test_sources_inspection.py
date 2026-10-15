from __future__ import annotations

import io
from pathlib import Path

import kptn
from kptn.inspection import inspect_pipeline
from kptn.profiles.schema import KptnConfig
from kptn.runner.console import ConsoleEventSink
from kptn.runner.events import EventEmitter, EventKind


def _pipeline():
    a = kptn.source("asa24", files="in/asa24")
    bronze = kptn.Stage("bronze", a)
    return kptn.Pipeline("p", bronze >> kptn.source_views(bronze))


def test_inspection_shows_sources_as_tasks(tmp_path: Path) -> None:
    inspection = inspect_pipeline(_pipeline(), KptnConfig(), None, tmp_path)
    by_name = {i.name: i for i in inspection.items}
    src = by_name["asa24"]
    assert (src.kind, src.executable) == ("source", True)
    assert src.inputs == ("in/asa24",)
    assert src.source_path is not None and src.source_path.name == "test_sources_inspection.py"
    assert src.source_line is not None
    views = by_name["bronze_views"]
    assert (views.kind, views.executable) == ("views", True)
    assert views.inputs == ("_bronze/asa24/*.parquet",)


def test_inspection_marks_the_ingest_steps(tmp_path: Path) -> None:
    @kptn.task(outputs=[])
    def transform() -> None: ...

    a = kptn.source("asa24", files="in/asa24")
    bronze = kptn.Stage("bronze", a)
    other = kptn.Stage("silver", transform)
    pipeline = kptn.Pipeline("p", bronze >> kptn.source_views(bronze) >> other)

    inspection = inspect_pipeline(pipeline, KptnConfig(), None, tmp_path)
    ingest = {i.name for i in inspection.items if i.ingest}
    assert ingest == {"bronze", "asa24", "bronze_views"}


def test_console_prints_source_line() -> None:
    out = io.StringIO()
    emitter = EventEmitter("r", "p", None, ConsoleEventSink(stream_out=out, stream_err=io.StringIO()))
    emitter.emit(EventKind.TASK_STARTED, task_name="asa24", mode="source", count=9)
    line = out.getvalue().strip()
    assert line.startswith("[SOURCE] ") and line.endswith("asa24 — 9 tables")


def test_ui_task_kinds_include_sources() -> None:
    from kptn_server.routes.inspect import TASK_KINDS

    assert {"source", "views"} <= TASK_KINDS


def test_console_source_line_says_one_table() -> None:
    out = io.StringIO()
    emitter = EventEmitter("r", "p", None, ConsoleEventSink(stream_out=out, stream_err=io.StringIO()))
    emitter.emit(EventKind.TASK_STARTED, task_name="devices", mode="source", count=1)
    assert out.getvalue().strip().endswith("devices — 1 table")


def test_pipeline_outline_lists_steps_with_their_groups(tmp_path: Path) -> None:
    from kptn.inspection import pipeline_outline

    @kptn.task(outputs=[])
    def init_database() -> None: ...

    @kptn.task(outputs=[])
    def load() -> None: ...

    @kptn.task(outputs=[])
    def report() -> None: ...

    a = kptn.source("asa24", files="in/asa24")
    bronze = kptn.Stage("bronze", a)
    module = kptn.Pipeline("load_module", load)  # ty: ignore[invalid-argument-type]
    pipeline = kptn.Pipeline(
        "p",
        kptn.parallel(init_database, bronze)
        >> kptn.source_views(bronze)
        >> kptn.Stage("datasets", module)
        >> report,
    )

    outline = pipeline_outline(pipeline, KptnConfig(), None)
    assert [(e.name, e.kind, e.groups) for e in outline] == [
        ("init_database", "python", ()),
        ("asa24", "source", ("bronze",)),
        ("bronze_views", "views", ()),
        ("load", "python", ("datasets", "load_module")),
        ("report", "python", ()),
    ]


def test_console_prints_restore_and_share_lines() -> None:
    out = io.StringIO()
    emitter = EventEmitter("r", "p", None, ConsoleEventSink(stream_out=out, stream_err=io.StringIO()))
    emitter.emit(EventKind.TASK_STARTED, task_name="s[a]", mode="source_table", shared="restored")
    emitter.emit(EventKind.TASK_FINISHED, task_name="s[a]", mode="source_table", status="succeeded", shared="restored")
    emitter.emit(EventKind.TASK_STARTED, task_name="s[b]", mode="source_table")
    emitter.emit(EventKind.TASK_FINISHED, task_name="s[b]", mode="source_table", status="succeeded", shared="published")
    emitter.emit(EventKind.TASK_FINISHED, task_name="s[c]", mode="source_table", status="succeeded", shared="linked")
    lines = out.getvalue().splitlines()
    assert len(lines) == 4
    assert lines[0].startswith("[RESTORE] ") and lines[0].endswith("s[a] — from shared cache")
    assert lines[1].startswith("[RUN] ") and lines[1].endswith("s[b]")
    assert lines[2].startswith("[SHARE] ") and lines[2].endswith("s[b] — published")
    assert lines[3].startswith("[SHARE] ") and lines[3].endswith("s[c] — linked to an identical build")
