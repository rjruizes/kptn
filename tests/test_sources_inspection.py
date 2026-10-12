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
