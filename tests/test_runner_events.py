from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO

from kptn.runner.console import ConsoleEventSink
from kptn.runner.events import EventEmitter, EventKind, RunEvent, current_task_name


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)


def _event(
    kind: EventKind,
    *,
    sequence: int = 1,
    task_name: str | None = "task_a",
    payload: dict[str, object] | None = None,
) -> RunEvent:
    return RunEvent(
        run_id="run-1",
        sequence=sequence,
        timestamp=datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        kind=kind,
        pipeline="default",
        profile=None,
        task_name=task_name,
        payload=payload or {},
    )


def test_event_emitter_tracks_sequence_and_task_scope() -> None:
    sink = RecordingSink()
    emitter = EventEmitter("run-1", "default", None, sink)

    assert current_task_name() is None

    with emitter.task_scope("task_a"):
        emitter.emit(EventKind.WARNING, message="careful")

    assert current_task_name() is None
    assert [event.sequence for event in sink.events] == [1]
    assert sink.events[0].task_name == "task_a"
    assert sink.events[0].payload == {"message": "careful"}


def test_console_event_sink_preserves_executor_cli_contract() -> None:
    out = StringIO()
    err = StringIO()
    sink = ConsoleEventSink(out, err)

    sink.emit(_event(EventKind.TASK_STARTED, payload={"mode": "python"}))
    sink.emit(_event(EventKind.TASK_SKIPPED, sequence=2, payload={"mode": "python", "cached": True}))
    sink.emit(_event(EventKind.TASK_STARTED, sequence=3, task_name="expand_items", payload={"mode": "map", "count": 3}))
    sink.emit(_event(EventKind.TASK_FINISHED, sequence=4, payload={"status": "failed", "error": "boom"}))
    sink.emit(_event(EventKind.LOG, sequence=5, payload={"stream": "stdout", "message": "ignored"}))

    assert out.getvalue() == (
        "[RUN] 03:04:05 task_a\n"
        "[SKIP] 03:04:05 task_a — cached\n"
        "[MAP] expand_items — expanding over 3 items\n"
    )
    assert err.getvalue() == "[FAIL] 03:04:05 task_a — boom\n"
