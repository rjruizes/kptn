from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from threading import Event, Thread

from kptn.runner.console import ConsoleEventSink
from kptn.runner.events import EventEmitter, EventKind, RunEvent, current_task_name


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)


class ReentrantSink:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []
        self.emitter: EventEmitter | None = None
        self._triggered = False

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)
        if self._triggered:
            return
        self._triggered = True
        assert self.emitter is not None
        self.emitter.emit(EventKind.LOG, message="nested")


class BlockingSink:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []
        self.first_entered = Event()
        self.release_first = Event()
        self.second_delivered = Event()

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)
        if event.sequence == 1:
            self.first_entered.set()
            assert self.release_first.wait(1), "first sink call was not released"
            return
        if event.sequence == 2:
            self.second_delivered.set()


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


def test_event_emitter_reentrant_sink_does_not_deadlock_and_preserves_sequence() -> None:
    sink = ReentrantSink()
    emitter = EventEmitter("run-1", "default", None, sink)
    sink.emitter = emitter
    finished = Event()
    failures: list[BaseException] = []

    def emit_outer_event() -> None:
        try:
            emitter.emit(EventKind.WARNING, message="outer")
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)
        finally:
            finished.set()

    thread = Thread(target=emit_outer_event, daemon=True)
    thread.start()

    assert finished.wait(1), "re-entrant sink deadlocked while EventEmitter.emit held the lock"
    thread.join(timeout=0.1)
    assert failures == []
    assert [event.kind for event in sink.events] == [EventKind.WARNING, EventKind.LOG]
    assert [event.sequence for event in sink.events] == [1, 2]
    assert sink.events[0].payload == {"message": "outer"}
    assert sink.events[1].payload == {"message": "nested"}


def test_event_emitter_concurrent_emit_does_not_deliver_out_of_order() -> None:
    sink = BlockingSink()
    emitter = EventEmitter("run-1", "default", None, sink)
    first_done = Event()
    second_done = Event()

    def emit_first() -> None:
        try:
            emitter.emit(EventKind.WARNING, message="first")
        finally:
            first_done.set()

    def emit_second() -> None:
        try:
            emitter.emit(EventKind.LOG, message="second")
        finally:
            second_done.set()

    first_thread = Thread(target=emit_first, daemon=True)
    second_thread = Thread(target=emit_second, daemon=True)

    first_thread.start()
    assert sink.first_entered.wait(1), "first sink call did not start"

    second_thread.start()
    assert second_done.wait(1), "second emitter call did not complete"
    assert sink.second_delivered.is_set() is False
    assert [event.sequence for event in sink.events] == [1]

    sink.release_first.set()

    assert first_done.wait(1), "first emitter call did not complete"
    first_thread.join(timeout=0.1)
    second_thread.join(timeout=0.1)
    assert [event.sequence for event in sink.events] == [1, 2]
    assert [event.payload["message"] for event in sink.events] == ["first", "second"]


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
