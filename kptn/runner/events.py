from __future__ import annotations

from collections import deque
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from threading import RLock
from types import MappingProxyType
from typing import Iterator, Mapping, Protocol, TypeAlias

JSONScalar: TypeAlias = None | bool | int | float | str
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

_CURRENT_TASK_NAME: ContextVar[str | None] = ContextVar(
    "kptn_runner_task_name",
    default=None,
)


class EventKind(StrEnum):
    RUN_STARTED = "run_started"
    TASK_STARTED = "task_started"
    TASK_SKIPPED = "task_skipped"
    LOG = "log"
    WARNING = "warning"
    TASK_FINISHED = "task_finished"
    RUN_FINISHED = "run_finished"


@dataclass(frozen=True)
class RunEvent:
    run_id: str
    sequence: int
    timestamp: datetime
    kind: EventKind
    pipeline: str
    profile: str | None
    task_name: str | None
    payload: Mapping[str, JSONValue]


class EventSink(Protocol):
    def emit(self, event: RunEvent) -> None: ...


def current_task_name() -> str | None:
    return _CURRENT_TASK_NAME.get()


@contextmanager
def task_scope(task_name: str) -> Iterator[None]:
    token = _CURRENT_TASK_NAME.set(task_name)
    try:
        yield
    finally:
        _CURRENT_TASK_NAME.reset(token)


class EventEmitter:
    def __init__(
        self,
        run_id: str,
        pipeline: str,
        profile: str | None,
        sink: EventSink,
    ) -> None:
        self._run_id = run_id
        self._pipeline = pipeline
        self._profile = profile
        self._sink = sink
        self._sequence = 0
        self._lock = RLock()
        self._pending: deque[RunEvent] = deque()
        self._dispatching = False

    def emit(
        self,
        kind: EventKind,
        *,
        task_name: str | None = None,
        **payload: JSONValue,
    ) -> None:
        resolved_task_name = task_name if task_name is not None else current_task_name()
        with self._lock:
            self._sequence += 1
            self._pending.append(
                RunEvent(
                    run_id=self._run_id,
                    sequence=self._sequence,
                    timestamp=datetime.now(timezone.utc),
                    kind=kind,
                    pipeline=self._pipeline,
                    profile=self._profile,
                    task_name=resolved_task_name,
                    payload=MappingProxyType(dict(payload)),
                )
            )
            if self._dispatching:
                return
            self._dispatching = True

        self._drain_pending()

    def _drain_pending(self) -> None:
        while True:
            with self._lock:
                if not self._pending:
                    self._dispatching = False
                    return
                event = self._pending.popleft()
            try:
                self._sink.emit(event)
            except BaseException:
                with self._lock:
                    self._dispatching = False
                raise

    def task_scope(self, task_name: str) -> AbstractContextManager[None]:
        # ``contextmanager`` returns a context manager, not the iterator its
        # annotation would suggest -- and every caller uses it in a ``with``.
        return task_scope(task_name)


__all__ = [
    "EventEmitter",
    "EventKind",
    "EventSink",
    "JSONValue",
    "RunEvent",
    "current_task_name",
    "task_scope",
]
