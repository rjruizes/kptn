from __future__ import annotations

import sys
from typing import TextIO

from kptn.runner.events import EventKind, RunEvent

#: What a succeeded table's publish to the shared cache did, in the console's words.
_SHARE_WORDS = {"published": "published", "linked": "linked to an identical build"}


class ConsoleEventSink:
    def __init__(
        self,
        stream_out: TextIO | None = None,
        stream_err: TextIO | None = None,
    ) -> None:
        self.out = sys.stdout if stream_out is None else stream_out
        self.err = sys.stderr if stream_err is None else stream_err

    def emit(self, event: RunEvent) -> None:
        if event.kind is EventKind.TASK_STARTED:
            mode = event.payload.get("mode", "run")
            if mode == "source_table" and event.payload.get("shared") == "restored":
                print(
                    self._timestamped("[RESTORE]", event, " — from shared cache"),
                    file=self.out,
                    flush=True,
                )
            elif mode == "source":
                count = event.payload["count"]
                tables = "table" if count == 1 else "tables"
                print(
                    self._timestamped("[SOURCE]", event, f" — {count} {tables}"),
                    file=self.out,
                    flush=True,
                )
            elif mode == "map":
                print(
                    f"[MAP] {self._task_name(event)} — expanding over {event.payload['count']} items",
                    file=self.out,
                    flush=True,
                )
            else:
                print(self._timestamped("[RUN]", event), file=self.out, flush=True)
            return

        if event.kind is EventKind.TASK_SKIPPED:
            print(
                self._timestamped("[SKIP]", event, " — cached"),
                file=self.out,
                flush=True,
            )
            return

        if (
            event.kind is EventKind.TASK_FINISHED
            and event.payload.get("status") == "failed"
        ):
            print(
                self._timestamped("[FAIL]", event, f" — {event.payload['error']}"),
                file=self.err,
                flush=True,
            )
            return

        if (
            event.kind is EventKind.TASK_FINISHED
            and event.payload.get("status") == "succeeded"
            and event.payload.get("shared") in _SHARE_WORDS
        ):
            print(
                self._timestamped("[SHARE]", event, f" — {_SHARE_WORDS[str(event.payload['shared'])]}"),
                file=self.out,
                flush=True,
            )

    def _timestamped(self, prefix: str, event: RunEvent, suffix: str = "") -> str:
        return f"{prefix} {event.timestamp.strftime('%H:%M:%S')} {self._task_name(event)}{suffix}"

    @staticmethod
    def _task_name(event: RunEvent) -> str:
        if event.task_name is None:
            raise ValueError(f"{event.kind.value} events rendered to console require a task name.")
        return event.task_name
