"""Render a run's downloadable log from its durable event stream.

``kptn.run`` takes exactly one event sink. The CLI leaves it unset and gets
``ConsoleEventSink``, which prints ``[RUN]``/``[SKIP]``/``[FAIL]`` to stdout;
the worker passes ``RunStoreSink``, so those lines are never printed and never
reach the captured log file. A run whose tasks were all cached and printed
nothing of their own leaves a zero-byte file behind.

The progress was never lost -- it is in the event stream -- so the download is
rendered from that stream rather than from whatever happened to be printed.
``ConsoleEventSink`` itself does the formatting, pointed at a buffer instead
of stdout: the download matches the CLI because it *is* the CLI's formatter,
not a second copy of its format that can drift.

A run's ``.jsonl`` file holds every event and all of its output, so a download
is rendered from that file alone (:func:`render_run_file`) -- the same bytes
for the owner and for a colleague reading it from another pod. Runs recorded
before run files existed have a ``.log`` of raw output and their events in
``ui.db``, and :func:`render_run_log` splices the two together as it always
did.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Iterable

from kptn.runner.console import ConsoleEventSink
from kptn.runner.events import EventKind, RunEvent
from kptn_server.run_files import RUN_FILE_SUFFIX
from kptn_server.run_store import StoredEvent, read_run_file

# The kinds ConsoleEventSink renders. Everything else in the stream is either
# already in the captured bytes (``log``, ``warning``) or is run-level
# bookkeeping the CLI prints nothing for (``run_started``, ``run_finished``).
_RENDERED_KINDS = frozenset(
    {
        EventKind.TASK_STARTED.value,
        EventKind.TASK_SKIPPED.value,
        EventKind.TASK_FINISHED.value,
    }
)


def render_run_file(path: Path, run_id: str) -> bytes:
    """The log of the run whose ``.jsonl`` file is *path*, as the CLI printed it."""
    events, _ = read_run_file(path, run_id)
    return render_events(events)


def render_events(events: Iterable[StoredEvent]) -> bytes:
    """Progress lines and output, in sequence order, from events that carry their text."""
    buffer = io.StringIO()
    sink = ConsoleEventSink(stream_out=buffer, stream_err=buffer)
    out = bytearray()

    def flush_rendered() -> None:
        text = buffer.getvalue()
        if text:
            out.extend(text.encode("utf-8"))
            buffer.seek(0)
            buffer.truncate(0)

    for stored in events:
        if stored.kind in _RENDERED_KINDS:
            sink.emit(_as_run_event(stored))
            continue
        text = _inline_text(stored)
        if text:
            flush_rendered()
            out.extend(text.encode("utf-8", errors="replace"))
    flush_rendered()
    return bytes(out)


def render_run_log(events: Iterable[StoredEvent], log_path: Path) -> bytes:
    """The run's log as the CLI would have printed it.

    A ``.jsonl`` run is rendered from its file (see :func:`render_run_file`);
    *events* are not needed for it. For an older ``.log`` run, captured
    output is spliced in at the offsets recorded with it, so task output
    stays interleaved with the progress lines around it in the order the run
    actually produced them -- sequence order, which is the order the durable
    stream already guarantees.
    """
    if log_path.suffix == RUN_FILE_SUFFIX:
        run_id = log_path.stem
        return render_run_file(log_path, run_id)

    buffer = io.StringIO()
    # One sink for both streams: a terminal interleaves stdout and stderr, and
    # a download with the [FAIL] lines sorted away from the tasks they belong
    # to would misrepresent the order the developer saw.
    sink = ConsoleEventSink(stream_out=buffer, stream_err=buffer)

    out = bytearray()
    handle = None
    # How far into the file the spans have consumed. Everything before this
    # has been emitted; everything after it has not.
    cursor = 0

    def flush_rendered() -> None:
        text = buffer.getvalue()
        if text:
            out.extend(text.encode("utf-8"))
            buffer.seek(0)
            buffer.truncate(0)

    def open_log() -> io.BufferedReader | None:
        nonlocal handle
        if handle is None:
            try:
                handle = open(log_path, "rb")
            except OSError:
                # The bytes are gone (deleted, rotated, an unmounted volume).
                # The events are not, so the download degrades to the progress
                # lines rather than failing outright.
                return None
        return handle

    try:
        for stored in events:
            if stored.kind in _RENDERED_KINDS:
                sink.emit(_as_run_event(stored))
                continue

            if stored.log_start is None or stored.log_end is None:
                # An R task's output, inline in the payload in runs of this
                # vintage; it never reached the file.
                inline = _inline_text(stored)
                if inline:
                    flush_rendered()
                    out.extend(inline.encode("utf-8", errors="replace"))
                continue
            if stored.log_end <= stored.log_start:
                continue

            # Rendered lines first: this span was captured *after* them, and
            # the buffer is what holds them until now.
            flush_rendered()
            log = open_log()
            if log is None:
                continue
            # Bytes between the last span and this one belong to nobody's
            # event -- everything written through the capture records a span,
            # so this is a file that disagrees with the stream. They were
            # still written, and dropping them would make this download lose
            # content the old whole-file response kept.
            if stored.log_start > cursor:
                out.extend(_read_span(log, cursor, stored.log_start))
            out.extend(_read_span(log, stored.log_start, stored.log_end))
            cursor = max(cursor, stored.log_end)

        flush_rendered()

        # Same reasoning past the final span: a file longer than the stream
        # accounts for keeps its tail.
        log = open_log()
        if log is not None:
            out.extend(_read_tail(log, cursor))
    finally:
        if handle is not None:
            handle.close()

    return bytes(out)


def _inline_text(stored: StoredEvent) -> str:
    """Output an event carries itself: read from a run file, or inline from R."""
    if stored.kind != EventKind.LOG.value:
        return ""
    if stored.text is not None:
        return stored.text
    message = stored.payload.get("message")
    return message if isinstance(message, str) else ""


def _read_tail(handle: io.BufferedReader, start: int) -> bytes:
    """Whatever follows the last recorded span, if anything does."""
    try:
        handle.seek(start)
        return handle.read()
    except OSError:
        return b""


def _read_span(handle: io.BufferedReader, start: int, end: int) -> bytes:
    """The recorded byte span, tolerating a file that no longer contains it.

    Offsets come from the run that wrote them; a truncated or rotated file
    makes them point past the end. A short read is the honest answer -- the
    alternative is a 500 on a download whose progress lines are all intact.
    """
    try:
        handle.seek(start)
        return handle.read(end - start)
    except OSError:
        return b""


def _as_run_event(stored: StoredEvent) -> RunEvent:
    """A ``StoredEvent`` in the shape ``ConsoleEventSink`` renders.

    Timestamps are stored in UTC and displayed in local time: the CLI printed
    the developer's wall clock, and a download whose times are five hours off
    the terminal they remember is a worse answer than no timestamps at all.

    ``pipeline`` and ``profile`` are not stored per event and are not read by
    any branch of the console sink; they are filled rather than faked into
    something a future renderer might trust.
    """
    return RunEvent(
        run_id=stored.run_id,
        sequence=stored.sequence,
        timestamp=stored.timestamp.astimezone(),
        kind=EventKind(stored.kind),
        pipeline="",
        profile=None,
        task_name=stored.task_name,
        payload=stored.payload,
    )
