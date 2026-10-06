"""The run files: each run's ``.jsonl`` and the project's ``index.json``.

The store writes both, and other people's servers read them from another pod.
These tests pin the two halves of that contract: what the store writes, and
what a reader does with a file it cannot trust.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType

import pytest

from kptn.runner.events import EventKind, RunEvent
from kptn_server import run_files
from kptn_server.capture import RunStoreSink
from kptn_server.log_render import render_run_file
from kptn_server.run_files import (
    INDEX_FILENAME,
    INDEX_SCHEMA,
    RunFileError,
    read_events,
    read_index,
    read_span_text,
    run_file_text,
)
from kptn_server.run_store import (
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    RunRequest,
    RunStore,
    read_run_file,
)


def _store_and_run(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    store = RunStore(project / ".kptn" / "ui.db")
    record = store.create_run(
        RunRequest(project_root=project, pipeline="two_tasks", profile="dev")
    )
    return store, record, project.resolve() / ".kptn" / "runs"


def _two_tasks(store: RunStore, run_id: str) -> None:
    store.append_event(run_id, EventKind.RUN_STARTED.value)
    for task in ("extract", "transform"):
        store.append_event(run_id, EventKind.TASK_STARTED.value, task_name=task)
        for line in (f"{task}: one\n", f"{task}: two\n"):
            store.append_event(
                run_id,
                EventKind.LOG.value,
                task_name=task,
                payload={"stream": "stdout", "severity": "output"},
                text=line,
            )
        store.append_event(
            run_id,
            EventKind.TASK_FINISHED.value,
            task_name=task,
            payload={"status": "succeeded"},
        )
    store.append_event(
        run_id, EventKind.RUN_FINISHED.value, payload={"status": "succeeded"}
    )
    store.finish_run(run_id, STATUS_SUCCEEDED, exit_code=0)


# -- what the store writes ---------------------------------------------------


def test_new_runs_record_to_a_jsonl_file(tmp_path: Path) -> None:
    _, record, runs_dir = _store_and_run(tmp_path)

    assert record.log_path == runs_dir / f"{record.run_id}.jsonl"


def test_every_event_is_a_line_in_sequence_order_with_its_text(tmp_path: Path) -> None:
    store, record, _ = _store_and_run(tmp_path)
    _two_tasks(store, record.run_id)

    events, _ = read_events(record.log_path)
    stored = store.events_after(record.run_id)

    assert [e.sequence for e in events] == [e.sequence for e in stored]
    assert [e.kind for e in events] == [e.kind for e in stored]
    assert run_file_text(record.log_path) == (
        "extract: one\nextract: two\ntransform: one\ntransform: two\n"
    )


def test_a_log_rows_span_is_its_line_and_holds_no_text(tmp_path: Path) -> None:
    store, record, _ = _store_and_run(tmp_path)
    _two_tasks(store, record.run_id)

    logs = [e for e in store.events_after(record.run_id) if e.kind == "log"]
    assert [read_span_text(record.log_path, e.log_start, e.log_end) for e in logs] == [
        "extract: one\n",
        "extract: two\n",
        "transform: one\n",
        "transform: two\n",
    ]
    # The text lives in the run file only; ui.db holds where it is.
    assert all(e.text is None and "message" not in e.payload for e in logs)


def test_a_batch_lands_in_the_file_as_one_run_of_lines(tmp_path: Path) -> None:
    from kptn_server.run_store import PendingEvent

    store, record, _ = _store_and_run(tmp_path)
    store.append_event(record.run_id, EventKind.RUN_STARTED.value)
    now = datetime.now(timezone.utc)
    stored = store.append_events(
        record.run_id,
        [PendingEvent(kind="log", timestamp=now, text=f"line {i}\n") for i in range(3)],
    )

    assert [e.text for e in stored] == ["line 0\n", "line 1\n", "line 2\n"]
    assert stored[0].log_end == stored[1].log_start
    assert stored[2].log_end == record.log_path.stat().st_size


def test_an_append_that_rolls_back_is_cut_back_out_of_the_file(tmp_path: Path) -> None:
    """The file and ui.db never disagree about an event that did not happen."""
    store, record, _ = _store_and_run(tmp_path)
    store.append_event(record.run_id, EventKind.RUN_STARTED.value)
    size = record.log_path.stat().st_size

    conn = sqlite3.connect(store.path)
    conn.execute(
        "CREATE TRIGGER refuse BEFORE INSERT ON run_events "
        "BEGIN SELECT RAISE(ABORT, 'refused'); END"
    )
    conn.commit()
    conn.close()

    with pytest.raises(sqlite3.IntegrityError):
        store.append_event(
            record.run_id, "log", payload={"stream": "stdout"}, text="lost\n"
        )

    assert record.log_path.stat().st_size == size
    assert "lost" not in run_file_text(record.log_path)


def test_r_output_moves_from_the_payload_into_the_run_file(tmp_path: Path) -> None:
    """The executor's inline ``message`` reaches the file, and so the download."""
    store, record, _ = _store_and_run(tmp_path)
    store.append_event(record.run_id, EventKind.RUN_STARTED.value)
    sink = RunStoreSink(store, record.run_id)

    sink.emit(
        RunEvent(
            run_id=record.run_id,
            sequence=1,
            timestamp=datetime.now(timezone.utc),
            kind=EventKind.LOG,
            pipeline="p",
            profile=None,
            task_name="r_task",
            payload=MappingProxyType({"stream": "stderr", "message": "from R\n"}),
        )
    )

    (row,) = [e for e in store.events_after(record.run_id) if e.kind == "log"]
    assert "message" not in row.payload
    assert row.payload["severity"] == "stderr"
    assert read_span_text(record.log_path, row.log_start, row.log_end) == "from R\n"
    assert b"from R\n" in render_run_file(record.log_path, record.run_id)


def test_the_download_is_the_cli_transcript(tmp_path: Path) -> None:
    store, record, _ = _store_and_run(tmp_path)
    _two_tasks(store, record.run_id)

    text = render_run_file(record.log_path, record.run_id).decode()

    assert text.index("extract") < text.index("extract: one\n")
    assert "[RUN]" in text
    assert text.index("extract: two\n") < text.index("transform: one\n")


# -- index.json --------------------------------------------------------------


def test_creating_and_finishing_a_run_publish_the_index(tmp_path: Path) -> None:
    store, record, runs_dir = _store_and_run(tmp_path)
    (listed,) = read_index(runs_dir)
    assert listed.run_id == record.run_id
    assert listed.status == "queued"

    _two_tasks(store, record.run_id)

    (listed,) = read_index(runs_dir)
    assert listed.status == STATUS_SUCCEEDED
    assert listed.exit_code == 0
    assert listed.profile == "dev"
    assert listed.counters["tasks"] == 2
    assert listed.counters["succeeded"] == 2


def test_unforced_publishes_are_rate_limited(tmp_path: Path) -> None:
    store, record, runs_dir = _store_and_run(tmp_path)
    store.append_event(record.run_id, EventKind.RUN_STARTED.value)  # forced
    store.append_event(record.run_id, EventKind.TASK_STARTED.value, task_name="a")

    (listed,) = read_index(runs_dir)
    assert listed.status == STATUS_RUNNING
    assert listed.current_task is None  # within the interval: not republished

    store.publish_index(record.project_root)
    (listed,) = read_index(runs_dir)
    assert listed.current_task == "a"


def test_the_index_leaves_out_worker_identity(tmp_path: Path) -> None:
    store, record, runs_dir = _store_and_run(tmp_path)
    store.record_worker_start(record.run_id, pid=4242, started_at=1.0)
    store.publish_index(record.project_root)

    document = json.loads((runs_dir / INDEX_FILENAME).read_text())
    assert "worker_pid" not in json.dumps(document)


def test_a_failed_publish_never_fails_the_write(tmp_path: Path) -> None:
    store, record, runs_dir = _store_and_run(tmp_path)
    (runs_dir / INDEX_FILENAME).unlink()
    (runs_dir / INDEX_FILENAME).mkdir()  # os.replace onto a directory fails

    store.append_event(record.run_id, EventKind.RUN_STARTED.value)
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    assert store.get_run(record.run_id).status == STATUS_SUCCEEDED
    assert not [p for p in runs_dir.iterdir() if p.name.endswith(".tmp")]


def test_ensure_index_backfills_a_project_with_none(tmp_path: Path) -> None:
    store, record, runs_dir = _store_and_run(tmp_path)
    (runs_dir / INDEX_FILENAME).unlink()

    store.ensure_index(record.project_root)

    assert [run.run_id for run in read_index(runs_dir)] == [record.run_id]


# -- reading files somebody else wrote -----------------------------------------


def _write_index(runs_dir: Path, document: object) -> None:
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / INDEX_FILENAME).write_text(json.dumps(document))


def test_a_missing_index_is_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_index(tmp_path)


def test_a_newer_schema_is_refused_not_guessed_at(tmp_path: Path) -> None:
    _write_index(tmp_path, {"schema": INDEX_SCHEMA + 1, "runs": []})
    with pytest.raises(RunFileError, match="Upgrade kptn"):
        read_index(tmp_path)


@pytest.mark.parametrize("content", ["not json", "[]", '{"runs": []}', '{"schema": 1}'])
def test_a_malformed_index_is_an_error(tmp_path: Path, content: str) -> None:
    (tmp_path / INDEX_FILENAME).write_text(content)
    with pytest.raises(RunFileError):
        read_index(tmp_path)


def test_malformed_rows_are_dropped_and_crafted_run_ids_refused(tmp_path: Path) -> None:
    good = "a" * 32
    _write_index(
        tmp_path,
        {
            "schema": 1,
            "runs": [
                {"run_id": good, "status": "succeeded", "counters": {"tasks": 3}},
                {"run_id": "../../etc/passwd", "status": "succeeded"},
                {"run_id": "b" * 32},
                "not a row",
            ],
        },
    )

    (run,) = read_index(tmp_path)
    assert run.run_id == good
    assert run.counters["tasks"] == 3
    assert run.counters["failed"] == 0


def test_an_index_reached_through_a_symlink_is_refused(tmp_path: Path) -> None:
    elsewhere = tmp_path / "elsewhere.json"
    elsewhere.write_text(json.dumps({"schema": 1, "runs": []}))
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()
    (runs_dir / INDEX_FILENAME).symlink_to(elsewhere)

    with pytest.raises(RunFileError):
        read_index(runs_dir)


def test_a_runs_directory_reached_through_a_symlink_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "project"
    (root / ".kptn").mkdir(parents=True)
    (tmp_path / "elsewhere").mkdir()
    (root / ".kptn" / "runs").symlink_to(tmp_path / "elsewhere")

    with pytest.raises(RunFileError):
        run_files.published_runs_dir(root.resolve())


def test_an_oversized_index_is_refused(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(run_files, "MAX_INDEX_BYTES", 16)
    _write_index(tmp_path, {"schema": 1, "runs": []})
    with pytest.raises(RunFileError, match="larger"):
        read_index(tmp_path)


def test_reading_stops_at_a_line_still_being_written(tmp_path: Path) -> None:
    path = tmp_path / "run.jsonl"
    whole = run_files.encode_event(
        sequence=1, timestamp="2026-10-06T00:00:00+00:00", kind="log",
        task_name=None, payload={}, text="whole\n",
    )
    path.write_bytes(whole + b'{"seq":2,"ts":"2026-10-06T00:00:01+00:00","ki')

    events, offset = read_events(path)

    assert [e.text for e in events] == ["whole\n"]
    assert offset == len(whole)
    # Resuming there, once the line is finished, picks it up.
    with open(path, "ab") as handle:
        handle.write(b'nd":"log","text":"later\\n"}\n')
    more, _ = read_events(path, offset=offset)
    assert [e.text for e in more] == ["later\n"]


def test_an_undecodable_line_costs_that_event_not_the_run(tmp_path: Path) -> None:
    path = tmp_path / "run.jsonl"
    line = run_files.encode_event(
        sequence=2, timestamp="2026-10-06T00:00:00+00:00", kind="log",
        task_name=None, payload={}, text="kept\n",
    )
    path.write_bytes(b"garbage\n" + b'{"seq":"one"}\n' + line)

    events, _ = read_run_file(path, "r")

    assert [e.text for e in events] == ["kept\n"]


def test_text_with_newlines_and_markup_round_trips_on_one_line(tmp_path: Path) -> None:
    text = "a\nb\r\n<script> é\x00\n"
    line = run_files.encode_event(
        sequence=1, timestamp="2026-10-06T00:00:00+00:00", kind="log",
        task_name="t", payload={"stream": "stdout"}, text=text,
    )
    assert line.count(b"\n") == 1
    path = tmp_path / "run.jsonl"
    path.write_bytes(line)
    assert run_file_text(path) == text


def test_legacy_log_spans_are_raw_bytes(tmp_path: Path) -> None:
    path = tmp_path / "old.log"
    path.write_bytes("héllo\nworld\n".encode())
    assert read_span_text(path, 0, len("héllo\n".encode())) == "héllo\n"
    assert read_span_text(path, 100, 200) == ""


def test_published_index_timestamps_read_back_as_aware_datetimes(tmp_path: Path) -> None:
    store, record, runs_dir = _store_and_run(tmp_path)
    (listed,) = read_index(runs_dir)
    assert listed.created_at is not None
    assert listed.created_at.tzinfo is not None
    assert abs(listed.created_at - datetime.now(timezone.utc)) < timedelta(minutes=5)


def test_index_writes_leave_no_temporary_files(tmp_path: Path) -> None:
    store, record, runs_dir = _store_and_run(tmp_path)
    for _ in range(3):
        store.publish_index(record.project_root)
    assert not [name for name in os.listdir(runs_dir) if name.endswith(".tmp")]
