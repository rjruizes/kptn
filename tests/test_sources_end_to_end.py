from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

import kptn
from kptn.change_detector.detector import is_stale as real_is_stale
from kptn.exceptions import TaskError
from kptn.runner.events import EventKind


class Recorder:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)

    def names(self, kind: EventKind) -> list[str]:
        return [e.task_name for e in self.events if e.kind is kind]


def _write(root: Path, rel: str, text: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


@pytest.fixture
def project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "kptn.yaml").write_text(
        "settings:\n  db: duckdb\nprofiles:\n  only_a:\n    stage_selections:\n      bronze: [a]\n"
    )
    _write(tmp_path, "in/a/t.csv", "x\n1\n")
    _write(tmp_path, "in/a/u.csv", "y\n2\n")
    _write(tmp_path, "in/b/v.csv", "z\n3\n")
    seen: list[int] = []

    def get_engine():
        return duckdb.connect(str(tmp_path / "pipeline.db"))

    @kptn.task(outputs=[])
    def init_database(engine) -> None:
        engine.execute("CREATE SCHEMA IF NOT EXISTS curated")

    @kptn.task(outputs=[])
    def consumer(engine) -> None:
        seen.append(engine.sql('SELECT count(*) FROM raw."a_t"').fetchone()[0])

    a = kptn.source("a", files="in/a")
    b = kptn.source("b", files="in/b")
    bronze = kptn.Stage("bronze", a, b)
    pipeline = kptn.Pipeline(
        "p",
        kptn.config(duckdb=(get_engine, "engine"))
        >> kptn.parallel(init_database, bronze)
        >> kptn.source_views(bronze)
        >> consumer,
    )
    return tmp_path, pipeline, seen


def _run(pipeline, **kwargs) -> Recorder:
    rec = Recorder()
    kptn.run(pipeline, event_sink=rec, **kwargs)
    return rec


def test_first_run_builds_and_consumer_reads_views(project) -> None:
    root, pipeline, seen = project
    with pytest.warns(UserWarning):
        rec = _run(pipeline)
    assert sorted(p.name for p in (root / "_bronze" / "a").iterdir()) == ["t.parquet", "u.parquet"]
    assert seen == [1]
    assert "bronze_views" in rec.names(EventKind.TASK_STARTED)


def test_second_run_is_fully_cached(project) -> None:
    _, pipeline, seen = project
    with pytest.warns(UserWarning):
        _run(pipeline)
    rec = _run(pipeline)
    assert set(rec.names(EventKind.TASK_SKIPPED)) >= {"a[t]", "a[u]", "b[v]", "init_database", "consumer"}
    assert seen == [1]


def test_schema_change_leaves_sources_cached(project) -> None:
    _, pipeline, seen = project
    with pytest.warns(UserWarning):
        _run(pipeline)

    def init_database_changed(node, *args, **kwargs):
        if node.name == "init_database":
            return True, "code changed"
        return real_is_stale(node, *args, **kwargs)

    with patch("kptn.runner.executor.is_stale", side_effect=init_database_changed):
        rec = _run(pipeline)
    assert "init_database" in rec.names(EventKind.TASK_STARTED)
    assert set(rec.names(EventKind.TASK_SKIPPED)) >= {"a[t]", "a[u]", "b[v]"}
    assert "consumer" in rec.names(EventKind.TASK_STARTED)
    assert seen == [1, 1]


def test_new_file_rebuilds_only_its_table(project) -> None:
    root, pipeline, _ = project
    with pytest.warns(UserWarning):
        _run(pipeline)
    _write(root, "in/a/t.csv", "x\n1\n2\n")
    with pytest.warns(UserWarning):
        rec = _run(pipeline)
    assert "a[t]" in rec.names(EventKind.TASK_STARTED)
    assert set(rec.names(EventKind.TASK_SKIPPED)) >= {"a[u]", "b[v]"}


def test_deselected_source_never_touches_its_files(project) -> None:
    root, pipeline, _ = project
    for p in (root / "in" / "b").iterdir():
        p.unlink()
    (root / "in" / "b").rmdir()
    with pytest.warns(UserWarning):
        rec = _run(pipeline, profile="only_a")
    assert "b" not in rec.names(EventKind.TASK_STARTED)


def test_failing_check_stops_the_run_and_keeps_old_data(project) -> None:
    pytest.importorskip("soda.scan")
    root, pipeline, _ = project
    with pytest.warns(UserWarning):
        _run(pipeline)
    before = (root / "_bronze" / "a" / "t.parquet").read_bytes()
    _write(
        root,
        "checks/a/t.yml",
        "checks for a_t:\n- schema:\n    fail:\n      when required column missing: [not_there]\n",
    )
    with pytest.raises(TaskError, match="a.t"):
        _run(pipeline)
    assert (root / "_bronze" / "a" / "t.parquet").read_bytes() == before
