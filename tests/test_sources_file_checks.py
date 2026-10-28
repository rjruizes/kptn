"""File checks: ``checks/<source>/<table>.file.yml`` leaves bad files out, with a record."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

import kptn
from kptn.exceptions import TaskError
from kptn.runner.events import EventKind
from kptn.sources.build import (
    build_table,
    is_current,
    plan_source,
    plan_tables,
    remove_stale_tables,
    state_value,
)
from kptn.sources.checks import file_check_path
from kptn.sources.files import SourceFile
from kptn.sources.load import load
from kptn.sources.read import read_file_table
from kptn.sources.rejects import NO_TABLE, RejectedFile, rejects_path, write_rejects
from kptn.sources.shared import ENTRY_LABEL, SHARED_BRONZE_ENV, SharedBronze
from kptn.sources.spec import SourceSpec
from kptn.sources.views import apply_search_path, register_views

#: Each file holds one subject's meals; a file whose subject is not its name is bad.
FILE_CHECK = (
    "checks for meals:\n"
    "  - row_count > 0\n"
    "  - failed rows:\n"
    "      name: subject matches the filename\n"
    "      fail condition: subject <> regexp_extract(_source_file, '([^/]+)\\.csv$', 1)\n"
)


def _write(root: Path, rel: str, text: str) -> Path:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    return p


def _project(root: Path, *, file_check: bool = True) -> SourceSpec:
    """Three subjects' files: one good, one naming another subject, one unreadable."""
    _write(root, "in/meals/p001.csv", "subject,kcal\np001,2000\np001,1800\n")
    _write(root, "in/meals/p002.csv", "subject,kcal\np009,2100\n")
    (root / "in/meals/p003.csv").write_bytes(b"subject,kcal\np003,\xff\xfe\n")
    if file_check:
        _write(root, "checks/meals/meals.file.yml", FILE_CHECK)
    return SourceSpec(files=str(root / "in/meals"), table="meals")


def _rows(path: Path, sql: str = "SELECT * FROM t ORDER BY ALL") -> list[tuple]:
    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{path}')")
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _soda() -> None:
    pytest.importorskip("soda.scan")


def test_file_check_path_convention(tmp_path: Path) -> None:
    assert file_check_path(tmp_path, "s", "meals") == tmp_path / "checks/s/meals.file.yml"


def test_bad_files_are_left_out_and_recorded(tmp_path: Path) -> None:
    spec = _project(tmp_path)
    [plan] = plan_tables("meals", spec, {}, root=tmp_path)
    assert plan.rejects == rejects_path(tmp_path, "meals", "meals")

    assert build_table(plan, spec, root=tmp_path) is True  # no table check, so no warning

    assert _rows(plan.parquet, "SELECT subject, kcal, _source_file FROM t ORDER BY ALL") == [
        ("p001", "1800", "meals/p001.csv"),
        ("p001", "2000", "meals/p001.csv"),
    ]
    rejected = _rows(plan.rejects)
    assert [(t, f) for t, f, _ in rejected] == [("meals", "meals/p002.csv"), ("meals", "meals/p003.csv")]
    assert rejected[0][2] == "failed subject matches the filename (1)"
    assert rejected[1][2].startswith("cannot be read:")


def test_table_check_still_runs_on_the_files_kept(tmp_path: Path) -> None:
    spec = _project(tmp_path)
    _write(tmp_path, "checks/meals/meals.yml", "checks for meals:\n  - row_count = 2\n")
    [plan] = plan_tables("meals", spec, {}, root=tmp_path)
    assert build_table(plan, spec, root=tmp_path) is True

    _write(tmp_path, "checks/meals/meals.yml", "checks for meals:\n  - row_count = 3\n")
    [plan] = plan_tables("meals", spec, {}, root=tmp_path)
    with pytest.raises(TaskError, match="Checks failed for meals.meals"):
        build_table(plan, spec, root=tmp_path)


def test_every_file_rejected_fails_the_table(tmp_path: Path) -> None:
    spec = _project(tmp_path)
    (tmp_path / "in/meals/p001.csv").unlink()
    [plan] = plan_tables("meals", spec, {}, root=tmp_path)
    with pytest.raises(TaskError, match=r"Every file of meals\.meals was rejected"):
        build_table(plan, spec, root=tmp_path)
    assert not plan.parquet.exists()
    assert not plan.rejects.exists()


def test_a_file_missing_a_checked_column_is_rejected(tmp_path: Path) -> None:
    spec = _project(tmp_path)
    _write(tmp_path, "in/meals/p004.csv", "kcal\n1900\n")
    [plan] = plan_tables("meals", spec, {}, root=tmp_path)
    build_table(plan, spec, root=tmp_path)
    reasons = dict((f, r) for _, f, r in _rows(plan.rejects))
    assert 'column "subject" not found' in reasons["meals/p004.csv"]


def test_a_file_check_without_soda_fails_rather_than_rejecting(tmp_path: Path, monkeypatch) -> None:
    import sys

    spec = _project(tmp_path)
    monkeypatch.setitem(sys.modules, "soda", None)
    monkeypatch.setitem(sys.modules, "soda.scan", None)
    [plan] = plan_tables("meals", spec, {}, root=tmp_path)
    with pytest.raises(TaskError, match=r"kptn\[soda\]"):
        build_table(plan, spec, root=tmp_path)


def test_without_a_file_check_a_bad_file_fails_the_table(tmp_path: Path) -> None:
    spec = _project(tmp_path, file_check=False)
    [plan] = plan_tables("meals", spec, {}, root=tmp_path)
    assert plan.file_check is None and plan.rejects is None
    with pytest.raises(duckdb.Error):  # before checks, so no missing-check warning
        build_table(plan, spec, root=tmp_path)


def test_a_file_check_changes_the_key_only_when_it_exists(tmp_path: Path) -> None:
    spec = _project(tmp_path, file_check=False)
    [before] = plan_tables("meals", spec, {}, root=tmp_path)
    _write(tmp_path, "checks/meals/meals.file.yml", FILE_CHECK)
    [checked] = plan_tables("meals", spec, {}, root=tmp_path)
    _write(tmp_path, "checks/meals/meals.file.yml", FILE_CHECK + "  - row_count < 10\n")
    [edited] = plan_tables("meals", spec, {}, root=tmp_path)
    (tmp_path / "checks/meals/meals.file.yml").unlink()
    [after] = plan_tables("meals", spec, {}, root=tmp_path)

    assert len({before.key, checked.key, edited.key}) == 3
    assert after.key == before.key


def test_a_late_csv_error_is_caught_when_its_file_is_read(tmp_path: Path) -> None:
    # Past the rows DuckDB samples, so creating the relation alone succeeds.
    path = _write(tmp_path, "in/big.csv", "a,b\n" + "1,2\n" * 30000 + "1,2,3,4\n")
    conn = duckdb.connect()
    with pytest.raises(duckdb.Error, match="CSV Error"):
        read_file_table(conn, "s", "big", SourceFile(path, path.parent), 0)


def test_table_none_leaves_a_file_out_of_every_table(tmp_path: Path) -> None:
    _write(tmp_path, "in/p001_day.csv", "x\n1\n")
    _write(tmp_path, "in/p001_unknown.csv", "x\n1\n")
    spec = SourceSpec(
        files=str(tmp_path / "in"),
        table=lambda f: "day" if "day" in f.name else None,
    )
    plans, no_table = plan_source("s", spec, {}, root=tmp_path)
    assert [p.table for p in plans] == ["day"]
    assert [f.rel for f in no_table] == ["in/p001_unknown.csv"]


def test_a_missing_record_makes_the_table_stale(tmp_path: Path) -> None:
    spec = _project(tmp_path)
    [plan] = plan_tables("meals", spec, {}, root=tmp_path)
    build_table(plan, spec, root=tmp_path)

    class Store:
        def read_hash(self, *_: str) -> str:
            return state_value(plan)

    assert is_current(plan, Store(), "k", "p")
    plan.rejects.unlink()
    assert not is_current(plan, Store(), "k", "p")


def test_remove_stale_tables_keeps_only_named_records(tmp_path: Path) -> None:
    for name in ("meals", "gone", NO_TABLE):
        write_rejects([], rejects_path(tmp_path, "s", name))
    removed = remove_stale_tables("s", {"meals"}, root=tmp_path, rejects={"meals", NO_TABLE})
    assert removed == []
    left = sorted(p.stem for p in (tmp_path / ".kptn/_bronze/s/_rejected").iterdir())
    assert left == [NO_TABLE, "meals"]


def test_register_views_serves_the_records_and_drops_the_view_without_them(tmp_path: Path) -> None:
    bronze = tmp_path / ".kptn/_bronze"
    (bronze / "s").mkdir(parents=True)
    duckdb.connect().execute(f"COPY (SELECT 1 AS x) TO '{bronze}/s/meals.parquet' (FORMAT parquet)")
    write_rejects([RejectedFile("meals", "meals/p002.csv", "bad")], rejects_path(tmp_path, "s", "meals"))
    write_rejects([RejectedFile(None, "meals/odd.csv", "none")], rejects_path(tmp_path, "s", NO_TABLE))
    conn = duckdb.connect()
    apply_search_path(conn, bronze)

    assert register_views(conn, bronze, "s") == ['raw_s."meals"', 'raw_s."_rejected"']
    assert conn.execute("SELECT * FROM raw_s._rejected ORDER BY ALL").fetchall() == [
        ("meals", "meals/p002.csv", "bad"),
        (None, "meals/odd.csv", "none"),
    ]

    for p in (bronze / "s/_rejected").iterdir():
        p.unlink()
    assert register_views(conn, bronze, "s") == ['raw_s."meals"']
    views = conn.execute("SELECT view_name FROM duckdb_views() WHERE schema_name = 'raw_s'").fetchall()
    assert views == [("meals",)]


def test_load_lists_rejected_files(tmp_path: Path) -> None:
    _project(tmp_path)
    _write(tmp_path, "in/meals/notes.txt", "x\n1\n")
    source = kptn.source(
        "meals",
        files=str(tmp_path / "in/meals"),
        table=lambda f: "meals" if f.path.suffix == ".csv" else None,
    )
    conn = duckdb.connect()
    created = load(conn, source, root=tmp_path, checks=True)
    assert created == ['raw_meals."meals"', 'raw_meals."_rejected"']
    assert conn.execute("SELECT count(*) FROM raw_meals.meals").fetchall() == [(2,)]
    assert conn.execute(
        "SELECT table_name, _source_file FROM raw_meals._rejected ORDER BY _source_file"
    ).fetchall() == [
        (None, "meals/notes.txt"),
        ("meals", "meals/p002.csv"),
        ("meals", "meals/p003.csv"),
    ]


# -- through kptn.run ---------------------------------------------------------


class Recorder:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)

    def finished(self, name: str):
        [e] = [e for e in self.events if e.kind is EventKind.TASK_FINISHED and e.task_name == name]
        return e.payload


def _pipeline(
    root: Path, settings: str = ""
) -> tuple[kptn.Pipeline, list[list[tuple]], kptn.SourceHandle]:
    (root / "kptn.yaml").write_text(f"settings:\n  db: duckdb\n{settings}")
    seen: list[list[tuple]] = []

    def get_engine():
        return duckdb.connect(str(root / "pipeline.db"))

    @kptn.task(outputs=[])
    def consumer(engine) -> None:
        seen.append(engine.sql("SELECT _source_file FROM raw_meals._rejected ORDER BY ALL").fetchall())

    meals = kptn.source(
        "meals",
        files="in/meals",
        table=lambda f: "meals" if f.path.suffix == ".csv" else None,
    )
    pipeline = kptn.Pipeline(
        "p", kptn.config(duckdb=(get_engine, "engine")) >> kptn.Stage("raw", meals) >> consumer
    )
    return pipeline, seen, meals


def test_run_warns_records_and_serves_rejected_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _project(tmp_path)
    _write(tmp_path, "in/meals/notes.txt", "x\n1\n")
    pipeline, seen, _ = _pipeline(tmp_path)

    rec = Recorder()
    with pytest.warns(UserWarning) as caught:
        kptn.run(pipeline, event_sink=rec)
    messages = [str(w.message) for w in caught]
    assert any("skipped 2 of 3 files; see raw_meals._rejected" in m for m in messages)
    assert any("1 file(s) feed no table, such as meals/notes.txt" in m for m in messages)
    assert rec.finished("meals[meals]")["rejected"] == 2
    assert seen[-1] == [("meals/notes.txt",), ("meals/p002.csv",), ("meals/p003.csv",)]

    # Fixing a file rebuilds its table, and the record forgets it.
    _write(tmp_path, "in/meals/p002.csv", "subject,kcal\np002,2100\n")
    rec = Recorder()
    with pytest.warns(UserWarning):
        kptn.run(pipeline, event_sink=rec)
    assert rec.finished("meals[meals]")["rejected"] == 1
    assert seen[-1] == [("meals/notes.txt",), ("meals/p003.csv",)]


def test_shared_bronze_carries_the_record(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv(SHARED_BRONZE_ENV, raising=False)
    cache = tmp_path / "_kptn_bronze"
    cache.mkdir()
    project = tmp_path / "alice"
    project.mkdir()
    monkeypatch.chdir(project)
    _project(project)
    pipeline, seen, meals = _pipeline(project, "  shared_bronze: ../_kptn_bronze\n")

    rec = Recorder()
    with pytest.warns(UserWarning):
        kptn.run(pipeline, event_sink=rec)
    assert rec.finished("meals[meals]")["shared"] == "published"
    [plan] = plan_tables("meals", meals.__kptn__, {}, root=project)
    entry = SharedBronze(cache.resolve()).entry(plan)
    assert json.loads((entry / ENTRY_LABEL).read_text())["rejected"] == [
        "meals/p002.csv",
        "meals/p003.csv",
    ]
    assert plan.rejects.is_symlink()

    # A fresh working directory restores the table and its record.
    plan.rejects.unlink()
    plan.parquet.unlink()
    rec = Recorder()
    with pytest.warns(UserWarning):
        kptn.run(pipeline, event_sink=rec)
    assert rec.finished("meals[meals]")["shared"] == "restored"
    assert seen[-1] == [("meals/p002.csv",), ("meals/p003.csv",)]

