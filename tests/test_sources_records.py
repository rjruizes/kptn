"""The records a checked table keeps beside it: the checks that warned
(``raw_<source>._warnings``) and each file's columns (``raw_<source>._columns``)."""

from __future__ import annotations

import shutil
import warnings
from pathlib import Path

import duckdb
import pytest

import kptn
from kptn.exceptions import TaskError
from kptn.sources import load
from kptn.sources.build import build_table, is_current, plan_tables, remove_stale_tables, state_value
from kptn.sources.checks import ChecksFailed, check_path, run_checks
from kptn.sources.records import CheckWarning
from kptn.sources.shared import SHARED_BRONZE_ENV, SharedBronze
from kptn.sources.views import register_views
from tests.fakes import FakeStateStore

pytest.importorskip("soda.scan")

WARNS = "checks for orders:\n- row_count:\n    name: has rows\n    warn: when > 0\n"
FAILS_TOO = WARNS + "- schema:\n    fail:\n      when required column missing: [not_there]\n"
PASSES = "checks for orders:\n- row_count > 0\n"
NEW_COLUMNS = (
    "checks for orders:\n"
    "- group by:\n"
    "    name: columns nobody listed\n"
    "    query: |\n"
    "      SELECT column_name, 1 AS new_column FROM raw_sales._columns\n"
    "      WHERE column_name NOT IN ('orderid', 'amount')\n"
    "      GROUP BY column_name\n"
    "    fields:\n"
    "    - column_name\n"
    "    checks:\n"
    "    - new_column:\n"
    "        name: is new\n"
    "        warn: when = 1\n"
)


def _project(root: Path) -> Path:
    for rel, text in {
        "in/m1/orders_2024-01-24.csv": "OrderID,Amount\n1,200\n",
        "in/m2/orders_2024-02-01.csv": "orderid,amount,Tax\n2,300,10\n",
        "in/m1/refunds_2024-01-24.csv": "orderid,x\n1,a\n",
    }.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return root


def _source(**kwargs):
    return kptn.source("sales", files=_files, table=lambda f: f.name.split("_")[0], **kwargs)


def _files(base: str) -> list[str]:
    return [f"{base}/m1", f"{base}/m2"]


def _check(root: Path, body: str, table: str = "orders") -> None:
    path = check_path(root, "sales", table)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


def _plans(root: Path):
    plans = plan_tables("sales", _source().__kptn__, {"base": str(root / "in")}, root=root)
    return {p.table: p for p in plans}


def _rows(path: Path) -> list[tuple]:
    return duckdb.sql(f"SELECT * FROM '{path}' ORDER BY ALL").fetchall()


def _view_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA raw_sales")
    conn.execute("CREATE VIEW raw_sales.orders AS SELECT '1' AS orderid")
    return conn


# -- run_checks --------------------------------------------------------------


def test_run_checks_returns_the_checks_that_warned(tmp_path: Path) -> None:
    _check(tmp_path, WARNS)
    warned = run_checks(_view_conn(), source="sales", table="orders", root=tmp_path)
    assert warned == [CheckWarning("has rows")]


def test_failed_checks_carry_the_checks_that_warned(tmp_path: Path) -> None:
    _check(tmp_path, FAILS_TOO)
    with pytest.raises(ChecksFailed, match=r"sales\.orders") as failure:
        run_checks(_view_conn(), source="sales", table="orders", root=tmp_path)
    assert failure.value.warnings == [CheckWarning("has rows")]
    assert isinstance(failure.value, TaskError)


# -- build_table -------------------------------------------------------------


def test_checked_table_keeps_its_warnings_and_columns(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, WARNS)
    plan = _plans(root)["orders"]
    build_table(plan, _source().__kptn__, root=root)
    bronze = root / ".kptn" / "_bronze" / "sales"
    assert plan.warnings == bronze / "_warnings" / "orders.parquet"
    assert plan.columns == bronze / "_columns" / "orders.parquet"
    assert _rows(plan.warnings) == [("orders", "has rows")]
    assert _rows(plan.columns) == [
        ("orders", "m1/orders_2024-01-24.csv", "amount", "Amount"),
        ("orders", "m1/orders_2024-01-24.csv", "orderid", "OrderID"),
        ("orders", "m2/orders_2024-02-01.csv", "amount", "amount"),
        ("orders", "m2/orders_2024-02-01.csv", "orderid", "orderid"),
        ("orders", "m2/orders_2024-02-01.csv", "tax", "Tax"),
    ]


def test_a_table_whose_checks_all_pass_keeps_an_empty_warnings_record(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, PASSES)
    plan = _plans(root)["orders"]
    build_table(plan, _source().__kptn__, root=root)
    assert _rows(plan.warnings) == []


def test_unchecked_table_keeps_no_records(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, WARNS)
    plan = _plans(root)["refunds"]
    assert (plan.warnings, plan.columns) == (None, None)
    with pytest.warns(UserWarning):  # no check file
        build_table(plan, _source().__kptn__, root=root)
    assert sorted(p.name for p in plan.parquet.parent.iterdir()) == ["refunds.parquet"]


def test_a_check_file_can_ask_which_columns_the_files_have(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, NEW_COLUMNS)
    plan = _plans(root)["orders"]
    build_table(plan, _source().__kptn__, root=root)
    [(table, check)] = _rows(plan.warnings)
    assert table == "orders" and check.startswith("is new") and "tax" in check


def test_columns_leave_out_files_the_file_check_rejected(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, PASSES)
    spec = _source(check=lambda file: "no tax" if "m1" in file.rel else None).__kptn__
    [plan] = [p for p in plan_tables("sales", spec, {"base": str(root / "in")}, root=root) if p.table == "orders"]
    build_table(plan, spec, root=root)
    assert {r[1] for r in _rows(plan.columns)} == {"m2/orders_2024-02-01.csv"}


def test_failed_check_keeps_the_previous_records(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, WARNS)
    build_table(_plans(root)["orders"], _source().__kptn__, root=root)
    _check(root, FAILS_TOO.replace("has rows", "renamed"))
    plan = _plans(root)["orders"]
    with pytest.raises(ChecksFailed):
        build_table(plan, _source().__kptn__, root=root)
    assert _rows(plan.warnings) == [("orders", "has rows")]
    assert sorted(p.name for p in plan.warnings.parent.iterdir()) == ["orders.parquet"]


def test_a_table_is_not_current_without_its_records(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, WARNS)
    plan = _plans(root)["orders"]
    build_table(plan, _source().__kptn__, root=root)
    store = FakeStateStore()
    store.write_hash("k", "p", plan.state_name, state_value(plan))
    assert is_current(plan, store, "k", "p")
    plan.columns.unlink()
    assert not is_current(plan, store, "k", "p")


def test_adding_a_check_file_changes_only_that_tables_key(tmp_path: Path) -> None:
    root = _project(tmp_path)
    before = {t: p.key for t, p in _plans(root).items()}
    _check(root, WARNS)
    after = {t: p.key for t, p in _plans(root).items()}
    assert after["orders"] != before["orders"] and after["refunds"] == before["refunds"]


def test_records_of_tables_no_longer_fed_are_removed(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, WARNS)
    plan = _plans(root)["orders"]
    build_table(plan, _source().__kptn__, root=root)
    remove_stale_tables("sales", {"refunds"}, root=root)
    assert not plan.warnings.exists() and not plan.columns.exists()


# -- views -------------------------------------------------------------------


def test_register_views_serves_the_records(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, WARNS)
    build_table(_plans(root)["orders"], _source().__kptn__, root=root)
    conn = duckdb.connect()
    bronze = root / ".kptn" / "_bronze"
    conn.execute(f"SET file_search_path = '{bronze}'")
    created = register_views(conn, bronze, "sales")
    assert {"raw_sales._warnings", "raw_sales._columns"} <= {c.replace('"', "") for c in created}
    assert conn.sql("SELECT * FROM raw_sales._warnings").fetchall() == [("orders", "has rows")]
    assert conn.sql("SELECT count(*) FROM raw_sales._columns").fetchall() == [(5,)]


# -- load --------------------------------------------------------------------


def test_load_lists_columns_and_with_checks_the_warnings(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, WARNS)
    conn = duckdb.connect()
    with pytest.warns(UserWarning):  # refunds has no check file
        load(conn, _source(), {"base": str(root / "in")}, root=root, soda_checks=True)
    assert conn.sql("SELECT * FROM raw_sales._warnings").fetchall() == [("orders", "has rows")]
    assert conn.sql("SELECT DISTINCT table_name FROM raw_sales._columns").fetchall() == [("orders",)]


def test_load_without_checks_has_no_warnings_table(tmp_path: Path) -> None:
    root = _project(tmp_path)
    _check(root, WARNS)
    conn = duckdb.connect()
    created = load(conn, _source(), {"base": str(root / "in")}, root=root)
    assert "raw_sales._warnings" not in [c.replace('"', "") for c in created]
    assert conn.sql("SELECT count(*) FROM raw_sales._columns").fetchall() == [(5,)]


# -- shared bronze -----------------------------------------------------------


def test_published_entry_holds_the_records(tmp_path: Path) -> None:
    root = _project(tmp_path / "w")
    _check(root, WARNS)
    (tmp_path / "cache").mkdir()
    cache = SharedBronze(tmp_path / "cache")
    plan = _plans(root)["orders"]
    cache.publish(plan, _source().__kptn__, root=root)
    assert _rows(cache.warnings(plan)) == [("orders", "has rows")]
    assert len(_rows(cache.columns(plan))) == 5
    assert cache.lookup(plan) is not None
    cache.columns(plan).unlink()
    assert cache.lookup(plan) is None


# -- a run -------------------------------------------------------------------


def _pipeline(root: Path) -> tuple[kptn.Pipeline, list]:
    seen: list = []

    def get_engine():
        return duckdb.connect(str(root / "pipeline.db"))

    @kptn.task(outputs=[])
    def consumer(engine) -> None:
        seen.append(engine.sql("SELECT check_name FROM raw_sales._warnings").fetchall())
        seen.append(engine.sql("SELECT count(*) FROM raw_sales._columns").fetchall())

    sales = kptn.source("sales", files=["in/m1", "in/m2"], table=lambda f: f.name.split("_")[0])
    return kptn.Pipeline("p", kptn.config(duckdb=(get_engine, "engine")) >> sales >> consumer), seen


def test_a_run_serves_the_records_to_the_tasks_after_the_source(tmp_path: Path, monkeypatch) -> None:
    root = _project(tmp_path)
    (root / "kptn.yaml").write_text("settings:\n  db: duckdb\n")
    _check(root, WARNS)
    monkeypatch.chdir(root)
    pipeline, seen = _pipeline(root)
    with pytest.warns(UserWarning):  # refunds has no check file
        kptn.run(pipeline)
    assert seen == [[("has rows",)], [(5,)]]


def test_a_failed_table_still_records_what_warned(tmp_path: Path, monkeypatch) -> None:
    root = _project(tmp_path)
    (root / "kptn.yaml").write_text("settings:\n  db: duckdb\n")
    _check(root, FAILS_TOO)
    monkeypatch.chdir(root)
    pipeline, _ = _pipeline(root)
    with pytest.raises(TaskError, match="orders"):
        kptn.run(pipeline)
    bronze = root / ".kptn" / "_bronze" / "sales"
    assert _rows(bronze / "_warnings" / "orders.parquet") == [("orders", "has rows")]
    assert not (bronze / "orders.parquet").exists()


def test_a_second_working_directory_restores_the_records(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv(SHARED_BRONZE_ENV, raising=False)
    alice = _project(tmp_path / "r6" / "alice")
    (alice / "kptn.yaml").write_text("settings:\n  db: duckdb\n  shared_bronze: ../_kptn_bronze\n")
    _check(alice, WARNS)
    bob = tmp_path / "r6" / "bob"
    shutil.copytree(alice, bob)  # copy2 keeps sizes and modification times
    (tmp_path / "r6" / "_kptn_bronze").mkdir()
    for home in (alice, bob):
        monkeypatch.chdir(home)
        pipeline, seen = _pipeline(home)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # refunds has no check file, where it is built
            kptn.run(pipeline)
        assert seen == [[("has rows",)], [(5,)]]
    assert (bob / ".kptn" / "_bronze" / "sales" / "_warnings" / "orders.parquet").is_symlink()
