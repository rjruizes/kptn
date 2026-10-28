from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import kptn
from kptn.exceptions import HashError, TaskError
from kptn.sources import load
from kptn.sources.build import build_table, plan_tables
from kptn.sources.checks import check_path


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


def _files(base: str) -> list[str]:
    return [f"{base}/m1", f"{base}/m2"]


def _source(**kwargs):
    return kptn.source("sales", files=_files, table=lambda f: f.name.split("_")[0], **kwargs)


def _rows(conn: duckdb.DuckDBPyConnection, name: str) -> list[tuple]:
    return conn.sql(f"SELECT * FROM {name} ORDER BY _source_file").fetchall()


def _write_check(root: Path, table: str, body: str) -> None:
    path = check_path(root, "sales", table)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


def test_loads_every_table_as_a_run_builds_it(tmp_path: Path) -> None:
    root = _project(tmp_path)
    source = _source()
    conn = duckdb.connect()
    with pytest.warns(UserWarning):  # building runs checks, and there are none
        for plan in plan_tables("sales", source.__kptn__, {"base": str(root / "in")}, root=root):
            build_table(plan, source.__kptn__, root=root)

    created = load(conn, source, {"base": str(root / "in")}, root=root)

    assert created == ['raw_sales."orders"', 'raw_sales."refunds"']
    for name, table in zip(created, ["orders", "refunds"]):
        built = f"read_parquet('{root / '.kptn' / '_bronze' / 'sales' / table}.parquet')"
        assert conn.sql(f"SELECT * FROM {name}").columns == conn.sql(f"SELECT * FROM {built}").columns
        assert _rows(conn, name) == _rows(conn, built)
    assert _rows(conn, created[0]) == [
        ("1", "200", "m1/orders_2024-01-24.csv", None),
        ("2", "300", "m2/orders_2024-02-01.csv", "10"),
    ]


def test_writes_nothing_to_disk_and_leaves_only_tables(tmp_path: Path) -> None:
    root = _project(tmp_path)
    conn = duckdb.connect()
    load(conn, _source(), {"base": str(root / "in")}, root=root)

    assert not (root / ".kptn").exists()
    assert conn.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() ORDER BY table_name"
    ).fetchall() == [("raw_sales", "orders"), ("raw_sales", "refunds")]
    assert conn.execute("SELECT count(*) FROM duckdb_views() WHERE NOT internal").fetchall() == [(0,)]
    assert [r[0] for r in conn.execute("SELECT database_name FROM duckdb_databases()").fetchall()] == [
        "memory", "system", "temp"
    ]


def test_tables_go_to_the_current_database_and_its_schema_stays_current(tmp_path: Path) -> None:
    root = _project(tmp_path)
    conn = duckdb.connect(str(tmp_path / "x.db"))
    conn.execute("CREATE SCHEMA work")
    conn.execute("USE x.work")
    load(conn, _source(), {"base": str(root / "in")}, root=root)

    assert conn.execute("SELECT current_database(), current_schema()").fetchall() == [("x", "work")]
    assert conn.execute("SELECT count(*) FROM x.raw_sales.orders").fetchall() == [(2,)]


def test_loading_again_replaces_the_tables(tmp_path: Path) -> None:
    root = _project(tmp_path)
    conn = duckdb.connect()
    load(conn, _source(), {"base": str(root / "in")}, root=root)
    (root / "in" / "m2" / "orders_2024-02-01.csv").write_text("orderid\n9\n")
    load(conn, _source(), {"base": str(root / "in")}, root=root)

    assert _rows(conn, "raw_sales.orders") == [
        ("1", "200", "m1/orders_2024-01-24.csv"),
        ("9", None, "m2/orders_2024-02-01.csv"),
    ]


def test_uses_the_sources_reader(tmp_path: Path) -> None:
    root = _project(tmp_path)
    source = _source(read=lambda path: duckdb.sql("SELECT 7 AS n"))
    conn = duckdb.connect()
    load(conn, source, {"base": str(root / "in")}, root=root)

    assert _rows(conn, "raw_sales.refunds") == [("7", "m1/refunds_2024-01-24.csv")]


def test_unmatched_files_raise(tmp_path: Path) -> None:
    with pytest.raises(HashError):
        load(duckdb.connect(), _source(), {"base": str(tmp_path / "missing")}, root=tmp_path)


def test_checks_run_only_when_asked(tmp_path: Path) -> None:
    pytest.importorskip("soda.scan")
    root = _project(tmp_path)
    _write_check(root, "orders", "checks for orders:\n- row_count = 0\n")
    conn = duckdb.connect()
    load(conn, _source(), {"base": str(root / "in")}, root=root)

    with pytest.raises(TaskError, match=r"sales\.orders"):
        load(conn, _source(), {"base": str(root / "in")}, root=root, checks=True)
    # A failure leaves the connection as it was found.
    assert conn.execute("SELECT current_database(), current_schema()").fetchall() == [("memory", "main")]
    assert "_kptn_load" not in [r[0] for r in conn.execute("SELECT database_name FROM duckdb_databases()").fetchall()]


def test_passing_checks_see_only_the_table_being_read(tmp_path: Path) -> None:
    pytest.importorskip("soda.scan")
    root = _project(tmp_path)
    for table in ("orders", "refunds"):
        _write_check(
            root,
            table,
            f"checks for {table}:\n- schema:\n    fail:\n      when required column missing: [orderid]\n",
        )
    conn = duckdb.connect()
    # Twice, so the second read happens with the tables already on conn.
    for _ in range(2):
        load(conn, _source(), {"base": str(root / "in")}, root=root, checks=True)

    assert conn.execute("SELECT count(*) FROM raw_sales.orders").fetchall() == [(2,)]
