from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

from kptn.exceptions import TaskError
from kptn.sources.checks import check_path, run_checks
from kptn.sources.files import SourceFile
from kptn.sources.read import create_table_view


def _conn_with_view() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA raw_sales")
    conn.execute("CREATE VIEW raw_sales.orders AS SELECT '1' AS orderid, '200' AS amount")
    return conn


def _write_check(root: Path, body: str) -> Path:
    path = check_path(root, "sales", "orders")
    path.parent.mkdir(parents=True)
    path.write_text(body)
    return path


def test_check_path_convention(tmp_path: Path) -> None:
    assert check_path(tmp_path, "sales", "orders") == tmp_path / "checks" / "sales" / "orders.yml"


def test_missing_check_file_warns_and_returns_false(tmp_path: Path) -> None:
    with pytest.warns(UserWarning, match=r"checks/sales/orders\.yml"):
        assert run_checks(_conn_with_view(), source="sales", table="orders", root=tmp_path) is False


def test_check_file_without_soda_is_an_error(tmp_path: Path, monkeypatch) -> None:
    _write_check(tmp_path, "checks for orders:\n- row_count > 0\n")
    monkeypatch.setitem(sys.modules, "soda", None)
    monkeypatch.setitem(sys.modules, "soda.scan", None)
    with pytest.raises(TaskError, match=r"kptn\[soda\]"):
        run_checks(_conn_with_view(), source="sales", table="orders", root=tmp_path)


def test_passing_checks_return_true(tmp_path: Path) -> None:
    pytest.importorskip("soda.scan")
    _write_check(
        tmp_path,
        "checks for orders:\n"
        "- schema:\n"
        "    fail:\n"
        "      when required column missing: [orderid, amount]\n",
    )
    assert run_checks(_conn_with_view(), source="sales", table="orders", root=tmp_path) is True


def test_checks_name_columns_in_lowercase_whatever_the_header(tmp_path: Path) -> None:
    pytest.importorskip("soda.scan")
    data = tmp_path / "data" / "orders.csv"
    data.parent.mkdir()
    data.write_text("OrderID,AMOUNT\n1,200\n")
    conn = duckdb.connect()
    create_table_view(conn, "sales", "orders", [SourceFile(data, data.parent)])
    _write_check(
        tmp_path,
        "checks for orders:\n"
        "- schema:\n"
        "    fail:\n"
        "      when required column missing: [orderid, amount]\n"
        "- missing_count(orderid) = 0\n",
    )
    assert run_checks(conn, source="sales", table="orders", root=tmp_path) is True


def test_failing_checks_raise(tmp_path: Path) -> None:
    pytest.importorskip("soda.scan")
    _write_check(
        tmp_path,
        "checks for orders:\n"
        "- schema:\n"
        "    fail:\n"
        "      when required column missing: [orderid, not_there]\n",
    )
    with pytest.raises(TaskError, match="sales.orders"):
        run_checks(_conn_with_view(), source="sales", table="orders", root=tmp_path)


def test_missing_check_warning_points_at_the_declaration(tmp_path: Path) -> None:
    with pytest.warns(UserWarning) as record:
        run_checks(
            _conn_with_view(),
            source="sales",
            table="orders",
            root=tmp_path,
            declared_at=("/project/pipeline.py", 31),
        )
    assert (record[0].filename, record[0].lineno) == ("/project/pipeline.py", 31)
