from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

from kptn.exceptions import TaskError
from kptn.sources.checks import check_path, run_checks


def _conn_with_view() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE VIEW raw.asa24_foodtotals AS SELECT '1' AS recallid, '200' AS kcal")
    return conn


def _write_check(root: Path, body: str) -> Path:
    path = check_path(root, "asa24", "foodtotals")
    path.parent.mkdir(parents=True)
    path.write_text(body)
    return path


def test_check_path_convention(tmp_path: Path) -> None:
    assert check_path(tmp_path, "asa24", "foodtotals") == tmp_path / "checks" / "asa24" / "foodtotals.yml"


def test_missing_check_file_warns_and_returns_false(tmp_path: Path) -> None:
    with pytest.warns(UserWarning, match=r"checks/asa24/foodtotals\.yml"):
        assert run_checks(_conn_with_view(), source="asa24", table="foodtotals", root=tmp_path) is False


def test_check_file_without_soda_is_an_error(tmp_path: Path, monkeypatch) -> None:
    _write_check(tmp_path, "checks for asa24_foodtotals:\n- row_count > 0\n")
    monkeypatch.setitem(sys.modules, "soda", None)
    monkeypatch.setitem(sys.modules, "soda.scan", None)
    with pytest.raises(TaskError, match=r"kptn\[soda\]"):
        run_checks(_conn_with_view(), source="asa24", table="foodtotals", root=tmp_path)


def test_passing_checks_return_true(tmp_path: Path) -> None:
    pytest.importorskip("soda.scan")
    _write_check(
        tmp_path,
        "checks for asa24_foodtotals:\n"
        "- schema:\n"
        "    fail:\n"
        "      when required column missing: [recallid, kcal]\n",
    )
    assert run_checks(_conn_with_view(), source="asa24", table="foodtotals", root=tmp_path) is True


def test_failing_checks_raise(tmp_path: Path) -> None:
    pytest.importorskip("soda.scan")
    _write_check(
        tmp_path,
        "checks for asa24_foodtotals:\n"
        "- schema:\n"
        "    fail:\n"
        "      when required column missing: [recallid, not_there]\n",
    )
    with pytest.raises(TaskError, match="asa24.foodtotals"):
        run_checks(_conn_with_view(), source="asa24", table="foodtotals", root=tmp_path)


def test_missing_check_warning_points_at_the_declaration(tmp_path: Path) -> None:
    with pytest.warns(UserWarning) as record:
        run_checks(
            _conn_with_view(),
            source="asa24",
            table="foodtotals",
            root=tmp_path,
            declared_at=("/project/pipeline.py", 31),
        )
    assert (record[0].filename, record[0].lineno) == ("/project/pipeline.py", 31)
