from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from kptn.sources.files import SourceFile
from kptn.sources.read import create_table_view


def _file(root: Path, rel: str, text: str) -> SourceFile:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    return SourceFile(p, p.parent)


def _rows(conn: duckdb.DuckDBPyConnection, view: str) -> list[tuple]:
    return conn.sql(f"SELECT * FROM {view}").fetchall()


def test_csv_values_are_text_with_source_file(tmp_path: Path) -> None:
    f = _file(tmp_path, "Batch1/orders_2024-01-24.csv", "OrderID,Amount\n1,200\n")
    conn = duckdb.connect()
    view = create_table_view(conn, "sales", "orders", [f])
    assert view == 'raw."sales_orders"'
    rel = conn.sql(f"SELECT * FROM {view}")
    assert rel.columns == ["OrderID", "Amount", "_source_file"]
    assert {str(t) for t in rel.types} == {"VARCHAR"}
    assert rel.fetchall() == [("1", "200", "Batch1/orders_2024-01-24.csv")]


def test_union_by_name_is_case_insensitive_and_fills_missing(tmp_path: Path) -> None:
    a = _file(tmp_path, "m1/t.csv", "OrderID,Amount\n1,200\n")
    b = _file(tmp_path, "m2/t.csv", "orderid,amount,Tax\n2,300,10\n")
    conn = duckdb.connect()
    view = create_table_view(conn, "s", "t", [b, a])  # order given does not matter
    rel = conn.sql(f"SELECT * FROM {view} ORDER BY _source_file")
    assert rel.columns == ["OrderID", "Amount", "_source_file", "Tax"]
    assert rel.fetchall() == [("1", "200", "m1/t.csv", None), ("2", "300", "m2/t.csv", "10")]


def test_tsv(tmp_path: Path) -> None:
    f = _file(tmp_path, "d/x.tsv", "a\tb\n1\t2\n")
    conn = duckdb.connect()
    assert _rows(conn, create_table_view(conn, "s", "x", [f])) == [("1", "2", "d/x.tsv")]


def test_json_nested_values_become_json_text(tmp_path: Path) -> None:
    f = _file(tmp_path, "d/x.json", json.dumps([{"a": 1, "b": {"c": 2}, "l": [1, 2]}]))
    conn = duckdb.connect()
    [(a, b, lst, src)] = _rows(conn, create_table_view(conn, "s", "x", [f]))
    assert a == "1"
    assert json.loads(b) == {"c": 2}
    assert json.loads(lst) == [1, 2]
    assert src == "d/x.json"


def test_parquet_typed_columns_become_text(tmp_path: Path) -> None:
    p = tmp_path / "d" / "x.parquet"
    p.parent.mkdir()
    duckdb.sql(f"COPY (SELECT 42 AS n, DATE '2024-01-24' AS d) TO '{p}' (FORMAT parquet)")
    conn = duckdb.connect()
    assert _rows(conn, create_table_view(conn, "s", "x", [SourceFile(p, p.parent)])) == [
        ("42", "2024-01-24", "d/x.parquet")
    ]


def test_read_callable_dataframe_is_cast_to_text(tmp_path: Path) -> None:
    p = tmp_path / "d" / "x.xlsx"
    p.parent.mkdir()
    p.write_bytes(b"not really excel")
    conn = duckdb.connect()
    view = create_table_view(
        conn, "s", "x", [SourceFile(p, p.parent)], read=lambda path: pd.DataFrame({"n": [1, 2]})
    )
    assert _rows(conn, view) == [("1", "d/x.xlsx"), ("2", "d/x.xlsx")]


def test_read_callable_relation_from_another_connection(tmp_path: Path) -> None:
    p = tmp_path / "d" / "x.dat"
    p.parent.mkdir()
    p.write_text("")
    other = duckdb.connect()
    conn = duckdb.connect()
    view = create_table_view(
        conn, "s", "x", [SourceFile(p, p.parent)], read=lambda path: other.sql("SELECT 7 AS n")
    )
    assert _rows(conn, view) == [("7", "d/x.dat")]


def test_existing_source_file_column_is_an_error(tmp_path: Path) -> None:
    f = _file(tmp_path, "d/x.csv", "_source_file,a\nq,1\n")
    with pytest.raises(ValueError, match="_source_file"):
        create_table_view(duckdb.connect(), "s", "x", [f])
