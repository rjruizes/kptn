from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from kptn.sources.files import SourceFile
import kptn.sources.read as read_module
from kptn.sources.read import create_table_view, union_view


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
    assert view == 'raw_sales."orders"'
    rel = conn.sql(f"SELECT * FROM {view}")
    assert rel.columns == ["orderid", "amount", "_source_file"]
    assert {str(t) for t in rel.types} == {"VARCHAR"}
    assert rel.fetchall() == [("1", "200", "Batch1/orders_2024-01-24.csv")]


def test_union_by_name_lowercases_and_fills_missing(tmp_path: Path) -> None:
    a = _file(tmp_path, "m1/t.csv", "OrderID,Amount\n1,200\n")
    b = _file(tmp_path, "m2/t.csv", "orderid,amount,Tax\n2,300,10\n")
    conn = duckdb.connect()
    view = create_table_view(conn, "s", "t", [b, a])  # order given does not matter
    rel = conn.sql(f"SELECT * FROM {view} ORDER BY _source_file")
    assert rel.columns == ["orderid", "amount", "_source_file", "tax"]
    assert rel.fetchall() == [("1", "200", "m1/t.csv", None), ("2", "300", "m2/t.csv", "10")]


@pytest.mark.parametrize("first", ["m1", "m2"])
def test_spellings_of_a_column_feed_one_lowercase_column(tmp_path: Path, first: str) -> None:
    second = "m2" if first == "m1" else "m1"
    a = _file(tmp_path, f"{first}/t.csv", "id,mealTime\n1,08:00\n")
    b = _file(tmp_path, f"{second}/t.csv", "id,MEALTIME\n2,12:30\n")
    conn = duckdb.connect()
    rel = conn.sql(f"SELECT * FROM {create_table_view(conn, 's', 't', [a, b])} ORDER BY id")
    assert rel.columns == ["id", "mealtime", "_source_file"]
    assert rel.fetchall() == [("1", "08:00", f"{first}/t.csv"), ("2", "12:30", f"{second}/t.csv")]


def test_names_differing_only_by_case_in_one_file_stay_apart(tmp_path: Path) -> None:
    f = _file(tmp_path, "d/x.csv", "Foo,foo\n1,2\n")
    conn = duckdb.connect()
    rel = conn.sql(f"SELECT * FROM {create_table_view(conn, 's', 'x', [f])}")
    assert rel.columns == ["foo", "foo_1", "_source_file"]
    assert rel.fetchall() == [("1", "2", "d/x.csv")]


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


def test_tables_on_one_connection_keep_their_own_files(tmp_path: Path) -> None:
    a = _file(tmp_path, "d/a.csv", "x\n1\n")
    b = _file(tmp_path, "d/b.csv", "y\n2\n")
    conn = duckdb.connect()
    first = create_table_view(conn, "s", "a", [a])
    create_table_view(conn, "s", "b", [b])
    assert _rows(conn, first) == [("1", "d/a.csv")]


def test_a_union_past_duckdbs_expression_depth(tmp_path: Path) -> None:
    # One chain of 1000 unions is nested past DuckDB's default depth limit.
    selects = [f"SELECT '{i}' AS n" for i in range(1200)]
    conn = duckdb.connect()
    view = union_view(conn, "s", "t", selects)
    assert conn.sql(f"SELECT count(*), count(DISTINCT n) FROM {view}").fetchone() == (1200, 1200)


@pytest.mark.parametrize("width", [2, 3])
def test_grouped_unions_match_one_union(tmp_path: Path, monkeypatch, width: int) -> None:
    # Rows in file order, too: views or subqueries in place of the group
    # tables scramble it.
    files = [
        _file(tmp_path, f"d/{i}.csv", f"id,{'abcdefg'[i % 7]}\n" + f"{i},v{i}\n" * 50)
        for i in range(10)
    ]
    conn = duckdb.connect()
    whole = conn.sql(f"SELECT * FROM {create_table_view(conn, 's', 'whole', files)}")
    monkeypatch.setattr(read_module, "_UNION_WIDTH", width)  # 10 files nest 2 or 3 deep
    grouped = conn.sql(f"SELECT * FROM {create_table_view(conn, 's', 'grouped', files)}")
    assert grouped.columns == whole.columns
    assert grouped.fetchall() == whole.fetchall()
