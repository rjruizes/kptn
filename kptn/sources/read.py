"""Reading a table's files by convention: text values, original names, provenance."""

from __future__ import annotations

from typing import Any, Callable

import duckdb

from kptn.sources.files import SourceFile

SOURCE_FILE_COLUMN = "_source_file"
RAW_SCHEMA = "raw"

_NESTED_TYPE_IDS = frozenset({"struct", "list", "map", "array", "union"})


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def view_name(source: str, table: str) -> str:
    """The qualified view name for a source table: ``raw."<source>_<table>"``."""
    return f"{RAW_SCHEMA}.{_ident(f'{source}_{table}')}"


def _read_builtin(conn: duckdb.DuckDBPyConnection, file: SourceFile) -> duckdb.DuckDBPyRelation:
    path = _sql_str(str(file.path))
    suffix = file.path.suffix.lower()
    if suffix in (".csv", ".txt"):
        return conn.sql(f"SELECT * FROM read_csv({path}, all_varchar = true)")
    if suffix == ".tsv":
        return conn.sql(f"SELECT * FROM read_csv({path}, all_varchar = true, delim = '\t')")
    if suffix in (".json", ".jsonl", ".ndjson"):
        return conn.sql(f"SELECT * FROM read_json({path})")
    if suffix == ".parquet":
        return conn.sql(f"SELECT * FROM read_parquet({path})")
    raise ValueError(f"kptn cannot read {file.path}; pass read= to kptn.source.")


def _read_custom(
    conn: duckdb.DuckDBPyConnection, file: SourceFile, read: Callable[[str], Any]
) -> duckdb.DuckDBPyRelation:
    obj = read(str(file.path))
    if isinstance(obj, duckdb.DuckDBPyRelation):
        # It may belong to another connection, so materialize it first.
        try:
            obj = obj.df()
        except ModuleNotFoundError:
            obj = obj.arrow()
    if type(obj).__module__.startswith("pandas"):
        return conn.from_df(obj)
    return conn.from_arrow(obj)


def _text_select(rel: duckdb.DuckDBPyRelation, file: SourceFile, temp_view: str) -> str:
    if any(c.lower() == SOURCE_FILE_COLUMN for c in rel.columns):
        raise ValueError(f"{file.path} already has a column named {SOURCE_FILE_COLUMN}.")
    exprs: list[str] = []
    for column, dtype in zip(rel.columns, rel.types):
        col = _ident(column)
        type_id = getattr(dtype, "id", str(dtype).lower())
        if type_id == "varchar":
            expr = col
        elif type_id in _NESTED_TYPE_IDS:
            expr = f"CAST(to_json({col}) AS VARCHAR)"
        else:
            expr = f"CAST({col} AS VARCHAR)"
        exprs.append(f"{expr} AS {col}")
    exprs.append(f"{_sql_str(file.rel)} AS {SOURCE_FILE_COLUMN}")
    return f"SELECT {', '.join(exprs)} FROM {temp_view}"


def create_table_view(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    table: str,
    files: list[SourceFile],
    read: Callable[[str], Any] | None = None,
) -> str:
    """Create ``raw."<source>_<table>"`` over *files* and return its name.

    Files are combined in ``_source_file`` order with ``UNION ALL BY NAME``, so
    the result does not depend on the order *files* arrive in.
    """
    if not files:
        raise ValueError(f"Table {source}.{table} has no files.")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    selects: list[str] = []
    for i, file in enumerate(sorted(files, key=lambda f: f.rel)):
        rel = _read_custom(conn, file, read) if read is not None else _read_builtin(conn, file)
        temp_view = f"_kptn_source_file_{i}"
        rel.create_view(temp_view, replace=True)
        selects.append(_text_select(rel, file, temp_view))
    name = view_name(source, table)
    conn.execute(f"CREATE OR REPLACE VIEW {name} AS " + " UNION ALL BY NAME ".join(selects))
    return name
