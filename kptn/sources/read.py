"""Reading a table's files by convention: text values, lowercase names, provenance."""

from __future__ import annotations

import hashlib
from typing import Any, Callable

import duckdb

from kptn.sources.files import SourceFile
from kptn.sources.records import FileColumn

SOURCE_FILE_COLUMN = "_source_file"
#: Each source's views live in their own schema, ``raw_<source>``, so two
#: sources can never claim the same view name.
RAW_SCHEMA_PREFIX = "raw_"

_NESTED_TYPE_IDS = frozenset({"struct", "list", "map", "array", "union"})

#: The most SELECTs one ``UNION ALL BY NAME`` joins. DuckDB parses a chain of
#: unions as nested expressions and refuses one nested 1000 deep, which a
#: table of about 1000 files reaches; lifting that limit crashes it instead.
_UNION_WIDTH = 250


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def schema_name(source: str) -> str:
    """The schema holding a source's views: ``raw_<source>``. Source names are
    lowercase identifiers, so it needs no quoting."""
    return f"{RAW_SCHEMA_PREFIX}{source}"


def view_name(source: str, table: str) -> str:
    """The qualified view name for a source table: ``raw_<source>."<table>"``."""
    return f"{schema_name(source)}.{_ident(table)}"


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
    # Names are lowercased so a table's columns do not depend on which file's
    # spelling sorts first. DuckDB already renames names that differ only by
    # case (``Foo,foo`` reads as ``Foo, foo_1``), so nothing collides.
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
        exprs.append(f"{expr} AS {_ident(column.lower())}")
    exprs.append(f"{_sql_str(file.rel)} AS {SOURCE_FILE_COLUMN}")
    return f"SELECT {', '.join(exprs)} FROM {temp_view}"


def _file_columns(rel: duckdb.DuckDBPyRelation, file: SourceFile) -> list[FileColumn]:
    return [FileColumn(file.rel, c.lower(), c) for c in rel.columns]


def _file_relation(
    conn: duckdb.DuckDBPyConnection, file: SourceFile, read: Callable[[str], Any] | None
) -> duckdb.DuckDBPyRelation:
    return _read_custom(conn, file, read) if read is not None else _read_builtin(conn, file)


def _prefix(source: str, table: str) -> str:
    # Views and tables read their per-file relations by name, so each table
    # needs its own; shared names would repoint an earlier table's view at
    # this one's files.
    return hashlib.sha256(f"{source}\x1f{table}".encode()).hexdigest()[:16]


def union_view(conn: duckdb.DuckDBPyConnection, source: str, table: str, selects: list[str]) -> str:
    """Create ``raw_<source>."<table>"`` as the ``UNION ALL BY NAME`` of *selects*.

    More than ``_UNION_WIDTH`` selects are unioned in groups, each written to
    a table, and the groups' tables unioned in turn: the same columns, in the
    same order, and the rows still in file order, which unions nested as
    views or subqueries do not keep.
    """
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name(source)}")
    prefix = _prefix(source, table)
    level = 0
    while len(selects) > _UNION_WIDTH:
        groups: list[str] = []
        for start in range(0, len(selects), _UNION_WIDTH):
            group = f"_kptn_source_union_{prefix}_{level}_{start // _UNION_WIDTH}"
            conn.execute(
                f"CREATE OR REPLACE TABLE {group} AS "
                + " UNION ALL BY NAME ".join(selects[start : start + _UNION_WIDTH])
            )
            groups.append(f"SELECT * FROM {group}")
        selects = groups
        level += 1
    name = view_name(source, table)
    conn.execute(f"CREATE OR REPLACE VIEW {name} AS " + " UNION ALL BY NAME ".join(selects))
    return name


def create_table_view(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    table: str,
    files: list[SourceFile],
    read: Callable[[str], Any] | None = None,
    columns: list[FileColumn] | None = None,
) -> str:
    """Create ``raw_<source>."<table>"`` over *files* and return its name.

    Files are combined in ``_source_file`` order with ``UNION ALL BY NAME``, so
    the result does not depend on the order *files* arrive in. Each file's
    columns are added to *columns*, when given.
    """
    if not files:
        raise ValueError(f"Table {source}.{table} has no files.")
    prefix = _prefix(source, table)
    selects: list[str] = []
    for i, file in enumerate(sorted(files, key=lambda f: f.rel)):
        rel = _file_relation(conn, file, read)
        temp_view = f"_kptn_source_file_{prefix}_{i}"
        rel.create_view(temp_view, replace=True)
        selects.append(_text_select(rel, file, temp_view))
        if columns is not None:
            columns.extend(_file_columns(rel, file))
    return union_view(conn, source, table, selects)


def read_file_table(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    table: str,
    file: SourceFile,
    index: int,
    read: Callable[[str], Any] | None = None,
) -> tuple[str, list[FileColumn]]:
    """Read one file of a table into its own table, as ``create_table_view``
    would serve it, and return that table's name and the file's columns.

    The file is read in full here: DuckDB reads CSV lazily, so an error past
    the rows it samples would otherwise surface only when the whole table is
    copied, with no file to blame.
    """
    prefix = _prefix(source, table)
    raw_view = f"_kptn_source_raw_{prefix}_{index}"
    name = f"_kptn_source_file_{prefix}_{index}"
    rel = _file_relation(conn, file, read)
    rel.create_view(raw_view, replace=True)
    try:
        conn.execute(f"CREATE OR REPLACE TABLE {name} AS {_text_select(rel, file, raw_view)}")
        columns = _file_columns(rel, file)
    finally:
        conn.execute(f"DROP VIEW IF EXISTS {raw_view}")
    return name, columns
