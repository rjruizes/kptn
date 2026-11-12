"""What a table with a check file keeps beside it, served as
``raw_<source>._warnings`` and ``raw_<source>._columns``.

A check that warns does not stop a run, so its warning would be lost once the
table is cached and the checks no longer run. Each checked table therefore
keeps ``.kptn/_bronze/<source>/_warnings/<table>.parquet``, the checks that
warned when it was built, and ``_columns/<table>.parquet``, the columns each
of its files had: the table itself fills a column a file lacks with NULLs, so
it cannot say which file lacked it. Both are written with the table, even
when empty. A table without a check file keeps neither.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import duckdb

from kptn.layout import BRONZE_DIR

#: The folder of each record under a source's bronze, and its view's name.
WARNINGS = "_warnings"
COLUMNS = "_columns"


@dataclass(frozen=True)
class CheckWarning:
    #: The check's name as Soda reports it; a ``group by`` check's ends with
    #: its group, such as ``is empty [m1]``.
    check: str


@dataclass(frozen=True)
class FileColumn:
    #: The file's ``_source_file`` value.
    file: str
    #: The column's name in the table: lowercase.
    column: str
    #: Its name as the file spells it.
    original: str


def warnings_path(root: Path, source: str, table: str) -> Path:
    return root / BRONZE_DIR / source / WARNINGS / f"{table}.parquet"


def columns_path(root: Path, source: str, table: str) -> Path:
    return root / BRONZE_DIR / source / COLUMNS / f"{table}.parquet"


def create_warnings_table(
    conn: duckdb.DuckDBPyConnection, name: str, rows: list[tuple[str, CheckWarning]]
) -> None:
    """Create table *name* (already quoted as needed) holding each
    (table, warning) of *rows*."""
    conn.execute(f"CREATE OR REPLACE TABLE {name} (table_name VARCHAR, check_name VARCHAR)")
    if rows:
        conn.executemany(f"INSERT INTO {name} VALUES (?, ?)", [(t, w.check) for t, w in rows])


def create_columns_table(
    conn: duckdb.DuckDBPyConnection, name: str, rows: list[tuple[str, FileColumn]]
) -> None:
    """Create table *name* (already quoted as needed) holding each
    (table, column) of *rows*."""
    conn.execute(
        f"CREATE OR REPLACE TABLE {name} "
        "(table_name VARCHAR, _source_file VARCHAR, column_name VARCHAR, original_name VARCHAR)"
    )
    if rows:
        conn.executemany(
            f"INSERT INTO {name} VALUES (?, ?, ?, ?)",
            [(t, c.file, c.column, c.original) for t, c in rows],
        )


def write_warnings(table: str, warnings: list[CheckWarning], dest: Path) -> None:
    """Atomically write *table*'s *warnings* to the parquet file *dest*."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{uuid4().hex}.tmp")
    conn = duckdb.connect()
    try:
        create_warnings_table(conn, "warned", [(table, w) for w in warnings])
        target = "'" + str(tmp).replace("'", "''") + "'"
        conn.execute(f"COPY warned TO {target} (FORMAT parquet)")
        os.replace(tmp, dest)
    finally:
        conn.close()
        tmp.unlink(missing_ok=True)
