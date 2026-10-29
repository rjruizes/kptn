"""The record of files a source left out, served as ``raw_<source>._rejected``.

Each table of a source with ``check=`` keeps one record,
``.kptn/_bronze/<source>/_rejected/<table>.parquet``, written with the table
even when it is empty. Files a ``table=`` callable assigns to no table
are kept in ``_rejected/_no_table.parquet``. Table names never start with an
underscore, so neither name can belong to a table.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import duckdb

from kptn.layout import BRONZE_DIR

REJECTED_DIR = "_rejected"
#: The view over a source's records, in its ``raw_<source>`` schema.
REJECTED_VIEW = "_rejected"
#: The record of files that feed no table.
NO_TABLE = "_no_table"


@dataclass(frozen=True)
class RejectedFile:
    #: The table the file would have fed; None when ``table=`` gave it none.
    table: str | None
    #: The file's ``_source_file`` value.
    file: str
    reason: str


def rejects_path(root: Path, source: str, name: str) -> Path:
    """Where record *name* (a table, or ``NO_TABLE``) of *source* lives."""
    return root / BRONZE_DIR / source / REJECTED_DIR / f"{name}.parquet"


def create_rejects_table(conn: duckdb.DuckDBPyConnection, name: str, rows: list[RejectedFile]) -> None:
    """Create table *name* (already quoted as needed) holding *rows*."""
    conn.execute(
        f"CREATE OR REPLACE TABLE {name} "
        "(table_name VARCHAR, _source_file VARCHAR, reason VARCHAR)"
    )
    if rows:
        conn.executemany(
            f"INSERT INTO {name} VALUES (?, ?, ?)", [(r.table, r.file, r.reason) for r in rows]
        )


def write_rejects(rows: list[RejectedFile], dest: Path) -> None:
    """Atomically write *rows* to the parquet file *dest*, empty or not."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{uuid4().hex}.tmp")
    conn = duckdb.connect()
    try:
        create_rejects_table(conn, "rejected", rows)
        target = "'" + str(tmp).replace("'", "''") + "'"
        conn.execute(f"COPY rejected TO {target} (FORMAT parquet)")
        os.replace(tmp, dest)
    finally:
        conn.close()
        tmp.unlink(missing_ok=True)


def count_rejects(path: Path) -> int:
    """How many files the record at *path* holds."""
    target = "'" + str(path).replace("'", "''") + "'"
    conn = duckdb.connect()
    try:
        return conn.execute(f"SELECT count(*) FROM read_parquet({target})").fetchall()[0][0]
    finally:
        conn.close()
