"""Exposing built source tables as ``raw_<source>.<table>`` views."""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Any, Callable

from kptn.sources.read import schema_name, view_name


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def apply_search_path(conn: Any, bronze_root: Path) -> None:
    """Add *bronze_root* to the connection's ``file_search_path``, once.

    Views store paths relative to ``.kptn/_bronze/``; DuckDB resolves them through
    this per-connection setting. Existing entries are kept, and paths relative
    to the working directory still resolve.
    """
    root = str(Path(bronze_root).resolve())
    # fetchall, not fetchone: a partly fetched result on a shared connection
    # can wedge a later CHECKPOINT on DuckDB 1.4.
    current = conn.execute("SELECT current_setting('file_search_path')").fetchall()[0][0] or ""
    entries = [e for e in current.split(",") if e]
    if root not in entries:
        entries.append(root)
        conn.execute(f"SET file_search_path = {_sql_str(','.join(entries))}")


def with_search_path(factory: Callable[..., Any], bronze_root: Path) -> Callable[..., Any]:
    """Wrap a DuckDB factory so every connection it returns can resolve views."""

    @functools.wraps(factory)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        conn = factory(*args, **kwargs)
        apply_search_path(conn, bronze_root)
        return conn

    return wrapped


def register_views(conn: Any, bronze_root: Path, source: str) -> list[str]:
    """Make ``raw_<source>`` hold exactly one view per built table of *source*.

    Views of tables the source no longer has are dropped, whichever run
    removed their files. The schema is the source's alone, so nothing else
    in it is a view kptn did not make; other kinds of object are left alone.
    Returns the views created.
    """
    schema = schema_name(source)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    folder = Path(bronze_root) / source
    tables = sorted(p.stem for p in folder.glob("*.parquet")) if folder.is_dir() else []
    # fetchall, not fetchone: a partly fetched result on a shared connection
    # can wedge a later CHECKPOINT on DuckDB 1.4.
    existing = conn.execute(
        "SELECT view_name FROM duckdb_views() "
        "WHERE database_name = current_database() AND schema_name = ? AND NOT internal",
        [schema],
    ).fetchall()
    for (stale,) in existing:
        if stale not in tables:
            conn.execute(f"DROP VIEW {view_name(source, stale)}")
    created: list[str] = []
    for table in tables:
        name = view_name(source, table)
        relative = _sql_str(f"{source}/{table}.parquet")
        conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({relative})")
        created.append(name)
    return created
