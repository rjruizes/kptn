"""Exposing built source tables as ``raw.<source>_<table>`` views."""

from __future__ import annotations

import functools
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Callable

from kptn.sources.read import RAW_SCHEMA, view_name


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def apply_search_path(conn: Any, bronze_root: Path) -> None:
    """Add *bronze_root* to the connection's ``file_search_path``, once.

    Views store paths relative to ``_bronze/``; DuckDB resolves them through
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


def register_views(conn: Any, bronze_root: Path, sources: Iterable[str]) -> list[str]:
    """Create or replace a view for every built table of *sources*."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    created: list[str] = []
    for source in sources:
        folder = Path(bronze_root) / source
        if not folder.is_dir():
            continue
        for parquet in sorted(folder.glob("*.parquet")):
            name = view_name(source, parquet.stem)
            relative = _sql_str(f"{source}/{parquet.name}")
            conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({relative})")
            created.append(name)
    return created
