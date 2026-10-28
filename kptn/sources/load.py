"""Loading a source straight into a DuckDB connection, with no bronze."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import duckdb

from kptn.sources.build import plan_source, read_table
from kptn.sources.read import _ident, schema_name, view_name
from kptn.sources.rejects import REJECTED_VIEW, RejectedFile, create_rejects_table
from kptn.sources.spec import SourceHandle

#: The in-memory database each table is read in, attached to the caller's
#: connection only while that table is read.
_SCRATCH = "_kptn_load"


def load(
    conn: duckdb.DuckDBPyConnection,
    source: SourceHandle,
    config_values: Mapping[str, Any] | None = None,
    *,
    root: Path | str = ".",
    checks: bool = False,
) -> list[str]:
    """Read *source*'s files into ``raw_<source>.<table>`` tables on *conn*.

    The tables hold what a run's ``raw_<source>`` views serve -- the same
    files, table names, text values and ``_source_file`` column -- but are
    read straight from the files: nothing is written under ``.kptn/`` and no
    state is kept. For tests and notebooks that need a source's tables without
    running the pipeline. Tables of the same names are replaced.

    Each table is read in an in-memory database attached to *conn*, so the
    views kptn reads through never land in *conn*'s own database.

    Files ``table=`` assigns to no table, and with *checks* files a file
    check rejects, are left out and listed in ``raw_<source>._rejected``, as
    a run's view of that name lists them.

    Args:
        conn: The connection to create the tables on, in its current database.
        source: What ``kptn.source`` returned.
        config_values: The values a ``files=`` callable takes by name, as a
            task's parameters would be filled from config.
        root: The project root, where ``checks/<source>/`` is found.
        checks: Run each table's check file, as a run does.

    Returns:
        The tables created, as ``raw_<source>."<table>"``.

    Raises:
        HashError: a pattern matched nothing or a file is unreadable.
        TaskError: with *checks*, a check failed.
    """
    spec = source.__kptn__
    root = Path(root)
    plans, no_table = plan_source(source.__name__, spec, config_values or {}, root=root)
    rejected = [RejectedFile(None, f.rel, "table= gave it no table") for f in no_table]
    database, schema = conn.execute("SELECT current_database(), current_schema()").fetchall()[0]
    home = f"{_ident(database)}.{_ident(schema)}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(database)}.{schema_name(source.__name__)}")
    created: list[str] = []
    for plan in plans:
        conn.execute(f"ATTACH ':memory:' AS {_SCRATCH}")
        try:
            # Views resolve the names in them in the current database, so the
            # copy runs from inside the scratch one.
            conn.execute(f"USE {_SCRATCH}")
            read = read_table(conn, plan, spec, root=root, checks=checks)
            view = read.view
            conn.execute(f"CREATE OR REPLACE TABLE {_ident(database)}.{view} AS SELECT * FROM {view}")
        finally:
            conn.execute(f"USE {home}")
            conn.execute(f"DETACH {_SCRATCH}")
        created.append(view)
        rejected.extend(read.rejected)
    if no_table or (checks and any(p.file_check is not None for p in plans)):
        name = view_name(source.__name__, REJECTED_VIEW)
        create_rejects_table(conn, f"{_ident(database)}.{name}", rejected)
        created.append(name)
    return created
