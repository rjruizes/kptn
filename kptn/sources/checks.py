"""Validating a source table with its check file, by convention."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import duckdb

from kptn.exceptions import TaskError
from kptn.sources.read import schema_name

CHECKS_DIR = Path("checks")


def check_path(root: Path, source: str, table: str) -> Path:
    """Where a table's check file lives: ``checks/<source>/<table>.yml``."""
    return root / CHECKS_DIR / source / f"{table}.yml"


def _scan_in_schema(scan: Any, schema: str) -> None:
    # Soda resolves unqualified table names in one schema. Its public API has
    # no setter for DuckDB, so this mirrors what nph's _validate_with_soda does.
    scan._configuration.data_source_properties_by_name["duckdb"]["schema"] = schema


def run_checks(
    conn: duckdb.DuckDBPyConnection,
    *,
    source: str,
    table: str,
    root: Path,
    declared_at: tuple[str, int] | None = None,
) -> bool:
    """Run the table's check file against ``raw_<source>.<table>`` on *conn*.

    Soda looks tables up in the source's schema, so a check file names the
    table alone: ``checks for <table>:``.

    *declared_at* is where ``kptn.source(...)`` was called; a missing-check
    warning points there rather than into kptn.

    Returns:
        True if checks ran and passed; False if the table has no check file.

    Raises:
        TaskError: a check failed, or a check file exists but Soda is missing.
    """
    path = check_path(root, source, table)
    if not path.exists():
        message = f"No checks for {source}.{table}: add {path.relative_to(root)} to validate it."
        if declared_at is not None:
            filename, lineno = declared_at
            warnings.warn_explicit(message, UserWarning, filename, lineno)
        else:
            warnings.warn(message, UserWarning, stacklevel=2)
        return False
    try:
        from soda.scan import Scan
    except ImportError as exc:
        raise TaskError(
            f"{path} exists, but Soda is not installed. Install kptn[soda] to run it."
        ) from exc

    scan = Scan()
    scan.set_scan_definition_name(f"{source}_{table}")
    scan.set_data_source_name("duckdb")
    scan.add_duckdb_connection(conn)
    _scan_in_schema(scan, schema_name(source))
    scan.add_sodacl_yaml_file(str(path))
    scan.execute()
    if scan.has_check_fails() or scan.has_error_logs():
        detail = scan.get_checks_fail_text() or scan.get_logs_text()
        raise TaskError(f"Checks failed for {source}.{table} ({path}):\n{detail}")
    return True
