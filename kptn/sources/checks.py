"""Validating a source table, or each of its files, with its check files, by convention."""

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


def file_check_path(root: Path, source: str, table: str) -> Path:
    """Where a table's file check lives: ``checks/<source>/<table>.file.yml``.
    Table names never hold a dot, so it cannot be another table's check file."""
    return root / CHECKS_DIR / source / f"{table}.file.yml"


def warn_at(message: str, declared_at: tuple[str, int] | None) -> None:
    """Warn at *declared_at*, where ``kptn.source(...)`` was called, rather than in kptn."""
    if declared_at is not None:
        filename, lineno = declared_at
        warnings.warn_explicit(message, UserWarning, filename, lineno)
    else:
        warnings.warn(message, UserWarning, stacklevel=3)


#: The schema a file check reads the file's rows from.
_FILE_CHECK_SCHEMA = "_kptn_file_check"


def _scan_in_schema(scan: Any, schema: str) -> None:
    # Soda resolves unqualified table names in one schema. Its public API has
    # no setter for DuckDB, so this mirrors what nph's _validate_with_soda does.
    scan._configuration.data_source_properties_by_name["duckdb"]["schema"] = schema


def _scan(conn: duckdb.DuckDBPyConnection, path: Path, name: str, schema: str) -> Any:
    """Run the check file at *path* against *schema* on *conn*; return the scan."""
    try:
        from soda.scan import Scan
    except ImportError as exc:
        raise TaskError(
            f"{path} exists, but Soda is not installed. Install kptn[soda] to run it."
        ) from exc

    scan = Scan()
    scan.set_scan_definition_name(name)
    scan.set_data_source_name("duckdb")
    scan.add_duckdb_connection(conn)
    _scan_in_schema(scan, schema)
    scan.add_sodacl_yaml_file(str(path))
    scan.execute()
    return scan


def _failure(scan: Any) -> str | None:
    if scan.has_check_fails() or scan.has_error_logs():
        return (scan.get_checks_fail_text() or scan.get_logs_text() or "checks failed").strip()
    return None


def _file_failure(scan: Any) -> str | None:
    """One line per scan: the failed checks by name, with their values, or
    the first error, such as a column the file lacks."""
    failed = scan.get_checks_fail()
    if failed:
        parts = []
        for check in failed:
            value = check.get_log_diagnostic_dict().get("check_value")
            parts.append(check.name if value is None else f"{check.name} ({value})")
        return "failed " + "; ".join(parts)
    if scan.has_error_logs():
        return scan.get_error_logs()[0].message.splitlines()[0]
    return None


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
    warning points there rather than into kptn. A table with a file check
    but no check file is validated already, so it gets no warning.

    Returns:
        True if checks ran and passed; False if the table has no check file.

    Raises:
        TaskError: a check failed, or a check file exists but Soda is missing.
    """
    path = check_path(root, source, table)
    if not path.exists():
        if not file_check_path(root, source, table).exists():
            warn_at(
                f"No checks for {source}.{table}: add {path.relative_to(root)} to validate it.",
                declared_at,
            )
        return False
    failure = _failure(_scan(conn, path, f"{source}_{table}", schema_name(source)))
    if failure is not None:
        raise TaskError(f"Checks failed for {source}.{table} ({path}):\n{failure}")
    return True


def file_check_failure(
    conn: duckdb.DuckDBPyConnection, path: Path, *, source: str, table: str, relation: str
) -> str | None:
    """Run the file check at *path* against one file's rows, held in *relation*.

    The rows are served as *table*, so a file check reads like a check file:
    ``checks for <table>:``.

    Returns:
        Why the file failed, or None if it passed.

    Raises:
        TaskError: Soda is missing.
    """
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_FILE_CHECK_SCHEMA}")
    view = f'{_FILE_CHECK_SCHEMA}."{table}"'
    conn.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {relation}")
    try:
        return _file_failure(_scan(conn, path, f"{source}_{table}_file", _FILE_CHECK_SCHEMA))
    finally:
        conn.execute(f"DROP VIEW IF EXISTS {view}")
