"""Validating a source table with its check file, by convention, and each of
its files with the source's ``check=`` callable."""

from __future__ import annotations

import inspect
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

from kptn.exceptions import TaskError
from kptn.sources.files import SourceFile
from kptn.sources.read import schema_name

CHECKS_DIR = Path("checks")


def check_path(root: Path, source: str, table: str) -> Path:
    """Where a table's check file lives: ``checks/<source>/<table>.yml``."""
    return root / CHECKS_DIR / source / f"{table}.yml"


def warn_at(message: str, declared_at: tuple[str, int] | None) -> None:
    """Warn at *declared_at*, where ``kptn.source(...)`` was called, rather than in kptn."""
    if declared_at is not None:
        filename, lineno = declared_at
        warnings.warn_explicit(message, UserWarning, filename, lineno)
    else:
        warnings.warn(message, UserWarning, stacklevel=3)


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
    file_checked: bool = False,
) -> bool:
    """Run the table's check file against ``raw_<source>.<table>`` on *conn*.

    Soda looks tables up in the source's schema, so a check file names the
    table alone: ``checks for <table>:``.

    *declared_at* is where ``kptn.source(...)`` was called; a missing-check
    warning points there rather than into kptn. A table whose files a
    ``check=`` callable checked (*file_checked*) is validated already, so it
    gets no warning.

    Returns:
        True if checks ran and passed; False if the table has no check file.

    Raises:
        TaskError: a check failed, or a check file exists but Soda is missing.
    """
    path = check_path(root, source, table)
    if not path.exists():
        if not file_checked:
            warn_at(
                f"No checks for {source}.{table}: add {path.relative_to(root)} to validate it.",
                declared_at,
            )
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


def file_check_reason(
    check: Callable[..., Any], *, rows: duckdb.DuckDBPyRelation, file: SourceFile, table: str
) -> str | None:
    """Run a source's ``check=`` callable on one file.

    Returns:
        Why the file is left out, or None to keep it. A check that raises
        leaves the file out, with the error as the reason.

    Raises:
        TypeError: the check returned something other than None or a string.
    """
    values = {"rows": rows, "file": file, "table": table}
    params = inspect.signature(check).parameters
    if not any(p.kind is p.VAR_KEYWORD for p in params.values()):
        values = {k: v for k, v in values.items() if k in params}
    try:
        result = check(**values)
    except Exception as exc:  # a bug, or a file the check did not expect
        return f"check= raised {type(exc).__name__}: {exc}"
    if result is None:
        return None
    if isinstance(result, str):
        return result or "rejected by check="
    raise TypeError(
        f"check= returned {result!r} for {file.rel}; return None to keep a file "
        "or a string saying why to leave it out."
    )
