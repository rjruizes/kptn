"""Deterministic hash computation functions for kptn change detection."""

import ast
import hashlib
import inspect
import logging
import re
import sqlite3
import urllib.parse
from typing import Any

from kptn.exceptions import HashError

logger = logging.getLogger(__name__)

_TABLE_URI_SEP = "::"

EMPTY_TABLE_HASH = hashlib.md5(b"empty:").hexdigest()


def _escape_identifier(name: str) -> str:
    """Escape a SQL identifier by doubling internal double-quotes."""
    return name.replace('"', '""')


def _quote_qualified_name(name: str) -> str:
    """Return a properly SQL-quoted identifier, handling schema.table notation.

    Each dot-separated part is quoted independently so that
    ``"redcap_m1"."participants"`` is produced instead of the invalid
    ``"redcap_m1.participants"``.
    """
    return ".".join(f'"{_escape_identifier(part)}"' for part in name.split("."))


def _parse_table_uri(table_uri: str) -> tuple[str, str]:
    if _TABLE_URI_SEP not in table_uri:
        raise HashError(
            f"Invalid table_uri {table_uri!r}: missing '::' separator"
        )
    db_path, table_name = table_uri.split(_TABLE_URI_SEP, 1)
    return db_path, table_name


def hash_duckdb_table(table_name: str, *, conn: "Any") -> str:
    """Hash a DuckDB table using an existing active connection.

    *table_name* is the bare table reference after stripping the ``duckdb://``
    prefix, e.g. ``schema.table`` or just ``table``.  The caller supplies the
    active connection; kptn never opens a separate file handle for hashing.
    """
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise HashError("duckdb extra not installed: pip install kptn[duckdb]") from exc

    quoted_table = _quote_qualified_name(table_name)
    try:
        cols_result = conn.execute(f'DESCRIBE {quoted_table}').fetchall()
        col_names = sorted(row[0] for row in cols_result)

        row_count = conn.execute(f'SELECT COUNT(*) FROM {quoted_table}').fetchone()[0]

        if row_count == 0:
            return EMPTY_TABLE_HASH

        col_checksums = []
        for col in col_names:
            safe_col = _escape_identifier(col)
            result = conn.execute(
                f'SELECT md5(string_agg("{safe_col}"::VARCHAR, \',\' ORDER BY "{safe_col}"::VARCHAR)) FROM {quoted_table}'
            ).fetchone()[0]
            col_checksums.append(result if result is not None else "null")

        combined = f"{row_count}:{';'.join(col_checksums)}"
        return hashlib.md5(combined.encode()).hexdigest()
    except HashError:
        raise
    except Exception as exc:
        raise HashError(f"DuckDB hashing failed for {table_name!r}: {exc}") from exc


def hash_sqlite_table(table_uri: str) -> str:
    db_path, table_name = _parse_table_uri(table_uri)
    safe_table = _escape_identifier(table_name)
    db_uri = f"file:{urllib.parse.quote(db_path, safe='/')}?mode=ro"
    try:
        conn = sqlite3.connect(db_uri, uri=True)
        try:
            pragma = conn.execute(f'PRAGMA table_info("{safe_table}")').fetchall()
            col_names = sorted(row[1] for row in pragma)

            row_count = conn.execute(f'SELECT COUNT(*) FROM "{safe_table}"').fetchone()[0]

            if row_count == 0:
                return EMPTY_TABLE_HASH

            col_checksums = []
            for col in col_names:
                safe_col = _escape_identifier(col)
                rows = conn.execute(
                    f'SELECT "{safe_col}" FROM "{safe_table}" ORDER BY "{safe_col}" NULLS LAST'
                ).fetchall()
                values = [str(r[0]) if r[0] is not None else "\x00" for r in rows]
                col_hash = hashlib.md5(",".join(values).encode()).hexdigest()
                col_checksums.append(col_hash)

            combined = f"{row_count}:{';'.join(col_checksums)}"
            return hashlib.md5(combined.encode()).hexdigest()
        finally:
            conn.close()
    except HashError:
        raise
    except sqlite3.Error as exc:
        raise HashError(f"SQLite hashing failed for {table_uri!r}: {exc}") from exc


def hash_file(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return hashlib.file_digest(f, "sha256").hexdigest()
    except OSError as exc:
        raise HashError(f"Cannot hash file {path!r}: {exc}") from exc


class _StripDocstrings(ast.NodeTransformer):
    """Remove Expr(Constant(str)) nodes that represent docstrings."""

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        self.generic_visit(node)
        if (
            node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        ):
            node.body = node.body[1:]
            if not node.body:
                node.body = [ast.Pass()]
        return node

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]


def hash_task_source(fn: Any) -> str:
    try:
        source = inspect.getsource(fn)
    except (OSError, TypeError) as exc:
        raise HashError(f"Cannot get source for {fn!r}") from exc

    source = re.sub(r"#[^\n]*", "", source)

    try:
        tree = ast.parse(source)
        _StripDocstrings().visit(tree)
        source = ast.unparse(tree)
    except SyntaxError:
        pass

    normalized = re.sub(r"\s+", " ", source).strip()
    return hashlib.sha256(normalized.encode()).hexdigest()
