"""Deterministic hash computation functions for kptn change detection."""

import ast
import hashlib
import inspect
import logging
import re
import sqlite3
import urllib.parse
from pathlib import Path
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


def _normalize_fn_source(source: str) -> str:
    """Strip comments/docstrings and collapse whitespace for deterministic hashing."""
    source = re.sub(r"#[^\n]*", "", source)
    try:
        tree = ast.parse(source)
        _StripDocstrings().visit(tree)
        source = ast.unparse(tree)
    except SyntaxError:
        pass
    return re.sub(r"\s+", " ", source).strip()


def _find_package_root(file_path: Path) -> Path:
    """Walk up from *file_path* until we leave the Python package boundary."""
    current = file_path.resolve().parent
    while (current / "__init__.py").exists():
        parent = current.parent
        if parent == current:
            break
        current = parent
    return current


class _ModuleSummary:
    """Lightweight AST index for a single Python source file."""

    def __init__(self, file_path: Path, source: str, package_root: Path) -> None:
        self.file_path = file_path
        self.source = source
        self.package_root = package_root
        self.functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        self.module_aliases: dict[str, str] = {}          # bound → full module
        self.symbol_aliases: dict[str, tuple[str, str]] = {}  # bound → (module, orig)
        self._package_parts: list[str] = self._infer_package_parts()
        self._index()

    def _infer_package_parts(self) -> list[str]:
        try:
            rel = self.file_path.relative_to(self.package_root)
            parts = list(rel.with_suffix("").parts)
            if parts and parts[-1] == "__init__":
                parts = parts[:-1]
            return parts
        except ValueError:
            return []

    def _index(self) -> None:
        try:
            tree = ast.parse(self.source, filename=str(self.file_path))
        except SyntaxError:
            return
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.functions[node.name] = node
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    bound = alias.asname or alias.name.split(".")[0]
                    self.module_aliases[bound] = alias.name
            elif isinstance(node, ast.ImportFrom):
                if any(a.name == "*" for a in node.names):
                    continue
                mod = self._resolve_module(node.module, node.level)
                if mod is None:
                    continue
                for alias in node.names:
                    bound = alias.asname or alias.name
                    self.symbol_aliases[bound] = (mod, alias.name)

    def _resolve_module(self, module: str | None, level: int) -> str | None:
        if level == 0:
            return module
        n = len(self._package_parts)
        if level - 1 > n:
            return None
        base = self._package_parts[: n - (level - 1)]
        extra = module.split(".") if module else []
        parts = base + extra
        return ".".join(parts) if parts else None

    def iter_call_targets(self, fn_node: ast.AST):
        """Yield ``(kind, payload)`` for every direct call site within *fn_node*."""
        stack = [fn_node]
        root_ids = {id(fn_node)}
        while stack:
            node = stack.pop()
            if id(node) not in root_ids and isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            ):
                continue
            for child in ast.iter_child_nodes(node):
                stack.append(child)
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    yield "name", func.id
                elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                    yield "attr", (func.value.id, func.attr)


class _SourceCollector:
    """Collect source snippets for a function and all user-defined callees."""

    def __init__(self, package_root: Path) -> None:
        self._root = package_root
        self._cache: dict[Path, _ModuleSummary | None] = {}

    def collect(self, fn: Any) -> list[str]:
        """Return source strings for *fn* and every transitively called function
        that lives within the package root (stdlib and third-party are skipped)."""
        try:
            file_path = Path(inspect.getfile(fn)).resolve()
        except (OSError, TypeError):
            return []

        fn_name = fn.__name__
        stack: list[tuple[Path, str]] = [(file_path, fn_name)]
        visited: set[tuple[Path, str]] = set()
        sources: list[str] = []

        while stack:
            fp, name = stack.pop()
            key = (fp, name)
            if key in visited:
                continue
            visited.add(key)

            summary = self._load(fp)
            if summary is None:
                continue
            fn_node = summary.functions.get(name)
            if fn_node is None:
                continue

            src = ast.get_source_segment(summary.source, fn_node)
            if src is None:
                lines = summary.source.splitlines(keepends=True)
                start = getattr(fn_node, "lineno", None)
                end = getattr(fn_node, "end_lineno", None)
                if start is not None and end is not None:
                    src = "".join(lines[start - 1 : end])
            if src:
                sources.append(src)

            for kind, payload in summary.iter_call_targets(fn_node):
                dep = self._resolve(summary, kind, payload)
                if dep and dep not in visited:
                    stack.append(dep)

        return sources

    def _load(self, file_path: Path) -> _ModuleSummary | None:
        resolved = file_path.resolve()
        if resolved in self._cache:
            return self._cache[resolved]
        try:
            source = resolved.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            self._cache[resolved] = None
            return None
        summary = _ModuleSummary(resolved, source, self._root)
        self._cache[resolved] = summary
        return summary

    def _resolve(
        self, summary: _ModuleSummary, kind: str, payload: object
    ) -> tuple[Path, str] | None:
        if kind == "name":
            if not isinstance(payload, str):
                return None
            if payload in summary.functions:
                return (summary.file_path, payload)
            sym = summary.symbol_aliases.get(payload)
            if sym:
                mod_path = self._find_module(sym[0])
                if mod_path:
                    return (mod_path, sym[1])
            return None
        if kind == "attr":
            if not isinstance(payload, tuple) or len(payload) != 2:
                return None
            base, attr = payload
            if not isinstance(base, str) or not isinstance(attr, str):
                return None
            mod_name = summary.module_aliases.get(base)
            if not mod_name:
                sym = summary.symbol_aliases.get(base)
                if sym:
                    mod_name = sym[0]
            if not mod_name:
                return None
            mod_path = self._find_module(mod_name)
            if mod_path:
                return (mod_path, attr)
        return None

    def _find_module(self, module_name: str) -> Path | None:
        """Resolve *module_name* to a file path within the package root only."""
        mod_rel = Path(*module_name.split("."))
        candidate = (self._root / mod_rel).with_suffix(".py")
        if candidate.exists():
            return candidate.resolve()
        init_candidate = self._root / mod_rel / "__init__.py"
        if init_candidate.exists():
            return init_candidate.resolve()
        return None


def hash_task_source(fn: Any) -> str:
    """Hash *fn* and all user-defined functions it transitively calls.

    Only functions resolvable within the same package root are included;
    standard-library and third-party callees are ignored.  Falls back to
    hashing the raw source of *fn* alone when AST-based collection fails
    (e.g. lambdas, dynamically-defined functions, or C extensions).
    """
    fn = inspect.unwrap(fn)  # follow __wrapped__ chains (e.g. _KptnCallable)
    sources: list[str] = []
    try:
        file_path = Path(inspect.getfile(fn)).resolve()
        package_root = _find_package_root(file_path)
        sources = _SourceCollector(package_root).collect(fn)
    except (OSError, TypeError):
        pass

    if not sources:
        try:
            raw = inspect.getsource(fn)
        except (OSError, TypeError) as exc:
            raise HashError(f"Cannot get source for {fn!r}") from exc
        sources = [raw]

    normalized = sorted(_normalize_fn_source(s) for s in sources)
    combined = ":".join(normalized)
    return hashlib.sha256(combined.encode()).hexdigest()
