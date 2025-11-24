"""Shared lineage and table preview helpers for the kptn backend surfaces."""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
import re
from typing import Any, Optional, Tuple

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from kptn.cli import (
    _build_lineage_payload,
    _infer_lineage_dialect,
    _task_order_from_graph,
)
from kptn.lineage import SqlLineageAnalyzer, SqlLineageError
from kptn.lineage.html_renderer import render_lineage_html
from kptn.read_config import read_config
from kptn.util.runtime_config import RuntimeConfig, RuntimeConfigError

_template_env: Optional[Environment] = None
KPTN_CONFIG_EXCLUDE = {".git", "node_modules", "dist", "out", ".venv", ".mypy_cache", ".pytest_cache", ".ruff_cache", "__pycache__"}


def generate_lineage_html(
    config_path: Path, graph: Optional[str]
) -> Tuple[str, int, int]:
    """Build lineage HTML for a kptn.yaml."""
    if not config_path.exists():
        raise FileNotFoundError(f"kptn.yaml not found at {config_path}")

    original_dir = Path.cwd()
    project_dir = config_path.parent
    os.chdir(project_dir)

    try:
        kap_conf = read_config()
        resolved_dialect = _infer_lineage_dialect(kap_conf, None)
        analyzer = SqlLineageAnalyzer(kap_conf, project_dir, dialect=resolved_dialect)
        analyzer.build()

        tasks_conf = kap_conf.get("tasks", {}) if isinstance(kap_conf, dict) else {}
        task_order: Optional[list[str]] = None
        try:
            task_order = _task_order_from_graph(kap_conf, graph)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc

        if not task_order and isinstance(tasks_conf, dict):
            task_order = list(tasks_conf.keys())

        tables_payload, lineage_payload = _build_lineage_payload(
            analyzer,
            task_order=task_order,
            tasks_config=tasks_conf if isinstance(tasks_conf, dict) else None,
        )

        return (
            render_lineage_html(tables_payload, lineage_payload),
            len(tables_payload),
            len(lineage_payload),
        )
    except (SqlLineageError, FileNotFoundError) as exc:
        raise RuntimeError(str(exc)) from exc
    finally:
        os.chdir(original_dir)


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _split_table_identifier(name: str) -> tuple[Optional[str], str]:
    cleaned = name.strip().strip('"')
    if "." in cleaned:
        schema, table = cleaned.split(".", 1)
        schema = schema or None
    else:
        schema, table = None, cleaned
    return schema, table


def _resolve_duckdb_output_table(tasks_conf: Any, table_name: str) -> Optional[str]:
    if not isinstance(tasks_conf, dict):
        return None

    requested = SqlLineageAnalyzer._output_identifier(table_name).strip()
    if not requested:
        return None

    requested_normalized = SqlLineageAnalyzer._normalize_table(requested)
    fallback: Optional[str] = None

    for task in tasks_conf.values():
        if not isinstance(task, dict):
            continue
        outputs = task.get("outputs") or []
        if not isinstance(outputs, list):
            continue
        for output in outputs:
            if not isinstance(output, str) or not output.startswith("duckdb://"):
                continue
            cleaned_output = SqlLineageAnalyzer._output_identifier(output)
            normalized_output = SqlLineageAnalyzer._normalize_table(cleaned_output)
            if cleaned_output.lower() == requested.lower():
                return cleaned_output
            if (
                normalized_output
                and normalized_output == requested_normalized
                and fallback is None
            ):
                fallback = cleaned_output

    return fallback


def _duckdb_connection_from_runtime_config(
    runtime_config: RuntimeConfig,
) -> tuple[Optional[object], Optional[str]]:
    try:
        import duckdb  # type: ignore import-not-found

        have_duckdb = True
    except ImportError:
        duckdb = None  # type: ignore[assignment]
        have_duckdb = False

    def _is_duckdb_conn(candidate: object) -> bool:
        if candidate is None:
            return False
        if have_duckdb and isinstance(candidate, duckdb.DuckDBPyConnection):  # type: ignore[arg-type]
            return True
        return hasattr(candidate, "execute")

    candidates: list[tuple[str, object]] = []

    primary = getattr(runtime_config, "duckdb", None)
    candidates.append(("duckdb", primary))

    try:
        mapping = runtime_config.as_dict()
        for key, value in mapping.items():
            if key == "duckdb" or _is_duckdb_conn(value):
                candidates.append((key, value))
    except Exception:
        # as_dict is best-effort; ignore if unavailable
        pass

    for key, candidate in candidates:
        if _is_duckdb_conn(candidate):
            return candidate, None

    keys = [key for key, value in candidates if value is not None]
    keys_label = f" (checked {', '.join(keys)})" if keys else ""
    if have_duckdb:
        return None, f"Runtime config has no DuckDB connection{keys_label}"
    return (
        None,
        f"duckdb is not installed and no connection-like value was found{keys_label}",
    )


def _coerce_json_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.decode("utf-8", errors="replace")
    return str(value)


def _duckdb_table_columns(conn: object, table_name: str) -> tuple[list[str], Optional[str]]:
    schema, table = _split_table_identifier(table_name)
    if not table:
        return [], "Table name is empty"

    qualified = _quote_duckdb_identifier(table)
    if schema:
        qualified = f"{_quote_duckdb_identifier(schema)}.{qualified}"

    query = f"PRAGMA table_info({qualified})"
    try:
        cursor = conn.execute(query)
    except Exception as exc:  # noqa: BLE001 - surface DuckDB errors
        return [], f"Unable to read columns for '{table_name}': {exc}"

    rows = cursor.fetchall() if cursor else []
    columns: list[str] = []
    for row in rows:
        name: Optional[str] = None
        for key in ("column_name", "name"):
            try:
                candidate = row[key]  # type: ignore[index]
            except Exception:
                candidate = None
            if isinstance(candidate, str):
                name = candidate
                break
        if name is None and isinstance(row, (list, tuple)) and len(row) > 1 and isinstance(row[1], str):
            name = row[1]
        if name:
            columns.append(name)

    return columns, None


def _preview_duckdb_table(conn: object, table_name: str, limit: int) -> dict[str, Any]:
    schema, table = _split_table_identifier(table_name)
    qualified = _quote_duckdb_identifier(table)
    if schema:
        qualified = f"{_quote_duckdb_identifier(schema)}.{qualified}"

    query = f"SELECT * FROM {qualified} LIMIT {limit}"
    try:
        cursor = conn.execute(query)
    except Exception as exc:  # noqa: BLE001 - surface duckdb binder/runtime errors
        return {"message": f"Unable to query '{table_name}': {exc}"}

    columns = [col[0] for col in cursor.description] if cursor.description else []
    rows = cursor.fetchmany(limit) if cursor else []
    if not rows:
        return {
            "columns": columns,
            "row": [],
            "message": f"Table '{table_name}' is empty",
        }

    return {
        "columns": columns,
        "row": [_coerce_json_value(value) for value in rows[0]],
        "rows": [[_coerce_json_value(value) for value in row] for row in rows],
        "limit": limit,
    }


def _prepare_client_sql(sql: str, limit: int) -> tuple[Optional[str], Optional[str]]:
    cleaned = sql.strip()
    if not cleaned:
        return None, "SQL query is empty"

    # Only allow a single statement (optional trailing semicolon)
    stripped_semicolon = cleaned.rstrip(";")
    if ";" in stripped_semicolon:
        return None, "Only a single SQL statement is allowed for preview"

    target = stripped_semicolon
    has_limit = bool(re.search(r"\blimit\b", target, flags=re.IGNORECASE))
    if not has_limit:
        target = f"{target} LIMIT {limit}"

    return target, None


def _pad_rows_with_requested_columns(
    rows: list[tuple[Any, ...]],
    result_columns: list[str],
    requested_columns: Optional[list[str]],
) -> tuple[list[list[Any]], list[str]]:
    """Pad DuckDB rows to requested columns for consistent display."""
    if not requested_columns:
        padded = [[_coerce_json_value(value) for value in row] for row in rows]
        return padded, result_columns

    normalized = {str(col).lower(): idx for idx, col in enumerate(result_columns)}
    padded_rows: list[list[Any]] = []
    for row in rows:
        padded_row: list[Any] = []
        for col in requested_columns:
            idx = normalized.get(str(col).lower())
            value = row[idx] if idx is not None and idx < len(row) else None
            padded_row.append(_coerce_json_value(value))
        padded_rows.append(padded_row)
    return padded_rows, requested_columns


def _preview_duckdb_sql(
    conn: object, sql: str, limit: int, requested_columns: Optional[list[str]] = None
) -> dict[str, Any]:
    try:
        cursor = conn.execute(sql)
    except Exception as exc:  # noqa: BLE001 - surface precise DuckDB errors
        return {"message": f"Unable to run preview query: {exc}"}

    columns = [col[0] for col in cursor.description] if cursor.description else []
    rows = cursor.fetchmany(limit) if cursor else []
    padded_rows, display_columns = _pad_rows_with_requested_columns(
        rows, columns, requested_columns
    )
    if not rows:
        return {
            "columns": display_columns,
            "row": [],
            "message": "Query returned no rows",
        }

    return {
        "columns": display_columns,
        "row": padded_rows[0] if padded_rows else [],
        "rows": padded_rows,
        "limit": limit,
    }


def get_duckdb_table_columns(config_path: Path, table_name: str) -> dict[str, Any]:
    """Return column metadata for a DuckDB table configured in kptn.yaml."""
    if not config_path.exists():
        raise FileNotFoundError(f"kptn.yaml not found at {config_path}")

    original_dir = Path.cwd()
    project_dir = config_path.parent
    os.chdir(project_dir)

    try:
        kap_conf = read_config()
        tasks_conf = kap_conf.get("tasks", {}) if isinstance(kap_conf, dict) else {}
        config_block = kap_conf.get("config", {}) if isinstance(kap_conf, dict) else {}

        try:
            runtime_config = RuntimeConfig.from_config(
                config_block, base_dir=project_dir
            )
        except RuntimeConfigError as exc:
            return {"message": f"Unable to build runtime config: {exc}"}

        conn, conn_message = _duckdb_connection_from_runtime_config(runtime_config)
        if conn is None:
            return {"message": conn_message or "DuckDB connection unavailable"}

        resolved_table = _resolve_duckdb_output_table(tasks_conf, table_name or "")
        if not resolved_table:
            return {
                "message": "Table is not configured as a DuckDB output in kptn.yaml"
            }

        columns, error = _duckdb_table_columns(conn, resolved_table)
        if error:
            return {"message": error, "resolvedTable": resolved_table}

        return {"columns": columns, "resolvedTable": resolved_table}
    finally:
        os.chdir(original_dir)


def get_duckdb_preview(
    config_path: Path,
    table_name: Optional[str] = None,
    sql: Optional[str] = None,
    limit: int = 5,
    requested_columns: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Return DuckDB sample and metadata for either a configured output or client SQL."""
    if not config_path.exists():
        raise FileNotFoundError(f"kptn.yaml not found at {config_path}")

    original_dir = Path.cwd()
    project_dir = config_path.parent
    os.chdir(project_dir)

    try:
        kap_conf = read_config()
        tasks_conf = kap_conf.get("tasks", {}) if isinstance(kap_conf, dict) else {}
        config_block = kap_conf.get("config", {}) if isinstance(kap_conf, dict) else {}

        try:
            runtime_config = RuntimeConfig.from_config(
                config_block, base_dir=project_dir
            )
        except RuntimeConfigError as exc:
            return {"message": f"Unable to build runtime config: {exc}"}

        conn, conn_message = _duckdb_connection_from_runtime_config(runtime_config)
        if conn is None:
            return {"message": conn_message or "DuckDB connection unavailable"}

        if sql is not None:
            prepared_sql, error = _prepare_client_sql(sql, limit=limit)
            if error:
                return {"message": error}

            preview = _preview_duckdb_sql(
                conn, prepared_sql, limit, requested_columns=requested_columns
            )
            preview["resolvedTable"] = None
            preview["sql"] = prepared_sql
            return preview

        resolved_table = _resolve_duckdb_output_table(tasks_conf, table_name or "")
        if not resolved_table:
            return {
                "message": "Table is not configured as a DuckDB output in kptn.yaml"
            }

        preview = _preview_duckdb_table(conn, resolved_table, limit)
        preview["resolvedTable"] = resolved_table
        return preview
    finally:
        os.chdir(original_dir)


def _get_template_env() -> Environment:
    global _template_env
    if _template_env is None:
        templates_dir = Path(__file__).parent / "templates"
        _template_env = Environment(
            loader=FileSystemLoader(str(templates_dir)),
            autoescape=select_autoescape(["html", "xml"]),
        )
    return _template_env


def _normalize_table_name(value: str) -> Optional[str]:
    cleaned = value.strip()
    if "://" in cleaned:
        cleaned = cleaned.split("://", 1)[1]
    if "." in cleaned:
        cleaned = cleaned.split(".")[-1]
    cleaned = cleaned.lower()
    return cleaned or None


def build_table_file_map(config_path: Path) -> dict[str, str]:
    """Map table/output names to absolute file paths from kptn.yaml tasks."""
    try:
        raw = config_path.read_text(encoding="utf-8")
        doc = yaml.safe_load(raw)
    except Exception:
        return {}

    if not isinstance(doc, dict):
        return {}

    tasks = doc.get("tasks")
    if not isinstance(tasks, dict):
        return {}

    base_dir = config_path.parent
    mapping: dict[str, str] = {}

    for task_name, task_val in tasks.items():
        if not isinstance(task_val, dict):
            continue
        file_entry = task_val.get("file")
        file_path = file_entry.split(":")[0] if isinstance(file_entry, str) else None
        if file_path:
            file_path = (
                file_path
                if Path(file_path).is_absolute()
                else str(base_dir / file_path)
            )

        outputs = (
            task_val.get("outputs") if isinstance(task_val.get("outputs"), list) else []
        )
        for output in outputs:
            if not isinstance(output, str):
                continue
            normalized = _normalize_table_name(output)
            if normalized and file_path:
                mapping[normalized] = file_path

        task_normalized = (
            _normalize_table_name(task_name) if isinstance(task_name, str) else None
        )
        if task_normalized and file_path:
            mapping[task_normalized] = file_path

    return mapping


def render_lineage_page(
    config_path: Path,
    graph: Optional[str],
    base_url: str = "",
    is_webview: bool = False,
    fragment: bool = False,
) -> Tuple[str, int, int]:
    """Render a lineage page via Jinja for web or VS Code webview."""
    lineage_html, tables, edges = generate_lineage_html(config_path, graph)
    table_map = build_table_file_map(config_path)
    static_prefix = f"{base_url}/static" if base_url else "/static"
    env = _get_template_env()
    template = env.get_template("lineage.html.jinja")
    rendered = template.render(
        lineage_html=lineage_html,
        table_map_json=json.dumps(table_map),
        config_path=str(config_path),
        config_path_json=json.dumps(str(config_path)),
        graph=graph or "",
        base_url=base_url,
        base_url_json=json.dumps(base_url),
        is_webview=is_webview,
        static_prefix=static_prefix,
        is_fragment=fragment,
    )
    return rendered, tables, edges


def render_table_preview_fragment(payload: dict[str, Any]) -> str:
    """Render a preview snippet for HTMX swaps."""
    env = _get_template_env()
    template = env.get_template("table_preview.html")
    return template.render(payload=payload)


def render_index_page(config_path: str = "", graph: str = "") -> str:
    """Render a simple landing page with an HTMX form."""
    env = _get_template_env()
    template = env.get_template("index.html")
    root = Path(os.environ.get("KPTN_SERVER_ROOT", Path.cwd()))
    configs = discover_kptn_configs(root)
    return template.render(config_path=config_path, graph=graph, configs=configs, root=root)


def discover_kptn_configs(base_dir: Path, limit: int = 50) -> list[dict[str, str]]:
    """Find kptn.yaml files under a base directory."""
    results: list[dict[str, str]] = []
    for current_root, dirs, files in os.walk(base_dir):
        # prune excluded dirs
        dirs[:] = [d for d in dirs if d not in KPTN_CONFIG_EXCLUDE]
        if "kptn.yaml" in files:
            full_path = Path(current_root) / "kptn.yaml"
            relative = str(full_path.relative_to(base_dir)) if full_path.is_absolute() else str(full_path)
            label = relative
            results.append({"label": label, "path": str(full_path)})
            if len(results) >= limit:
                break
    return results
