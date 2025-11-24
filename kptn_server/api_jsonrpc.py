"""JSON-RPC interface for the kptn backend (used by the VS Code extension)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from kptn_server.service import (
    get_duckdb_table_columns,
    get_duckdb_preview,
    render_lineage_page,
    render_table_preview_fragment,
)


def build_response(
    request_id: Any, result: Any = None, error: str | None = None
) -> Dict[str, Any]:
    """Construct a JSON-RPC 2.0 response envelope."""
    if error:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32000, "message": error},
        }

    return {"jsonrpc": "2.0", "id": request_id, "result": result}


LINEAGE_SERVER: Optional[HTTPServer] = None
LINEAGE_PORT: Optional[int] = None


def handle_request(request: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single JSON-RPC request and return a response payload."""
    request_id = request.get("id")
    method = request.get("method")

    if request.get("jsonrpc") != "2.0":
        return build_response(request_id, error="Invalid JSON-RPC version")

    if method == "getMessage":
        from datetime import datetime

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return build_response(
            request_id, result={"message": f"Python backend time: {now}"}
        )

    if method == "generateLineageHtml":
        params = request.get("params") or {}
        config_path = params.get("configPath")
        graph = params.get("graph")
        if not isinstance(config_path, str):
            return build_response(request_id, error="Missing configPath")

        try:
            html, tables_count, edges_count = render_lineage_page(
                Path(config_path),
                graph if isinstance(graph, str) else None,
                base_url=f"http://127.0.0.1:{LINEAGE_PORT}" if LINEAGE_PORT else "",
                is_webview=True,
            )
        except Exception as exc:  # noqa: BLE001 - surface precise cause to the client
            return build_response(
                request_id, error=f"Failed to generate lineage: {exc}"
            )

        return build_response(
            request_id,
            result={"html": html, "tables": tables_count, "edges": edges_count},
        )

    if method == "getTablePreview":
        params = request.get("params") or {}
        config_path = params.get("configPath")
        table_name = params.get("table") or params.get("tableName")
        sql = params.get("sql")
        limit = params.get("limit")
        columns = params.get("columns")
        limit_value = limit if isinstance(limit, int) else 50
        requested_columns = columns if isinstance(columns, list) else None
        if not isinstance(config_path, str) or (
            not isinstance(table_name, str) and not isinstance(sql, str)
        ):
            return build_response(request_id, error="Missing configPath or table/sql")

        try:
            preview = get_duckdb_preview(
                Path(config_path),
                table_name if isinstance(table_name, str) else None,
                sql=sql if isinstance(sql, str) else None,
                limit=limit_value,
                requested_columns=requested_columns,
            )
        except Exception as exc:  # noqa: BLE001 - expose original message for debugging
            return build_response(
                request_id, error=f"Failed to load table details: {exc}"
            )

        return build_response(request_id, result=preview)

    if method == "getTableColumns":
        params = request.get("params") or {}
        config_path = params.get("configPath")
        table_name = params.get("table") or params.get("tableName")
        if not isinstance(config_path, str) or not isinstance(table_name, str):
            return build_response(request_id, error="Missing configPath or table")

        try:
            metadata = get_duckdb_table_columns(Path(config_path), table_name)
        except Exception as exc:  # noqa: BLE001
            return build_response(
                request_id, error=f"Failed to load columns: {exc}"
            )

        return build_response(request_id, result=metadata)

    if method == "getLineageServer":
        if LINEAGE_PORT is None:
            return build_response(request_id, error="Lineage server not available")
        return build_response(
            request_id, result={"baseUrl": f"http://127.0.0.1:{LINEAGE_PORT}"}
        )

    return build_response(request_id, error=f"Unknown method: {method}")


def _generate_lineage_html_from_http(
    config: str, graph: Optional[str]
) -> Tuple[str, int, int]:
    return render_lineage_page(Path(config), graph)


class _LineageHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return  # Silence stdout logging

    def do_GET(self) -> None:  # noqa: N802 - HTTP verb naming
        parsed = urlparse(self.path)
        if parsed.path.startswith("/static/"):
            self._serve_static(parsed.path[len("/static/") :])
            return

        if parsed.path == "/table-preview" or parsed.path == "/table-preview-fragment":
            self._serve_table_preview(parsed, fragment=parsed.path.endswith("fragment"))
            return

        if parsed.path != "/lineage":
            self.send_error(404, "Not Found")
            return

        params = parse_qs(parsed.query)
        config = params.get("configPath", [None])[0]
        graph = params.get("graph", [None])[0]
        if not config:
            self.send_error(400, "Missing configPath")
            return

        try:
            html, _, _ = _generate_lineage_html_from_http(config, graph)
        except Exception as exc:  # noqa: BLE001
            self.send_error(500, f"Failed to build lineage: {exc}")
            return

        encoded = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _serve_static(self, filename: str) -> None:
        static_path = Path(__file__).parent / "static" / filename
        if not static_path.exists():
            self.send_error(404, "Not Found")
            return
        content = static_path.read_bytes()
        self.send_response(200)
        ctype = (
            "application/javascript"
            if static_path.suffix == ".js"
            else "application/octet-stream"
        )
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_table_preview(self, parsed, fragment: bool) -> None:
        params = parse_qs(parsed.query)
        config = params.get("configPath", [None])[0]
        table = params.get("table", [None])[0]
        if not config or not table:
            self.send_error(400, "Missing configPath or table")
            return

        try:
            payload = get_duckdb_preview(Path(config), table)
        except FileNotFoundError as exc:  # noqa: PERF203 - clarity
            self.send_error(404, str(exc))
            return
        except Exception as exc:  # noqa: BLE001
            self.send_error(500, f"Failed to load preview: {exc}")
            return

        if fragment:
            html = render_table_preview_fragment(payload)
            encoded = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
            return

        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def start_lineage_server() -> Tuple[Optional[HTTPServer], Optional[int]]:
    try:
        server = HTTPServer(("127.0.0.1", 0), _LineageHandler)
    except OSError:
        return None, None

    port = server.server_address[1]

    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    return server, port


def main() -> None:
    """Main loop: read JSON lines from stdin and write responses to stdout."""
    global LINEAGE_SERVER, LINEAGE_PORT
    LINEAGE_SERVER, LINEAGE_PORT = start_lineage_server()

    if LINEAGE_PORT is None:
        sys.stderr.write(
            "Failed to start lineage HTTP server; lineage webviews will be unavailable.\n"
        )

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
        except Exception as exc:  # noqa: BLE001 - surface parsing errors directly
            response = build_response(None, error=f"Malformed JSON: {exc}")
        else:
            try:
                response = handle_request(request)
            except Exception as exc:  # noqa: BLE001 - downstream failures get wrapped
                response = build_response(
                    request.get("id"), error=f"Internal error: {exc}"
                )

        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
