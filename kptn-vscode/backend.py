"""Minimal long-lived JSON-RPC backend for the kptn VS Code extension.

The daemon communicates over stdin/stdout using a newline-delimited JSON-RPC 2.0
envelope. It currently supports a single method, `getMessage`, for proof-of-concept
purposes.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from kptn.cli import _build_lineage_payload, _infer_lineage_dialect, _task_order_from_graph
from kptn.lineage import SqlLineageAnalyzer, SqlLineageError
from kptn.lineage.html_renderer import render_lineage_html
from kptn.read_config import read_config


def build_response(request_id: Any, result: Any = None, error: str | None = None) -> Dict[str, Any]:
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
    """Process a single request and return a response payload."""
    request_id = request.get("id")
    method = request.get("method")

    if request.get("jsonrpc") != "2.0":
        return build_response(request_id, error="Invalid JSON-RPC version")

    if method == "getMessage":
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return build_response(request_id, result={"message": f"Python backend time: {now}"})

    if method == "generateLineageHtml":
        params = request.get("params") or {}
        config_path = params.get("configPath")
        graph = params.get("graph")
        if not isinstance(config_path, str):
            return build_response(request_id, error="Missing configPath")

        try:
            html, tables_count, edges_count = _generate_lineage_html(Path(config_path), graph if isinstance(graph, str) else None)
        except Exception as exc:  # noqa: BLE001 - surface precise cause to the client
            return build_response(request_id, error=f"Failed to generate lineage: {exc}")

        return build_response(request_id, result={"html": html, "tables": tables_count, "edges": edges_count})

    if method == "getLineageServer":
        if LINEAGE_PORT is None:
            return build_response(request_id, error="Lineage server not available")
        return build_response(request_id, result={"baseUrl": f"http://127.0.0.1:{LINEAGE_PORT}"})

    return build_response(request_id, error=f"Unknown method: {method}")


def _generate_lineage_html(config_path: Path, graph: Optional[str]) -> Tuple[str, int, int]:
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

        return render_lineage_html(tables_payload, lineage_payload), len(tables_payload), len(lineage_payload)
    except (SqlLineageError, FileNotFoundError) as exc:
        raise RuntimeError(str(exc)) from exc
    finally:
        os.chdir(original_dir)


class _LineageHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return  # Silence stdout logging

    def do_GET(self) -> None:  # noqa: N802 - HTTP verb naming
        parsed = urlparse(self.path)
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
            html, _, _ = _generate_lineage_html(Path(config), graph)
        except Exception as exc:  # noqa: BLE001
            self.send_error(500, f"Failed to build lineage: {exc}")
            return

        encoded = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
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
        sys.stderr.write("Failed to start lineage HTTP server; lineage webviews will be unavailable.\n")

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
                response = build_response(request.get("id"), error=f"Internal error: {exc}")

        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
