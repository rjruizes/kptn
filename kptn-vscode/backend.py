"""Minimal long-lived JSON-RPC backend for the kptn VS Code extension.

The daemon communicates over stdin/stdout using a newline-delimited JSON-RPC 2.0
envelope. It currently supports a single method, `getMessage`, for proof-of-concept
purposes.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any, Dict


def build_response(request_id: Any, result: Any = None, error: str | None = None) -> Dict[str, Any]:
    """Construct a JSON-RPC 2.0 response envelope."""
    if error:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32000, "message": error},
        }

    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def handle_request(request: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single request and return a response payload."""
    request_id = request.get("id")
    method = request.get("method")

    if request.get("jsonrpc") != "2.0":
        return build_response(request_id, error="Invalid JSON-RPC version")

    if method == "getMessage":
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return build_response(request_id, result={"message": f"Python backend time: {now}"})

    return build_response(request_id, error=f"Unknown method: {method}")


def main() -> None:
    """Main loop: read JSON lines from stdin and write responses to stdout."""
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
