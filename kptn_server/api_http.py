"""HTTP surface for the kptn backend (for web app or headless testing)."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
except (
    ImportError
) as exc:  # pragma: no cover - import guard for environments without extras
    raise ImportError("Install kptn[web] to use the HTTP API.") from exc

from kptn_server.service import (
    generate_lineage_html,
    get_duckdb_table_columns,
    get_duckdb_preview,
    render_index_page,
    render_lineage_page,
    render_table_preview_fragment,
)

app = FastAPI()
app.mount(
    "/static",
    StaticFiles(directory=Path(__file__).parent / "static"),
    name="static",
)


class TablePreviewQuery(BaseModel):
    configPath: str
    sql: str
    table: Optional[str] = None
    limit: Optional[int] = None
    columns: Optional[list[str]] = None


@app.get("/healthz")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    html = render_index_page()
    return HTMLResponse(content=html)


@app.get("/lineage")
def lineage(configPath: str, graph: Optional[str] = None) -> dict[str, object]:  # noqa: N802 - query param name is user-facing
    try:
        html, tables, edges = generate_lineage_html(Path(configPath), graph)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"html": html, "tables": tables, "edges": edges}


@app.get("/lineage-page", response_class=HTMLResponse)
def lineage_page(configPath: str, graph: Optional[str] = None) -> HTMLResponse:  # noqa: N802 - query param name is user-facing
    try:
        html, _, _ = render_lineage_page(Path(configPath), graph, base_url="")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return HTMLResponse(content=html)


@app.get("/lineage-fragment", response_class=HTMLResponse)
def lineage_fragment(configPath: str, graph: Optional[str] = None) -> HTMLResponse:  # noqa: N802 - query param name is user-facing
    try:
        html, _, _ = render_lineage_page(
            Path(configPath), graph, base_url="", fragment=True
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return HTMLResponse(content=html)


@app.get("/table-preview")
def table_preview(configPath: str, table: str) -> dict[str, object]:  # noqa: N802 - query param name is user-facing
    try:
        return get_duckdb_preview(Path(configPath), table)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/table-preview-fragment", response_class=HTMLResponse)
def table_preview_fragment(configPath: str, table: str) -> HTMLResponse:  # noqa: N802 - query param name is user-facing
    try:
        payload = get_duckdb_preview(Path(configPath), table)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    html = render_table_preview_fragment(payload)
    return HTMLResponse(content=html)


@app.post("/table-preview-query")
def table_preview_query(body: TablePreviewQuery) -> dict[str, object]:
    try:
        return get_duckdb_preview(
            Path(body.configPath),
            body.table,
            sql=body.sql,
            limit=body.limit or 50,
            requested_columns=body.columns,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/table-columns")
def table_columns(configPath: str, table: str) -> dict[str, object]:  # noqa: N802 - query param name is user-facing
    try:
        return get_duckdb_table_columns(Path(configPath), table)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
