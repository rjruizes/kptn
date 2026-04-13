# Technology Stack

**Analysis Date:** 2025-01-31

## Languages

**Primary:**
- Python 3.11+ — Core CLI tool (`kptn/`), backend server (`kptn_server/`), AWS Lambda handlers, pipeline execution engine
- TypeScript ~5.6 — React UI (`ui/`), VS Code extension (`kptn-vscode/`)

**Secondary:**
- R — Pipeline task execution via `Rscript` subprocess calls (supported through `RTaskNode` and `kptn/util/rscript.py`)
- SQL — Pipeline tasks parsed for lineage analysis (`kptn/lineage/sql_lineage.py`)

## Runtime

**Environment:**
- Python ≥ 3.11 (required by `pyproject.toml`)
- Node.js (for UI and VS Code extension builds; exact version not pinned)

**Package Manager:**
- Python: `uv` — lockfile `uv.lock` present and committed
- JavaScript: `npm` — `package-lock.json` present at root and `ui/package-lock.json`

## Frameworks

**Core Python:**
- `typer` ≥ 0.12.5 — CLI interface (`kptn/cli/commands.py`, `kptn/cli/__init__.py`)
- `pydantic` — Config schema validation (`kptn/profiles/schema.py`, throughout)
- `pyyaml` — YAML config parsing (`kptn.yaml` files)
- `fastapi` 0.120.3 — HTTP server for web app and headless testing (`kptn_server/api_http.py`), installed via `kptn[web]` extra
- `uvicorn` 0.38.0 — ASGI server for FastAPI, installed via `kptn[web]` extra
- `watchfiles` 1.1.1 — File watching for dev mode (`kptn/filewatcher/`), installed via `kptn[web]` extra
- `jinja2` 3.1.6 — HTML template rendering for lineage pages (`kptn_server/service.py`, `kptn/lineage/html_renderer.py`)

**Optional / Integration Extras:**
- `boto3` — AWS SDK for DynamoDB, ECR, S3; installed via `kptn[aws]` extra
- `prefect` 3.4.25 — Workflow orchestration engine; installed via `kptn[prefect]` extra
- `duckdb` 1.4.1 — Embedded analytics database for state store and table previews; installed via `kptn[duckdb]` extra
- `requests` — HTTP client for authproxy and ECR interactions (`kptn/deploy/push.py`, `kptn/deploy/ecr_image.py`)

**Frontend (UI — `ui/`):**
- React 18.3.1 — Component framework
- `@tanstack/react-router` 1.76.1 — Client-side routing
- `zustand` 5.0.0 — State management
- `zod` 3.23.8 — Runtime schema validation
- `ag-grid-react` 32.3.0 — Data grid for table previews
- Radix UI primitives (`@radix-ui/react-*`) — Accessible headless UI components
- `tailwindcss` 3.4.14 — Utility CSS framework
- `lucide-react` / Font Awesome — Icons
- `react-use-websocket` 4.10.1 — WebSocket client
- `mande` 2.0.9 — HTTP client wrapper

**Build / Dev Tools:**
- `vite` 5.4.9 — Frontend build tool and dev server (`ui/vite.config.ts`)
- `@vitejs/plugin-react` 4.3.3 — React fast refresh
- `typescript` ~5.6.2 (UI) / ^5.9.3 (root) — Static typing
- `ruff` 0.14.6 — Python linter and formatter (`[dependency-groups] dev`)
- `ty` — Python type checker (Astral's type checker; dev dependency)
- `pytest` 8.4.2 — Python test runner (`[dependency-groups] dev`)
- `cypress` 15.7.0 — End-to-end browser testing (root `package.json`)
- `eslint` 9.13.0 — JavaScript/TypeScript linter (`ui/eslint.config.js`)

## Key Dependencies

**Critical:**
- `kptn/graph/` — Core pipeline graph model; `Graph`, `Pipeline`, `TaskNode`, `RTaskNode`, `SqlTaskNode` etc. define the execution DAG
- `kptn/state_store/` — Pluggable state backend; SQLite (default) or DuckDB (`kptn/state_store/factory.py`)
- `kptn/caching/` — Legacy task state cache; DynamoDB, SQLite, and DuckDB clients for cloud/local execution
- `kptn/runner/executor.py` — Local pipeline execution engine; dispatches Python tasks, R scripts, map tasks
- `kptn_server/api_http.py` — FastAPI web server; lineage visualization, table preview, healthcheck
- `kptn_server/api_jsonrpc.py` — JSON-RPC 2.0 server over stdin/stdout; consumed by VS Code extension

**Infrastructure:**
- `hatchling` — Python wheel build backend (`[build-system]` in `pyproject.toml`)
- `amazon/dynamodb-local:latest` — Local DynamoDB emulator via Docker (`docker-compose-ddb.yml`)

## Configuration

**Project Config (`kptn.yaml`):**
- Every kptn project has a `kptn.yaml` in the project root
- Contains `settings` block (`db`, `db_path`) and optional `profiles` blocks
- Loaded by `kptn/profiles/loader.py` using `ProfileLoader.load()`
- Schema defined in `kptn/profiles/schema.py` (`KptnConfig`, `KptnSettings`)

**Pipeline Discovery:**
- kptn projects declare their pipeline module in `pyproject.toml` under `[tool.kptn]`
  ```toml
  [tool.kptn]
  pipeline = "my_package.pipeline"
  ```
- Loaded dynamically by `kptn/cli/commands.py` `_load_pipeline_from_pyproject()`

**State Store:**
- Default: SQLite at `.kptn/kptn.db` (local file, relative to project root)
- Alternative: DuckDB (set `db: duckdb` in `kptn.yaml settings`)
- Cloud: DynamoDB (table name via `DYNAMODB_TABLE_NAME` env var)

**Build:**
- Python wheel: `hatchling` (packages `kptn/` and `kptn_server/`)
- UI: `vite build` outputs to `dist/` (Dockerfile in `ui/Dockerfile`)
- VS Code extension: TypeScript compiled to `kptn-vscode/out/`

## Platform Requirements

**Development:**
- Python ≥ 3.11
- `uv` for Python dependency management
- Node.js + npm for UI and VS Code extension
- Docker (for local DynamoDB via `docker-compose-ddb.yml`)
- R (optional, only required if running R pipeline tasks via `Rscript`)

**Production:**
- AWS (optional): DynamoDB for task state caching, ECR for Docker images, ECS/Batch via Step Functions
- Prefect (optional): workflow orchestration; self-hosted Prefect server or Prefect Cloud
- VS Code 1.100.1+ for the IDE extension (`kptn-vscode/`)

---

*Stack analysis: 2025-01-31*
