# Architecture

**Analysis Date:** 2025-07-10

## Pattern Overview

**Overall:** Multi-component meta-orchestration framework — Python library + FastAPI server + React SPA + VS Code extension

**Key Characteristics:**
- DAG-first: pipelines are Python objects composed with the `>>` operator, not YAML config
- Content-hash caching: tasks are skipped when their output hashes match the stored state
- Protocol-based extensibility: storage backends implement `StateStoreBackend` protocol (SQLite or DuckDB)
- Dual CLI surface: legacy v0.1 YAML-driven commands (`kptn/cli/_v01.py`) coexist with v0.2 Python-native commands (`kptn/cli/commands.py`)
- Multi-runtime task dispatch: Python functions, SQL files, and R scripts all first-class

## Layers

**Public API Layer:**
- Purpose: Surface the composable task DSL to end-users
- Location: `kptn/__init__.py`
- Contains: Re-exports of `task`, `sql_task`, `r_task`, `noop`, `parallel`, `Stage`, `map`, `Pipeline`, `config`, `run`
- Depends on: `kptn/graph/`, `kptn/runner/`
- Used by: User pipeline modules (e.g. `example/example_migration/pipeline.py`)

**Graph / DAG Layer:**
- Purpose: Represent pipeline topology as an immutable directed acyclic graph
- Location: `kptn/graph/`
- Contains: Node types (`nodes.py`), `Graph` dataclass with `>>` operator (`graph.py`), `Pipeline` sentinel wrapper (`pipeline.py`), decorator factories (`decorators.py`), composition helpers (`composition.py`), topological sort (`topo.py`), config nodes (`config.py`)
- Depends on: `kptn/exceptions.py` only
- Used by: `kptn/runner/`, `kptn/cli/`

**Profile / Configuration Layer:**
- Purpose: Load `kptn.yaml`, validate settings, and resolve profile overrides into a `ResolvedGraph`
- Location: `kptn/profiles/`, `kptn/read_config.py`
- Contains: Pydantic schema (`profiles/schema.py`), YAML loader (`profiles/loader.py`), profile resolver (`profiles/resolver.py`), resolved dataclass (`profiles/resolved.py`)
- Depends on: `kptn/graph/`, `pydantic`, `pyyaml`
- Used by: `kptn/runner/api.py`, `kptn/cli/commands.py`

**Execution / Runner Layer:**
- Purpose: Execute a resolved pipeline — topological sort → staleness check → task dispatch
- Location: `kptn/runner/`
- Contains: Public `run()` entrypoint (`runner/api.py`), execution engine (`runner/executor.py`), dry-run planner (`runner/plan.py`)
- Depends on: `kptn/graph/`, `kptn/profiles/`, `kptn/state_store/`, `kptn/change_detector/`, `kptn/exceptions.py`
- Used by: `kptn/cli/commands.py`, `kptn_server/`

**State Persistence Layer:**
- Purpose: Store and retrieve content hashes keyed by `(storage_key, pipeline, task)`
- Location: `kptn/state_store/`
- Contains: `StateStoreBackend` Protocol (`protocol.py`), SQLite backend (`sqlite.py`), DuckDB backend (`duckdb.py`), factory (`factory.py`)
- Depends on: `kptn/exceptions.py`, stdlib only (`sqlite3`, optional `duckdb`)
- Used by: `kptn/runner/executor.py`, `kptn/runner/plan.py`

**Change Detection Layer:**
- Purpose: Hash task output files/tables; compare against stored hashes to decide staleness
- Location: `kptn/change_detector/`
- Contains: File/table hasher (`hasher.py`), staleness detector (`detector.py`)
- Depends on: `kptn/state_store/`, `kptn/graph/nodes.py`, `kptn/exceptions.py`
- Used by: `kptn/runner/executor.py`, `kptn/runner/plan.py`

**CLI Layer:**
- Purpose: `kptn` command-line entrypoint via Typer
- Location: `kptn/cli/`
- Contains: v0.2 commands (`commands.py`), legacy v0.1 commands (`_v01.py`), config validation (`config_validation.py`), task validation (`task_validation.py`), infra commands (`infra_commands.py`), AWS run command (`run_aws.py`), decider bundle (`decider_bundle.py`)
- Depends on: All layers above; `typer`
- Used by: Shell (entrypoint declared in `pyproject.toml` as `kptn = "kptn.cli:app"`)

**Legacy Caching Layer:**
- Purpose: DynamoDB / DuckDB / SQLite caching from v0.1 architecture (being superseded by `state_store/`)
- Location: `kptn/caching/`
- Contains: `TaskStateCache`, `TaskStateDbClient`, `Hasher`, `DbClientDDB`, `DbClientDuckDB`, `DbClientSQLite`, batch utilities
- Depends on: `boto3` (optional), `duckdb` (optional)
- Used by: `kptn/watcher/` (legacy watcher), `kptn/cli/_v01.py`

**Code Generation Layer:**
- Purpose: Render Jinja2 templates to produce flow files (Prefect, vanilla, Step Functions) from YAML config
- Location: `kptn/codegen/`
- Contains: Main codegen driver (`codegen.py`), infra scaffolder (`infra_scaffolder.py`), Jinja env setup (`lib/setup_jinja_env.py`), Step Functions context builder (`lib/stepfunctions.py`)
- Depends on: `kptn/read_config.py`, `jinja2`
- Used by: `kptn/cli/_v01.py`

**Deployment Layer:**
- Purpose: Build Docker images, push to ECR, deploy to Prefect
- Location: `kptn/deploy/`, `kptn/dockerbuild/`
- Contains: ECR image utilities (`ecr_image.py`), Prefect deployment (`prefect_deploy.py`), storage key resolution (`storage_key.py`), auth proxy (`authproxy_endpoint.py`), branch name helper (`get_active_branch_name.py`)
- Depends on: `boto3` (optional), `prefect` (optional)
- Used by: `kptn/cli/_v01.py`, `kptn/watcher/`

**Lineage Layer:**
- Purpose: Parse SQL files to extract table-level data lineage; render as interactive HTML
- Location: `kptn/lineage/`
- Contains: SQL lineage analyzer (`sql_lineage.py`), HTML renderer (`html_renderer.py`)
- Depends on: `kptn/read_config.py`
- Used by: `kptn_server/service.py`, `kptn/cli/_v01.py`

**Server Layer:**
- Purpose: Expose kptn functionality over HTTP (web app) and JSON-RPC (VS Code extension)
- Location: `kptn_server/`
- Contains: FastAPI HTTP routes (`api_http.py`), JSON-RPC over stdin/stdout (`api_jsonrpc.py`), shared service logic (`service.py`), Jinja2 templates (`templates/`), static assets (`static/`)
- Depends on: `kptn/` package, `fastapi`, `uvicorn` (optional `kptn[web]` extra)
- Used by: VS Code extension (spawns `api_jsonrpc.py` as subprocess); web browser (via `api_http.py`)

**Watcher / Local Dev Server Layer:**
- Purpose: FastAPI WebSocket server powering the React UI during local development
- Location: `kptn/watcher/`
- Contains: FastAPI app with WebSocket support (`app.py`), local task enrichment (`local.py`), stack management (`stacks.py`), utilities (`util.py`), file watcher (`filewatcher/`)
- Depends on: `kptn/caching/` (legacy), `fastapi`, `watchfiles`
- Used by: React UI (`ui/`) via HTTP + WebSocket on `localhost:8000`

**React UI Layer:**
- Purpose: Browser-based dashboard for monitoring and triggering pipelines
- Location: `ui/`
- Contains: TanStack Router routes (`src/routes/`), Zustand state store (`src/hooks/use-state.tsx`), AG Grid tables (`src/components/Table.tsx`), shadcn/ui components (`src/components/ui/`), sidebar navigation (`src/components/app-sidebar.tsx`)
- Depends on: `kptn/watcher/` backend via HTTP (`mande` HTTP client, `react-use-websocket`)
- Used by: Browser

**VS Code Extension Layer:**
- Purpose: Inline lineage view and pipeline tree within VS Code
- Location: `kptn-vscode/`
- Contains: Extension entry (`src/extension.ts`), spawns `kptn_server/api_jsonrpc.py` as a child process
- Communicates via: JSON-RPC 2.0 over stdin/stdout
- Depends on: Python runtime with `kptn[web]` installed

## Data Flow

**Pipeline Execution (v0.2 Python API):**
1. User declares tasks with `@kptn.task(outputs=[...])` in a Python module
2. Tasks composed with `>>` operator into a `Graph` or `Pipeline` object
3. `kptn run` CLI reads `pyproject.toml` → locates `[tool.kptn].pipeline` module → imports it
4. `ProfileLoader.load(kptn.yaml)` reads YAML config and profiles
5. `ProfileResolver.compile(pipeline, profile)` produces `ResolvedGraph` with bypassed task names and profile args
6. `init_state_store(settings)` creates `SqliteBackend` or `DuckDbBackend`
7. `execute(resolved, state_store, cwd)` calls `topo_sort()` → Kahn's BFS algorithm
8. For each node in topological order: `is_stale()` checks content hash vs stored hash
9. Stale tasks dispatched: Python fn called directly, SQL via DuckDB/SQLite, R via `subprocess.run(["Rscript", ...])`
10. After success: `write_hash()` stores new content hash in state store

**Web UI Data Flow:**
1. `kptn/watcher/app.py` FastAPI server starts, loads branch/stack/task state on lifespan
2. React UI (`ui/`) calls `GET /api/state` via `mande` HTTP client
3. `useStateStore` (Zustand) receives full state and stores it
4. User triggers task run → WebSocket message sent to watcher server
5. Server executes task, sends status updates over WebSocket
6. UI re-renders task status cells via AG Grid custom cell renderers

**VS Code Lineage Flow:**
1. User triggers `kptn.viewLineage` command in VS Code
2. Extension spawns `kptn-vscode/backend.py` as subprocess (wraps `api_jsonrpc.py`)
3. Extension sends `{"jsonrpc": "2.0", "method": "generateLineageHtml", "params": {...}}` over stdin
4. Backend reads `kptn.yaml`, runs `SqlLineageAnalyzer`, renders HTML via `render_lineage_html()`
5. Backend writes JSON-RPC response to stdout
6. Extension receives response, opens WebView panel with the HTML

**State Management (UI):**
- Zustand store (`ui/src/hooks/use-state.tsx`) holds global app state: branch, storage_key, tasks, stacks
- TanStack Router loaders call `fetchState()` on route load and search param changes
- Task state updates flow through `updateTask()` action (partial record updates)

## Key Abstractions

**`Graph` / `Pipeline`:**
- Purpose: Immutable DAG of task nodes; `Pipeline` adds a named `PipelineNode` sentinel head
- Examples: `kptn/graph/graph.py`, `kptn/graph/pipeline.py`
- Pattern: Dataclass with `__rshift__` operator for sequential composition; `Graph._from_node()` auto-wraps handles

**Node Types (`AnyNode`):**
- Purpose: Typed union representing every kind of work item the runner can dispatch
- Examples: `kptn/graph/nodes.py`
- Pattern: Plain `@dataclass` — `TaskNode` (Python fn), `SqlTaskNode` (`.sql` file), `RTaskNode` (`.R` script), `ParallelNode`, `StageNode`, `NoopNode`, `MapNode`, `PipelineNode`, `ConfigNode`

**Decorator Handles (`_KptnCallable`, `_SqlTaskHandle`, `_RTaskHandle`):**
- Purpose: Thin wrapper returned by `@kptn.task`, `kptn.sql_task()`, `kptn.r_task()` that carries `__kptn__` metadata and enables `>>`
- Examples: `kptn/graph/decorators.py`
- Pattern: Not a node itself — `Graph._from_node()` converts handle → node on first `>>`

**`ResolvedGraph`:**
- Purpose: A `Graph` plus runtime metadata: which tasks are bypassed, profile arg overrides, storage key
- Examples: `kptn/profiles/resolved.py`
- Pattern: `@dataclass` produced by `ProfileResolver.compile(pipeline, profile)`

**`StateStoreBackend` Protocol:**
- Purpose: Abstract interface for task-hash persistence; allows SQLite and DuckDB backends
- Examples: `kptn/state_store/protocol.py`
- Pattern: `typing.Protocol` with `@runtime_checkable`; concrete implementations in `sqlite.py`, `duckdb.py`

**`KptnConfig` / `ProfileSpec` (Pydantic):**
- Purpose: Validated, frozen schema for `kptn.yaml` contents
- Examples: `kptn/profiles/schema.py`
- Pattern: `pydantic.BaseModel` with `model_config = ConfigDict(frozen=True)`

## Entry Points

**CLI (`kptn`):**
- Location: `kptn/cli/__init__.py` → `kptn/cli/_v01.py` (legacy) / `kptn/cli/commands.py` (v0.2)
- Triggers: `kptn run`, `kptn plan`, `kptn validate`, etc. from shell
- Responsibilities: Load pipeline from `pyproject.toml`, resolve profiles, delegate to `runner/api.py`

**FastAPI Watcher Server:**
- Location: `kptn/watcher/app.py`
- Triggers: Started manually or via CLI; listens on `localhost:8000`
- Responsibilities: Serve `GET /api/state`, `WebSocket /ws`, run tasks on demand for the React UI

**FastAPI HTTP Server (`kptn_server`):**
- Location: `kptn_server/api_http.py`
- Triggers: `uvicorn kptn_server.api_http:app`
- Responsibilities: `/lineage`, `/table-preview`, `/healthz` endpoints; serves HTML fragments and JSON

**JSON-RPC Backend (VS Code):**
- Location: `kptn_server/api_jsonrpc.py` (via `kptn-vscode/backend.py` shim)
- Triggers: Spawned as subprocess by VS Code extension
- Responsibilities: Handle `generateLineageHtml`, `getTablePreview` RPC methods over stdin/stdout

**React UI:**
- Location: `ui/src/main.tsx`
- Triggers: Browser; built with `vite build`, dev served with `vite --host`
- Responsibilities: Dashboard rendering, task triggering, lineage display

## Error Handling

**Strategy:** Custom exception hierarchy under `KptnError`; caught at CLI boundary and converted to user-facing messages with non-zero exit codes

**Patterns:**
- `GraphError` — raised by `topo_sort()` on cycles or edge/node mismatches (`kptn/graph/topo.py`)
- `ProfileError` — raised by `ProfileLoader` / `ProfileResolver` on bad YAML or unknown profile names (`kptn/profiles/`)
- `TaskError` — raised by `executor.py` when a dispatched task raises an exception; wraps original exception message
- `StateStoreError` — raised by state store backends on DB failures (`kptn/state_store/`)
- `HashError` — raised by `change_detector/hasher.py` when output files/tables cannot be hashed; treated as "stale" by executor
- HTTP layer: `HTTPException` (FastAPI) used in `kptn_server/api_http.py`; JSON-RPC error envelope used in `kptn_server/api_jsonrpc.py`

## Cross-Cutting Concerns

**Logging:** `logging.getLogger(__name__)` used throughout Python library; `uvicorn.error` logger used in `kptn/watcher/app.py`; structured `console.log` in React UI

**Validation:** Pydantic v2 models for `kptn.yaml` schema; `kptn/cli/config_validation.py` and `kptn/cli/task_validation.py` for legacy YAML config validation

**Authentication:** None in core library; AWS credentials handled by `kptn/aws/creds.py` using boto3; Prefect auth in `kptn/deploy/prefect_deploy.py`

**Optional Dependencies:** `boto3` (AWS), `prefect`, `duckdb`, `fastapi`/`uvicorn` are all optional extras defined in `pyproject.toml`; guarded with try/except ImportError or deferred imports

---

*Architecture analysis: 2025-07-10*
