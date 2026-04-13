<!-- GSD:project-start source:PROJECT.md -->
## Project

**kptn v0.2.0 — nph-curation Port**

A brownfield milestone: complete the kptn 0.2.0 rewrite so that **nph-curation** runs on it in production.

kptn is a Python-native pipeline orchestration framework (DAG-first, content-hash caching, multi-runtime tasks). The 0.1.x design was YAML-driven codegen; 0.2.0 introduces a composable Python DSL (`@kptn.task`, `>>` operator, `Pipeline`, profiles).

The driving use case is:

```python
load_asa24_raw >> validate_asa24_raw >> load_asa24_reports
```

…where `validate_asa24_raw` is toggleable via a profile (`"*.validate": false`), and disabling it should **bypass** the node (reconnect predecessors directly to successors) — not cascade-kill downstream tasks.
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Python 3.11+ — Core CLI tool (`kptn/`), backend server (`kptn_server/`), AWS Lambda handlers, pipeline execution engine
- TypeScript ~5.6 — React UI (`ui/`), VS Code extension (`kptn-vscode/`)
- R — Pipeline task execution via `Rscript` subprocess calls (supported through `RTaskNode` and `kptn/util/rscript.py`)
- SQL — Pipeline tasks parsed for lineage analysis (`kptn/lineage/sql_lineage.py`)
## Runtime
- Python ≥ 3.11 (required by `pyproject.toml`)
- Node.js (for UI and VS Code extension builds; exact version not pinned)
- Python: `uv` — lockfile `uv.lock` present and committed
- JavaScript: `npm` — `package-lock.json` present at root and `ui/package-lock.json`
## Frameworks
- `typer` ≥ 0.12.5 — CLI interface (`kptn/cli/commands.py`, `kptn/cli/__init__.py`)
- `pydantic` — Config schema validation (`kptn/profiles/schema.py`, throughout)
- `pyyaml` — YAML config parsing (`kptn.yaml` files)
- `fastapi` 0.120.3 — HTTP server for web app and headless testing (`kptn_server/api_http.py`), installed via `kptn[web]` extra
- `uvicorn` 0.38.0 — ASGI server for FastAPI, installed via `kptn[web]` extra
- `watchfiles` 1.1.1 — File watching for dev mode (`kptn/filewatcher/`), installed via `kptn[web]` extra
- `jinja2` 3.1.6 — HTML template rendering for lineage pages (`kptn_server/service.py`, `kptn/lineage/html_renderer.py`)
- `boto3` — AWS SDK for DynamoDB, ECR, S3; installed via `kptn[aws]` extra
- `prefect` 3.4.25 — Workflow orchestration engine; installed via `kptn[prefect]` extra
- `duckdb` 1.4.1 — Embedded analytics database for state store and table previews; installed via `kptn[duckdb]` extra
- `requests` — HTTP client for authproxy and ECR interactions (`kptn/deploy/push.py`, `kptn/deploy/ecr_image.py`)
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
- `vite` 5.4.9 — Frontend build tool and dev server (`ui/vite.config.ts`)
- `@vitejs/plugin-react` 4.3.3 — React fast refresh
- `typescript` ~5.6.2 (UI) / ^5.9.3 (root) — Static typing
- `ruff` 0.14.6 — Python linter and formatter (`[dependency-groups] dev`)
- `ty` — Python type checker (Astral's type checker; dev dependency)
- `pytest` 8.4.2 — Python test runner (`[dependency-groups] dev`)
- `cypress` 15.7.0 — End-to-end browser testing (root `package.json`)
- `eslint` 9.13.0 — JavaScript/TypeScript linter (`ui/eslint.config.js`)
## Key Dependencies
- `kptn/graph/` — Core pipeline graph model; `Graph`, `Pipeline`, `TaskNode`, `RTaskNode`, `SqlTaskNode` etc. define the execution DAG
- `kptn/state_store/` — Pluggable state backend; SQLite (default) or DuckDB (`kptn/state_store/factory.py`)
- `kptn/caching/` — Legacy task state cache; DynamoDB, SQLite, and DuckDB clients for cloud/local execution
- `kptn/runner/executor.py` — Local pipeline execution engine; dispatches Python tasks, R scripts, map tasks
- `kptn_server/api_http.py` — FastAPI web server; lineage visualization, table preview, healthcheck
- `kptn_server/api_jsonrpc.py` — JSON-RPC 2.0 server over stdin/stdout; consumed by VS Code extension
- `hatchling` — Python wheel build backend (`[build-system]` in `pyproject.toml`)
- `amazon/dynamodb-local:latest` — Local DynamoDB emulator via Docker (`docker-compose-ddb.yml`)
## Configuration
- Every kptn project has a `kptn.yaml` in the project root
- Contains `settings` block (`db`, `db_path`) and optional `profiles` blocks
- Loaded by `kptn/profiles/loader.py` using `ProfileLoader.load()`
- Schema defined in `kptn/profiles/schema.py` (`KptnConfig`, `KptnSettings`)
- kptn projects declare their pipeline module in `pyproject.toml` under `[tool.kptn]`
- Loaded dynamically by `kptn/cli/commands.py` `_load_pipeline_from_pyproject()`
- Default: SQLite at `.kptn/kptn.db` (local file, relative to project root)
- Alternative: DuckDB (set `db: duckdb` in `kptn.yaml settings`)
- Cloud: DynamoDB (table name via `DYNAMODB_TABLE_NAME` env var)
- Python wheel: `hatchling` (packages `kptn/` and `kptn_server/`)
- UI: `vite build` outputs to `dist/` (Dockerfile in `ui/Dockerfile`)
- VS Code extension: TypeScript compiled to `kptn-vscode/out/`
## Platform Requirements
- Python ≥ 3.11
- `uv` for Python dependency management
- Node.js + npm for UI and VS Code extension
- Docker (for local DynamoDB via `docker-compose-ddb.yml`)
- R (optional, only required if running R pipeline tasks via `Rscript`)
- AWS (optional): DynamoDB for task state caching, ECR for Docker images, ECS/Batch via Step Functions
- Prefect (optional): workflow orchestration; self-hosted Prefect server or Prefect Cloud
- VS Code 1.100.1+ for the IDE extension (`kptn-vscode/`)
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- Python source: `snake_case.py` — e.g., `task_args.py`, `infra_scaffolder.py`
- Python classes used as modules: `PascalCase.py` where the file is named after its primary class — e.g., `TaskStateCache.py`, `Hasher.py`
- TypeScript/TSX: `PascalCase.tsx` for components — e.g., `Table.tsx`, `DeployBtnRenderer.tsx`; `kebab-case.ts` for hooks — e.g., `use-state.tsx`, `use-grid-api.tsx`
- Test files: `test_<module_name>.py` matching the module under test
- Python: `snake_case` — e.g., `build_task_argument_plan`, `hash_code_for_task`, `_dispatch_task`
- Private helpers: prefixed with `_` — e.g., `_has_stream_handler`, `_normalize_code_hashes`, `_dispatch_r_task`
- TypeScript: `camelCase` — e.g., `useStateStore`, `onGridReady`, `setSelectedRows`
- Python: `snake_case`
- TypeScript: `camelCase`
- Constants: `UPPER_SNAKE_CASE` in Python (e.g., `_NON_EXEC_NODES`)
- Python: `PascalCase` — e.g., `TaskNode`, `GraphError`, `ResolvedGraph`, `FakeDbClient`
- TypeScript interfaces: `PascalCase` — e.g., `AppState`, `Task`
- Internal/private Python classes: prefixed with `_` — e.g., `_KptnCallable`, `_SqlTaskHandle`, `_Sentinel`
## Code Style
- Tool: `ruff` (configured in `pyproject.toml`)
- No explicit line-length config detected; ruff defaults apply
- Trailing commas present in multi-line structures
- Tool: `eslint` with `typescript-eslint` + `eslint-plugin-react-hooks` + `eslint-plugin-react-refresh`
- Config: `ui/eslint.config.js`
- Rule: `react-refresh/only-export-components` at `warn` level
- React hooks rules enforced (`eslint-plugin-react-hooks`)
- All Python public functions have return type annotations (e.g., `-> None`, `-> str | None`, `-> list[AnyNode]`)
- `from __future__ import annotations` present in ~38% of Python files — used in files with complex forward references (especially `kptn/runner/`, `kptn/graph/`, test files)
- Union syntax: modern `X | Y` style (Python 3.10+) in new code
- `TYPE_CHECKING` guard used for circular import avoidance in `kptn/graph/decorators.py`, `kptn/graph/nodes.py`
- `@dataclass` is the preferred way to define data-holding classes — e.g., `TaskNode`, `TaskSpec`, `Graph`, `RTaskSpec`
- `field(default_factory=list)` used for mutable defaults
## Import Organization
- `@` → `./src` (configured in `ui/vite.config.ts` and `ui/tsconfig.json`)
- Use `@/hooks/`, `@/components/`, `@/lib/` for non-relative imports
## Error Handling
- Wrap external calls in specific exception handlers, re-raise as domain exceptions:
- `HashError` and `TaskError` propagated deliberately; other `Exception` types are wrapped
- Errors include context (task name, file path, return code) in the message string
- Optional deps (boto3, duckdb, prefect) use try/except at import time with `= None` fallback:
- React components return `null` early when data is not ready: `if (!state) return null`
- Cypress tests suppress known framework errors via `Cypress.on("uncaught:exception", ...)`
## Logging
- Module-level `logger = logging.getLogger(__name__)` in core execution modules (`executor.py`, etc.)
- `get_logger()` used in higher-level code — checks for Prefect env vars and returns appropriate logger
- Custom `CustomFormatter` adds ANSI colors (disabled when `IS_PROD=1`)
- Log format: `{asctime} {filename}:{lineno} {levelname}: {message}`
- Logger uses `logger.propagate = False` to prevent double-logging
## Comments
- Docstrings on public API classes and functions (e.g., `_KptnCallable`, `noop()`, `task()`)
- Section dividers using `# ─── Section Name ─────` pattern in both source and test files
- Story/AC references in test docstrings: `"""Tasks must execute predecessor before successor (AC-1)."""`
- Inline comments for non-obvious decisions (e.g., `# type: ignore[assignment]` with reason)
- `# noqa: ...` suppressions include justification comments (e.g., `# noqa: A001 — shadows builtin intentionally`)
- Not systematically used in TypeScript; inline comments preferred
- Python docstrings present on public-facing decorators and classes
## Function Design
## Module Design
- Package `__init__.py` files explicitly list `__all__` — e.g., `kptn/__init__.py` and `kptn/state_store/__init__.py`
- `# noqa: F401` on re-export lines to silence unused-import linting
- `kptn/__init__.py` is the public API surface — exposes `task`, `sql_task`, `r_task`, `noop`, `parallel`, `Stage`, `map`, `Pipeline`, `config`, `run`
- Sub-packages expose their own `__init__.py` with `__all__`
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## Pattern Overview
- DAG-first: pipelines are Python objects composed with the `>>` operator, not YAML config
- Content-hash caching: tasks are skipped when their output hashes match the stored state
- Protocol-based extensibility: storage backends implement `StateStoreBackend` protocol (SQLite or DuckDB)
- Dual CLI surface: legacy v0.1 YAML-driven commands (`kptn/cli/_v01.py`) coexist with v0.2 Python-native commands (`kptn/cli/commands.py`)
- Multi-runtime task dispatch: Python functions, SQL files, and R scripts all first-class
## Layers
- Purpose: Surface the composable task DSL to end-users
- Location: `kptn/__init__.py`
- Contains: Re-exports of `task`, `sql_task`, `r_task`, `noop`, `parallel`, `Stage`, `map`, `Pipeline`, `config`, `run`
- Depends on: `kptn/graph/`, `kptn/runner/`
- Used by: User pipeline modules (e.g. `example/example_migration/pipeline.py`)
- Purpose: Represent pipeline topology as an immutable directed acyclic graph
- Location: `kptn/graph/`
- Contains: Node types (`nodes.py`), `Graph` dataclass with `>>` operator (`graph.py`), `Pipeline` sentinel wrapper (`pipeline.py`), decorator factories (`decorators.py`), composition helpers (`composition.py`), topological sort (`topo.py`), config nodes (`config.py`)
- Depends on: `kptn/exceptions.py` only
- Used by: `kptn/runner/`, `kptn/cli/`
- Purpose: Load `kptn.yaml`, validate settings, and resolve profile overrides into a `ResolvedGraph`
- Location: `kptn/profiles/`, `kptn/read_config.py`
- Contains: Pydantic schema (`profiles/schema.py`), YAML loader (`profiles/loader.py`), profile resolver (`profiles/resolver.py`), resolved dataclass (`profiles/resolved.py`)
- Depends on: `kptn/graph/`, `pydantic`, `pyyaml`
- Used by: `kptn/runner/api.py`, `kptn/cli/commands.py`
- Purpose: Execute a resolved pipeline — topological sort → staleness check → task dispatch
- Location: `kptn/runner/`
- Contains: Public `run()` entrypoint (`runner/api.py`), execution engine (`runner/executor.py`), dry-run planner (`runner/plan.py`)
- Depends on: `kptn/graph/`, `kptn/profiles/`, `kptn/state_store/`, `kptn/change_detector/`, `kptn/exceptions.py`
- Used by: `kptn/cli/commands.py`, `kptn_server/`
- Purpose: Store and retrieve content hashes keyed by `(storage_key, pipeline, task)`
- Location: `kptn/state_store/`
- Contains: `StateStoreBackend` Protocol (`protocol.py`), SQLite backend (`sqlite.py`), DuckDB backend (`duckdb.py`), factory (`factory.py`)
- Depends on: `kptn/exceptions.py`, stdlib only (`sqlite3`, optional `duckdb`)
- Used by: `kptn/runner/executor.py`, `kptn/runner/plan.py`
- Purpose: Hash task output files/tables; compare against stored hashes to decide staleness
- Location: `kptn/change_detector/`
- Contains: File/table hasher (`hasher.py`), staleness detector (`detector.py`)
- Depends on: `kptn/state_store/`, `kptn/graph/nodes.py`, `kptn/exceptions.py`
- Used by: `kptn/runner/executor.py`, `kptn/runner/plan.py`
- Purpose: `kptn` command-line entrypoint via Typer
- Location: `kptn/cli/`
- Contains: v0.2 commands (`commands.py`), legacy v0.1 commands (`_v01.py`), config validation (`config_validation.py`), task validation (`task_validation.py`), infra commands (`infra_commands.py`), AWS run command (`run_aws.py`), decider bundle (`decider_bundle.py`)
- Depends on: All layers above; `typer`
- Used by: Shell (entrypoint declared in `pyproject.toml` as `kptn = "kptn.cli:app"`)
- Purpose: DynamoDB / DuckDB / SQLite caching from v0.1 architecture (being superseded by `state_store/`)
- Location: `kptn/caching/`
- Contains: `TaskStateCache`, `TaskStateDbClient`, `Hasher`, `DbClientDDB`, `DbClientDuckDB`, `DbClientSQLite`, batch utilities
- Depends on: `boto3` (optional), `duckdb` (optional)
- Used by: `kptn/watcher/` (legacy watcher), `kptn/cli/_v01.py`
- Purpose: Render Jinja2 templates to produce flow files (Prefect, vanilla, Step Functions) from YAML config
- Location: `kptn/codegen/`
- Contains: Main codegen driver (`codegen.py`), infra scaffolder (`infra_scaffolder.py`), Jinja env setup (`lib/setup_jinja_env.py`), Step Functions context builder (`lib/stepfunctions.py`)
- Depends on: `kptn/read_config.py`, `jinja2`
- Used by: `kptn/cli/_v01.py`
- Purpose: Build Docker images, push to ECR, deploy to Prefect
- Location: `kptn/deploy/`, `kptn/dockerbuild/`
- Contains: ECR image utilities (`ecr_image.py`), Prefect deployment (`prefect_deploy.py`), storage key resolution (`storage_key.py`), auth proxy (`authproxy_endpoint.py`), branch name helper (`get_active_branch_name.py`)
- Depends on: `boto3` (optional), `prefect` (optional)
- Used by: `kptn/cli/_v01.py`, `kptn/watcher/`
- Purpose: Parse SQL files to extract table-level data lineage; render as interactive HTML
- Location: `kptn/lineage/`
- Contains: SQL lineage analyzer (`sql_lineage.py`), HTML renderer (`html_renderer.py`)
- Depends on: `kptn/read_config.py`
- Used by: `kptn_server/service.py`, `kptn/cli/_v01.py`
- Purpose: Expose kptn functionality over HTTP (web app) and JSON-RPC (VS Code extension)
- Location: `kptn_server/`
- Contains: FastAPI HTTP routes (`api_http.py`), JSON-RPC over stdin/stdout (`api_jsonrpc.py`), shared service logic (`service.py`), Jinja2 templates (`templates/`), static assets (`static/`)
- Depends on: `kptn/` package, `fastapi`, `uvicorn` (optional `kptn[web]` extra)
- Used by: VS Code extension (spawns `api_jsonrpc.py` as subprocess); web browser (via `api_http.py`)
- Purpose: FastAPI WebSocket server powering the React UI during local development
- Location: `kptn/watcher/`
- Contains: FastAPI app with WebSocket support (`app.py`), local task enrichment (`local.py`), stack management (`stacks.py`), utilities (`util.py`), file watcher (`filewatcher/`)
- Depends on: `kptn/caching/` (legacy), `fastapi`, `watchfiles`
- Used by: React UI (`ui/`) via HTTP + WebSocket on `localhost:8000`
- Purpose: Browser-based dashboard for monitoring and triggering pipelines
- Location: `ui/`
- Contains: TanStack Router routes (`src/routes/`), Zustand state store (`src/hooks/use-state.tsx`), AG Grid tables (`src/components/Table.tsx`), shadcn/ui components (`src/components/ui/`), sidebar navigation (`src/components/app-sidebar.tsx`)
- Depends on: `kptn/watcher/` backend via HTTP (`mande` HTTP client, `react-use-websocket`)
- Used by: Browser
- Purpose: Inline lineage view and pipeline tree within VS Code
- Location: `kptn-vscode/`
- Contains: Extension entry (`src/extension.ts`), spawns `kptn_server/api_jsonrpc.py` as a child process
- Communicates via: JSON-RPC 2.0 over stdin/stdout
- Depends on: Python runtime with `kptn[web]` installed
## Data Flow
- Zustand store (`ui/src/hooks/use-state.tsx`) holds global app state: branch, storage_key, tasks, stacks
- TanStack Router loaders call `fetchState()` on route load and search param changes
- Task state updates flow through `updateTask()` action (partial record updates)
## Key Abstractions
- Purpose: Immutable DAG of task nodes; `Pipeline` adds a named `PipelineNode` sentinel head
- Examples: `kptn/graph/graph.py`, `kptn/graph/pipeline.py`
- Pattern: Dataclass with `__rshift__` operator for sequential composition; `Graph._from_node()` auto-wraps handles
- Purpose: Typed union representing every kind of work item the runner can dispatch
- Examples: `kptn/graph/nodes.py`
- Pattern: Plain `@dataclass` — `TaskNode` (Python fn), `SqlTaskNode` (`.sql` file), `RTaskNode` (`.R` script), `ParallelNode`, `StageNode`, `NoopNode`, `MapNode`, `PipelineNode`, `ConfigNode`
- Purpose: Thin wrapper returned by `@kptn.task`, `kptn.sql_task()`, `kptn.r_task()` that carries `__kptn__` metadata and enables `>>`
- Examples: `kptn/graph/decorators.py`
- Pattern: Not a node itself — `Graph._from_node()` converts handle → node on first `>>`
- Purpose: A `Graph` plus runtime metadata: which tasks are bypassed, profile arg overrides, storage key
- Examples: `kptn/profiles/resolved.py`
- Pattern: `@dataclass` produced by `ProfileResolver.compile(pipeline, profile)`
- Purpose: Abstract interface for task-hash persistence; allows SQLite and DuckDB backends
- Examples: `kptn/state_store/protocol.py`
- Pattern: `typing.Protocol` with `@runtime_checkable`; concrete implementations in `sqlite.py`, `duckdb.py`
- Purpose: Validated, frozen schema for `kptn.yaml` contents
- Examples: `kptn/profiles/schema.py`
- Pattern: `pydantic.BaseModel` with `model_config = ConfigDict(frozen=True)`
## Entry Points
- Location: `kptn/cli/__init__.py` → `kptn/cli/_v01.py` (legacy) / `kptn/cli/commands.py` (v0.2)
- Triggers: `kptn run`, `kptn plan`, `kptn validate`, etc. from shell
- Responsibilities: Load pipeline from `pyproject.toml`, resolve profiles, delegate to `runner/api.py`
- Location: `kptn/watcher/app.py`
- Triggers: Started manually or via CLI; listens on `localhost:8000`
- Responsibilities: Serve `GET /api/state`, `WebSocket /ws`, run tasks on demand for the React UI
- Location: `kptn_server/api_http.py`
- Triggers: `uvicorn kptn_server.api_http:app`
- Responsibilities: `/lineage`, `/table-preview`, `/healthz` endpoints; serves HTML fragments and JSON
- Location: `kptn_server/api_jsonrpc.py` (via `kptn-vscode/backend.py` shim)
- Triggers: Spawned as subprocess by VS Code extension
- Responsibilities: Handle `generateLineageHtml`, `getTablePreview` RPC methods over stdin/stdout
- Location: `ui/src/main.tsx`
- Triggers: Browser; built with `vite build`, dev served with `vite --host`
- Responsibilities: Dashboard rendering, task triggering, lineage display
## Error Handling
- `GraphError` — raised by `topo_sort()` on cycles or edge/node mismatches (`kptn/graph/topo.py`)
- `ProfileError` — raised by `ProfileLoader` / `ProfileResolver` on bad YAML or unknown profile names (`kptn/profiles/`)
- `TaskError` — raised by `executor.py` when a dispatched task raises an exception; wraps original exception message
- `StateStoreError` — raised by state store backends on DB failures (`kptn/state_store/`)
- `HashError` — raised by `change_detector/hasher.py` when output files/tables cannot be hashed; treated as "stale" by executor
- HTTP layer: `HTTPException` (FastAPI) used in `kptn_server/api_http.py`; JSON-RPC error envelope used in `kptn_server/api_jsonrpc.py`
## Cross-Cutting Concerns
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

| Skill | Description | Path |
|-------|-------------|------|
| bmad-advanced-elicitation | 'Push the LLM to reconsider, refine, and improve its recent output. Use when user asks for deeper critique or mentions a known deeper critique method, e.g. socratic, first principles, pre-mortem, red team.' | `.github/skills/bmad-advanced-elicitation/SKILL.md` |
| bmad-agent-analyst | Strategic business analyst and requirements expert. Use when the user asks to talk to Mary or requests the business analyst. | `.github/skills/bmad-agent-analyst/SKILL.md` |
| bmad-agent-architect | System architect and technical design leader. Use when the user asks to talk to Winston or requests the architect. | `.github/skills/bmad-agent-architect/SKILL.md` |
| bmad-agent-dev | Senior software engineer for story execution and code implementation. Use when the user asks to talk to Amelia or requests the developer agent. | `.github/skills/bmad-agent-dev/SKILL.md` |
| bmad-agent-pm | Product manager for PRD creation and requirements discovery. Use when the user asks to talk to John or requests the product manager. | `.github/skills/bmad-agent-pm/SKILL.md` |
| bmad-agent-tech-writer | Technical documentation specialist and knowledge curator. Use when the user asks to talk to Paige or requests the tech writer. | `.github/skills/bmad-agent-tech-writer/SKILL.md` |
| bmad-agent-ux-designer | UX designer and UI specialist. Use when the user asks to talk to Sally or requests the UX designer. | `.github/skills/bmad-agent-ux-designer/SKILL.md` |
| bmad-brainstorming | 'Facilitate interactive brainstorming sessions using diverse creative techniques and ideation methods. Use when the user says help me brainstorm or help me ideate.' | `.github/skills/bmad-brainstorming/SKILL.md` |
| bmad-check-implementation-readiness | 'Validate PRD, UX, Architecture and Epics specs are complete. Use when the user says "check implementation readiness".' | `.github/skills/bmad-check-implementation-readiness/SKILL.md` |
| bmad-checkpoint-preview | 'LLM-assisted human-in-the-loop review. Make sense of a change, focus attention where it matters, test. Use when the user says "checkpoint", "human review", or "walk me through this change".' | `.github/skills/bmad-checkpoint-preview/SKILL.md` |
| bmad-code-review | 'Review code changes adversarially using parallel review layers (Blind Hunter, Edge Case Hunter, Acceptance Auditor) with structured triage into actionable categories. Use when the user says "run code review" or "review this code"' | `.github/skills/bmad-code-review/SKILL.md` |
| bmad-correct-course | 'Manage significant changes during sprint execution. Use when the user says "correct course" or "propose sprint change"' | `.github/skills/bmad-correct-course/SKILL.md` |
| bmad-create-architecture | 'Create architecture solution design decisions for AI agent consistency. Use when the user says "lets create architecture" or "create technical architecture" or "create a solution design"' | `.github/skills/bmad-create-architecture/SKILL.md` |
| bmad-create-epics-and-stories | 'Break requirements into epics and user stories. Use when the user says "create the epics and stories list"' | `.github/skills/bmad-create-epics-and-stories/SKILL.md` |
| bmad-create-prd | 'Create a PRD from scratch. Use when the user says "lets create a product requirements document" or "I want to create a new PRD"' | `.github/skills/bmad-create-prd/SKILL.md` |
| bmad-create-story | 'Creates a dedicated story file with all the context the agent will need to implement it later. Use when the user says "create the next story" or "create story [story identifier]"' | `.github/skills/bmad-create-story/SKILL.md` |
| bmad-create-ux-design | 'Plan UX patterns and design specifications. Use when the user says "lets create UX design" or "create UX specifications" or "help me plan the UX"' | `.github/skills/bmad-create-ux-design/SKILL.md` |
| bmad-dev-story | 'Execute story implementation following a context filled story spec file. Use when the user says "dev this story [story file]" or "implement the next story in the sprint plan"' | `.github/skills/bmad-dev-story/SKILL.md` |
| bmad-distillator | Lossless LLM-optimized compression of source documents. Use when the user requests to 'distill documents' or 'create a distillate'. | `.github/skills/bmad-distillator/SKILL.md` |
| bmad-document-project | 'Document brownfield projects for AI context. Use when the user says "document this project" or "generate project docs"' | `.github/skills/bmad-document-project/SKILL.md` |
| bmad-domain-research | 'Conduct domain and industry research. Use when the user says wants to do domain research for a topic or industry' | `.github/skills/bmad-domain-research/SKILL.md` |
| bmad-edit-prd | 'Edit an existing PRD. Use when the user says "edit this PRD".' | `.github/skills/bmad-edit-prd/SKILL.md` |
| bmad-editorial-review-prose | 'Clinical copy-editor that reviews text for communication issues. Use when user says review for prose or improve the prose' | `.github/skills/bmad-editorial-review-prose/SKILL.md` |
| bmad-editorial-review-structure | 'Structural editor that proposes cuts, reorganization, and simplification while preserving comprehension. Use when user requests structural review or editorial review of structure' | `.github/skills/bmad-editorial-review-structure/SKILL.md` |
| bmad-generate-project-context | 'Create project-context.md with AI rules. Use when the user says "generate project context" or "create project context"' | `.github/skills/bmad-generate-project-context/SKILL.md` |
| bmad-help | 'Analyzes current state and user query to answer BMad questions or recommend the next skill(s) to use. Use when user asks for help, bmad help, what to do next, or what to start with in BMad.' | `.github/skills/bmad-help/SKILL.md` |
| bmad-index-docs | 'Generates or updates an index.md to reference all docs in the folder. Use if user requests to create or update an index of all files in a specific folder' | `.github/skills/bmad-index-docs/SKILL.md` |
| bmad-market-research | 'Conduct market research on competition and customers. Use when the user says they need market research' | `.github/skills/bmad-market-research/SKILL.md` |
| bmad-party-mode | 'Orchestrates group discussions between installed BMAD agents, enabling natural multi-agent conversations where each agent is a real subagent with independent thinking. Use when user requests party mode, wants multiple agent perspectives, group discussion, roundtable, or multi-agent conversation about their project.' | `.github/skills/bmad-party-mode/SKILL.md` |
| bmad-prfaq | Working Backwards PRFAQ challenge to forge product concepts. Use when the user requests to 'create a PRFAQ', 'work backwards', or 'run the PRFAQ challenge'. | `.github/skills/bmad-prfaq/SKILL.md` |
| bmad-product-brief | Create or update product briefs through guided or autonomous discovery. Use when the user requests to create or update a Product Brief. | `.github/skills/bmad-product-brief/SKILL.md` |
| bmad-qa-generate-e2e-tests | 'Generate end to end automated tests for existing features. Use when the user says "create qa automated tests for [feature]"' | `.github/skills/bmad-qa-generate-e2e-tests/SKILL.md` |
| bmad-quick-dev | 'Implements any user intent, requirement, story, bug fix or change request by producing clean working code artifacts that follow the project''s existing architecture, patterns and conventions. Use when the user wants to build, fix, tweak, refactor, add or modify any code, component or feature.' | `.github/skills/bmad-quick-dev/SKILL.md` |
| bmad-retrospective | 'Post-epic review to extract lessons and assess success. Use when the user says "run a retrospective" or "lets retro the epic [epic]"' | `.github/skills/bmad-retrospective/SKILL.md` |
| bmad-review-adversarial-general | 'Perform a Cynical Review and produce a findings report. Use when the user requests a critical review of something' | `.github/skills/bmad-review-adversarial-general/SKILL.md` |
| bmad-review-edge-case-hunter | 'Walk every branching path and boundary condition in content, report only unhandled edge cases. Orthogonal to adversarial review - method-driven not attitude-driven. Use when you need exhaustive edge-case analysis of code, specs, or diffs.' | `.github/skills/bmad-review-edge-case-hunter/SKILL.md` |
| bmad-shard-doc | 'Splits large markdown documents into smaller, organized files based on level 2 (default) sections. Use if the user says perform shard document' | `.github/skills/bmad-shard-doc/SKILL.md` |
| bmad-sprint-planning | 'Generate sprint status tracking from epics. Use when the user says "run sprint planning" or "generate sprint plan"' | `.github/skills/bmad-sprint-planning/SKILL.md` |
| bmad-sprint-status | 'Summarize sprint status and surface risks. Use when the user says "check sprint status" or "show sprint status"' | `.github/skills/bmad-sprint-status/SKILL.md` |
| bmad-technical-research | 'Conduct technical research on technologies and architecture. Use when the user says they would like to do or produce a technical research report' | `.github/skills/bmad-technical-research/SKILL.md` |
| bmad-validate-prd | 'Validate a PRD against standards. Use when the user says "validate this PRD" or "run PRD validation"' | `.github/skills/bmad-validate-prd/SKILL.md` |
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



## Git Hygiene

`.planning/` is listed in `.gitignore` and must **never** be committed or force-added (`git add -f`). All files under `.planning/` are local-only planning artifacts. If a tool or workflow attempts to stage them, skip that step.

<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
