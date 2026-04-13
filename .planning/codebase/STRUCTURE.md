# Codebase Structure

**Analysis Date:** 2025-07-10

## Directory Layout

```
kptn/                           # Python library + CLI (pip-installable package)
├── kptn/                       # Core Python package
│   ├── __init__.py             # Public API surface (task, sql_task, r_task, Pipeline, run, ...)
│   ├── exceptions.py           # KptnError hierarchy (GraphError, ProfileError, TaskError, ...)
│   ├── read_config.py          # Reads kptn.yaml from cwd (legacy v0.1 config reader)
│   ├── runner_legacy.py        # Legacy v0.1 runner (superseded by kptn/runner/)
│   ├── aws/                    # AWS credential helpers
│   │   ├── creds.py
│   │   └── decider.py
│   ├── caching/                # Legacy v0.1 caching (DDB/DuckDB/SQLite) — being superseded
│   │   ├── Hasher.py
│   │   ├── TaskStateCache.py
│   │   ├── TaskStateDbClient.py
│   │   ├── TSCacheUtils.py
│   │   ├── batch.py
│   │   ├── models.py
│   │   ├── prefect.py
│   │   ├── r_imports.py
│   │   ├── submit.py
│   │   ├── vanilla.py
│   │   ├── wrapper.py
│   │   └── client/             # DB client implementations (DDB, DuckDB, SQLite)
│   │       ├── DbClientBase.py
│   │       ├── DbClientDDB.py
│   │       ├── DbClientDuckDB.py
│   │       ├── DbClientSQLite.py
│   │       ├── dynamodb/       # DynamoDB-specific query functions
│   │       └── sqlite/         # SQLite-specific query functions
│   ├── change_detector/        # Content-hash based staleness detection
│   │   ├── detector.py         # is_stale() — compares current vs stored hash
│   │   └── hasher.py           # hash_file(), hash_sqlite_table(), hash_duckdb_table()
│   ├── cli/                    # Typer CLI commands
│   │   ├── __init__.py         # Re-exports _v01 for backward compat
│   │   ├── _v01.py             # Legacy v0.1 full command implementation
│   │   ├── commands.py         # v0.2 Python-native commands (run, plan, validate)
│   │   ├── config_validation.py
│   │   ├── task_validation.py
│   │   ├── decider_bundle.py
│   │   ├── infra_commands.py
│   │   └── run_aws.py
│   ├── codegen/                # Jinja2 template-based flow code generation
│   │   ├── codegen.py          # Main driver: renders flow files from YAML config
│   │   ├── infra_scaffolder.py
│   │   └── lib/
│   │       ├── modify_tasks_obj.py
│   │       ├── setup_jinja_env.py
│   │       └── stepfunctions.py
│   ├── deploy/                 # Docker / ECR / Prefect deployment helpers
│   │   ├── authproxy_endpoint.py
│   │   ├── ecr_image.py
│   │   ├── get_active_branch_name.py
│   │   ├── prefect_deploy.py
│   │   ├── push.py
│   │   └── storage_key.py
│   ├── dockerbuild/
│   │   └── dockerbuild.py
│   ├── filewatcher/
│   │   └── filewatcher.py
│   ├── graph/                  # Core DAG model (no external deps except kptn/exceptions.py)
│   │   ├── __init__.py
│   │   ├── nodes.py            # AnyNode union: TaskNode, SqlTaskNode, RTaskNode, etc.
│   │   ├── graph.py            # Graph dataclass with >> operator
│   │   ├── pipeline.py         # Pipeline(Graph) — named pipeline with sentinel PipelineNode
│   │   ├── decorators.py       # @task, sql_task(), r_task() factories + handle classes
│   │   ├── composition.py      # parallel(), Stage(), map() helpers
│   │   ├── topo.py             # topo_sort() via Kahn's algorithm
│   │   └── config.py           # config() node factory + invoke_config()
│   ├── lineage/                # SQL lineage analysis and HTML rendering
│   │   ├── __init__.py
│   │   ├── sql_lineage.py      # SqlLineageAnalyzer
│   │   └── html_renderer.py
│   ├── profiles/               # kptn.yaml profile system
│   │   ├── __init__.py
│   │   ├── schema.py           # Pydantic models: KptnConfig, KptnSettings, ProfileSpec
│   │   ├── loader.py           # ProfileLoader.load() — reads kptn.yaml
│   │   ├── resolver.py         # ProfileResolver.compile() — resolves profile overrides
│   │   └── resolved.py         # ResolvedGraph dataclass
│   ├── quality/                # (empty / placeholder)
│   │   └── __init__.py
│   ├── runner/                 # v0.2 execution engine
│   │   ├── __init__.py
│   │   ├── api.py              # run(pipeline, profile) — public entrypoint
│   │   ├── executor.py         # execute() — dispatch loop
│   │   └── plan.py             # plan() — dry run with staleness output
│   ├── state_store/            # Task hash persistence (Protocol-based)
│   │   ├── __init__.py
│   │   ├── protocol.py         # StateStoreBackend Protocol
│   │   ├── factory.py          # init_state_store() factory
│   │   ├── sqlite.py           # SqliteBackend
│   │   └── duckdb.py           # DuckDbBackend (optional)
│   ├── util/                   # Shared utilities
│   │   ├── compute.py
│   │   ├── filepaths.py
│   │   ├── flow_type.py
│   │   ├── hash.py
│   │   ├── logger.py
│   │   ├── pipeline_config.py
│   │   ├── read_tasks_config.py
│   │   ├── rscript.py
│   │   ├── runtime_config.py
│   │   ├── task_args.py
│   │   └── task_dirs.py
│   └── watcher/                # FastAPI dev server (powers React UI)
│       ├── __init__.py
│       ├── app.py              # FastAPI app: GET /api/state, WebSocket /ws
│       ├── local.py            # Task enrichment + hashing helpers
│       ├── stacks.py           # Stack list reading / auth proxy config
│       └── util.py
├── kptn_server/                # Server package: HTTP API + JSON-RPC (VS Code)
│   ├── __init__.py
│   ├── api_http.py             # FastAPI HTTP routes (lineage, table-preview, healthz)
│   ├── api_jsonrpc.py          # JSON-RPC 2.0 over stdin/stdout for VS Code extension
│   ├── service.py              # Shared service logic (generate_lineage_html, etc.)
│   ├── templates/              # Jinja2 HTML templates
│   │   ├── index.html
│   │   ├── lineage.html.jinja
│   │   └── table_preview.html
│   └── static/                 # Vendored JS (Alpine.js, htmx)
│       ├── alpine.min.js
│       └── htmx.min.js
├── ui/                         # React SPA (separate package; communicates with kptn/watcher/)
│   ├── index.html
│   ├── package.json
│   ├── vite.config.ts (implicit via vite)
│   └── src/
│       ├── main.tsx            # Entry point: TanStack Router setup
│       ├── layout.tsx          # Root layout with sidebar
│       ├── Contexts.tsx        # React Context provider (legacy; Zustand preferred)
│       ├── index.css           # Tailwind base styles
│       ├── ui.d.ts             # Global type declarations
│       ├── routeTree.gen.ts    # Auto-generated by TanStack Router plugin
│       ├── routes/             # File-based routing
│       │   ├── __root.tsx      # Root route: loads state, applies ThemeProvider
│       │   ├── index.lazy.tsx  # Home route
│       │   ├── about.lazy.tsx
│       │   ├── stack.index.tsx # /stack index
│       │   └── stack.$stack.tsx # /stack/:stack dynamic route
│       ├── components/         # Feature components
│       │   ├── app-sidebar.tsx
│       │   ├── Table.tsx       # AG Grid table wrapper
│       │   ├── buildAndDeploy.tsx
│       │   ├── CodeStatusRenderer.tsx
│       │   ├── DataRenderer.tsx
│       │   ├── DeployBtnRenderer.tsx
│       │   ├── InputDataStatusRenderer.tsx
│       │   ├── InputStatusRenderer.tsx
│       │   ├── LanguageIconRenderer.tsx
│       │   ├── LogsIconRenderer.tsx
│       │   ├── MappedRenderer.tsx
│       │   ├── RunTasksBtn.tsx
│       │   ├── WebSocketDemo.tsx
│       │   ├── theme-provider.tsx
│       │   ├── theme-toggle.tsx
│       │   └── ui/             # shadcn/ui primitives (badge, button, dialog, etc.)
│       ├── hooks/              # React hooks + Zustand store
│       │   ├── use-state.tsx   # Zustand store (primary global state)
│       │   ├── use-grid-api.tsx
│       │   ├── use-ignorecache.tsx
│       │   ├── use-mobile.tsx
│       │   └── use-row.tsx
│       ├── lib/
│       │   └── utils.ts        # cn() utility (clsx + tailwind-merge)
│       └── util/
│           └── fetchState.tsx  # fetchState() — calls GET /api/state, hydrates Zustand
├── kptn-vscode/                # VS Code extension (TypeScript)
│   ├── src/
│   │   └── extension.ts       # Extension entry: spawns Python backend, handles commands
│   ├── backend.py              # Shim that imports and runs api_jsonrpc.main()
│   ├── package.json
│   └── tsconfig.json
├── tests/                      # Python test suite (pytest)
│   ├── __init__.py
│   ├── fakes.py                # Fake/stub implementations
│   ├── fixture_constants.py
│   ├── runtime_config_fixtures.py
│   ├── base_db_client_test.py
│   └── test_*.py               # ~40 test modules covering graph, caching, profiles, etc.
├── example/                    # Reference pipeline examples
│   ├── basic/
│   ├── duckdb_example/         # DuckDB pipeline (has pipeline.py with @kptn.task usage)
│   ├── example_migration/      # Migration pipeline (pyproject.toml + pipeline.py + kptn.yaml)
│   ├── minimal/
│   ├── mock_pipeline/
│   ├── nibrs/
│   └── step_example/
├── docs/                       # Documentation source
├── doc-site/                   # Built documentation site
├── dynamodb/                   # DynamoDB local setup files
├── scratch/                    # Scratch/exploratory files (not production)
├── output/                     # Pipeline output artifacts (gitignored)
├── .kptn/                      # kptn state database per-project (gitignored)
├── pyproject.toml              # Python package manifest + kptn tool config
├── package.json                # Root JS package (Cypress E2E tests)
├── cypress.config.ts           # Cypress E2E test config
├── uv.lock                     # uv lockfile
├── kptn-schema.json            # JSON Schema for kptn.yaml validation
└── docker-compose-ddb.yml      # Local DynamoDB docker setup
```

## Directory Purposes

**`kptn/`** (Python package):
- Purpose: Installable Python package; the core library
- Contains: All Python source for the `kptn` package
- Key files: `kptn/__init__.py` (public API), `kptn/graph/` (DAG), `kptn/runner/` (executor), `kptn/cli/` (CLI)

**`kptn/graph/`**:
- Purpose: Pure DAG construction — no I/O, no side effects
- Contains: Node types, Graph/Pipeline dataclasses, decorators, topo sort
- Key files: `kptn/graph/nodes.py`, `kptn/graph/graph.py`, `kptn/graph/decorators.py`

**`kptn/runner/`**:
- Purpose: v0.2 execution engine (replaces `kptn/runner_legacy.py`)
- Contains: `api.py` (public `run()`), `executor.py` (dispatch loop), `plan.py` (dry run)
- Key files: `kptn/runner/api.py`, `kptn/runner/executor.py`

**`kptn/state_store/`**:
- Purpose: Abstract hash persistence; choose between SQLite (default) and DuckDB
- Contains: Protocol + two backend implementations + factory
- Key files: `kptn/state_store/protocol.py`, `kptn/state_store/factory.py`

**`kptn/profiles/`**:
- Purpose: Load and resolve `kptn.yaml` profile configs
- Contains: Pydantic schema, YAML loader, resolver, resolved dataclass
- Key files: `kptn/profiles/schema.py`, `kptn/profiles/resolver.py`

**`kptn/caching/`**:
- Purpose: Legacy v0.1 caching layer (DynamoDB, DuckDB, SQLite)
- Status: Being superseded by `kptn/state_store/`; still used by `kptn/watcher/`

**`kptn_server/`** (Python package):
- Purpose: Web server surfaces — HTTP API for browser, JSON-RPC for VS Code
- Contains: FastAPI app, JSON-RPC handler, Jinja2 templates, static assets
- Key files: `kptn_server/api_http.py`, `kptn_server/api_jsonrpc.py`, `kptn_server/service.py`

**`ui/src/`**:
- Purpose: React SPA source
- Contains: TanStack Router routes, Zustand store, AG Grid tables, shadcn/ui components
- Key files: `ui/src/main.tsx`, `ui/src/hooks/use-state.tsx`, `ui/src/routes/__root.tsx`

**`ui/src/routes/`**:
- Purpose: File-based routing via TanStack Router
- Convention: `__root.tsx` = root layout; `*.lazy.tsx` = lazy-loaded; `*.$param.tsx` = dynamic route

**`ui/src/components/ui/`**:
- Purpose: shadcn/ui component primitives (auto-generated; do not edit manually)

**`kptn-vscode/`**:
- Purpose: VS Code extension TypeScript source + Python backend shim
- Key files: `kptn-vscode/src/extension.ts`, `kptn-vscode/backend.py`

**`tests/`**:
- Purpose: Python pytest test suite
- Contains: Unit tests for graph, caching, profiles, CLI, state store
- Key files: `tests/fakes.py` (test doubles), `tests/fixture_constants.py`

**`example/`**:
- Purpose: Reference pipeline examples showing real-world `@kptn.task` usage
- Key directory: `example/example_migration/` — has full `pipeline.py` + `kptn.yaml` + `pyproject.toml`

## Key File Locations

**Entry Points:**
- `kptn/cli/__init__.py`: CLI entrypoint (`kptn` command, registered in `pyproject.toml`)
- `kptn/cli/commands.py`: v0.2 `run` and `plan` commands
- `kptn/cli/_v01.py`: Legacy v0.1 commands (still default via `__init__.py` re-export)
- `kptn/__init__.py`: Public Python API (`from kptn import task, Pipeline, run`)
- `kptn/watcher/app.py`: FastAPI dev server (for React UI)
- `kptn_server/api_http.py`: FastAPI HTTP server (for web browser / headless)
- `kptn_server/api_jsonrpc.py`: JSON-RPC server (for VS Code extension)
- `ui/src/main.tsx`: React SPA entry
- `kptn-vscode/src/extension.ts`: VS Code extension entry

**Configuration:**
- `pyproject.toml`: Python package config + dev dependencies + pytest config
- `kptn.yaml` (per-project): Pipeline profiles and settings (read from project cwd at runtime)
- `kptn-schema.json`: JSON Schema for `kptn.yaml` validation
- `docker-compose-ddb.yml`: Local DynamoDB setup
- `ui/components.json`: shadcn/ui component config
- `cypress.config.ts`: Cypress E2E test config

**Core Logic:**
- `kptn/graph/graph.py`: `Graph` class with `>>` operator
- `kptn/graph/decorators.py`: `@task`, `sql_task()`, `r_task()` — the user-facing DSL
- `kptn/runner/executor.py`: Main task dispatch loop
- `kptn/runner/api.py`: `run()` — the top-level public function
- `kptn/state_store/protocol.py`: `StateStoreBackend` Protocol (extension point)
- `kptn/profiles/schema.py`: Pydantic models for `kptn.yaml`
- `kptn_server/service.py`: Shared server service logic

**Testing:**
- `tests/` — Python unit tests
- `example/step_example/tests/` — Additional example-level tests (also in pytest testpaths)
- `cypress/` — E2E browser tests (Cypress)

## Naming Conventions

**Python Files:**
- `snake_case.py` for most modules: `read_config.py`, `change_detector.py`, `task_args.py`
- `PascalCase.py` for class-centric modules in `caching/`: `Hasher.py`, `TaskStateCache.py`, `DbClientDDB.py`
- `_v01.py` prefix convention for versioned implementation files

**Python Classes:**
- `PascalCase` throughout: `Graph`, `Pipeline`, `TaskNode`, `ResolvedGraph`, `ProfileLoader`
- Internal implementation classes prefixed with `_`: `_KptnCallable`, `_SqlTaskHandle`, `_RTaskHandle`

**Python Functions:**
- `snake_case` throughout: `topo_sort()`, `is_stale()`, `init_state_store()`, `hash_file()`
- Private helpers prefixed with `_`: `_to_task_node()`, `_dispatch_task()`, `_hash_outputs()`

**TypeScript / React Files:**
- `PascalCase.tsx` for components: `Table.tsx`, `DataRenderer.tsx`, `RunTasksBtn.tsx`
- `kebab-case.tsx` for shadcn/ui primitives: `ui/badge.tsx`, `ui/button.tsx`
- `use-kebab-case.tsx` for hooks: `use-state.tsx`, `use-grid-api.tsx`, `use-mobile.tsx`
- `camelCase.tsx` for utilities: `fetchState.tsx`

**Route Files (TanStack Router):**
- `__root.tsx` — root layout route
- `index.lazy.tsx` — index lazy route
- `routeName.lazy.tsx` — lazy-loaded route
- `routeName.$param.tsx` — dynamic parameter route

**Directories:**
- Python: `snake_case/` for all subdirectories: `change_detector/`, `state_store/`, `caching/client/`
- React: `kebab-case/` not used; flat `src/components/`, `src/hooks/`, `src/routes/`, `src/util/`

## Where to Add New Code

**New Python task type (e.g., a dbt task):**
- Add node dataclass to `kptn/graph/nodes.py` (extend `AnyNode` union)
- Add handle class + factory function to `kptn/graph/decorators.py`
- Export from `kptn/__init__.py`
- Add dispatch case to `kptn/runner/executor.py`
- Add plan case to `kptn/runner/plan.py`

**New CLI command (v0.2):**
- Add `@app.command()` function to `kptn/cli/commands.py`
- If it needs new shared logic, add to a new file in `kptn/cli/`

**New state store backend:**
- Implement `StateStoreBackend` protocol from `kptn/state_store/protocol.py`
- Add file at `kptn/state_store/<backend>.py`
- Register in `kptn/state_store/factory.py`

**New API endpoint (HTTP):**
- Add route to `kptn_server/api_http.py`
- Add shared service logic to `kptn_server/service.py`

**New API method (JSON-RPC for VS Code):**
- Add handler branch in `kptn_server/api_jsonrpc.py`:`handle_request()`

**New React UI feature:**
- Feature components: `ui/src/components/<FeatureName>.tsx`
- New route: `ui/src/routes/<routeName>.lazy.tsx` (TanStack Router picks up automatically)
- Shared hooks: `ui/src/hooks/use-<name>.tsx`
- New AG Grid cell renderers follow pattern of existing: `ui/src/components/<Name>Renderer.tsx`

**New shadcn/ui primitive:**
- Add via `shadcn` CLI; goes to `ui/src/components/ui/<name>.tsx`

**Python tests:**
- All new tests in `tests/test_<area>.py`
- Test doubles / fakes in `tests/fakes.py`
- Shared constants in `tests/fixture_constants.py`

**New pipeline example:**
- Create directory under `example/<example_name>/`
- Must include: `pipeline.py` (with `@kptn.task` definitions and a `pipeline` attribute), `kptn.yaml`, `pyproject.toml` with `[tool.kptn]` section

## Special Directories

**`.kptn/`:**
- Purpose: Stores the SQLite/DuckDB state database at runtime (default path `.kptn/kptn.db`)
- Generated: Yes (at pipeline run time)
- Committed: No (gitignored per-project)

**`output/`:**
- Purpose: Default output artifact directory used in examples
- Generated: Yes
- Committed: No

**`dist/`:**
- Purpose: Python build output (`hatchling`)
- Generated: Yes
- Committed: No

**`ui/src/routeTree.gen.ts`:**
- Purpose: Auto-generated by TanStack Router Vite plugin from `ui/src/routes/` file tree
- Generated: Yes
- Committed: Yes (type safety requires it)

**`__pycache__/`:**
- Purpose: Python bytecode cache
- Generated: Yes
- Committed: No

**`_bmad/`, `_bmad-output/`:**
- Purpose: BMAD AI planning framework artifacts (project planning docs)
- Generated: Partially (human-curated)
- Committed: Yes

**`.planning/`:**
- Purpose: GSD codebase analysis documents (this directory)
- Generated: Yes (by GSD mapper)
- Committed: Yes

---

*Structure analysis: 2025-07-10*
