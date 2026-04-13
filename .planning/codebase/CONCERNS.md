# Codebase Concerns

**Analysis Date:** 2025-01-27

---

## Tech Debt

**Legacy CLI monolith (`_v01.py`):**
- Issue: 1,118-line god-file contains all v0.1.x CLI logic, exported via wildcard `import *`. The `cli/__init__.py` comment explicitly notes "This file will be replaced by the v0.2.0 thin shell in Story 5.x."
- Files: `kptn/cli/_v01.py`, `kptn/cli/__init__.py`
- Impact: High coupling; every CLI command change risks regressions across unrelated commands; wildcard export pollutes namespace; `from kptn.cli._v01 import *` makes imports opaque.
- Fix approach: Continue migration to `kptn/cli/commands.py`; stub each command in thin `commands.py` and remove from `_v01.py` incrementally.

**Version mismatch (`__version__` stale at `"0.1.0"`):**
- Issue: `kptn/__init__.py` declares `__version__ = "0.1.0"` while `pyproject.toml` declares `version = "0.1.17"` and the active development branch targets v0.2.0.
- Files: `kptn/__init__.py`, `pyproject.toml`
- Impact: Tools relying on `kptn.__version__` (e.g., the VS Code extension) will report the wrong version. Deferred explicitly across Stories 1.2–1.5 in `_bmad-output/implementation-artifacts/deferred-work.md`.
- Fix approach: Single-source version from `pyproject.toml` via `importlib.metadata.version("kptn")`.

**`TaskStateCache` god-class (1,755 lines):**
- Issue: Monolithic class handles caching, hashing, task execution orchestration, Prefect integration, and DynamoDB/SQLite client selection.
- Files: `kptn/caching/TaskStateCache.py`
- Impact: Extremely difficult to test in isolation; any change risks broad side effects; parallel new runner (`kptn/runner/`) exists but the legacy cache is still wired to the v0.1.x CLI and cloud paths.
- Fix approach: Continue replacing with dedicated modules (`kptn/state_store/`, `kptn/change_detector/`, `kptn/runner/`). The cache class is being superseded but not yet deleted.

**Duplicate `pyproject.toml` parsing in `commands.py`:**
- Issue: `_load_graph_from_pyproject` and `_load_pipeline_from_pyproject` independently open and parse the same `pyproject.toml` file.
- Files: `kptn/cli/commands.py`
- Impact: Drift risk — config key names could diverge; minor double-IO on every invocation.
- Fix approach: Extract a single `_load_pyproject_config()` helper.

**`AnyNode` union and `isinstance` tuples manually mirrored:**
- Issue: `AnyNode` type alias in `kptn/graph/nodes.py` and the `isinstance()` dispatch tuples in `kptn/graph/graph.py`, `kptn/runner/executor.py`, `kptn/runner/plan.py` must all be updated when a new node type is added.
- Files: `kptn/graph/nodes.py`, `kptn/graph/graph.py`, `kptn/runner/executor.py`, `kptn/runner/plan.py`
- Impact: Adding a new node type requires 4+ coordinated edits; missing one causes silent fallthrough.
- Fix approach: Derive `_EXEC_NODE_TYPES` from `AnyNode.__args__` or introduce a node registry.

**`PipelineConfig` hardcoded default path:**
- Issue: `PipelineConfig.TASKS_CONFIG_PATH` defaults to `"/code/tests/mock_pipeline/kptn.yaml"` — an absolute path that only exists inside the Docker container used for v0.1.x cloud runs.
- Files: `kptn/util/pipeline_config.py` (line 115)
- Impact: Any code that instantiates `PipelineConfig()` without explicitly setting `TASKS_CONFIG_PATH` will silently attempt to read a non-existent file on developer machines.
- Fix approach: Default to `""` or `None` and raise on missing value rather than a container-specific path.

**`DATA_YEAR` hardcoded default:**
- Issue: `PipelineConfig.DATA_YEAR` defaults to `"2022"` (lines 108, 222 of `kptn/util/pipeline_config.py`).
- Files: `kptn/util/pipeline_config.py`, `kptn/util/rscript.py`
- Impact: Pipelines that don't explicitly override `DATA_YEAR` will silently process the wrong year's data.
- Fix approach: Remove default; require explicit configuration or read from environment variable.

---

## Known Bugs

**`_compute_hash_for_map_item` ignores `item_task_name` parameter:**
- Symptoms: All items in a `MapNode` map hash the same static `spec.outputs` paths — per-item output isolation is only provided by the state store key, not the hash. Two `MapNode` items writing to the same output file cannot both be cached correctly.
- Files: `kptn/runner/executor.py` (line ~83–100)
- Trigger: Any `kptn.map()` usage where tasks write to per-item output paths.
- Workaround: None. Documented in `deferred-work.md` as "Known v0.2.0 limitation."

**Bypassed task not added to `runtime_ctx`:**
- Symptoms: A `MapNode` with `over="bypassed_task.items"` silently resolves to an empty list `[]` when `bypassed_task` was skipped by a `start_from` cursor.
- Files: `kptn/runner/executor.py` (line ~121–145)
- Trigger: Profile with `start_from` that bypasses the task feeding a `MapNode`.
- Workaround: None. Documented in `deferred-work.md`.

**Orphaned `StageNode` when all branches deactivated:**
- Symptoms: `stage_selections: {"ds": []}` removes all branches; `StageNode` sentinel survives with no outgoing edges; runner receives a topologically broken graph with a dangling source node.
- Files: `kptn/profiles/resolver.py` (lines 97–101)
- Trigger: Profile that disables all branches of a `Stage`.
- Workaround: Avoid empty `stage_selections`. Fix belongs in `_prune()`.

**`state_store.read_hash` / `write_hash` exceptions unhandled in `execute()`:**
- Symptoms: A locked or corrupted state DB causes an unhandled `StateStoreError` propagating as a raw traceback instead of a clean error message.
- Files: `kptn/runner/executor.py` (lines 145, 168, 182, 195), `kptn/runner/plan.py`
- Trigger: Concurrent access, disk full, or corrupted `.kptn/kptn.db`.
- Workaround: None; deferred in `deferred-work.md`.

**`StateStoreError` / `GraphError` uncaught in `plan()` command:**
- Symptoms: Cyclic graph or corrupt DB produces raw Python traceback at the CLI instead of a user-friendly message.
- Files: `kptn/runner/plan.py`, `kptn/cli/commands.py`
- Trigger: `kptn plan` with a cyclic graph or corrupt state store.
- Workaround: None; deferred in `deferred-work.md`.

---

## Security Considerations

**`shell=True` subprocess with interpolated credentials in `push.py`:**
- Risk: `subprocess.run(f"{login_cmd}; {tag_cmd}; {push_cmd}", shell=True)` where `login_cmd` contains an ECR Docker password inline. Passwords appear in process listing (`ps aux`) and shell history. Shell injection is possible if any of the interpolated values contain shell metacharacters.
- Files: `kptn/deploy/push.py` (line 46)
- Current mitigation: None.
- Recommendations: Use `subprocess.run([...], shell=False)` with a list of arguments. Pass the password via stdin (`--password-stdin`) rather than `-p`.

**Arbitrary module import from user-controlled `pyproject.toml`:**
- Risk: `importlib.import_module(pipeline_module)` in `_load_pipeline_from_pyproject` executes arbitrary Python code from a value read out of `pyproject.toml`. Any project that kptn is run against could execute malicious code on `kptn run` / `kptn plan`.
- Files: `kptn/cli/commands.py` (line 35), `kptn/cli/_v01.py` (same pattern)
- Current mitigation: None. Explicitly noted in `deferred-work.md` as a pre-existing pattern.
- Recommendations: Sandbox execution or at minimum validate module name against an allowlist/path constraint.

**Unauthenticated HTTP API accepts arbitrary filesystem paths (`configPath`):**
- Risk: Every HTTP endpoint in the FastAPI server accepts a raw `configPath: str` query parameter that is converted directly to `Path(configPath)` and used to read files from disk. There is no path traversal guard or allowlist. An attacker with network access to the server can read any file accessible to the process.
- Files: `kptn_server/api_http.py` (lines 55, 65, 75, 87, 97, 126), `kptn_server/service.py`
- Current mitigation: Server is intended as a local dev tool only (spawned by VS Code extension). No auth layer present.
- Recommendations: Pin `configPath` to a project root allowlist; add authentication if ever exposed beyond localhost.

**`os.chdir()` used as a state mutation for path resolution:**
- Risk: `service.py` and `_v01.py` use `os.chdir(project_dir)` to change the process working directory before calling `read_config()`. This is not thread-safe — concurrent FastAPI requests will race on the global CWD, causing one request to read the wrong project's config.
- Files: `kptn_server/service.py` (lines 37, 319, 365), `kptn/cli/_v01.py` (10+ occurrences), `kptn/cli/infra_commands.py` (line 683)
- Current mitigation: try/finally restores CWD, but offers no protection under concurrency.
- Recommendations: Pass `cwd` as an explicit argument to `read_config()` rather than mutating the process-global CWD.

---

## Performance Bottlenecks

**`fetchall()` loads entire SQLite table columns into memory for hashing:**
- Problem: `hash_sqlite_table` in the change detector issues per-column `SELECT` queries and calls `fetchall()`, loading entire columns into Python memory before hashing.
- Files: `kptn/change_detector/hasher.py`
- Cause: Spec-defined algorithm; no chunked streaming.
- Improvement path: Stream rows in chunks (`fetchmany(N)`) and hash incrementally. Noted in `deferred-work.md` as a concern for >1M row tables.

**No performance regression guard for large graphs:**
- Problem: The `plan` command AC-1 specifies a ≤1 second NFR for an 80+ task graph, but no timing test exists. The largest test fixture has 2 nodes.
- Files: `tests/test_runner_plan.py`
- Cause: AC acceptance test only covers correctness; NFR timing guard explicitly deferred.
- Improvement path: Add a single benchmark test with a synthetic 100-node chain.

**Duplicate stage branch refs checked twice in profile validation:**
- Problem: `_resolve()` merges `stage_selections` additively without deduplication; `["A"] + ["A"]` causes each ref to be validated twice per `_validate_stage_refs` call.
- Files: `kptn/profiles/resolver.py`
- Cause: Pre-existing additive merge design. No correctness impact; only extra work.
- Improvement path: Deduplicate branch ref list before calling `_validate_stage_refs`.

---

## Fragile Areas

**`ProfileResolver._apply_cursors()` — duplicate node names silently target wrong node:**
- Files: `kptn/profiles/resolver.py` (`_apply_cursors`)
- Why fragile: `name_to_node` is a plain dict; duplicate node names silently resolve to the last topological occurrence, causing `start_from`/`stop_after` cursors to target the wrong node with no error.
- Safe modification: Enforce unique node names at `Graph.__rshift__` or `Pipeline` construction time before this function is called.
- Test coverage: No test exercises duplicate-named node cursor resolution.

**`ProfileResolver._prune()` — optional group key mismatch prunes silently:**
- Files: `kptn/profiles/resolver.py` (line ~107)
- Why fragile: `_prune` constructs lookup keys as `f"*.{opt}"`; a user config using `{my_group: true}` instead of `{*.my_group: true}` causes `.get()` to return `False`, silently pruning all tasks in that group.
- Safe modification: Add config key validation in the profile schema loader before `_prune` is called.
- Test coverage: No test for mismatched optional group key format.

**`watcher/app.py` — WebSocket broadcast non-atomic under error:**
- Files: `kptn/watcher/app.py` (lines 73–88)
- Why fragile: `broadcast()` iterates `active_connections` and catches `Exception` per connection, but a failed mid-broadcast leaves some clients updated and some not. `close_all_connections` similarly catches and logs but continues, leaving `active_connections` in an inconsistent state if `disconnect()` raises.
- Safe modification: Build a copy of `active_connections` before iterating; track and retry failed broadcasts.
- Test coverage: No tests for the watcher WebSocket layer.

**`_v01.py` wildcard export — namespace pollution:**
- Files: `kptn/cli/__init__.py`, `kptn/cli/_v01.py`
- Why fragile: `from kptn.cli._v01 import *` makes it impossible to know what names are in scope without reading all 1,118 lines; any name added to `_v01.py` is silently re-exported.
- Safe modification: Use explicit named imports; do not add new functionality to `_v01.py`.
- Test coverage: N/A — structural issue, not a runtime bug.

**`SqliteBackend` — no `check_same_thread=False`:**
- Files: `kptn/state_store/sqlite.py` (line 12)
- Why fragile: `sqlite3.connect(path)` uses the default `check_same_thread=True`; any FastAPI or async runner usage that creates the backend in one thread and uses it in another raises `ProgrammingError` with a misleading `StateStoreError` message.
- Safe modification: Pass `check_same_thread=False` and add a threading lock for write operations.
- Test coverage: No concurrency tests for the state store.

---

## Scaling Limits

**DynamoDB pagination in `get_tasks`:**
- Current capacity: Fetches all tasks for a pipeline with paginated `scan` calls.
- Limit: All pages are loaded into memory before returning; unbounded for large pipelines.
- Files: `kptn/caching/client/dynamodb/get_tasks.py` (lines 24–33)
- Scaling path: Stream results as a generator rather than accumulating into a list.

**In-memory hash computation for large DuckDB tables:**
- Current capacity: `hash_duckdb_table` reads the entire table column-by-column into Python.
- Limit: Tables larger than available RAM will OOM.
- Files: `kptn/change_detector/hasher.py`
- Scaling path: Use DuckDB's native aggregation for hashing (`MD5(CAST(col AS VARCHAR))`) in-database rather than Python-side.

---

## Dependencies at Risk

**`pyproject.toml` optional extras not pinned:**
- Risk: `boto3`, `prefect`, `duckdb`, `watchfiles`, `fastapi`, `uvicorn` all lack upper-bound version pins. Breaking changes in any of these (particularly Prefect, which has had major API changes between versions) will silently break the optional code paths.
- Files: `pyproject.toml`
- Impact: Prefect caching paths (`kptn/caching/prefect.py`) and DynamoDB paths (`kptn/caching/client/DbClientDDB.py`) may break on dependency upgrades.
- Migration plan: Add upper-bound pins for major dependencies; add CI matrix testing against minimum and latest versions.

**`pyyaml` without version constraint:**
- Risk: `pyyaml` has had security-relevant releases; no version constraint in `pyproject.toml`.
- Files: `pyproject.toml`
- Impact: Projects that install kptn may get an insecure `pyyaml` version.
- Migration plan: Pin `pyyaml>=6.0` (safe-load-only API stabilized) with an upper-bound pin.

---

## Missing Critical Features

**`SqlTaskNode` local execution not implemented:**
- Problem: `kptn/runner/executor.py` explicitly skips `SqlTaskNode` with a warning: _"SQL dispatch is out of scope for v0.2.0 local runner; skipping"_. `@kptn.sql_task` is part of the public API but silently does nothing when run locally.
- Blocks: Any pipeline using `@kptn.sql_task` tasks locally.
- Files: `kptn/runner/executor.py` (lines 256–271)

**Quality gate registry (`kptn/quality/__init__.py`) is empty:**
- Problem: `kptn/quality/` directory exists with an empty `__init__.py`. Story 5-1 in `_bmad-output/implementation-artifacts/` outlines a Quality Gate Registry that is not yet implemented.
- Blocks: Any programmatic data quality enforcement.
- Files: `kptn/quality/__init__.py`

**`ProfileError` not caught in `run` command:**
- Problem: `kptn/cli/commands.py` `run()` wraps `ProfileError` correctly, but the equivalent v0.1.x `run` path in `_v01.py` does not. `ProfileResolver.compile()` can raise `ProfileError` during execution.
- Blocks: Clean error handling for `kptn run --profile` on the legacy CLI path.
- Files: `kptn/cli/commands.py` (v0.2.0 path is correct; v0.1.x in `_v01.py` is not)

---

## Test Coverage Gaps

**AWS/cloud code paths have no integration tests:**
- What's not tested: `kptn/cli/run_aws.py`, `kptn/deploy/push.py`, `kptn/deploy/prefect_deploy.py`, `kptn/caching/client/DbClientDDB.py`, `kptn/caching/client/dynamodb/` — all marked `pragma: no cover` or have no tests at all.
- Files: Above files; `tests/test_run_aws.py` exists but uses mocks only with no real boto3 calls.
- Risk: Cloud deployment regressions are invisible until production.
- Priority: Medium — these paths are exercised at deploy time, not on every commit.

**Watcher/WebSocket layer has no tests:**
- What's not tested: `kptn/watcher/app.py`, `kptn/watcher/local.py`, `kptn/watcher/stacks.py`, `kptn/watcher/util.py`
- Files: No corresponding test files found in `tests/`.
- Risk: Watcher regressions in the VS Code extension integration surface only via manual testing.
- Priority: Medium.

**`SqlTaskNode` execution is permanently skipped — no test verifies skip behavior:**
- What's not tested: The skip-with-warning branch in `execute()` for `SqlTaskNode` has no dedicated test asserting the warning is emitted.
- Files: `kptn/runner/executor.py` (lines 256–271)
- Risk: Silent removal of the skip branch would go undetected.
- Priority: Low.

**Large-graph performance regression guard missing:**
- What's not tested: No timing test for the `plan` command against an 80+ node graph (AC-1 NFR in Story 3.6).
- Files: `tests/test_runner_plan.py`
- Risk: Quadratic or cubic topo-sort implementation regressions would not be caught until user-facing slowdowns are reported.
- Priority: Medium.

**`ProfileLoader.load` race condition (TOCTOU) has no test:**
- What's not tested: `Path.exists()` followed by `open()` without wrapping `PermissionError` as `ProfileError`.
- Files: `kptn/profiles/loader.py` (lines 33–37)
- Risk: Concurrent file deletion or permission change during load produces a raw OS error instead of a `ProfileError`.
- Priority: Low.

---

*Concerns audit: 2025-01-27*
