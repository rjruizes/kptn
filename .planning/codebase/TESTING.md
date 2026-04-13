# Testing Patterns

**Analysis Date:** 2025-01-31

## Test Framework

**Runner (Python):**
- `pytest` >= 8.4.2
- Config: `pyproject.toml` under `[tool.pytest.ini_options]`
- Test discovery paths: `tests/`, `example/step_example/tests`

**Runner (E2E):**
- `cypress` >= 15.7.0
- Config: `cypress.config.ts` (root)
- Base URL: `http://localhost:8000` (overridable via `CYPRESS_BASE_URL`)

**Runner (TypeScript unit — VSCode extension):**
- Node assert + VS Code extension test harness
- Config: `kptn-vscode/src/test/extension.test.ts` (minimal, mostly stubs)

**Assertion Library:**
- Python: built-in `assert` (pytest style)
- TypeScript: `assert` from Node stdlib, Cypress `expect()` chained assertions

**Run Commands:**
```bash
# Python tests
uv run pytest                          # Run all tests
uv run pytest tests/test_decider.py   # Single file
uv run pytest -k "test_name"          # Filter by name

# E2E Cypress
npm run cypress:open    # Interactive mode
npm run cypress:run     # Headless CI mode
```

## Test File Organization

**Python — Location:**
- Separate `tests/` directory at project root (not co-located with source)
- Integration tests in `tests/integration/` subdirectory
- Test support files: `tests/fakes.py`, `tests/fixture_constants.py`

**Python — Naming:**
- `test_<module_or_feature>.py` — e.g., `test_runner_executor.py`, `test_graph_map.py`
- No class-based test grouping; all tests are top-level functions

**Cypress E2E — Location:**
- `cypress/e2e/*.cy.ts` — all in flat directory
- Naming: `<feature>_<behavior>.cy.ts` — e.g., `lineage_hover.cy.ts`, `lineage_python_preview.cy.ts`

**Structure:**
```
tests/
├── __init__.py
├── fakes.py                    # In-memory fake implementations
├── fixture_constants.py        # Path constants for example pipeline files
├── test_*.py                   # One file per feature/module
└── integration/
    ├── __init__.py
    └── test_example_migration.py

cypress/
└── e2e/
    ├── lineage_hover.cy.ts
    ├── lineage_omnibar_filter.cy.ts
    ├── lineage_preview_scroll.cy.ts
    └── lineage_python_preview.cy.ts
```

## Test Structure

**Python suite organization — flat functions with AC comments:**
```python
# ─── AC-1: Topological execution order ──────────────────────── #

def test_execute_runs_tasks_in_topological_order() -> None:
    """Tasks must execute predecessor before successor (AC-1)."""
    # arrange
    call_order: list[str] = []
    ...
    # act
    execute(resolved, store)
    # assert
    assert call_order == ["a", "b"]
```

**Patterns:**
- Section dividers `# ─── AC-N: Description ─────────────────── #` group tests by acceptance criteria
- Test name encodes expected behavior: `test_<subject>_<verb>_<condition>`
- Docstrings quote the acceptance criterion being validated
- `-> None` return type annotation on all test functions
- No `describe`/`context` nesting — flat function list per file

**Cypress suite organization:**
```typescript
describe("Lineage hover interactions", () => {
  it("shows dependency edges on column hover", () => {
    cy.viewport(900, 700);
    cy.visit(`/lineage-page?configPath=...`);
    cy.get(".lineage-path", { timeout: 15000 }).should("have.length.greaterThan", 0);
    // ...assertions
  });
});
```

## Mocking

**Framework:** `unittest.mock` (`MagicMock`, `patch`, `call`)

**Core patterns:**
```python
from unittest.mock import MagicMock, patch

# Mock a callable function
fn = MagicMock(return_value=None)
fn.__name__ = "task_a"  # REQUIRED — kptn reads __name__ from task functions

# Patch at the call site (module path of the caller)
with patch("kptn.runner.executor.subprocess.run", return_value=mock_result):
    execute(resolved, store)

# Assert call details
assert mock_run.call_count == 1
_, kwargs = mock_run.call_args
assert "env" not in kwargs
```

**`monkeypatch` fixture for instance methods:**
```python
def patch_code_hashes(monkeypatch):
    def _no_op_build(self, task_name, task, **kwargs):
        return None, None
    monkeypatch.setattr(TaskStateCache, "build_task_code_hashes", _no_op_build)
```

**What to Mock:**
- External subprocesses (`subprocess.run` for R task execution)
- AWS clients and network calls
- `TaskStateCache` code-hashing methods to avoid touching real file paths
- Singletons that leak state between tests (reset via `autouse` fixture)

**What NOT to Mock:**
- `FakeStateStore` / `FakeDbClient` — use in-memory fakes instead (see Fakes section)
- Python task functions — use real `MagicMock` callables with `__name__` set
- File system operations on `tmp_path` — use pytest's `tmp_path` fixture with real files

## Fixtures and Factories

**Fakes (preferred over mocks for protocol implementations):**
```python
# tests/fakes.py
class FakeStateStore:
    """In-memory StateStoreBackend for testing cross-component boundaries."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str, str], str] = {}

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None:
        return self._store.get((storage_key, pipeline, task))

    def write_hash(self, storage_key: str, pipeline: str, task: str, hash: str) -> None:
        self._store[(storage_key, pipeline, task)] = hash

    def delete(self, storage_key: str, pipeline: str, task: str) -> None:
        self._store.pop((storage_key, pipeline, task), None)

    def list_tasks(self, storage_key: str, pipeline: str) -> list[str]:
        return [t for (sk, p, t) in self._store if sk == storage_key and p == pipeline]
```

**Builder helpers for test data:**
```python
# In test file — flexible **overrides pattern
def build_task_state(**overrides) -> TaskState:
    base = {
        "PK": overrides.get("PK", "task#demo"),
        "status": overrides.get("status", "SUCCESS"),
        # ... all fields with sensible defaults
    }
    return TaskState(**base)

# Usage
cached = build_task_state(status="FAILED", code_hashes=["abc"])
```

**Node builder helpers:**
```python
# Common in executor/cache tests
def _make_task_node(name: str, fn=None, outputs: list[str] | None = None) -> TaskNode:
    if fn is None:
        fn = MagicMock(return_value=None)
        fn.__name__ = name
    spec = TaskSpec(outputs=outputs or [])
    return TaskNode(fn=fn, spec=spec, name=name)
```

**Pytest fixtures:**
```python
@pytest.fixture
def mock_pipeline_config_path() -> str:
    return str(Path("example/mock_pipeline/kptn.yaml").resolve())

@pytest.fixture(autouse=True)
def reset_task_state_cache():
    """Ensure TaskStateCache singleton does not leak between tests."""
    TaskStateCache._instance = None
    yield
    TaskStateCache._instance = None

@pytest.fixture
def cleanup():
    yield
    scratch_dir = Path(mock_dir) / "scratch"
    if scratch_dir.exists():
        shutil.rmtree(scratch_dir)
```

**Fixture constants:**
```python
# tests/fixture_constants.py
mock_dir = Path(__file__).parent.parent / "example/mock_pipeline"
r_tasks_dir = str(Path(mock_dir) / "r_tasks")
tasks_yaml_path = str(Path(mock_dir) / "kptn.yaml")
```

**Parametrized fixtures:**
```python
@pytest.fixture(params=["sqlite", "duckdb"])
def backend(request, tmp_path, sqlite_backend):
    if request.param == "duckdb":
        pytest.importorskip("duckdb")  # skip if duckdb not installed
        from kptn.state_store.duckdb import DuckDbBackend
        return DuckDbBackend(path=str(tmp_path / "test.duckdb"))
    return sqlite_backend
```

## Coverage

**Requirements:** No coverage threshold enforced (not configured in `pyproject.toml`)

**View Coverage:**
```bash
uv run pytest --cov=kptn --cov-report=term-missing
```

## Test Types

**Unit Tests (`tests/test_*.py`):**
- Scope: single module or class in isolation
- Dependencies replaced with `FakeStateStore`, `FakeDbClient`, `MagicMock`
- Use `tmp_path` for any real file I/O
- Fast — no network, no real database

**Integration Tests (`tests/integration/`):**
- Scope: end-to-end pipeline execution through example configs
- Touch real file system via `example/` directory fixtures
- May require optional dependencies (duckdb, etc.)
- Currently: `test_example_migration.py`

**E2E Tests (`cypress/e2e/`):**
- Scope: full UI + backend server interaction
- Require running server at `localhost:8000`
- Use `cy.intercept()` to stub API calls and verify request shapes
- Test real DOM interactions (hover, click, scroll)

**Skipped/Obsolete Tests:**
- `tests/test_lineage_visualizer.py` uses `pytest.skip(allow_module_level=True)` — marks v0.1.x tests removed in v0.2.0 migration. This is the pattern for deprecating test files without deleting them.

## Common Patterns

**Async Testing:**
- Not used in Python test suite (synchronous `pytest` only)
- Cypress handles async UI interactions natively via command chaining

**Error Testing:**
```python
# Expect specific exception
with pytest.raises(TaskError):
    execute(resolved, store)

# Inspect exception message
with pytest.raises(TaskError) as exc_info:
    execute(resolved, store)
assert "exited with code 1" in str(exc_info.value)

# Expect TypeError with message match
with pytest.raises(TypeError, match="@kptn.task"):
    kptn.map(NoNameCallable(), over="ctx.items")
```

**Output capture testing:**
```python
def test_execute_map_node_emits_count_message(capsys) -> None:
    execute(resolved, store)
    captured = capsys.readouterr()
    assert "[MAP] process — expanding over 3 items" in captured.out
```

**Protocol conformance testing:**
```python
# Verify @runtime_checkable Protocol with isinstance
assert isinstance(sqlite_backend, StateStoreBackend)

# Verify public API surfaces
def test_state_store_all_exports():
    import kptn.state_store as ss
    assert "StateStoreBackend" in ss.__all__
```

**Importing optional deps in tests:**
```python
pytest.importorskip("duckdb")  # skips test if duckdb not installed
```

**Singleton reset pattern:**
```python
@pytest.fixture(autouse=True)
def reset_task_state_cache():
    TaskStateCache._instance = None
    yield
    TaskStateCache._instance = None
```

---

*Testing analysis: 2025-01-31*
