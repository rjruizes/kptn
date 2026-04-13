# Coding Conventions

**Analysis Date:** 2025-01-31

## Naming Patterns

**Files:**
- Python source: `snake_case.py` — e.g., `task_args.py`, `infra_scaffolder.py`
- Python classes used as modules: `PascalCase.py` where the file is named after its primary class — e.g., `TaskStateCache.py`, `Hasher.py`
- TypeScript/TSX: `PascalCase.tsx` for components — e.g., `Table.tsx`, `DeployBtnRenderer.tsx`; `kebab-case.ts` for hooks — e.g., `use-state.tsx`, `use-grid-api.tsx`
- Test files: `test_<module_name>.py` matching the module under test

**Functions:**
- Python: `snake_case` — e.g., `build_task_argument_plan`, `hash_code_for_task`, `_dispatch_task`
- Private helpers: prefixed with `_` — e.g., `_has_stream_handler`, `_normalize_code_hashes`, `_dispatch_r_task`
- TypeScript: `camelCase` — e.g., `useStateStore`, `onGridReady`, `setSelectedRows`

**Variables:**
- Python: `snake_case`
- TypeScript: `camelCase`
- Constants: `UPPER_SNAKE_CASE` in Python (e.g., `_NON_EXEC_NODES`)

**Types / Classes:**
- Python: `PascalCase` — e.g., `TaskNode`, `GraphError`, `ResolvedGraph`, `FakeDbClient`
- TypeScript interfaces: `PascalCase` — e.g., `AppState`, `Task`
- Internal/private Python classes: prefixed with `_` — e.g., `_KptnCallable`, `_SqlTaskHandle`, `_Sentinel`

**Exception Hierarchy:**
All custom exceptions live in `kptn/exceptions.py`:
```python
class KptnError(Exception): ...
class GraphError(KptnError): ...
class ProfileError(KptnError): ...
class TaskError(KptnError): ...
class StateStoreError(KptnError): ...
class HashError(KptnError): ...
```
Always raise domain-specific subclasses of `KptnError`, never bare `Exception`.

## Code Style

**Formatting (Python):**
- Tool: `ruff` (configured in `pyproject.toml`)
- No explicit line-length config detected; ruff defaults apply
- Trailing commas present in multi-line structures

**Linting (TypeScript/React):**
- Tool: `eslint` with `typescript-eslint` + `eslint-plugin-react-hooks` + `eslint-plugin-react-refresh`
- Config: `ui/eslint.config.js`
- Rule: `react-refresh/only-export-components` at `warn` level
- React hooks rules enforced (`eslint-plugin-react-hooks`)

**Type Annotations:**
- All Python public functions have return type annotations (e.g., `-> None`, `-> str | None`, `-> list[AnyNode]`)
- `from __future__ import annotations` present in ~38% of Python files — used in files with complex forward references (especially `kptn/runner/`, `kptn/graph/`, test files)
- Union syntax: modern `X | Y` style (Python 3.10+) in new code
- `TYPE_CHECKING` guard used for circular import avoidance in `kptn/graph/decorators.py`, `kptn/graph/nodes.py`

**Dataclasses:**
- `@dataclass` is the preferred way to define data-holding classes — e.g., `TaskNode`, `TaskSpec`, `Graph`, `RTaskSpec`
- `field(default_factory=list)` used for mutable defaults

## Import Organization

**Python order:**
1. `from __future__ import annotations` (when present)
2. Standard library (`pathlib`, `typing`, `dataclasses`, `logging`, `subprocess`)
3. Third-party (`pytest`, `pydantic`, `typer`)
4. Local package imports (`from kptn.graph.nodes import ...`)
5. Test-only imports from `tests.*` in test files

**TypeScript order:**
1. External packages (React, tanstack, ag-grid)
2. Internal `@/` alias imports (e.g., `@/hooks/use-row`)
3. Relative component imports

**Path Aliases (TypeScript):**
- `@` → `./src` (configured in `ui/vite.config.ts` and `ui/tsconfig.json`)
- Use `@/hooks/`, `@/components/`, `@/lib/` for non-relative imports

## Error Handling

**Python patterns:**
- Wrap external calls in specific exception handlers, re-raise as domain exceptions:
  ```python
  try:
      return node.fn(**kwargs)
  except TaskError:
      raise   # pass through already-typed errors
  except Exception as exc:
      raise TaskError(f"Task '{node.name}' raised an error: {exc}") from exc
  ```
- `HashError` and `TaskError` propagated deliberately; other `Exception` types are wrapped
- Errors include context (task name, file path, return code) in the message string
- Optional deps (boto3, duckdb, prefect) use try/except at import time with `= None` fallback:
  ```python
  try:
      import boto3
  except ImportError:
      boto3 = None  # type: ignore[assignment]
  ```

**TypeScript patterns:**
- React components return `null` early when data is not ready: `if (!state) return null`
- Cypress tests suppress known framework errors via `Cypress.on("uncaught:exception", ...)`

## Logging

**Framework:** Python `logging` module with custom formatter in `kptn/util/logger.py`

**Setup:**
```python
from kptn.util.logger import get_logger, setup_logger
logger = logging.getLogger(__name__)  # module-level logger for core modules
```

**Patterns:**
- Module-level `logger = logging.getLogger(__name__)` in core execution modules (`executor.py`, etc.)
- `get_logger()` used in higher-level code — checks for Prefect env vars and returns appropriate logger
- Custom `CustomFormatter` adds ANSI colors (disabled when `IS_PROD=1`)
- Log format: `{asctime} {filename}:{lineno} {levelname}: {message}`
- Logger uses `logger.propagate = False` to prevent double-logging

## Comments

**When to Comment:**
- Docstrings on public API classes and functions (e.g., `_KptnCallable`, `noop()`, `task()`)
- Section dividers using `# ─── Section Name ─────` pattern in both source and test files
- Story/AC references in test docstrings: `"""Tasks must execute predecessor before successor (AC-1)."""`
- Inline comments for non-obvious decisions (e.g., `# type: ignore[assignment]` with reason)
- `# noqa: ...` suppressions include justification comments (e.g., `# noqa: A001 — shadows builtin intentionally`)

**TSDoc:**
- Not systematically used in TypeScript; inline comments preferred
- Python docstrings present on public-facing decorators and classes

## Function Design

**Size:** Functions are generally focused; private helpers extracted with `_` prefix
**Parameters:** Keyword arguments preferred for optional config; explicit type annotations
**Return Values:** Always annotated; `None` explicit; `str | None` for missing-or-present values

**Factory/Builder pattern:**
Helper functions like `build_task_state(**overrides)` in tests use `**overrides` dict with base defaults — common pattern for flexible test data construction

## Module Design

**Exports:**
- Package `__init__.py` files explicitly list `__all__` — e.g., `kptn/__init__.py` and `kptn/state_store/__init__.py`
- `# noqa: F401` on re-export lines to silence unused-import linting

**Barrel Files:**
- `kptn/__init__.py` is the public API surface — exposes `task`, `sql_task`, `r_task`, `noop`, `parallel`, `Stage`, `map`, `Pipeline`, `config`, `run`
- Sub-packages expose their own `__init__.py` with `__all__`

**Dataclass Node Hierarchy:**
All graph nodes are plain `@dataclass` objects in `kptn/graph/nodes.py`. A Union type `AnyNode` covers all variants. New node types should be added there.

---

*Convention analysis: 2025-01-31*
