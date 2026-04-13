# kptn v0.2.0 — nph-curation Port

## What This Is

A brownfield milestone: complete the kptn 0.2.0 rewrite so that **nph-curation** runs on it in production.

kptn is a Python-native pipeline orchestration framework (DAG-first, content-hash caching, multi-runtime tasks). The 0.1.x design was YAML-driven codegen; 0.2.0 introduces a composable Python DSL (`@kptn.task`, `>>` operator, `Pipeline`, profiles).

The driving use case is:

```python
load_asa24_raw >> validate_asa24_raw >> load_asa24_reports
```

…where `validate_asa24_raw` is toggleable via a profile (`"*.validate": false`), and disabling it should **bypass** the node (reconnect predecessors directly to successors) — not cascade-kill downstream tasks.

## Why It Needs to Exist

The old 0.1.x codegen approach generates boilerplate flow files that are hard to read and maintain. The 0.2.0 API replaces 21 YAML graph definitions and 30+ auto-generated `flows/*.py` files in nph-curation with a single `pipeline.py` that is explicit, composable, and version-controlled.

## Who It's For

The author (data engineer / researcher) running nph-curation data pipelines locally and in production.

## What "Done" Looks Like

- `nph-curation/pipeline.py` exists and wires up all flows using the 0.2.0 API
- All task functions in `nph/` are decorated with `@kptn.task(outputs=[...])`
- Optional validation task (`validate_asa24_raw`) is toggleable via profile with bypass semantics
- `nph-curation/pyproject.toml` points at local kptn repo
- `kptn run` (or `kptn.run(pipeline)`) executes at least the asa24 pipeline without errors

---

## Context

**Existing kptn 0.2.0 state:**
- Graph model complete: `@kptn.task`, `>>`, `parallel`, `Stage`, `map`, `noop`, `Pipeline`, `config`
- Profile system complete: `extends`, `stage_selections`, `optional_groups`, `args`, cursors (`start_from`, `stop_after`)
- Runner complete: executor, state store (SQLite + DuckDB), change detector
- CLI: v0.2 commands in place (`kptn/cli/commands.py`)

**nph-curation current state:**
- 21 flows defined in `kptn.yaml` using the old YAML topology
- 30+ auto-generated `flows/*.py` files using the `kptn.caching.submit` API
- Task implementations live in `nph/*.py` as plain functions (no `@kptn.task` decorators)
- `pyproject.toml` pins `kptn>=0.1.15`; uv workspace sources are commented out

---

## Requirements

### Validated

- ✓ Graph model (`@kptn.task`, `>>`, `parallel`, `Stage`, `map`, `noop`, `Pipeline`, `config`) — existing
- ✓ Profile system (`extends`, `stage_selections`, `optional_groups`, `args`, cursors) — existing
- ✓ Runner (executor, state store, change detector) — existing
- ✓ CLI v0.2 commands — existing

### Active

- [ ] **OPT-01**: When an optional node is pruned, its predecessors are reconnected to its successors (bypass semantics) — disabling `validate_asa24_raw` must not kill `load_asa24_reports`
- [ ] **PIPE-01**: All nph-curation task functions decorated with `@kptn.task(outputs=[...])`
- [ ] **PIPE-02**: `nph-curation/pipeline.py` defines all pipelines using `>>` composition, replacing `kptn.yaml` graph topology
- [ ] **WIRE-01**: `nph-curation/pyproject.toml` uv workspace source points at local kptn repo
- [ ] **RUN-01**: `kptn run` (or `kptn.run(pipeline)`) executes asa24 pipeline without errors

### Out of Scope

- SQL dispatch in the local runner (already marked out of scope in executor.py)
- Quality gate (FR-Q1, FR-Q2) — P1, not this milestone
- kptn-vscode extension, FastAPI server, React UI — unchanged
- Full CI parity test suite (NFR-3) — post-milestone

---

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Optional prune → bypass instead of cascade | `validate_asa24_raw` disabled must leave `load_asa24_reports` reachable | Modify `_prune()` to reconnect edges around dead optional nodes |
| Decorators live in `nph/` task files, not pipeline.py | Tasks are reusable across flows; metadata belongs at the source | — Pending |
| Single `pipeline.py` in nph-curation replaces all `flows/` + `kptn.yaml` | One file of truth for graph topology | — Pending |

---

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-13 after initialization*
