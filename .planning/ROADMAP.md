# Roadmap: kptn v0.2.0 — nph-curation Port

## Overview

Four sequential phases drive this brownfield milestone to completion. The kptn framework
already has its 0.2.0 DSL in place; the work is (1) fixing a single graph-pruning bug that
breaks optional-task bypass, (2) wiring nph-curation to consume the local repo, (3) writing
the `pipeline.py` that replaces 63 generated files and all YAML graph topology, and (4)
deleting the old artifacts. Each phase fully unblocks the next.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

- [ ] **Phase 1: Bypass Fix** - Correct `_prune()` in resolver.py so disabled optional tasks reconnect predecessors to successors
- [ ] **Phase 2: Dependency Wiring** - Point nph-curation at the local kptn repo via uv path source
- [ ] **Phase 3: Pipeline Migration** - Write `nph/pipeline.py` with all 21 flows and smoke-test asa24
- [ ] **Phase 4: Cleanup** - Delete generated files, strip dead YAML keys, align version strings

## Phase Details

### Phase 1: Bypass Fix
**Goal**: The kptn pruner bypasses optional-dead nodes correctly — predecessors are reconnected to successors — without touching stage-kill cascade behavior
**Depends on**: Nothing (first phase)
**Requirements**: OPT-01, OPT-02, OPT-03, OPT-04
**Success Criteria** (what must be TRUE):
  1. Disabling an optional task via profile leaves all downstream tasks reachable in the resolved graph (predecessors connect directly to successors)
  2. Stage-dead nodes still cascade-kill their downstream tasks (existing behavior is unchanged)
  3. All 8 edge cases (single optional in chain, source optional, sink optional, adjacent optionals, fan-in optional, fan-out optional, stage-dead + optional, bypass edge dedup) pass as tests
  4. All pre-existing resolver tests continue to pass after the change
**Plans**: 1 plan

Plans:
- [ ] 01-01-PLAN.md — Rewrite `_prune()` with 4-phase bypass algorithm + 8 edge-case tests

### Phase 2: Dependency Wiring
**Goal**: nph-curation resolves kptn from the local repo on disk, not from PyPI
**Depends on**: Phase 1
**Requirements**: WIRE-01, WIRE-02
**Success Criteria** (what must be TRUE):
  1. `uv lock` in the nph-curation environment completes without errors using the path editable source
  2. `python -c "import kptn; print(kptn.__file__)"` in the nph-curation environment prints a path inside `/Users/rruizesparza/Code/kptn/`
**Plans**: TBD

### Phase 3: Pipeline Migration
**Goal**: All 21 nph-curation flows are declared as `kptn.Pipeline` objects in a single `nph/pipeline.py`, and the asa24 pipeline executes end-to-end with and without the optional validate task
**Depends on**: Phase 2
**Requirements**: PIPE-01, PIPE-02, PIPE-03, PIPE-04, PIPE-05, PIPE-06, RUN-01, RUN-02
**Success Criteria** (what must be TRUE):
  1. `kptn.run(asa24_pipeline)` completes without errors with at least one task completing (not all skipped due to cache)
  2. Running asa24 with a profile setting `"*.validate": false` skips `validate_asa24_raw` and still executes `load_asa24_reports`
  3. All 21 flows from `kptn.yaml` have a corresponding `kptn.Pipeline` declaration in `nph/pipeline.py`
  4. Export/composite flows (e.g., `all_export`) are implemented as Python orchestrator functions calling `kptn.run()` sequentially — not as composed `Pipeline` objects
**Plans**: TBD

### Phase 4: Cleanup
**Goal**: The nph-curation repo contains no 0.1.x artifacts and kptn version strings are consistent at `0.2.0`
**Depends on**: Phase 3
**Requirements**: CLEAN-01, CLEAN-02, CLEAN-03
**Success Criteria** (what must be TRUE):
  1. `nph-curation/flows/` is empty — all 63 auto-generated files are gone
  2. `nph-curation/kptn.yaml` contains only `settings:` and `profiles:` blocks; no `graphs:`, `codegen:`, `flows_dir:`, `flow_type:`, `imports_slot:`, or legacy `config:` string-ref keys remain
  3. kptn version reads `0.2.0` in both `kptn/pyproject.toml` and `kptn/__init__.py`
**Plans**: TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Bypass Fix | 1/1 | Planning complete | - |
| 2. Dependency Wiring | 0/? | Not started | - |
| 3. Pipeline Migration | 0/? | Not started | - |
| 4. Cleanup | 0/? | Not started | - |
