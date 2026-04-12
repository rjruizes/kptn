# v0.1.x → v0.2.0 Migration Guide

## Overview

v0.2.0 replaces the YAML-first, code-generation approach of v0.1.x with a Python-first API.
In v0.1.x, pipelines were declared in `kptn.yaml` (graphs, tasks, dependencies) and a
code-generation step produced runnable flow files.
In v0.2.0, pipelines are plain Python modules: tasks are decorated functions, graph edges are
expressed with the `>>` operator, and `kptn.yaml` is used exclusively for profiles and settings.
No code generation is needed.

---

## Workflow Changes: `kptn codegen` → pipeline authoring

### What `kptn codegen` did

In v0.1.x, the pipeline authoring loop was:

1. Declare graphs, tasks, and edges in `kptn.yaml`
2. Run `kptn codegen` — this read `kptn.yaml → graphs:` and templated a `.py` file per graph under `flows_dir/`
3. The runner imported those generated files at execution time

The generated files were never meant to be edited. They were build artifacts, not source files.

### What replaces it

In v0.2.0 there is **no `kptn codegen` command** and no generated files. You write `pipeline.py`
once, directly — it is plain Python, version-controlled alongside your tasks, and immediately
executable. The loop is:

1. Write `pipeline.py` — define tasks with decorators, wire edges with `>>`, wrap in `kptn.Pipeline`
2. Point `pyproject.toml` at the module (for the CLI)
3. Run with `kptn run` (CLI) or `kptn.run(pipeline)` (Python API)

### Minimum `pipeline.py`

```python
# v0.2.0 pipeline.py — you write this, no generation step
import kptn

@kptn.task(outputs=["raw.csv"])
def extract() -> None:
    ...

@kptn.task(outputs=["clean.csv"])
def transform() -> None:
    ...

load = kptn.sql_task("src/load.sql", outputs=["duckdb://schema.final"])

pipeline = kptn.Pipeline("my_flow", extract >> transform >> load)
```

This file is the complete replacement for both `kptn.yaml → graphs: + tasks:` and the
generated flow file.

---

### `kptn.run()` Python API

For programmatic execution — notebooks, scripts, CI — import your pipeline and call `kptn.run()`:

```python
import kptn
from my_package.pipeline import pipeline

# Run with default settings (reads kptn.yaml if present, falls back to SQLite defaults)
kptn.run(pipeline)

# Run with a named profile
kptn.run(pipeline, profile="dev")
```

`kptn.run()` signature:

```python
kptn.run(pipeline: Pipeline, *, profile: str | None = None) -> None
```

- `db` backend and `db_path` are sourced from `kptn.yaml → settings` when the file is present;
  SQLite at `.kptn/kptn.db` otherwise — no `kptn.yaml` required for a basic run
- `ProfileError` propagates to the caller when the requested profile does not exist
- `TaskError` propagates to the caller when a task raises during execution

---

### `kptn run` CLI

The CLI `run` command works the same way but loads the pipeline from `pyproject.toml`:

```toml
# pyproject.toml
[tool.kptn]
pipeline = "my_package.pipeline"   # module must expose `pipeline` or `graph`
```

```
kptn run                    # run with default settings
kptn run --profile dev      # run with a named profile
```

The CLI delegates to `kptn.run()` internally — `kptn run --profile dev` is equivalent to
calling `kptn.run(pipeline, profile="dev")` from Python.

---

### Removed commands

| v0.1.x command | v0.2.0 status | Replacement |
|---|---|---|
| `kptn codegen` | **Removed** | Write `pipeline.py` directly |
| `kptn run` (graph-loading) | **Replaced** | `kptn run` now delegates to `kptn.run()` |

---

## Quick Reference Table

| v0.1.x YAML Construct | v0.2.0 Python Equivalent | Notes |
|---|---|---|
| `graphs: <name>: tasks:` | `kptn.Pipeline("name", graph)` | Pipeline wraps a Graph; name is required |
| `tasks: file: module.py:fn` | `@kptn.task(outputs=[...])` decorator | Decorator returns a `_KptnCallable`; still callable normally |
| `tasks: file: query.sql` | `kptn.sql_task("query.sql", outputs=[...])` | SQL scripts have no kptn API inside them |
| `tasks: file: script.R` | `kptn.r_task("script.R", outputs=[...])` | R scripts run as subprocesses; no kptn API inside them |
| `extends: [flow_a, flow_b]` (one graph per variant) | One `kptn.Pipeline` with `kptn.Stage()` or `kptn.parallel()` | Variants collapse into one Pipeline; profiles select branches. See [extends Composition](#extends-composition) |
| `graph: <flow> args: task: {k: v}` (inside `extends:`) | `profiles.<name>.args: {task: {k: v}}` in `kptn.yaml` | Per-task arg injection for reused shared graphs; runner injects at dispatch time |
| `config: function: module:fn` | `kptn.config(key=get_fn)` | Callable passed directly; invoked by runner at dispatch time |
| `tasks: b: a` (YAML edge) | `a >> b` (Python operator) | `>>` wires dependency edges |
| Cache state | **Clean break — not preserved** | First run after migration is always a cold start; this is expected |

---

## Before / After Examples

### Pipeline Definition

```yaml
# v0.1.x kptn.yaml
graphs:
  my_flow:
    tasks:
      extract:
      transform: extract
      load: transform

tasks:
  extract:
    file: src/extract.py
    outputs: ["raw.csv"]
  transform:
    file: src/transform.py
    outputs: ["clean.csv"]
  load:
    file: src/load.sql
    outputs: ["duckdb://schema.final"]
```

```python
# v0.2.0 pipeline.py
import kptn

@kptn.task(outputs=["raw.csv"])
def extract() -> None: ...

@kptn.task(outputs=["clean.csv"])
def transform() -> None: ...

load = kptn.sql_task("src/load.sql", outputs=["duckdb://schema.final"])

pipeline = kptn.Pipeline("my_flow", extract >> transform >> load)
```

The `>>` operator expresses the same edges that v0.1.x encoded in the task dependency map
(`transform: extract` → `extract >> transform`).

---

### Python Task (`file: module.py:fn`)

```yaml
# v0.1.x
tasks:
  extract:
    file: src/extract.py
    outputs: ["raw.csv"]
```

```python
# v0.2.0
import kptn

@kptn.task(outputs=["raw.csv"])
def extract() -> None:
    # your implementation here
    ...
```

The decorator accepts:
- `outputs` (required) — list of file paths or `duckdb://schema.table` strings
- `optional` (str | None) — marks the task as a member of a named optional group
- `compute` (str | None) — compute requirement label

The decorated function is still callable normally; its original implementation is accessible
at `extract.__wrapped__`.

---

### SQL Task (`file: query.sql`)

```yaml
# v0.1.x
tasks:
  load:
    file: src/load.sql
    outputs: ["duckdb://schema.final"]
```

```python
# v0.2.0
import kptn

load = kptn.sql_task("src/load.sql", outputs=["duckdb://schema.final"])
```

`kptn.sql_task` signature:

```python
kptn.sql_task(
    path="queries/my_query.sql",  # path to .sql file
    outputs=["schema.table"],      # required
    optional=None,                 # optional group flag
)
```

`sql_task` objects are **not callable** — they are executed directly by the runner.

---

### R Task (`file: script.R`)

```yaml
# v0.1.x
tasks:
  analyse:
    file: scripts/analyse.R
    outputs: ["output.csv"]
```

```python
# v0.2.0
import kptn

analyse = kptn.r_task("scripts/analyse.R", outputs=["output.csv"])
```

`kptn.r_task` signature:

```python
kptn.r_task(
    path="scripts/my_script.R",  # path to .R file
    outputs=["output.csv"],       # required
    compute=None,                 # compute requirement label
    optional=None,
)
```

R scripts run as subprocesses; no kptn API is available inside them.

---

### Graph Edges (`tasks: b: a`)

```yaml
# v0.1.x (b depends on a)
graphs:
  my_flow:
    tasks:
      a:
      b: a
      c: b
```

```python
# v0.2.0
my_flow = a >> b >> c
```

Fan-in (multiple tasks must complete before the next):

```python
# v0.2.0 — both a and b must finish before c starts
my_flow = kptn.parallel(a, b) >> c
```

---

### Runtime Arguments (`graph: <flow> args:`)

Use profile `args` to pass runtime parameters to named tasks in your pipeline.

```yaml
# v0.1.x kptn.yaml
graphs:
  monthly_report:
    extends:
      - source_flow
      - graph: transform_flow
        args:
          load_data:
            tables: ["staging.sales", "staging.orders"]
          validate_data:
            strict: true
      - output_flow
```

In v0.2.0, per-task argument injection lives in **profile `args`** in `kptn.yaml`. The shared
graph is defined once in Python; each profile supplies its own parameter values. The runner
injects them at dispatch time.

```python
# v0.2.0 pipeline.py — shared graph defined once
import kptn

graph = kptn.parallel(source_flow, transform_flow, output_flow)
pipeline = kptn.Pipeline("my_pipeline", graph)
```

```yaml
# v0.2.0 kptn.yaml — per-task args live in profiles
profiles:
  monthly_report:
    args:
      load_data:
        tables: ["staging.sales", "staging.orders"]
      validate_data:
        strict: true
```

Run with `kptn run --profile monthly_report`. The runner looks up `profile_args[task_name]`
for each node and passes the matching kwargs automatically.

Profile `args` compose with `extends:` — a child profile inherits parent `args` and can
override individual task parameters.

---

### Config Injection (`config: function:`)

```yaml
# v0.1.x
config:
  function: "my_module:get_engine"
```

```python
# v0.2.0
import kptn
from my_module import get_engine

deps = kptn.config(engine=get_engine)
pipeline_graph = deps >> my_task
pipeline = kptn.Pipeline("my_flow", pipeline_graph)
```

`kptn.config(key=callable)` returns a graph node whose callable is invoked by the runner
at dispatch time. Wire it at the head of the graph with `>>`.

---

## `extends` Composition

In v0.1.x, `extends` was a graph-inheritance mechanism: a named graph could pull in one or
more parent graphs, merging their tasks. The typical pattern was one graph definition per
pipeline variant, run by name.

v0.2.0 inverts this model. You define **one `Pipeline`** in Python — composed of other
pipelines as branches — and use profiles to control which branches execute at runtime.
There is no longer a separate graph entry per environment.

### From multiple graph variants to one Pipeline

**v0.1.x — one named graph per variant:**

```yaml
# kptn.yaml
graphs:
  combine_flow:
    tasks:
      combine_processing:

  variant_a:
    extends: [dataset1, combine_flow]

  variant_b:
    extends: [dataset2, combine_flow]
```

Running `variant_a` or `variant_b` required selecting the graph by name at the CLI.

**v0.2.0 — one Pipeline, Stage controls the variation point:**

```python
# pipeline.py
import kptn

graph = kptn.Stage("source", dataset1, dataset2) >> combine_processing
pipeline = kptn.Pipeline("my_pipeline", graph)
```

```yaml
# kptn.yaml — profiles select which Stage branch runs
profiles:
  variant_a:
    stage_selections:
      source: [dataset1]
  variant_b:
    stage_selections:
      source: [dataset2]
```

Run with `kptn run --profile variant_a` or `kptn run --profile variant_b`. The runner
prunes non-selected branches at plan time — only the active branch executes.

---

### `kptn.Stage("name", *branches)` — profile-conditional

Use when different branches should execute depending on the active profile. The Stage name
identifies the variation point; `stage_selections` in `kptn.yaml` maps each profile to
the branch(es) it activates. All other branches are pruned before execution.

---

### `kptn.parallel(*branches)` — always-active

Use when ALL branches should always run together with no profile-based selection. This
corresponds to v0.1.x `extends:` lists where every parent graph was always merged in,
regardless of environment.

```python
# v0.2.0 — both branches always execute
graph = kptn.parallel(flow_a, flow_b) >> downstream
pipeline = kptn.Pipeline("my_flow", graph)
```

---

### Decision Rule

> If different branches run per environment → define a **`kptn.Stage()`** and select the
> branch via profile `stage_selections`.
>
> If all branches always run together → use **`kptn.parallel()`**.

---

## Cache State

**Cache state is not preserved across versions.**

v0.1.x and v0.2.0 use incompatible cache formats. After migrating, the first pipeline run is
always a cold start — every task will execute regardless of prior results.

This is expected behaviour. No manual cache cleanup is required; the runner will populate a
fresh cache automatically.

To verify your pipeline resolves correctly before the first run, use:

```
kptn plan --profile <your-profile>
```

The plan output shows `[RUN]`, `[SKIP]`, and `[MAP]` markers for each node, confirming that
graph wiring and profile selection are correct without actually executing any tasks.

---

## `kptn.yaml` Changes

The role of `kptn.yaml` changed entirely between versions.

### v0.1.x — pipeline structure file

```yaml
# v0.1.x kptn.yaml — defines graphs AND tasks
settings:
  flows_dir: "."        # removed in v0.2.0
  flow_type: vanilla    # removed in v0.2.0
  db: sqlite            # replaced by settings.db / settings.db_path

graphs:
  basic:
    tasks:
      a:
      b: a
      c: b

tasks:
  a:
    file: src/a.py
  b:
    file: src/b.py
  c:
    file: src/c.py
```

### v0.2.0 — profiles and settings only

```yaml
# v0.2.0 kptn.yaml — profiles and settings ONLY
# Pipeline structure is now Python (pipeline.py)
settings:
  db: sqlite           # "sqlite" (default) or "duckdb"
  db_path: .kptn/kptn.db
  cache_namespace: null  # optional namespace for cache isolation

profiles:
  dev:
    stage_selections:
      data_sources: [small_batch]
    start_from: null
    stop_after: null
    args: {}
    optional_groups: {}
  prod:
    extends: dev        # profiles can extend other profiles
    stage_selections:
      data_sources: [full_ingest]
```

**Key point:** The v0.2.0 `kptn.yaml` is **not** a replacement for the v0.1.x one — it is a
narrower, profiles-only configuration file. Pipeline structure (graphs, tasks, edges) moves
entirely to Python.

### Removed `settings` keys

| v0.1.x key | v0.2.0 status |
|---|---|
| `settings.flows_dir` | Removed — no code generation |
| `settings.flow_type` | Removed |
| `settings.py_tasks_dir` | Removed |
| `settings.r_tasks_dir` | Removed |
| `settings.tasks_conf_path` | Removed |
| `settings.docker_image` | Removed |
| `settings.db` | Kept (`"sqlite"` or `"duckdb"`) |
| `settings.db_path` | Kept |

---

## `pyproject.toml` Changes

v0.2.0 requires a `[tool.kptn]` section that points to the Python module exposing your
pipeline.

```toml
# v0.2.0 pyproject.toml
[tool.kptn]
pipeline = "my_package.pipeline"  # module path with a `pipeline` or `graph` attribute
```

v0.1.x used `flows_dir`, `py_tasks_dir`, `tasks_conf_path`, and `docker_image` under
`[tool.kptn]`. All of these are removed. The only required key is `pipeline`.

---

## Validation Checklist

After migrating, run the following to verify correctness before your first full run:

1. **Check graph wiring and profile selection:**
   ```
   kptn plan --profile <your-profile>
   ```
   Expected output includes `[RUN]` / `[SKIP]` / `[MAP]` markers for each node.
   If profiles reference stale stage names, `ProfileError` is raised with a did-you-mean
   suggestion.

2. **Run the pipeline (cold start):**
   All tasks execute on the first run — cache state from v0.1.x is not carried over.
