# kptn

**Lightweight, cacheable data pipelines in Python, R, and SQL.**

[![PyPI version](https://img.shields.io/pypi/v/kptn)](https://pypi.org/project/kptn/)
[![Python versions](https://img.shields.io/pypi/pyversions/kptn)](https://pypi.org/project/kptn/)
[![License](https://img.shields.io/pypi/l/kptn)](LICENSE)

kptn lets you define data pipelines as composable Python functions, SQL files, and R scripts. Pipelines are hash-cached by default — unchanged tasks are skipped automatically on re-runs, so you only pay for what changed. Profiles let you parameterize and filter runs by environment or dataset without changing code. Cloud deployment to AWS is on the roadmap.

---

## Installation

```shell
pip install kptn
```

**Optional extras:**

```shell
pip install kptn[duckdb]   # DuckDB state store and SQL tasks
pip install kptn[aws]      # AWS deployment (work in progress)
```

**CLI setup** — add to your project's `pyproject.toml`:

```toml
[tool.kptn]
pipeline = "your_package.pipeline"  # module that exposes a `pipeline` attribute
```

---

## Concepts

- **Tasks** — the unit of work. A task is a Python function decorated with `@kptn.task`, a SQL file registered with `kptn.sql_task()`, or an R script registered with `kptn.r_task()`. Each task declares its output files.
- **Graphs** — tasks are composed into a directed acyclic graph using the `>>` operator for sequential chaining and operators like `parallel()`, `Stage()`, and `map()` for branching.
- **Caching** — kptn hashes each task's outputs and source code. On re-run, tasks whose outputs and dependencies haven't changed are skipped. Use `kptn plan` to preview what will run before committing.
- **Profiles** — named configurations in `kptn.yaml` that filter stages, override task arguments, and control execution cursors. Useful for running a subset of the pipeline for a specific dataset or environment.

---

## Quick Start

```python
# pipeline.py
import kptn
from pathlib import Path


def get_greeting() -> str:
    return "Hello, kptn!"


@kptn.task(outputs=["output/extract.txt"])
def extract(greeting: str) -> None:
    Path("output").mkdir(exist_ok=True)
    Path("output/extract.txt").write_text(greeting)


@kptn.task(outputs=["output/transform.txt"])
def transform() -> None:
    data = Path("output/extract.txt").read_text()
    Path("output/transform.txt").write_text(data.upper())


@kptn.task(outputs=["output/load.txt"])
def load() -> None:
    data = Path("output/transform.txt").read_text()
    Path("output/load.txt").write_text(f"Loaded: {data}")


deps = kptn.config(greeting=get_greeting)
graph = deps >> extract >> transform >> load
pipeline = kptn.Pipeline("hello_kptn", graph)
```

Preview the plan, then run:

```shell
kptn plan
kptn run
```

Or run directly from Python:

```python
kptn.run(pipeline)
```

---

## Core Features

### Graph Composition

Chain tasks sequentially with `>>`:

```python
graph = extract >> transform >> load
```

Fan out to parallel branches with `kptn.parallel()`:

```python
graph = ingest >> kptn.parallel(transform_a, transform_b) >> merge
```

Use `kptn.Stage()` to define profile-selectable branches. The profile controls which branches are active at runtime:

```python
datasets = kptn.Stage(
    "datasets",
    load_full,
    load_subset,
)
graph = ingest >> datasets >> analyze
```

Use `kptn.map()` to fan out dynamically over a runtime collection:

```python
graph = list_items >> kptn.map(process_item, over="items")
```

### Profiles

Profiles are defined in `kptn.yaml` at your project root. They let you parameterize runs without changing code.

```yaml
settings:
  db: duckdb
  db_path: pipeline.db

profiles:
  full:
    stage_selections:
      datasets: [load_full]

  subset:
    stage_selections:
      datasets: [load_subset]
    args:
      analyze:
        limit: 1000

  subset_test:
    extends: subset
    stop_after: transform
    optional_groups:
      qa_checks: false
```

**Profile keys:**

| Key | Description |
|-----|-------------|
| `extends` | Inherit settings from another profile (or a list of profiles) |
| `stage_selections` | Map of stage name → list of branch names to activate |
| `args` | Per-task keyword argument overrides |
| `start_from` | Skip tasks before this task name |
| `stop_after` | Skip tasks after this task name |
| `optional_groups` | Enable or disable named optional task groups |

Run with a profile:

```shell
kptn run --profile subset
kptn plan --profile subset
```

### DuckDB Integration

Pass a DuckDB connection factory via `kptn.config()`. Tasks receive the connection as a keyword argument:

```python
import duckdb
import kptn
from pathlib import Path


def get_engine():
    return duckdb.connect("pipeline.db")


@kptn.task(outputs=["output/summary.parquet"])
def summarize(engine) -> None:
    engine.execute("COPY (SELECT * FROM raw) TO 'output/summary.parquet'")


ingest = kptn.sql_task("sql/ingest.sql", outputs=["raw"])
deps = kptn.config(duckdb=(get_engine, "engine"))
graph = deps >> ingest >> summarize
pipeline = kptn.Pipeline("my_pipeline", graph)
```

The `duckdb=(factory, "alias")` tuple tells kptn to inject the connection under the name `"engine"`. SQL tasks receive it automatically.

Add `duckdb_checkpoint=True` to a task to persist a DuckDB checkpoint after it runs, enabling incremental restores:

```python
@kptn.task(outputs=["output/final.parquet"], duckdb_checkpoint=True)
def finalize(engine) -> None:
    ...
```

### Caching and Re-runs

kptn hashes each task's declared outputs and source code. On re-run:

- Tasks whose outputs exist and haven't changed are **skipped**.
- Tasks whose source code or upstream dependencies changed are **re-run**.

To bypass the cache for a single run:

```shell
kptn run --force
```

To preview what would run without executing:

```shell
kptn plan
```

---

## API Reference

### Tasks

| Symbol | Description |
|--------|-------------|
| `@kptn.task(outputs, optional=None, compute=None, duckdb_checkpoint=False)` | Decorate a Python function as a kptn task |
| `kptn.sql_task(path, outputs, optional=None, duckdb_checkpoint=False)` | Register a SQL file as a task |
| `kptn.r_task(path, outputs, compute=None, optional=None, duckdb_checkpoint=False)` | Register an R script as a task |
| `kptn.noop()` | Placeholder / synchronization node |

### Graph Composition

| Symbol | Description |
|--------|-------------|
| `>>` | Chain tasks or graphs sequentially |
| `kptn.parallel(*branches)` | Fan out to parallel branches. Accepts an optional name as the first argument: `kptn.parallel("name", a, b)` |
| `kptn.Stage(name, *branches)` | Profile-selectable branches grouped under a named stage |
| `kptn.map(task_fn, over="key")` | Dynamic fanout over a runtime collection |

### Pipeline & Execution

| Symbol | Description |
|--------|-------------|
| `kptn.config(**kwargs)` | Declare dependency injection factories. Use `duckdb=(factory, "alias")` for DuckDB connections |
| `kptn.Pipeline(name, graph)` | Wrap a graph in a named pipeline |
| `kptn.run(pipeline, *, profile=None, keep_db_open=False, no_cache=False, force=False)` | Execute the pipeline |
| `kptn.plan(pipeline, *, profile=None)` | Dry-run: print which tasks would run or be skipped |

---

## CLI Reference

The `kptn` CLI discovers your pipeline from `[tool.kptn] pipeline = "..."` in `pyproject.toml`. The referenced module must expose a `pipeline` attribute of type `Pipeline`.

```shell
kptn plan [--profile PROFILE]          # preview what will run or be skipped
kptn run  [--profile PROFILE] [--force] # execute the pipeline
```
