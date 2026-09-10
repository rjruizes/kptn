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
pip install kptn[web]      # the pipeline UI (`kptn ui`)
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

### Demand-Driven Dependencies

Declare prerequisites with `requires=[...]` instead of chaining them with `>>`. A required task is pulled into the run **only when a consumer needs it**, runs **once** even if several consumers require it, and is ordered before every requirer. This is ideal for expensive shared prerequisites:

```python
@kptn.task(outputs=["duckdb://index"])
def build_index(engine) -> None:
    ...  # expensive; only worth running when something needs the index


@kptn.task(outputs=["output/report.txt"], requires=[build_index])
def report(engine) -> None:
    ...


# build_index is never chained with >> — `report` pulls it in:
pipeline = kptn.Pipeline("analysis", report)
```

`requires` is transitive: a required task's own `requires` are pulled in too. If you already place a task in the graph yourself (via `>>`), `requires` for that task is a no-op — your explicit wiring governs ordering. Note that conjunctive `requires` only injects and orders prerequisites — it never *drops* a consumer. If a user-placed prerequisite is later pruned by a profile, its consumer still runs; use `kptn.any_of(...)` when you need a missing prerequisite to skip the consumer.

Use `kptn.any_of(...)` for a disjunctive requirement — a *gate* that pulls nothing and is satisfied only if one of its members is already in the run. If none is present, the consumer is skipped:

```python
@kptn.task(
    outputs=["output/combined.txt"],
    requires=[kptn.any_of(load_full, load_subset)],
)
def summarize(engine) -> None:
    ...
```

This pairs naturally with `kptn.Stage()`: whichever branch the active profile selects satisfies the `any_of` gate, and `summarize` runs against it.

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
| `@kptn.task(outputs, optional=None, compute=None, duckdb_checkpoint=False, requires=None)` | Decorate a Python function as a kptn task |
| `kptn.sql_task(path, outputs, optional=None, duckdb_checkpoint=False, requires=None)` | Register a SQL file as a task |
| `kptn.r_task(path, outputs, compute=None, optional=None, duckdb_checkpoint=False, requires=None)` | Register an R script as a task |
| `kptn.noop()` | Placeholder / synchronization node |

### Graph Composition

| Symbol | Description |
|--------|-------------|
| `>>` | Chain tasks or graphs sequentially |
| `kptn.parallel(*branches)` | Fan out to parallel branches. Accepts an optional name as the first argument: `kptn.parallel("name", a, b)` |
| `kptn.Stage(name, *branches)` | Profile-selectable branches grouped under a named stage |
| `kptn.map(task_fn, over="key")` | Dynamic fanout over a runtime collection |
| `kptn.any_of(*tasks)` | Disjunctive requirement group for `requires=` — satisfied if any member task is present in the run (otherwise the consumer is skipped) |

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
kptn ui   [--port PORT] [--no-open]     # serve the pipeline UI for this project
```

---

## Pipeline UI

`kptn ui` serves a small web UI for the project in the current directory: start
a run, watch its console live, read the plan, and walk the resolved pipeline
with its documentation. It is the **only** supported UI, and the VS Code
extension launches this same command rather than embedding a second one.

```shell
uv sync --extra web                    # or: pip install 'kptn[web]'
uv run kptn ui                         # serve ./ and open a browser
uv run kptn ui --no-open --port 8000   # serve ./ and just print the URL
```

The command serves `Path.cwd()` and takes no project path — the project is
never something a request can choose.

### Loopback only, by design

The server binds `127.0.0.1` by default. There is **no authentication and no
remote-execution mode**, because the UI can start a pipeline: exposing it on a
routable interface would be an unauthenticated remote runner for anyone who can
reach the port. To use it from another machine, forward the port over SSH
instead of binding a public interface:

```shell
ssh -N -L 8000:127.0.0.1:8000 you@build-host
# then open http://127.0.0.1:8000 locally
```

`--host` exists for containers and similar, and nothing about a wider bind is
made safe by it. Treat it as your own responsibility.

### Where the UI keeps its state

Two paths inside the project, and nothing else:

| Path | Contents |
|------|----------|
| `.kptn/ui.db` | Run history: one row per run, plus every captured console event |
| `.kptn/runs/<run_id>.log` | The run's raw captured output, streamed and downloadable |

Both are inside the project on purpose, so run history travels with a checkout
and is removed by deleting `.kptn/`. `.kptn/ui.db` is separate from kptn's own
task-state database (`.kptn/kptn.db` by default): clearing UI history never
invalidates the cache, and `kptn run --force` never erases history.

### One active run per project

A project has at most one active run. Starting a second one is refused with a
409 that names the run holding the lock, so two runs can never write the same
task state at the same time — including a run started from the terminal while
the UI is open, since both take the same project lock.

### The run survives the server

A run is a **detached child process**, not a request handler. It is launched
into its own session and writes to `.kptn/ui.db` and its log file directly, so
the run keeps going when you:

- close the browser tab, or navigate away, or lose the SSE connection
- quit VS Code
- stop and restart `kptn ui` — including the automatic restart on a file save

Reopening the run page picks the console back up where it left off. The stream
is resumable: the page asks for events after the last sequence number it has,
so nothing is missed and nothing is shown twice.

What does *not* survive is the machine. A host reboot, an OOM kill, or a
`kill -9` takes the worker with it, and there is no way to resume a
half-finished pipeline. The server therefore reconciles on startup and on a
fixed interval: a run whose worker is provably gone is marked
**interrupted**, which releases the project lock and says plainly that the run
did not finish rather than leaving it "running" forever. Re-run it when you are
ready — kptn's cache means completed tasks are skipped.

If a run is wedged in a state the supervisor cannot prove is dead (an
un-inspectable process, say), the run page offers **Stop**, and then a
confirmed force-finish as an escape hatch — you type `abandon`, because the
worker may still be alive. It records the run as **interrupted**; it never
pretends the pipeline succeeded.

### Warnings

Every `warnings.warn` call and every log record at `WARNING` or above is captured,
attributed to the task that emitted it, and grouped on the run page by task and
category. Each group lists every occurrence as a link into the exact console
row that produced it, so a repeated warning is still individually reachable.
Warnings raised outside any task — at import time, for instance — are grouped
as "outside any task" rather than blamed on whichever task ran next. The run
history shows the same headline per run, so a run that warned is visible
without opening it.

### Plan and walkthrough

`/plan` renders the same entries `kptn plan` prints, from the same
`build_plan` — the page cannot develop its own opinion about what is stale.
Opening it never writes to the project: on a project that has never run, a
read-only stand-in answers "nothing cached" instead of creating a state
database as a side effect of a page view.

`/walkthrough` lists every node of the profile-resolved graph in the runner's
order, with bypassed tasks shown and marked rather than hidden. Task metadata
is a pure read model — `description`, `inputs`, `outputs`, and `docs` on a
task, `Stage`, or `Pipeline` — and declaring it changes nothing about
scheduling or execution:

```python
@kptn.task(
    outputs=["main.widgets"],
    inputs=["raw_widgets"],
    description="Aggregate widgets by region.",
    docs="docs/widgets.md#aggregation",
)
def build_widgets(): ...
```

A `docs` reference is **project-relative and read-only**. It is resolved
against the project root and refused if it escapes it, the Markdown is rendered
with raw HTML disabled, and no page in this UI edits a documentation file.
Where a declared output can be resolved to a file through `kptn.yaml`, the task
panel also links to its lineage graph and a preview of its rows.

### VS Code

The extension contributes one command, **`kptn: Open Pipeline UI`**
(`kptn.openUI`). It runs `kptn ui --no-open` for the workspace folder, waits
for `/healthz`, and opens the served URL in a webview — the same UI a browser
gets, from the same server. Reusing the command focuses the existing view
rather than starting a second server.

### Terminal output is unchanged

`kptn run` and `kptn plan` print exactly what they always did. The UI is an
additional surface over the same runner, not a replacement for it.
