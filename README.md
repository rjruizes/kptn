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

### Sources

A source reads raw files into the pipeline once per version of those files. Declare its name and files; everything else follows conventions:

```python
import kptn

survey = kptn.source("survey", files="data/survey")

# data/sales holds one export per day: orders_2024-01-24.csv, orders_2024-01-25.csv, ...
# Each file would be its own table by default; dropping the date puts them all in "orders".
sales = kptn.source(
    "sales",
    files="data/sales",
    table=lambda f: f.name.rsplit("_", 1)[0],
)

raw_ingest = kptn.Stage("raw_ingest", survey, sales)
pipeline = kptn.Pipeline(
    "main",
    kptn.config(duckdb=(get_engine, "engine"), config=get_config)
    >> raw_ingest
    >> kptn.source_views(raw_ingest)
    >> transform,
)
```

| Convention | |
|---|---|
| Reading | By extension: `.csv`, `.tsv`, `.txt`, `.json`, `.jsonl`, `.ndjson`, `.parquet`. Pass `read=` for anything else, such as Excel. |
| Values | Every column stored as text, under its name in lowercase, so files that spell a column differently (`mealTime`, `MEALTIME`) feed one `mealtime` column. Transform downstream. |
| Tables | One per file stem, unless `table=` says otherwise. Files feeding one table are combined by column name. |
| Provenance | A `_source_file` column, such as `sales/orders_2024-01-24.csv`. |
| Checks | `checks/<source>/<table>.yml`, run with Soda (`pip install kptn[soda]`) before the table is written, naming the table as its view does without the schema (`checks for sales_orders:`) and its columns in lowercase. A failure stops the run. |
| Storage | `_bronze/<source>/<table>.parquet`. |
| Views | `raw.<source>_<table>`, created by `kptn.source_views`. |

Each table is cached on its own files, check file and the source's code. A change upstream, such as a rebuilt schema, does not re-read unchanged files.

### Shared bronze

Working directories that read the same files can share built tables. Name a shared bronze directory in `kptn.yaml`, relative to the project, and create it where you want sharing:

```yaml
settings:
  shared_bronze: ../_bronze
```

kptn shares only through a directory that exists, and never creates it. One committed `kptn.yaml` therefore serves the server, where the directory was set up, and every laptop, where it never is and each working directory builds its own tables. A run that finds the setting but not the directory says so: `[SHARE] off — ../_bronze does not exist, so tables are built here`. To share between checkouts on a laptop, `mkdir ../_bronze`.

A table built in one working directory is published there under a key made from its files' paths, sizes and modification times, its check file, the source's code and kptn's reading conventions. Any working directory that computes the same key links to it instead of reading the files again: `_bronze/<source>/<table>.parquet` becomes a symlink into the shared bronze. A restored table counts as a change, so what reads it reruns.

The console says which happened: `[RESTORE] … — from shared bronze`, or `[SHARE] … — published` after a build (`— linked to an identical build` when another run published the same key first). `kptn plan` and the UI's plan page show "in shared bronze" for a table the next run would restore.

| To | |
|---|---|
| Build here without sharing, for one run | `kptn run --no-shared-bronze` |
| … for a whole session | `export KPTN_SHARED_BRONZE=off` |
| Rebuild here and ignore the shared bronze | `kptn run --force` (and `no_cache=True`) builds a local file, never publishes |
| Remove a bad entry | Delete `<shared_bronze>/<source>/<table>/<key>/`. Every working directory linked to it rebuilds on its next run. |

The shared bronze holds `entry.json` and one parquet file per entry, and nothing else. `entry.json` records the key's parts, every input file with its size and modification time, the row count and columns, and who built it, where, when and with which kptn; kptn never reads it. `.tmp/` holds publishes in progress, or left by a crashed run, and can be cleared when no run is active. Entries are never pruned; remove a release's shared bronze with its release.

**Several accounts on one share.** Every account that runs the pipeline must read every entry and create new ones. Put them all in one Unix group, with the same group ID on every machine or pod that mounts the share, and create the shared bronze directory:

```shell
mkdir ../_bronze
chgrp <pipeline-group> ../_bronze
chmod 2775 ../_bronze
```

kptn creates everything inside it with modes `2775` (directories) and `0664` (files), whatever each user's umask. kptn's state store and the pipeline database stay in each working directory; only built parquet files are shared.

### Shared logs

A working directory's run history normally lives in its `.kptn/runs/`, and goes when the directory is deleted. To keep it, name a shared logs directory in `kptn.yaml`, relative to the project, and create it where you want the logs kept:

```yaml
settings:
  shared_logs: ../_kptn_logs
```

As with the shared bronze, kptn uses the directory only where it exists and never creates it, so a laptop without one keeps its runs in `.kptn/runs/`. A recorded run that finds the setting but not the directory says so in its log: `[LOGS] ../_kptn_logs does not exist, so this run's log stays in .kptn/runs`.

Recorded runs, from `kptn run` or started in `kptn ui`, then write their `<run_id>.jsonl` and the project's `index.json` to `../_kptn_logs/<working directory name>/` from the start. Delete `r7/alice_main/` and its history stays in `r7/_kptn_logs/alice_main/`. Only the logs move: `.kptn/ui.db` and kptn's state store stay in the working directory and go with it. `kptn run --no-record` and `kptn.run()` from a notebook keep no log, with or without the setting.

Keep one logs directory per release, beside the working directories: a subdirectory is named by its working directory alone, so two releases sharing one would mix the histories of same-named directories.

In `kptn ui --projects-root`, a deleted working directory whose logs remain is listed under **Removed** in the project picker and opens read-only at `/view/<directory>/`, yours included. They are found through the logs directories that the release's remaining working directories name, so a release with no working directories left lists none (the files stay on disk). A logs directory outside the projects root is never read, whoever's `kptn.yaml` names it.

A directory recreated under the same name keeps one history. Its new `ui.db` numbers runs from 1 again, so the index can list two runs `#1`; each still has its own run id and page. Its own `/p/` history shows only the runs its current `ui.db` recorded.

| To | |
|---|---|
| Bring along runs recorded before the setting | `cp -p .kptn/runs/*.jsonl ../_kptn_logs/<name>/` — until then, colleagues see them listed without a log |
| Forget a directory's history | Delete `_kptn_logs/<name>/` |

**Several accounts on one share.** Use the pipeline group from the shared bronze and create the directory:

```shell
mkdir _kptn_logs
chgrp <pipeline-group> _kptn_logs
chmod 2775 _kptn_logs
```

kptn creates each working directory's subdirectory with mode `2755` and its files with `0644`, whatever the umask: everyone can read every history, and only its owner writes it.

### Caching and Re-runs

kptn hashes each task's declared outputs and source code. On re-run:

- Tasks whose outputs exist and haven't changed are **skipped**.
- Tasks whose source code or upstream dependencies changed are **re-run**.
- Source tables are **rebuilt** only when their own files, check file or the source's code change. Data files are recognised by size and modification time, so a touched file rebuilds its table; `kptn run --force` rebuilds everything.

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
| `kptn.source(name, files, *, table=None, read=None)` | Read raw files into `_bronze/<name>/`, one text-only parquet per table |
| `kptn.source_views(stage)` | Create `raw.<source>_<table>` views for the sources in a stage |

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
kptn run  [--profile PROFILE] [--force] [--no-record] [--no-shared-bronze] # execute the pipeline
kptn ui   [--port PORT] [--no-open]     # serve the pipeline UI for this project
```

---

## Pipeline UI

`kptn ui` serves a small web UI for the project in the current directory: start
a run, watch its console live, read the plan, and walk the resolved pipeline
with its documentation. It is the **only** supported UI, and
jupyter-server-proxy launches this same command from a config file rather
than embedding a second one.

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

Loopback alone does not stop a *page you visit* from posting at the server:
`POST /runs` is form-encoded, so a browser sends it cross-origin with no
preflight. Every state-changing request is therefore refused when its
`Sec-Fetch-Site` header says another origin initiated it. Requests with no
such header — `curl`, jupyter-server-proxy's own health check — are unaffected.

### Where the UI keeps its state

Two paths inside the project, and nothing else:

| Path | Contents |
|------|----------|
| `.kptn/ui.db` | Run history: one row per run, the run lock, and an index of every event |
| `.kptn/runs/<run_id>.jsonl` | Every event of the run, captured output included, one JSON object per line |
| `.kptn/runs/index.json` | A summary of the latest 50 runs, rewritten as runs start, progress, and end |

The `.jsonl` file *is* the run's log: the console reads output from it, and the
log download is rendered from it. Runs recorded by an earlier kptn have a
`.kptn/runs/<run_id>.log` of raw output instead, and are still shown.

All of it is inside the project on purpose, so run history travels with a checkout
and is removed by deleting `.kptn/` — unless `settings.shared_logs` moves the
run files out, so that they outlive the checkout (see [Shared logs](#shared-logs)). `.kptn/ui.db` is separate from kptn's own
task-state database (`.kptn/kptn.db` by default): clearing UI history never
invalidates the cache, and `kptn run --force` never erases history.

### Terminal runs are recorded too

`kptn run` records its run in the same history, so `kptn ui` lists it and
serves its log exactly as it does for a run it started. The run happens in your
terminal process as before, with its output on your terminal; the same output
is also written to `.kptn/runs/<run_id>.jsonl`. Stopping it from the UI ends it as
Ctrl-C does, and either way it is recorded as stopped. Pass `--no-record` to leave a run out of the
history. Output a task writes straight to a file descriptor (a subprocess
inheriting it, a C extension) reaches the terminal but not the log.

### One active run per project

A project has at most one active recorded run. Starting a second one from the
UI is refused with a 409 that names the run holding the lock; `kptn run` exits
with an error that names it. A run whose process was killed releases the lock
once its heartbeat is 15 seconds old.

The lock lives in `.kptn/ui.db`. `kptn run --no-record` knows nothing about it
— it neither takes the lock nor is blocked by one — so it can overlap a
recorded run and write the same task state.

### The run survives the server

A run is a **detached child process**, not a request handler. It is launched
into its own session and writes to `.kptn/ui.db` and its run file directly, so
the run keeps going when you:

- close the browser tab, or navigate away, or lose the SSE connection
- restart the notebook server behind the proxy
- stop and restart `kptn ui`

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
against the project root and refused if it escapes it — per reference, so one
bad path costs that task its documentation panel and marks its row, rather
than costing the whole walkthrough. The Markdown is rendered with raw HTML
disabled, and no page in this UI edits a documentation file.
Where a declared output can be resolved to a file through `kptn.yaml`, the task
panel also links to its lineage graph and a preview of its rows.

### Behind jupyter-server-proxy

In a JupyterHub notebook environment, jupyter-server-proxy takes over what a
VS Code extension used to do by hand: it finds the interpreter, reserves a
loopback port, resolves the external path prefix, and starts
`kptn ui --root-path <prefix>` — the same UI a direct browser gets, from the
same server, framed by the notebook's own page rather than a webview. See
`--root-path`, `KPTN_UI_ROOT_PATH`, and `kptn ui --projects-root` for the
pieces the proxy config drives.

### Browsing other people's projects

With `--projects-root`, the project picker at the left of the app bar lists
every working directory in the release. It opens by itself on the home page,
since nothing else can show until a project is chosen. Yours open in full, at
`/p/<directory>/`; everyone else's open read-only at `/view/<directory>/` —
their run history, each run's console (live while it runs), and its log
download.

Those pages read only `.kptn/runs/index.json` and the runs' `.jsonl` files
(from the project's shared logs directory, when it sets one).
They never open another person's `.kptn/ui.db`: each person's server runs in
its own pod, and SQLite's WAL mode is not safe to share between machines over
a network filesystem. Nor do they import another person's pipeline, so there
is no Plan, lineage, or walkthrough for their projects, and nothing that
starts, stops, or retries a run.

A run whose owner's pod stops heartbeating is shown as "not reporting" after a
minute. Only the owner's own `kptn ui` can tell that its process is gone and
mark it interrupted. Who may read whose files is decided by the shared
folder's permissions, not by kptn.

### Terminal output is unchanged

`kptn run` and `kptn plan` print exactly what they always did. The UI is an
additional surface over the same runner, not a replacement for it.
