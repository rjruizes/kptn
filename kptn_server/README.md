# kptn_server

The FastAPI application behind `kptn ui`. One factory call
(`kptn_server.app.create_app`) serves one project: the directory the command
was launched from. `kptn_server.app.create_multi_app` serves several -- the
working directories one person has under a shared release folder -- for
`kptn ui --projects-root`, which is how the app runs behind
jupyter-server-proxy.

There is one supported UI. The React application, its root Cypress harness,
the JSON-RPC backend a VS Code extension used to spawn, and the extension
itself were all removed once this UI reached parity and jupyter-server-proxy
took over launching it; the lineage and table-preview services the old
backend used are retained and are served by this same application.

## Run the server

```shell
uv sync --extra web
uv run kptn ui                        # serve ./ and open a browser
uv run kptn ui --no-open --port 8000  # serve ./ and print the URL only
```

`--host` defaults to `127.0.0.1`. There is no authentication and no remote
execution mode; see the README's "Pipeline UI" section for the SSH
port-forwarding recipe if you need to reach it from another machine.

## Layout

| Path | What it is |
|------|-----------|
| `app.py` | The application factory, and the reconciliation loop that settles runs whose worker is gone |
| `project.py` | `ProjectContext`: which project, which pipeline, which profiles, where its UI state lives |
| `run_store.py` | The SQLite run store (`.kptn/ui.db`), created by replaying `migrations/` |
| `processes.py` | Launching, inspecting, and reconciling detached run workers |
| `worker.py`, `capture.py` | The worker process and the capture that writes `.kptn/runs/<run_id>.log` |
| `routes/` | `runs.py` (console, history, SSE, log, stop), `inspect.py` (plan, walkthrough), `lineage.py` (lineage, table preview) |
| `service.py` | The retained lineage and table-preview helpers, driven by `routes/lineage.py` |
| `markdown.py` | Project-relative, read-only Markdown rendering for the walkthrough's docs panel |
| `templates/`, `static/`, `migrations/` | Read package-relative, and force-included into the wheel (see `pyproject.toml`) |

Assets are vendored: `static/` holds htmx and Alpine, and nothing on any page
loads from a CDN.

## Tests

```shell
uv run --extra web pytest -q
```

## jupyter-server-proxy

In a JupyterHub notebook environment, jupyter-server-proxy's config file
finds the interpreter, reserves a loopback port, resolves the external path
prefix, and starts `kptn ui --root-path <prefix>` -- what a VS Code extension
used to do by hand before the extension was retired. See the README's
"Behind jupyter-server-proxy" section.
