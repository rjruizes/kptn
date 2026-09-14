# kptn_server

The FastAPI application behind `kptn ui`. One factory call
(`kptn_server.app.create_app`) serves exactly one project: the directory the
command was launched from.

There is one supported UI and one VS Code launch path. The React application,
its root Cypress harness, and the JSON-RPC backend the extension used to spawn
were all removed once this UI reached parity; the lineage and table-preview
services they used are retained and are served by this same application.

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

## VS Code extension

The extension is a thin launcher: it runs `kptn ui --no-open`, waits for
`/healthz`, and opens the served URL in a webview.

- Debug the extension: open `kptn-vscode/src/extension.ts` in VS Code and press `F5`
- Package the extension for installation: `./kptn-vscode/scripts/package.sh`
