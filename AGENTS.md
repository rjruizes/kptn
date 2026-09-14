# Repository Guidelines

## Project Structure & Module Organization
The backend entrypoint is `cli.py`; domain packages live in `codegen/`, `caching/`, `deploy/`, `filewatcher/`, `watcher/`, and `aws/`, with shared helpers in `util/`. `codegen/` renders flows from `kptn.yaml`, `caching/` handles DynamoDB state, and `deploy/` or `dockerbuild/` manage image builds. The pipeline UI is server-rendered out of `kptn_server/`: the FastAPI application factory in `app.py`, page routers in `routes/`, Jinja templates in `kptn_server/templates/`, and vendored assets in `kptn_server/static/`. `kptn-vscode/` is a thin launcher for it; that extension and the Astro documentation site in `doc-site/` are the repository's two Node projects. `tests/` should mirror the package layout or host fixtures like `tests/mock_pipeline/`.

## Build, Test, and Development Commands
Create a virtual environment (`uv venv` or `python -m venv .venv`) and install with `uv pip install -e .` so the `kptn` CLI resolves locally. Run `python cli.py codegen` to regenerate flows, `python cli.py backend` for the FastAPI watcher, `python cli.py watch_files` for change events, and `python cli.py serve_docker` for the Docker helper. The UI has no build step: install its extra with `uv sync --extra web` and serve the project in the current directory with `uv run kptn ui` (or `uv run kptn ui --no-open --port 8000` to skip the browser). It binds `127.0.0.1` with no authentication -- forward a port over SSH rather than binding a routable interface. For the VS Code extension, `cd kptn-vscode && npm install`, then `npm run lint`, `npm run compile`, and `npm test`. Keep the `[tool.kptn]` block in `pyproject.toml` synced when pipeline paths change.

## Coding Style & Naming Conventions
Follow PEP 8 with four-space indentation, type hints, and docstrings, and align filenames with their Typer command or service (e.g., `caching/TaskStateCache.py`). Use snake_case for CLI commands, PascalCase for classes, and keep FastAPI route handlers thin by deferring logic to domain packages. UI pages are Jinja templates in `kptn_server/templates/` styled by `kptn_server/static/app.css`; the only client-side libraries are the htmx and Alpine builds vendored into `kptn_server/static/`, and no page may load from a CDN.

## Testing Guidelines
Prefer `pytest`; mirror the package path under `tests/` (`tests/test_task_state_cache.py`, fixtures in `tests/mock_pipeline/`). Run `pytest -q` (or `uv run pytest -q`) before pushing and extend regression coverage when touching caching, hashing, or deployment logic. UI behaviour is covered by `pytest` as well (`tests/test_ui_*.py`), which needs the extra: `uv run --extra web pytest -q`.

## Commit & Pull Request Guidelines
Keep commits small, present-tense, and under 72 characters, mirroring history like `sync from kptn3`. Each PR should explain motivation, list commands or tests, link tickets, and flag configuration or migration steps. Attach screenshots for UI changes and highlight new environment variables or AWS touchpoints.

## Configuration & Security
Avoid committing Prefect, AWS, or Docker credentials; load them via environment variables (`PREFECT_API_URL`, `SCRATCH_DIR`, `ARTIFACT_STORE`) or helpers in `aws/creds.py`. Treat `pyproject.toml` and `kptn.yaml` as secret-free configuration and keep logging tidy. The pipeline UI adds no CORS middleware and must not gain one: it is loopback-only by design. The `http://localhost:5173` origins in `kptn/watcher/` and `kptn/dockerbuild/` served the deleted React dev server and survive only in those dormant v0.1 modules.
