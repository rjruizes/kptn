# Repository Guidelines

## Project Structure & Module Organization
The backend entrypoint is `cli.py`; domain packages live in `codegen/`, `caching/`, `deploy/`, `filewatcher/`, `watcher/`, and `aws/`, with shared helpers in `util/`. `codegen/` renders flows from `kptn.yaml`, `caching/` handles DynamoDB state, and `deploy/` or `dockerbuild/` manage image builds. The React SPA resides in `ui/` (components in `ui/src`, static assets in `ui/public`), while `tests/` should mirror the package layout or host fixtures like `tests/mock_pipeline/`.

## Build, Test, and Development Commands
Create a virtual environment (`uv venv` or `python -m venv .venv`) and install with `uv pip install -e .` so the `kptn` CLI resolves locally. Run `python cli.py codegen` to regenerate flows, `python cli.py backend` for the FastAPI watcher, `python cli.py watch_files` for change events, and `python cli.py serve_docker` for the Docker helper. For the UI, `cd ui && npm install`, then use `npm run dev`, `npm run build`, and `npm run lint`; keep the `[tool.kptn]` block in `pyproject.toml` synced when pipeline paths change.

## Coding Style & Naming Conventions
Follow PEP 8 with four-space indentation, type hints, and docstrings, and align filenames with their Typer command or service (e.g., `caching/TaskStateCache.py`). Use snake_case for CLI commands, PascalCase for classes and React components, and keep FastAPI route handlers thin by deferring logic to domain packages. Tailwind utilities should style UI components, and hooks belong in `ui/src/hooks/`.

## Testing Guidelines
Prefer `pytest`; mirror the package path under `tests/` (`tests/test_task_state_cache.py`, fixtures in `tests/mock_pipeline/`). Run `pytest -q` (or `uv run pytest -q`) before pushing and extend regression coverage when touching caching, hashing, or deployment logic. Document manual UI test steps in the PR until Vitest coverage lands.

## Commit & Pull Request Guidelines
Keep commits small, present-tense, and under 72 characters, mirroring history like `sync from kptn3`. Each PR should explain motivation, list commands or tests, link tickets, and flag configuration or migration steps. Attach screenshots for UI changes and highlight new environment variables or AWS touchpoints.

## Configuration & Security
Avoid committing Prefect, AWS, or Docker credentials; load them via environment variables (`PREFECT_API_URL`, `SCRATCH_DIR`, `ARTIFACT_STORE`) or helpers in `aws/creds.py`. Treat `pyproject.toml` and `kptn.yaml` as secret-free configuration and ensure new FastAPI routes keep explicit CORS origins (`http://localhost:5173`) and tidy logging.
