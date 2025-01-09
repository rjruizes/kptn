FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

COPY pyproject.toml uv.lock README.md /app/

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"
RUN uv sync --all-extras
CMD [ "uv", "run", "watcher" ]