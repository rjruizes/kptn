FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install uv for dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN touch README.md
RUN uv sync --frozen --no-dev
# Copy kapten package from parent directory
COPY kapten-0.1.0-py3-none-any.whl /app/

# Update pyproject.toml to reference local kapten
RUN uv add /app/kapten-0.1.0-py3-none-any.whl
# Copy source files
COPY src/ ./src/

# Copy configuration and entry point
COPY kapten.yaml run.py ./

# Set Python path
ENV PYTHONPATH=/app

# Default command (can be overridden)
CMD [".venv/bin/python", "run.py"]
