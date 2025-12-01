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
# Copy kptn wheel from build context (provided by build script)
ARG KPTN_WHEEL
COPY ${KPTN_WHEEL} /app/

# Update pyproject.toml to reference local kptn
RUN test -n "${KPTN_WHEEL}" || (echo "KPTN_WHEEL build arg not set" && exit 1) \
    && uv add "/app/${KPTN_WHEEL}"
# Copy source files
COPY src/ ./src/

# Copy configuration and entry point
COPY kptn.yaml run.py ./

# Set Python path
ENV PYTHONPATH=/app

# Default command (can be overridden)
CMD [".venv/bin/python", "run.py"]
