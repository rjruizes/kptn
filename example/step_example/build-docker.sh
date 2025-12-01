#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EXAMPLE_DIR="${REPO_ROOT}/example/step_example"

cd "${REPO_ROOT}"

# Build the wheel for the current repo
uv build --wheel

# Grab the newest wheel and copy it into the build context, keeping the versioned filename
WHEEL_PATH=$(ls -t "${REPO_ROOT}"/dist/kptn-*.whl | head -n 1)
WHEEL_BASENAME=$(basename "${WHEEL_PATH}")
cp "${WHEEL_PATH}" "${EXAMPLE_DIR}/${WHEEL_BASENAME}"

cd "${EXAMPLE_DIR}"
docker build --build-arg KPTN_WHEEL="${WHEEL_BASENAME}" -t kptnstep .
