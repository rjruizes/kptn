#!/usr/bin/env bash
set -euo pipefail

# Package the kptn VS Code extension into a VSIX.
# Usable from any subdirectory within the kptn-vscode project.

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

if [ ! -d "node_modules" ]; then
    echo "node_modules not found; installing dependencies..."
    npm install
fi

echo "Packaging extension from ${PROJECT_ROOT}..."
npm exec vsce package "$@"
