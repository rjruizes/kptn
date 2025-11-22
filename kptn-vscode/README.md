# kptn README

# kptn VS Code extension

This extension now starts a long-lived Python backend and exchanges JSON-RPC messages with it over stdio. The bundled backend returns a timestamped greeting that is shown via VS Code notifications.

## Quick start

1. Launch the extension in the VS Code extension host (Run and Debug → `Run Extension` or `F5`).
2. Run the command `Kptn: Fetch kptn message` from the Command Palette.
3. The extension will spawn `backend.py` with your default `python` (override with `KPTN_VSCODE_PYTHON=/path/to/python`) and send a JSON-RPC `getMessage` request over stdio.
4. You should see the backend-provided message via `showInformationMessage`. Errors show as an error notification; backend stderr is forwarded to the “kptn backend” Output channel for troubleshooting.

## Python path resolution

- **Prerequisite**: The extension requires `kptn` to be installed in your active Python environment. Install it with `pip install kptn` or activate an environment where it's already installed.
- On activation, the extension sets `PYTHONPATH` for the backend: it uses the sibling checkout `../kptn` if present (for development in the monorepo). Any existing `PYTHONPATH` is appended.
- The backend interpreter remains configurable via `KPTN_VSCODE_PYTHON`.

## Packaging

- From anywhere inside `kptn-vscode`, run `./scripts/package.sh` to build a VSIX. The script will install dependencies if `node_modules/` is missing and then invoke `vsce package` (which triggers TypeScript compilation via `vscode:prepublish`).
- Users of the packaged extension must have `kptn` installed in their Python environment.
