# kptn README

# kptn VS Code extension

This extension now starts a long-lived Python backend and exchanges JSON-RPC messages with it over stdio. The bundled backend returns a timestamped greeting that is shown via VS Code notifications.

## Quick start

1. Launch the extension in the VS Code extension host (Run and Debug → `Run Extension` or `F5`).
2. Run the command `Kptn: Fetch kptn message` from the Command Palette.
3. The extension will spawn `backend.py` with your default `python` (override with `KPTN_VSCODE_PYTHON=/path/to/python`) and send a JSON-RPC `getMessage` request over stdio.
4. You should see the backend-provided message via `showInformationMessage`. Errors show as an error notification; backend stderr is forwarded to the “kptn backend” Output channel for troubleshooting.

## Python path resolution

- On activation, the extension sets `PYTHONPATH` for the backend: it prefers the sibling checkout `../kptn` if present, then falls back to a vendored copy at `python_libs/` inside the extension. Any existing `PYTHONPATH` is appended.
- The backend interpreter remains configurable via `KPTN_VSCODE_PYTHON`.

## Vendoring the kptn library for packaging

- Run `npm run vendor-python` from `kptn-vscode` to pip install the monorepo root (`../`) into `python_libs/` (it falls back to `../kptn` if the root lacks a `pyproject.toml`/`setup.py`). This is invoked automatically during `vsce package` via the `vscode:prepublish` hook.
- Ensure `python` (or `KPTN_VSCODE_PYTHON`) has `pip` available and can access the sibling checkout.
- Generated `python_libs/` is included in the VSIX (it is not excluded by `.vscodeignore`). Remove it or re-run the script to refresh before packaging.

## Packaging

- From anywhere inside `kptn-vscode`, run `./scripts/package.sh` to build a VSIX. The script will install dependencies if `node_modules/` is missing and then invoke `vsce package` (which triggers vendoring + compile via `vscode:prepublish`).
