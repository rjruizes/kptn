# kptn VS Code extension

A thin launcher for the shared kptn pipeline UI. The extension starts the same
FastAPI app you would get from `kptn ui` on a loopback port, hosts it in one
webview, and opens source files when the hosted page asks. It has no UI and no
protocol of its own.

## Quick start

1. Launch the extension in the VS Code extension host (Run and Debug → `Run Extension` or `F5`).
2. Run `kptn: Open Pipeline UI` from the Command Palette.
3. The extension reserves a free loopback port, spawns
   `python -m kptn ui --no-open --port <port>` with your selected interpreter,
   waits for `/healthz`, and frames the resulting URL.

A healthy server is reused across command invocations, so reopening the panel
never starts a second server and never restarts the one that is supervising
your runs. Closing the panel leaves the server running; the server is stopped
only when the extension is disposed, and only the `kptn ui` process itself is
signalled -- detached run workers keep going across editor, server and browser
restarts.

The server binds loopback only, with no authentication and no remote
execution. The webview's CSP allows exactly one frame origin: the URL the
server was started on.

## Python path resolution

- **Prerequisite**: `kptn` must be importable by your active Python environment
  (`pip install kptn`, or activate an environment that has it).
- The interpreter is resolved in this order: `KPTN_VSCODE_PYTHON`, the
  ms-python active interpreter, `python.defaultInterpreterPath`,
  `python.pythonPath`, `VIRTUAL_ENV`, then `python` from `PATH`.
- `PYTHONPATH` picks up the vendored `python_libs/` directory and the sibling
  `../kptn` checkout when present (monorepo development); any existing
  `PYTHONPATH` is appended.

## Tests

- `npm test` runs the host-free suites (`src/test/*.test.ts`) under plain mocha:
  launcher argv, port-reservation retries, process reuse, disposal, bridge-token
  and workspace-containment checks, and the hosted page's CSP.
- `npm run test:vscode` additionally runs `src/test/*.vstest.ts` inside a real
  VS Code host, which is where source-opening and one-based line selection are
  verified.

## Packaging

- From anywhere inside `kptn-vscode`, run `./scripts/package.sh` to build a VSIX. The script will install dependencies if `node_modules/` is missing and then invoke `vsce package` (which triggers TypeScript compilation via `vscode:prepublish`).
- Users of the packaged extension must have `kptn` installed in their Python environment.
