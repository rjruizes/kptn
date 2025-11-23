# kptn_server

Small FastAPI server that can run standalone or when launched from the VS Code extension.

## Run the server
- Install deps (root project uses `uv` for Python): `uv run uvicorn kptn_server.api_http:app --reload`

## End-to-end tests
- Start the Cypress app server: `npm run cypress:open`
- Run the test suite: `npm run cypress:run`

## VS Code extension
- Debug the extension: open `kptn-vscode/src/extension.ts` in VS Code and press `F5`
- Package the extension for installation: `./kptn-vscode/scripts/package.sh`
