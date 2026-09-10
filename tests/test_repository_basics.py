"""Basic smoke tests to validate the repository setup.

Beyond the README check, this module pins the *shape* of the repository after
the pipeline UI replaced the React application and the JSON-RPC VS Code
backend. There is now exactly one supported UI command (``kptn ui``) and one
VS Code launch path (the extension spawns that command and opens its URL in a
webview), and the removed surfaces must not creep back: a second frontend
would immediately drift from the served one, and a resurrected
``api_jsonrpc.py`` would be a second protocol nobody maintains.

The VS Code extension keeps its *own* ``package.json`` and ``package-lock.json``
-- deleting the root Node manifest must not take the extension with it -- so
that pairing is asserted here too.
"""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

#: Superseded surfaces. Each entry is (path, why it must stay gone).
REMOVED_SURFACES = (
    ("ui", "the React application the served Jinja UI replaced"),
    ("kptn_server/api_jsonrpc.py", "the JSON-RPC protocol the HTTP UI replaced"),
    ("kptn_server/api_http.py", "the standalone HTTP app the shared app replaced"),
    ("kptn-vscode/backend.py", "the JSON-RPC spawn shim the extension no longer uses"),
    ("cypress", "the root Cypress harness for the deleted React application"),
    ("cypress.config.ts", "the root Cypress configuration"),
    ("package.json", "the root Node manifest for the deleted React application"),
    ("package-lock.json", "the root Node lockfile"),
)

#: The VS Code extension's own Node project, which must survive the root one.
RETAINED_EXTENSION_FILES = (
    "kptn-vscode/package.json",
    "kptn-vscode/package-lock.json",
    "kptn-vscode/src/extension.ts",
)


def test_readme_present() -> None:
    """Ensure the top-level README exists as a quick health check."""
    assert (PROJECT_ROOT / "README.md").is_file()


def test_superseded_ui_surfaces_are_absent() -> None:
    """One supported UI, one VS Code launch path -- and no leftovers."""
    surviving = [
        f"{path} ({reason})"
        for path, reason in REMOVED_SURFACES
        if (PROJECT_ROOT / path).exists()
    ]
    assert not surviving, f"superseded surfaces still present: {surviving}"


def test_the_vscode_extension_keeps_its_own_node_project() -> None:
    """Removing the root Node manifest must not disarm the extension."""
    for relative in RETAINED_EXTENSION_FILES:
        assert (PROJECT_ROOT / relative).is_file(), f"missing {relative}"


def test_the_supported_ui_command_is_the_only_server_entry_point() -> None:
    """``kptn ui`` exists, and nothing documents a second way to serve.

    The old entry point was ``uvicorn kptn_server.api_http:app``. It is gone
    from the code; this keeps it gone from the documentation, which is where a
    developer would otherwise find it and file a bug against a module that no
    longer exists.
    """
    from kptn.cli.commands import ui

    assert callable(ui)

    documentation = [
        PROJECT_ROOT / "README.md",
        PROJECT_ROOT / "kptn_server" / "README.md",
        PROJECT_ROOT / "copilot-instructions.md",
    ]
    # Names of deleted modules and commands, not prose. A document is allowed
    # to *say* that the React UI and its Cypress harness were removed; it is
    # not allowed to tell a reader to run them.
    forbidden = (
        "api_http",
        "api_jsonrpc",
        "cypress:run",
        "cypress:open",
        "npx cypress",
    )
    for path in documentation:
        text = path.read_text(encoding="utf-8")
        for name in forbidden:
            assert name not in text, f"{path.name} still documents {name}"
