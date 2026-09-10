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

#: Developer-facing documentation. Every one of these is read by a human or an
#: agent *before* they touch the code, so a stale reference here costs more
#: than one in a comment: it sends the reader to a module or a directory that
#: is not there. ``AGENTS.md`` is first in the list because it is the first
#: file an agent reads.
DOCUMENTATION_FILES = (
    "AGENTS.md",
    "README.md",
    "kptn_server/README.md",
    "copilot-instructions.md",
)

#: Substrings none of the above may contain, matched case-insensitively.
#:
#: These are *names* -- of deleted modules, deleted directories, and npm
#: scripts that no longer exist -- never prose. A document is free to say that
#: the React application and its Cypress harness were removed; it may not name
#: a path a reader would then go looking for, or tell them to run a script
#: that is gone.
#:
#: The directory names are derived from :data:`REMOVED_SURFACES` rather than
#: retyped, so a surface added there is covered here without anyone having to
#: remember to do it twice.
FORBIDDEN_IN_DOCUMENTATION = tuple(
    sorted(
        # `ui/` and `cypress/`, from the removal list itself.
        {f"{path}/" for path, _ in REMOVED_SURFACES if "." not in path}
        | {
            # Deleted modules.
            "api_http",
            "api_jsonrpc",
            # Deleted directories, spelled the ways documentation spells them.
            "cd ui",
            "ui/src",
            "ui/public",
            # npm scripts that no longer exist in any package.json in the repo.
            "npm run dev",
            "npm run build",
            "cypress:run",
            "cypress:open",
            "npx cypress",
            # Dependencies only the deleted React application ever had.
            "tailwind",
            "vitest",
        }
    )
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
    """``kptn ui`` exists, and nothing documents a second way to serve."""
    from kptn.cli.commands import ui

    assert callable(ui)


def test_no_documentation_names_a_deleted_surface() -> None:
    """Documentation may describe the removal; it may not name what was removed.

    The old server entry point was ``uvicorn kptn_server.api_http:app`` and the
    old UI was ``cd ui && npm run dev``. Both are gone from the code. This
    keeps them gone from the documentation, which is where a developer -- or an
    agent reading ``AGENTS.md`` first -- would otherwise find them and act on
    them.

    Every file is checked against every name, and the failure reports all of
    the misses at once rather than only the first: a document that drifted
    usually drifted in more than one sentence.
    """
    stale = [
        f"{relative} names {name!r}"
        for relative in DOCUMENTATION_FILES
        for name in FORBIDDEN_IN_DOCUMENTATION
        if name in (PROJECT_ROOT / relative).read_text(encoding="utf-8").lower()
    ]
    assert not stale, "documentation still names deleted surfaces: " + "; ".join(stale)


def test_every_documentation_file_scanned_above_exists() -> None:
    """A typo in :data:`DOCUMENTATION_FILES` must not silently skip a file.

    The scan reads each path directly, so a renamed or missing file would raise
    rather than pass -- but this states the requirement outright, and keeps the
    forbidden list from being empty by accident.
    """
    for relative in DOCUMENTATION_FILES:
        assert (PROJECT_ROOT / relative).is_file(), f"missing {relative}"
    assert len(FORBIDDEN_IN_DOCUMENTATION) >= 12
    assert "ui/" in FORBIDDEN_IN_DOCUMENTATION
    assert "cypress/" in FORBIDDEN_IN_DOCUMENTATION
