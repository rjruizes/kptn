"""Basic smoke tests to validate the repository setup.

Beyond the README check, this module pins the *shape* of the repository after
the pipeline UI replaced the React application, the JSON-RPC backend a VS
Code extension used to spawn, and -- once jupyter-server-proxy could do from
a config file everything that extension did by hand -- the extension itself.
There is now exactly one supported UI command (``kptn ui``), launched in a
notebook environment by jupyter-server-proxy's config rather than by
anything in this repository, and the removed surfaces must not creep back: a
second frontend would immediately drift from the served one, and a
resurrected ``api_jsonrpc.py`` would be a second protocol nobody maintains.
"""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

#: Superseded surfaces. Each entry is (path, why it must stay gone).
REMOVED_SURFACES = (
    ("ui", "the React application the served Jinja UI replaced"),
    ("kptn_server/api_jsonrpc.py", "the JSON-RPC protocol the HTTP UI replaced"),
    ("kptn_server/api_http.py", "the standalone HTTP app the shared app replaced"),
    ("kptn-vscode/backend.py", "the JSON-RPC spawn shim the extension stopped using before it was removed"),
    ("kptn-vscode", "the VS Code extension; jupyter-server-proxy now does from a config file everything it did by hand"),
    ("cypress", "the root Cypress harness for the deleted React application"),
    ("cypress.config.ts", "the root Cypress configuration"),
    ("package.json", "the root Node manifest for the deleted React application"),
    ("package-lock.json", "the root Node lockfile"),
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
#: **Every** entry of :data:`REMOVED_SURFACES` contributes a name here, rather
#: than being retyped: a directory contributes ``<name>/`` and a file
#: contributes its basename, so adding a surface to the removal list covers it
#: in the documentation scan too.
#:
#: What that derivation cannot supply is the *module* spelling of a deleted
#: Python file -- ``api_http`` as it appears in
#: ``uvicorn kptn_server.api_http:app``, with no ``.py``. Deriving bare stems
#: would also yield ``backend`` from ``kptn-vscode/backend.py``, and "backend"
#: is an ordinary English word this very repository's ``AGENTS.md`` opens a
#: sentence with. So the two module names are listed explicitly below and the
#: derivation stops at basenames, which are unambiguous.
FORBIDDEN_IN_DOCUMENTATION = tuple(
    sorted(
        # `ui/`, `cypress/` -- the deleted directories.
        {f"{path}/" for path, _ in REMOVED_SURFACES if "." not in path}
        # `api_http.py`, `api_jsonrpc.py`, `backend.py`, `cypress.config.ts`,
        # `package.json`, `package-lock.json` -- the deleted files.
        | {
            path.rsplit("/", 1)[-1]
            for path, _ in REMOVED_SURFACES
            if "." in path
            and not path.endswith("package.json")
            and not path.endswith("package-lock.json")
        }
        | {
            # Module spellings the basename derivation cannot reach.
            "api_http",
            "api_jsonrpc",
            # Deleted directories, spelled the ways documentation spells them.
            "cd ui",
            "ui/src",
            "ui/public",
            # npm scripts no package.json in the repo still defines. Note
            # what is *absent*: `npm run dev` and `npm run build` are not
            # forbidden, because `doc-site/` (Astro) really does define both,
            # and a guard that fires on a true statement is a bug. The React
            # UI's invocation is caught by `cd ui` and the `ui/` paths instead.
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
    """One supported UI, launched from a notebook environment by jupyter-server-proxy -- and no leftovers."""
    surviving = [
        f"{path} ({reason})"
        for path, reason in REMOVED_SURFACES
        if (PROJECT_ROOT / path).exists()
    ]
    assert not surviving, f"superseded surfaces still present: {surviving}"


def test_the_vscode_extensions_own_files_stay_gone() -> None:
    """Deleting the extension must not leave selective leftovers behind.

    :data:`REMOVED_SURFACES` already checks the ``kptn-vscode`` directory
    itself, but a directory check passes even if a future change recreates
    the directory and repopulates only some of it -- an interpreter-finding
    shim without its own manifest, say. Naming the files the extension's own
    Node project depended on keeps that failure mode from creeping back in
    unnoticed.
    """
    for relative in ("kptn-vscode/package.json", "kptn-vscode/package-lock.json", "kptn-vscode/src/extension.ts"):
        assert not (PROJECT_ROOT / relative).exists(), f"leftover {relative}"


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

    # Every removed surface contributes a name: directories as `<name>/`,
    # files as their basename. `package.json` and `package-lock.json` are the
    # documented exception -- those basenames still exist under `doc-site/`,
    # so banning them outright would forbid documenting the Node project the
    # repository still has.
    for path, _ in REMOVED_SURFACES:
        basename = path.rsplit("/", 1)[-1]
        if basename in {"package.json", "package-lock.json"}:
            assert basename not in FORBIDDEN_IN_DOCUMENTATION
            continue
        expected = f"{path}/" if "." not in path else basename
        assert expected in FORBIDDEN_IN_DOCUMENTATION, f"{path} contributes nothing"

    assert "ui/" in FORBIDDEN_IN_DOCUMENTATION
    assert "cypress/" in FORBIDDEN_IN_DOCUMENTATION
    assert "backend.py" in FORBIDDEN_IN_DOCUMENTATION
    assert "kptn-vscode/" in FORBIDDEN_IN_DOCUMENTATION
