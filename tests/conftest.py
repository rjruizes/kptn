"""Shared fixtures for the pipeline-UI test modules.

Every UI test module needs the same two pieces of hygiene:

``restore_process_state``
    Loading a project pipeline mutates *this* process -- ``load_pipeline``
    prepends the project root to ``sys.path`` and evicts project modules from
    ``sys.modules`` so a reload picks up edited code. Each UI test loads a
    different temporary copy of the same fixture project, so without this the
    copies shadow one another and tests pass or fail depending on order.

``reap_spawned_workers``
    Kills and reaps any child process a test leaves behind, then asserts none
    leaked. A *failing* test must not be able to strand a detached worker on
    the developer's machine or in CI, and the fixture project's ``slow``
    profile blocks on a sentinel file forever if nobody releases it.

Both are autouse, and both are **gated on the ``ui_hygiene`` marker**. A
module opts in with one line::

    pytestmark = pytest.mark.ui_hygiene

The gate is the point. An ungated autouse fixture in this file would apply to
the whole suite -- ~750 tests that have no business having ``sys.modules``
pruned after each one, several of which legitimately spawn subprocesses the
child-process assertion would then fail on. Gating keeps the blast radius at
exactly the modules that asked, while still letting pytest resolve the
fixtures by name with no imports (and therefore no ``F811`` fixture-shadowing
noise from ruff).

``tests/test_run_worker.py`` and ``tests/test_run_processes.py`` define their
own same-named fixtures. A module-level fixture shadows a conftest one
completely, so those two files keep their own behaviour; they also do not set
the marker, so the versions here would no-op for them regardless.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import sysconfig
import warnings
from pathlib import Path

import psutil
import pytest

import kptn.project

#: The deterministic project every UI test serves. Its ``success``, ``slow``,
#: ``failure`` and ``db_error`` profiles are documented in
#: ``tests/fixtures/ui_project/ui_pipeline.py``.
FIXTURE_PROJECT = Path(__file__).parent / "fixtures" / "ui_project"

#: A project whose pipeline module raises while being imported.
BROKEN_FIXTURE_PROJECT = Path(__file__).parent / "fixtures" / "ui_broken_project"


#: Modules opting into the UI hygiene fixtures carry this marker.
UI_HYGIENE_MARKER = "ui_hygiene"

#: Directories holding *installed* packages. Modules imported from here are
#: never evicted by ``restore_process_state``.
#:
#: The eviction exists to unload the *project* modules ``load_pipeline``
#: imported, so the next test's copy of the same fixture project is not
#: shadowed. Installed dependencies are collateral damage, and for a library
#: with lazy submodule imports the damage is real: ``sqlglot`` pulls in ~40
#: submodules the first time lineage parses SQL, and dropping those from
#: ``sys.modules`` leaves a later import with fresh dialect classes that its
#: own already-held registry no longer recognizes ("Invalid dialect type for
#: <sqlglot.dialects.duckdb.DuckDB object>"). The second test to render
#: lineage in a session then failed, for reasons entirely internal to the
#: harness. Scoping the eviction to non-installed modules fixes that class of
#: bug rather than warming up one library.
_INSTALLED_PACKAGE_DIRS = tuple(
    sorted(
        {
            path
            for path in (
                sysconfig.get_paths().get("purelib"),
                sysconfig.get_paths().get("platlib"),
                sysconfig.get_paths().get("stdlib"),
            )
            if path
        }
    )
)


def _is_installed_module(name: str) -> bool:
    """Was this module imported from an installed package directory?

    Three cases, and the middle one is the subtle one.

    *Has a ``__file__``.* Installed exactly when that path is under one of
    :data:`_INSTALLED_PACKAGE_DIRS`.

    *No ``__file__`` but has a ``__path__`` -- a namespace package.* **Not**
    installed, so it is evicted. A fixture project directory without an
    ``__init__.py`` imports as precisely this: ``tests/test_ui_lineage_routes``
    writes a ``src/`` with no ``__init__.py``, and every other fixture project
    in this suite is free to do the same. Leaving such a name cached is the
    ``sys.modules['src']`` shadowing that already broke
    ``tests/test_table_preview_api`` once -- the next test's ``src`` would lose
    to the previous test's. A namespace package holds no state worth
    preserving, so evicting it is safe as well as necessary.

    *Neither ``__file__`` nor ``__path__``* -- a builtin, an extension baked
    into the interpreter, or a module whose import is still in flight. Treated
    as installed and left alone: none of these can come from a fixture
    project, and evicting a half-initialized module is how import machinery
    gets confused.
    """
    module = sys.modules.get(name)
    origin = getattr(module, "__file__", None)
    if origin is None:
        # A namespace package is a fixture-project directory as often as not.
        return not hasattr(module, "__path__")
    return origin.startswith(_INSTALLED_PACKAGE_DIRS)


def _wants_ui_hygiene(request: pytest.FixtureRequest) -> bool:
    return request.node.get_closest_marker(UI_HYGIENE_MARKER) is not None


@pytest.fixture(autouse=True)
def restore_process_state(request: pytest.FixtureRequest):
    """Undo what loading a project pipeline does to this process."""
    if not _wants_ui_hygiene(request):
        yield
        return

    original_cwd = Path.cwd()
    original_path = sys.path.copy()
    original_modules = set(sys.modules)
    original_showwarning = warnings.showwarning
    root = logging.getLogger()
    original_handlers = root.handlers.copy()
    original_roots = set(kptn.project._PROJECT_IMPORT_ROOTS)

    yield

    os.chdir(original_cwd)
    sys.path[:] = original_path
    for name in set(sys.modules) - original_modules:
        if _is_installed_module(name):
            continue
        sys.modules.pop(name, None)
    warnings.showwarning = original_showwarning
    root.handlers[:] = original_handlers
    _restore_project_import_roots(original_roots)


def _restore_project_import_roots(original: set[str]) -> None:
    """Forget the project roots this test taught ``kptn.project`` about.

    ``_prepare_project_imports`` records every root it has ever loaded and
    never prunes it, then rescans all of ``sys.modules`` against every
    recorded root on each load. For a server that is free: it serves one
    project, so the set stays at one entry. A test session loads a different
    temporary project per test, so the set grows once per test and the scan
    grows with it -- the suite's cost becomes quadratic in its own size.
    Measured over 120 apps: 48.9s inside ``create_app`` without this, 6.9s
    with it.

    The eviction above cannot do this job. It drops only what a test imported
    while it ran, and ``kptn.project`` is in ``sys.modules`` from collection
    onwards -- every UI test module imports ``kptn_server.app`` at its top --
    so it is never evicted and its registry is never reborn.

    Mutated in place rather than rebound, and through the module object this
    file holds: functions bound at collection close over *that* module's
    globals, so a re-imported ``kptn.project`` is not necessarily the one
    doing the recording.
    """
    kptn.project._PROJECT_IMPORT_ROOTS.clear()
    kptn.project._PROJECT_IMPORT_ROOTS.update(original)


@pytest.fixture(autouse=True)
def reap_spawned_workers(request: pytest.FixtureRequest):
    """Kill, reap, and then refuse to tolerate any leaked child process."""
    if not _wants_ui_hygiene(request):
        yield
        return

    before = {(child.pid, _safe_create_time(child)) for child in _own_children()}

    yield

    leaked = [
        child
        for child in _own_children()
        if (child.pid, _safe_create_time(child)) not in before
    ]
    for child in leaked:
        try:
            child.kill()
        except psutil.Error:  # pragma: no cover - defensive
            pass
    psutil.wait_procs(leaked, timeout=10)
    for child in leaked:
        try:
            os.waitpid(child.pid, os.WNOHANG)
        except (ChildProcessError, OSError):
            pass
    assert not leaked, f"the test leaked child processes: {leaked}"


def _own_children() -> list[psutil.Process]:
    try:
        return psutil.Process().children(recursive=True)
    except psutil.Error:  # pragma: no cover - defensive
        return []


def _safe_create_time(proc: psutil.Process) -> float:
    try:
        return proc.create_time()
    except psutil.Error:  # pragma: no cover - defensive
        return -1.0


#: Never copied out of a fixture project: everything here is *generated*.
#:
#: ``.kptn/`` is git-ignored rather than absent, so one local test run leaves
#: a task-state database sitting in the source project -- and a copy that
#: inherited it would skip ``noisy_task`` as cached, producing a run that
#: succeeds instantly having printed nothing. ``__pycache__`` is the same
#: trap one layer down: every copy imports a module of the same name.
#:
#: The failure this prevents lies about its cause. A fresh ``git worktree``
#: has no ignored files, so the identical test passes at baseline and
#: whatever change is under review looks responsible.
GENERATED_PROJECT_STATE = shutil.ignore_patterns(".kptn", "__pycache__")


def copy_fixture_project(source: Path, tmp_path: Path, name: str) -> Path:
    """Copy a fixture project so a test can write ``.kptn/`` into it freely.

    The copy carries the project and none of the state a previous run left
    in it -- see :data:`GENERATED_PROJECT_STATE` -- so a test's result does
    not depend on whether this machine has run the suite before.

    A plain function, not a fixture, so that each fixture below depends only
    on pytest built-ins. Fixtures imported into a module's namespace do not
    bring their own dependencies along, so a fixture-to-fixture dependency
    here would silently oblige every importing module to import the
    dependency too -- a trap worth designing out rather than documenting.
    """
    destination = tmp_path / name
    shutil.copytree(source, destination, ignore=GENERATED_PROJECT_STATE)
    return destination


@pytest.fixture
def ui_project(tmp_path: Path) -> Path:
    """A private copy of the fixture project."""
    return copy_fixture_project(FIXTURE_PROJECT, tmp_path, "project")


@pytest.fixture
def ui_project_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The fixture project, with the process chdir'd into it.

    ``kptn ui`` serves ``Path.cwd()`` and takes no project path, so the
    launcher's tests have to run from inside the project.
    """
    project = copy_fixture_project(FIXTURE_PROJECT, tmp_path, "project")
    monkeypatch.chdir(project)
    return project


@pytest.fixture
def broken_ui_project(tmp_path: Path) -> Path:
    """A copy of the project whose pipeline module raises on import."""
    return copy_fixture_project(BROKEN_FIXTURE_PROJECT, tmp_path, "broken")
