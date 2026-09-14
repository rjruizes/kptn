"""Tests for the UI hygiene fixtures in ``tests/conftest.py``.

``restore_process_state`` exists because loading a project pipeline mutates
this process: ``load_pipeline`` prepends the project root to ``sys.path`` and
evicts project modules from ``sys.modules``. Each UI test loads a *different*
temporary copy of the same fixture project, so a name left cached from one
test shadows the next test's copy of it. That is not hypothetical -- it is
what broke ``tests/test_table_preview_api``, whose project resolves
``src.utils:get_engine`` and got another test's ``src``.

The eviction is scoped: installed dependencies are left alone, because
dropping a lazily-imported library's submodules mid-session corrupts it (see
the constant's own comment in ``conftest``). The scoping is therefore a
*classifier*, and a classifier with two competing failure modes:

* too eager, and ``sqlglot`` breaks;
* too lax, and a fixture project's module survives to shadow the next test's.

A namespace package -- a project directory with no ``__init__.py`` -- has no
``__file__`` and used to land on the "installed, leave it" side by accident.
``tests/test_ui_lineage_routes`` creates exactly such a ``src/``. Nothing
failed because of it yet, and nothing would have failed loudly when it did:
the symptom is another module resolving the wrong ``src``. Hence these tests,
which pin the classification directly rather than waiting for a victim.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest

from kptn.project import _PROJECT_IMPORT_ROOTS, load_pipeline
from tests.conftest import _INSTALLED_PACKAGE_DIRS, _is_installed_module

pytestmark = pytest.mark.ui_hygiene

#: Set by the first test below and read by the second. Module state, because
#: the property under test is "what survives *between* two tests" and there is
#: no other way to observe a teardown from inside pytest.
_IMPORTED_NAMESPACE_PACKAGE: str | None = None


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    """Pin the ``pytestmark`` opt-in.

    Without it the fixtures this module tests would not run at all, and
    :func:`test_a_namespace_package_does_not_survive_teardown` would pass for
    the wrong reason.
    """
    assert request.node.get_closest_marker("ui_hygiene") is not None


# -- the classifier, case by case ------------------------------------------


def test_an_installed_dependency_is_left_alone() -> None:
    """A real third-party package classifies as installed."""
    import jinja2

    assert jinja2.__file__ is not None
    assert jinja2.__file__.startswith(_INSTALLED_PACKAGE_DIRS)
    assert _is_installed_module("jinja2") is True


def test_a_project_module_is_evictable() -> None:
    """A module loaded from outside the environment is not installed."""
    assert _is_installed_module("tests.conftest") is False


def test_a_builtin_without_a_file_is_left_alone() -> None:
    """No ``__file__`` and no ``__path__``: a builtin, and not ours to evict."""
    assert not hasattr(sys, "__file__")
    assert not hasattr(sys, "__path__")

    assert _is_installed_module("sys") is True


def test_a_namespace_package_is_evictable(tmp_path: Path, monkeypatch) -> None:
    """The case this test module exists for.

    A directory with no ``__init__.py`` imports as a namespace package: a
    module object with ``__path__`` and no ``__file__``. Classifying it as
    installed would leave a fixture project's ``src`` cached for the next
    test to trip over, which is the exact shape of the failure that already
    cost ``test_table_preview_api`` a debugging session.
    """
    (tmp_path / "kptn_ns_fixture").mkdir()
    (tmp_path / "kptn_ns_fixture" / "thing.py").write_text("VALUE = 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    import kptn_ns_fixture

    # Really a namespace package, or this test proves nothing.
    assert getattr(kptn_ns_fixture, "__file__", None) is None
    assert hasattr(kptn_ns_fixture, "__path__")

    assert _is_installed_module("kptn_ns_fixture") is False


def test_an_unknown_name_is_left_alone() -> None:
    """A name not in ``sys.modules`` at all: ``None`` has no ``__path__``."""
    assert "kptn_definitely_not_imported" not in sys.modules

    assert _is_installed_module("kptn_definitely_not_imported") is True


def test_the_classifier_reads_the_live_module_object() -> None:
    """Classification follows ``sys.modules``, not a cached decision."""
    stand_in = ModuleType("kptn_stand_in_fixture")
    stand_in.__path__ = []  # type: ignore[attr-defined]
    sys.modules["kptn_stand_in_fixture"] = stand_in
    try:
        assert _is_installed_module("kptn_stand_in_fixture") is False
    finally:
        sys.modules.pop("kptn_stand_in_fixture", None)


# -- and the fixture actually acts on it -----------------------------------
#
# These two run in definition order and are a pair: the first imports a
# namespace package during a marked test, the second checks that
# ``restore_process_state``'s teardown removed it. Splitting them is the only
# way to observe a teardown, and the guard in the second one keeps a failure
# in the first from reading as a pass in the second.


def test_import_a_namespace_package_during_a_marked_test(
    tmp_path: Path, monkeypatch
) -> None:
    """Leave a namespace package behind for the next test to look for."""
    global _IMPORTED_NAMESPACE_PACKAGE

    (tmp_path / "kptn_teardown_fixture").mkdir()
    (tmp_path / "kptn_teardown_fixture" / "leaf.py").write_text("VALUE = 2\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    import kptn_teardown_fixture.leaf

    assert kptn_teardown_fixture.leaf.VALUE == 2
    assert "kptn_teardown_fixture" in sys.modules
    _IMPORTED_NAMESPACE_PACKAGE = "kptn_teardown_fixture"


def test_a_namespace_package_does_not_survive_teardown() -> None:
    """The pair's payoff: teardown really did evict it.

    If this ever fails, a fixture project's package name is outliving the test
    that imported it, and the next test to use that name gets the wrong
    directory -- silently, with a confusing error somewhere else entirely.
    """
    assert _IMPORTED_NAMESPACE_PACKAGE is not None, (
        "the preceding test did not run; this pair must stay in definition order"
    )

    assert _IMPORTED_NAMESPACE_PACKAGE not in sys.modules
    assert f"{_IMPORTED_NAMESPACE_PACKAGE}.leaf" not in sys.modules


# -- the accumulating project-root registry --------------------------------

#: Set by the first test below, read by the second -- like the pair above,
#: the property under test is what survives *between* two tests.
#:
#: Only the root that test registered, not the whole registry: modules
#: without the ``ui_hygiene`` marker (``tests/test_project.py`` among them)
#: load projects too, and nothing clears theirs. Asserting on the whole set
#: would be asserting on those, which this fixture does not govern.
_ROOT_REGISTERED_BY_THE_FIRST_TEST: list[str] = []


def test_a_loaded_project_registers_its_root(ui_project: Path) -> None:
    """Loading a project adds its root to the module-level registry.

    ``kptn.project`` is imported at *module* scope here on purpose. The
    eviction in ``restore_process_state`` only drops what a test imported
    while it ran, so a module already in ``sys.modules`` at collection --
    which ``kptn.project`` always is, because every UI test module imports
    ``kptn_server.app`` at its top -- is never evicted, and its registry is
    never reborn. Importing it inside the test instead would let the
    eviction reset the registry as a side effect, and the test below would
    pass without the restore it exists to pin.
    """
    load_pipeline(ui_project)

    assert str(ui_project.resolve()) in _PROJECT_IMPORT_ROOTS
    _ROOT_REGISTERED_BY_THE_FIRST_TEST.append(str(ui_project.resolve()))


def test_the_registry_does_not_accumulate_across_tests() -> None:
    """The previous test's root is gone by the time this one runs.

    ``_PROJECT_IMPORT_ROOTS`` is never pruned in normal operation, which is
    right for a server: it serves one project, so the set stays at one entry.
    A test session loads a *different* temporary project per test, and
    ``_prepare_project_imports`` rescans every entry of ``sys.modules``
    against every accumulated root on each load -- so the suite's cost grows
    with the square of its own size. Measured over 120 apps: 48.9s of
    ``create_app`` without this reset, 6.9s with it.

    Restoring it belongs here rather than in ``kptn.project``: the growth is
    not a bug in a process that serves one project, and this fixture already
    exists to undo what loading a pipeline does to the process.
    """
    assert _ROOT_REGISTERED_BY_THE_FIRST_TEST, "the first test did not run"
    assert _ROOT_REGISTERED_BY_THE_FIRST_TEST[0] not in _PROJECT_IMPORT_ROOTS
