"""hash_task_source's source collection: hash stability and the module cache.

The golden hashes below were recorded before source segments were sliced by
line and module summaries were cached across tasks. Every stored code hash in
every project depends on them, so a change that moves them invalidates every
cache -- update them only on purpose.
"""

from __future__ import annotations

import importlib
import os
import re
import sys
from pathlib import Path

import pytest

import kptn.change_detector.hasher as hasher_mod
from kptn.change_detector.hasher import hash_task_source

# A form feed line, CRLF endings, trailing comments, a decorator and a
# one-liner: the cases where slicing whole lines could drift from the exact
# segment the hashes were first recorded from. Only function bodies are
# hashed, so the package name and the spelling of the imports in _TASKS do not
# reach the hash: every spelling that resolves gives the golden values.
_HELPERS = (
    "import functools\n"
    "\n"
    "def shared(x):\n"
    "    '''Docstring is ignored.'''\n"
    "    return x + 1  # trailing comment on the last line\n"
    "\x0c\n"
    "def _deco(fn):\n"
    "    @functools.wraps(fn)\n"
    "    def wrapper(*a, **k):\n"
    "        return fn(*a, **k)\n"
    "    return wrapper\n"
    "\n"
    "@_deco\n"
    "def decorated(x):\n"
    "    return shared(x) * 2\n"
)
_CRLF = "def crlf_helper(x):\r\n    return x - 1\r\n\r\ndef unused():\r\n    return 0\r\n"
_ABSOLUTE_IMPORTS = (
    "import PKG.helpers as helpers\n"
    "from PKG.helpers import decorated\n"
    "from PKG.crlf import crlf_helper\n"
)
_TASKS = (
    "\n"
    "def _inner(x):\n"
    "    return crlf_helper(x)\n"
    "\x0c\n"
    "def task_a(x):\n"
    "    return helpers.shared(_inner(x))\n"
    "\n"
    "def task_b(x): return decorated(x)  # one-liner\n"
)

GOLDEN_TASK_A = "823598c94e956682a6120907225304ccf0d7ade3fd1789b4377f7c4b853de1d9"
GOLDEN_TASK_B = "2062233125dea7d3606386430bddda9d6f1f2101478009c5e903f7db9c4dd212"


@pytest.fixture
def make_pkg(tmp_path, monkeypatch, request):
    """Build and import a fresh fixture package; return its tasks module.

    ``imports`` heads the tasks module, with ``PKG`` standing for the package
    name; ``tasks_in`` puts the tasks module in a subpackage of that name.
    """
    names: list[str] = []
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(hasher_mod, "_SUMMARY_CACHE", {})

    def make(imports: str, tasks_in: str | None = None):
        name = re.sub(r"\W", "_", f"kptn_hash_fixture_{request.node.name}_{len(names)}")
        names.append(name)
        pkg = tmp_path / name
        pkg.mkdir()
        (pkg / "__init__.py").write_text("")
        (pkg / "helpers.py").write_bytes(_HELPERS.encode())
        (pkg / "crlf.py").write_bytes(_CRLF.encode())
        home, module = pkg, f"{name}.tasks"
        if tasks_in:
            home, module = pkg / tasks_in, f"{name}.{tasks_in}.tasks"
            home.mkdir()
            (home / "__init__.py").write_text("")
        (home / "tasks.py").write_bytes((imports.replace("PKG", name) + _TASKS).encode())
        return importlib.import_module(module)

    yield make
    for name in names:
        for mod in [m for m in sys.modules if m == name or m.startswith(f"{name}.")]:
            del sys.modules[mod]


@pytest.fixture
def fixture_pkg(make_pkg):
    return make_pkg(_ABSOLUTE_IMPORTS)


def _count_parses(monkeypatch) -> list[Path]:
    parsed: list[Path] = []
    original = hasher_mod._ModuleSummary.__init__

    def counting_init(self, file_path, source, package_root):
        parsed.append(file_path)
        original(self, file_path, source, package_root)

    monkeypatch.setattr(hasher_mod._ModuleSummary, "__init__", counting_init)
    return parsed


def _rewrite_keeping_size(path: Path, old: str, new: str) -> None:
    """Edit *path* in place without changing its size, and move its mtime on."""
    assert len(old) == len(new)
    before = path.stat()
    path.write_bytes(path.read_bytes().replace(old.encode(), new.encode()))
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))


# ─── Hash stability ──────────────────────────────────────────────────────── #


def test_hashes_match_the_recorded_golden_values(fixture_pkg):
    assert hash_task_source(fixture_pkg.task_a) == GOLDEN_TASK_A
    assert hash_task_source(fixture_pkg.task_b) == GOLDEN_TASK_B


def test_hashes_are_the_same_from_a_warm_cache(fixture_pkg):
    hash_task_source(fixture_pkg.task_b)  # warms helpers.py and tasks.py
    assert hash_task_source(fixture_pkg.task_a) == GOLDEN_TASK_A
    assert hash_task_source(fixture_pkg.task_b) == GOLDEN_TASK_B


@pytest.mark.parametrize(
    ("imports", "tasks_in"),
    [
        pytest.param(
            "from . import helpers\nfrom .helpers import decorated\nfrom .crlf import crlf_helper\n",
            None,
            id="relative",
        ),
        pytest.param(
            "from .. import helpers\nfrom ..helpers import decorated\nfrom ..crlf import crlf_helper\n",
            "sub",
            id="relative-from-subpackage",
        ),
        pytest.param(
            "from PKG import helpers\nfrom PKG.helpers import decorated\nfrom PKG.crlf import crlf_helper\n",
            None,
            id="from-package-import-submodule",
        ),
    ],
)
def test_every_import_spelling_follows_the_same_callees(make_pkg, imports, tasks_in):
    tasks = make_pkg(imports, tasks_in)

    assert hash_task_source(tasks.task_a) == GOLDEN_TASK_A
    assert hash_task_source(tasks.task_b) == GOLDEN_TASK_B


def test_an_edit_to_a_relatively_imported_helper_changes_the_hash(make_pkg):
    tasks = make_pkg("from . import helpers\nfrom .helpers import decorated\nfrom .crlf import crlf_helper\n")
    before = hash_task_source(tasks.task_a)

    _rewrite_keeping_size(Path(tasks.helpers.__file__), "x + 1", "x + 2")

    assert hash_task_source(tasks.task_a) != before


def test_a_relative_import_above_the_top_level_package_is_not_followed():
    summary = hasher_mod._ModuleSummary(
        Path("/root/pkg/mod.py"), "from ..elsewhere import f\n", Path("/root")
    )
    assert "f" not in summary.symbol_aliases


def test_a_form_feed_does_not_shift_later_functions():
    # str.splitlines would split this into four lines and slice the wrong ones.
    summary = hasher_mod._ModuleSummary(
        Path("m.py"), "def a():\n    pass\n\x0c\ndef b():\n    return 2\n", Path(".")
    )
    b = summary.functions["b"]
    assert "".join(summary.lines[b.lineno - 1 : b.end_lineno]) == "def b():\n    return 2\n"


# ─── Module cache ────────────────────────────────────────────────────────── #


def test_each_module_is_parsed_once_across_tasks(fixture_pkg, monkeypatch):
    parsed = _count_parses(monkeypatch)

    hash_task_source(fixture_pkg.task_a)
    hash_task_source(fixture_pkg.task_b)
    hash_task_source(fixture_pkg.task_a)

    assert sorted(p.name for p in parsed) == ["crlf.py", "helpers.py", "tasks.py"]


def test_an_edit_to_a_helper_changes_the_hash(fixture_pkg):
    before = hash_task_source(fixture_pkg.task_a)

    _rewrite_keeping_size(Path(fixture_pkg.helpers.__file__), "x + 1", "x + 2")

    assert hash_task_source(fixture_pkg.task_a) != before


def test_a_size_change_is_seen_even_when_mtime_is_unchanged(fixture_pkg):
    helpers = Path(fixture_pkg.helpers.__file__)
    before = hash_task_source(fixture_pkg.task_a)
    stat = helpers.stat()

    helpers.write_bytes(helpers.read_bytes().replace(b"x + 1", b"x + 100"))
    os.utime(helpers, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    assert hash_task_source(fixture_pkg.task_a) != before


def test_reverting_an_edit_restores_the_original_hash(fixture_pkg):
    helpers = Path(fixture_pkg.helpers.__file__)
    _rewrite_keeping_size(helpers, "x + 1", "x + 2")
    hash_task_source(fixture_pkg.task_a)

    _rewrite_keeping_size(helpers, "x + 2", "x + 1")

    assert hash_task_source(fixture_pkg.task_a) == GOLDEN_TASK_A


def test_a_deleted_helper_drops_out_of_the_hash(fixture_pkg):
    hash_task_source(fixture_pkg.task_a)
    Path(fixture_pkg.helpers.__file__).unlink()

    # The cached summary must not outlive its file: the warm cache answers
    # exactly as a fresh process would.
    warm = hash_task_source(fixture_pkg.task_a)
    hasher_mod._SUMMARY_CACHE.clear()
    assert warm == hash_task_source(fixture_pkg.task_a)
    assert warm != GOLDEN_TASK_A
