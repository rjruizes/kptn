"""What the distributable wheel has to contain for the UI to work at all.

The pipeline UI is not importable Python alone. It renders Jinja templates out
of ``kptn_server/templates``, serves vendored assets out of
``kptn_server/static``, and creates its run store by replaying the SQL in
``kptn_server/migrations``. All three are read *package-relative*
(:data:`kptn_server.app.TEMPLATES_DIR` and friends), which is exactly why they
have to be inside the wheel: a non-editable install has no working directory
to fall back on, so a wheel missing any of them produces a UI that starts and
then 500s on its first page.

Two guards, deliberately at different levels.

:func:`test_wheel_contains_ui_assets` builds a real wheel and looks inside it.
That is the claim that matters, and it is checked end to end rather than by
reading configuration.

:func:`test_wheel_force_includes_the_ui_asset_directories` reads the build
configuration. The three trees are excluded from hatchling's package sweep and
arrive *only* through ``force-include`` -- they have to be, because hatchling
refuses to write the same archive path twice -- so deleting the block really
does empty them out of the wheel and the test above really does catch it. This
second test pins the arrangement that makes that true, since the two halves
(the ``exclude`` and the ``force-include``) are only correct together.
"""

from __future__ import annotations

import subprocess
import tomllib
import zipfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.ui_hygiene

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Directories the wheel build must force-include, mapped to their in-wheel
#: destination. Mirrors ``[tool.hatch.build.targets.wheel.force-include]``.
FORCE_INCLUDED_DIRECTORIES = {
    "kptn_server/templates": "kptn_server/templates",
    "kptn_server/static": "kptn_server/static",
    "kptn_server/migrations": "kptn_server/migrations",
}


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    """Pin the ``pytestmark`` opt-in.

    The hygiene fixtures in ``tests/conftest.py`` are gated on this marker and
    silently no-op without it, so losing the ``pytestmark`` line would leave
    this module's ``uv build`` subprocesses unchecked for leaks with no test
    failing to say so.
    """
    assert request.node.get_closest_marker("ui_hygiene") is not None


def test_wheel_contains_ui_assets(tmp_path: Path) -> None:
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path)],
        check=True,
        cwd=REPO_ROOT,
    )
    wheel = next(tmp_path.glob("*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    assert "kptn_server/templates/base.html" in names
    assert "kptn_server/static/htmx.min.js" in names
    assert "kptn_server/migrations/001_runs.sql" in names


def test_wheel_force_includes_the_ui_asset_directories() -> None:
    """The asset trees are declared, and declared exactly once."""
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    wheel = config["tool"]["hatch"]["build"]["targets"]["wheel"]

    assert wheel["packages"] == ["kptn", "kptn_server"]
    assert wheel["force-include"] == FORCE_INCLUDED_DIRECTORIES
    # Excluded from the package sweep so force-include is their only source.
    # Both halves together, or the build fails on a duplicate archive path.
    assert set(wheel["exclude"]) == set(FORCE_INCLUDED_DIRECTORIES)
    for source in FORCE_INCLUDED_DIRECTORIES:
        assert (REPO_ROOT / source).is_dir()


def test_the_web_extra_installs_the_lineage_dependency() -> None:
    """``sqlglot`` belongs to the ``web`` extra, not to nobody.

    ``kptn_server.service`` imports the lineage analyzer at module scope, and
    that needs ``sqlglot``. While no extra declared it, the retained lineage
    and table-preview surfaces were unimportable in every environment the UI
    actually ships to -- served routes that could only ever raise.
    """
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    web = config["project"]["optional-dependencies"]["web"]

    assert any(
        requirement.split(">=")[0].strip() == "sqlglot" for requirement in web
    ), f"the 'web' extra must declare sqlglot; got {web}"


def test_the_web_extra_makes_the_retained_lineage_service_importable() -> None:
    """And the extra actually delivers: the module imports here.

    The declaration above is a promise; this is the promise kept in the
    environment the rest of the suite runs in. A hard import on purpose --
    ``importorskip`` here would turn "the extra no longer installs sqlglot"
    into a green skip, which is the exact failure this test exists for.
    """
    from kptn_server import service

    assert callable(service.get_duckdb_preview)
    assert callable(service.build_table_file_map)


def test_the_superseded_render_index_page_helper_is_gone() -> None:
    """The React-era landing page renderer must not come back.

    ``templates/index.html`` is now the run console, rendered by
    :mod:`kptn_server.routes` with a ``project`` in its context.
    ``render_index_page`` rendered the same filename with no project and would
    raise on every request.
    """
    from kptn_server import service

    assert not hasattr(service, "render_index_page")
    assert not hasattr(service, "discover_kptn_configs")
