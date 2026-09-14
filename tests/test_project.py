from __future__ import annotations

import sys
from pathlib import Path

import pytest

from kptn.exceptions import ProjectConfigError
from kptn.project import load_pipeline


@pytest.fixture(autouse=True)
def restore_import_state() -> None:
    original_path = sys.path.copy()
    original_modules = set(sys.modules)

    yield

    sys.path[:] = original_path
    for name in set(sys.modules) - original_modules:
        sys.modules.pop(name, None)


def _write_pyproject(project_root: Path, pipeline_module: str, *, body: str | None = None) -> None:
    contents = body or f'[tool.kptn]\npipeline = "{pipeline_module}"\n'
    (project_root / "pyproject.toml").write_text(contents)


def _write_module(project_root: Path, module_name: str, source: str) -> None:
    module_path = project_root
    parts = module_name.split(".")

    for package in parts[:-1]:
        module_path /= package
        module_path.mkdir(exist_ok=True)
        init_path = module_path / "__init__.py"
        if not init_path.exists():
            init_path.write_text("")

    (module_path / f"{parts[-1]}.py").write_text(source)


def _pipeline_source(name_expr: str) -> str:
    return (
        "from kptn.graph.graph import Graph\n"
        "from kptn.graph.pipeline import Pipeline\n"
        f"pipeline = Pipeline({name_expr}, Graph())\n"
    )


def test_load_pipeline_missing_pyproject_raises_project_config_error(tmp_path: Path) -> None:
    """Missing pyproject.toml is normalized into a project configuration error."""
    with pytest.raises(ProjectConfigError) as exc_info:
        load_pipeline(tmp_path)

    assert str(exc_info.value) == (
        f"Missing pyproject.toml in {tmp_path}. "
        "Create one with a [tool.kptn] pipeline entry."
    )


def test_load_pipeline_invalid_toml_raises_project_config_error(tmp_path: Path) -> None:
    """Unreadable TOML is reported with an actionable project configuration error."""
    _write_pyproject(tmp_path, "broken", body='[tool.kptn]\npipeline = "broken"\ninvalid = [\n')

    with pytest.raises(ProjectConfigError) as exc_info:
        load_pipeline(tmp_path)

    message = str(exc_info.value)
    assert "Invalid pyproject.toml" in message
    assert "Fix the TOML syntax" in message


def test_load_pipeline_import_error_raises_project_config_error(tmp_path: Path) -> None:
    """Pipeline module import failures are wrapped in ProjectConfigError."""
    _write_pyproject(tmp_path, "demo_pkg.pipeline")
    _write_module(
        tmp_path,
        "demo_pkg.pipeline",
        "from .missing import PIPELINE_NAME\n" + _pipeline_source("PIPELINE_NAME"),
    )

    with pytest.raises(ProjectConfigError) as exc_info:
        load_pipeline(tmp_path)

    message = str(exc_info.value)
    assert "Could not import pipeline module 'demo_pkg.pipeline'" in message
    assert "demo_pkg.missing" in message


def test_load_pipeline_reloads_changed_project_modules(tmp_path: Path) -> None:
    """Repeated loads in one process re-import project modules instead of using stale cache."""
    _write_pyproject(tmp_path, "demo_pkg.pipeline")
    _write_module(tmp_path, "demo_pkg.helper", 'PIPELINE_NAME = "first"\n')
    _write_module(
        tmp_path,
        "demo_pkg.pipeline",
        "from .helper import PIPELINE_NAME\n" + _pipeline_source("PIPELINE_NAME"),
    )

    assert load_pipeline(tmp_path).name == "first"

    _write_module(tmp_path, "demo_pkg.helper", 'PIPELINE_NAME = "second"\n')

    assert load_pipeline(tmp_path).name == "second"


def test_load_pipeline_isolates_same_named_modules_across_projects(tmp_path: Path) -> None:
    """Cross-project loads do not reuse a stale same-named module from sys.modules."""
    first_project = tmp_path / "first"
    second_project = tmp_path / "second"
    first_project.mkdir()
    second_project.mkdir()

    _write_pyproject(first_project, "shared_pipeline")
    _write_module(first_project, "shared_pipeline", _pipeline_source('"first"'))

    _write_pyproject(second_project, "shared_pipeline")
    _write_module(second_project, "shared_pipeline", _pipeline_source('"second"'))

    assert load_pipeline(first_project).name == "first"
    assert load_pipeline(second_project).name == "second"


# -- the project's own virtualenv ------------------------------------------
#
# `uv` puts it at `<project>/.venv` by default, so every installed package
# lives *under* the project root. A purge that asks only "is this file under
# the root?" therefore evicts the whole dependency tree along with the
# project's own source -- and some of it does not survive being evicted.


def _install_into_project_venv(project_root: Path, module_name: str, source: str) -> Path:
    """Write *source* as an installed package in the project's in-tree venv."""
    site_packages = project_root / ".venv" / "lib" / "python3.11" / "site-packages"
    site_packages.mkdir(parents=True, exist_ok=True)
    (site_packages / f"{module_name}.py").write_text(source)
    return site_packages


#: A single-file module that registers a submodule in ``sys.modules`` itself,
#: attributing it to the parent's own file.
#:
#: This is the shape of a compiled extension -- ``duckdb``'s ``_duckdb`` is
#: exactly this, and registers ``_duckdb.functional`` and ``_duckdb.typing``
#: the same way -- and it is the shape that makes the purge unrecoverable
#: rather than merely wasteful. The parent has no ``__path__``, so once the
#: submodule is dropped from ``sys.modules`` no finder can locate it again
#: and the next import fails with "'_duckdb' is not a package". The
#: ``__file__`` is what puts it in the purge's sights, exactly as the real
#: extension's does.
_EXTENSION_SHAPED_SOURCE = (
    "import sys, types\n"
    "_sub = types.ModuleType('vendor_ext.functional')\n"
    "_sub.__file__ = __file__\n"
    "sys.modules['vendor_ext.functional'] = _sub\n"
)


def test_load_pipeline_keeps_packages_installed_in_the_projects_venv(
    tmp_path: Path,
) -> None:
    """An installed dependency must survive a load, submodules included.

    The purge exists to re-import the project's *own* source between loads.
    A dependency in `<project>/.venv` is not that, and evicting one whose
    submodules only exist because its initializer registered them leaves it
    permanently unimportable in this process.
    """
    _write_pyproject(tmp_path, "demo_pkg.pipeline")
    _write_module(tmp_path, "demo_pkg.pipeline", _pipeline_source('"demo"'))
    site_packages = _install_into_project_venv(
        tmp_path, "vendor_ext", _EXTENSION_SHAPED_SOURCE
    )

    sys.path.insert(0, str(site_packages))
    import vendor_ext  # noqa: F401

    installed = sys.modules["vendor_ext"]
    submodule = sys.modules["vendor_ext.functional"]

    load_pipeline(tmp_path)

    assert sys.modules.get("vendor_ext") is installed, (
        "the project's venv was purged along with its own source"
    )
    assert sys.modules.get("vendor_ext.functional") is submodule, (
        "an extension's registered submodule was evicted and cannot be re-found"
    )


def test_load_pipeline_still_reloads_project_source_beside_a_venv(
    tmp_path: Path,
) -> None:
    """Sparing the venv must not spare the project's own modules.

    The exclusion above is easy to write too broadly -- skip anything under a
    directory that looks installed, and a project laid out beside its venv
    stops reloading at all. This is the same reload contract as
    ``test_load_pipeline_reloads_changed_project_modules``, asserted with a
    venv present.
    """
    _write_pyproject(tmp_path, "demo_pkg.pipeline")
    _install_into_project_venv(tmp_path, "vendor_ext", _EXTENSION_SHAPED_SOURCE)
    _write_module(tmp_path, "demo_pkg.helper", 'PIPELINE_NAME = "first"\n')
    _write_module(
        tmp_path,
        "demo_pkg.pipeline",
        "from .helper import PIPELINE_NAME\n" + _pipeline_source("PIPELINE_NAME"),
    )

    assert load_pipeline(tmp_path).name == "first"

    _write_module(tmp_path, "demo_pkg.helper", 'PIPELINE_NAME = "second"\n')

    assert load_pipeline(tmp_path).name == "second"
