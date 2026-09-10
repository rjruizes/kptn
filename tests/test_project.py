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
