from __future__ import annotations

import importlib
import sys
import tomllib
from pathlib import Path
from types import ModuleType
from typing import Iterable

from kptn.exceptions import ProjectConfigError
from kptn.graph.graph import Graph
from kptn.graph.pipeline import Pipeline

_PROJECT_IMPORT_ROOTS: set[str] = set()


def _iter_module_locations(module: ModuleType) -> Iterable[Path]:
    file_path = getattr(module, "__file__", None)
    if file_path:
        yield Path(file_path)

    spec = getattr(module, "__spec__", None)
    if spec is not None:
        origin = getattr(spec, "origin", None)
        if origin not in (None, "built-in", "frozen"):
            yield Path(origin)

        search_locations = getattr(spec, "submodule_search_locations", None)
        if search_locations is not None:
            for location in search_locations:
                yield Path(location)

    module_path = getattr(module, "__path__", None)
    if module_path is not None:
        for location in module_path:
            yield Path(location)


def _module_belongs_to_project(module: ModuleType, project_roots: set[Path]) -> bool:
    for location in _iter_module_locations(module):
        try:
            resolved_location = location.resolve()
        except OSError:
            continue

        if any(resolved_location.is_relative_to(project_root) for project_root in project_roots):
            return True

    return False


def _prepare_project_imports(project_root: Path) -> None:
    resolved_root = project_root.resolve()
    known_roots = _PROJECT_IMPORT_ROOTS | {str(resolved_root)}
    sys.path[:] = [path_entry for path_entry in sys.path if path_entry not in known_roots]
    sys.path.insert(0, str(resolved_root))

    _PROJECT_IMPORT_ROOTS.add(str(resolved_root))
    project_roots = {Path(root) for root in _PROJECT_IMPORT_ROOTS}

    for module_name, module in list(sys.modules.items()):
        if isinstance(module, ModuleType) and _module_belongs_to_project(module, project_roots):
            sys.modules.pop(module_name, None)

    importlib.invalidate_caches()


def load_pipeline(project_root: Path) -> Pipeline:
    """Load the configured pipeline from a project root."""
    pyproject_path = project_root / "pyproject.toml"

    try:
        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError as exc:
        raise ProjectConfigError(
            f"Missing pyproject.toml in {project_root}. "
            "Create one with a [tool.kptn] pipeline entry."
        ) from exc
    except tomllib.TOMLDecodeError as exc:
        raise ProjectConfigError(
            f"Invalid pyproject.toml at {pyproject_path}: {exc}. "
            "Fix the TOML syntax and retry."
        ) from exc

    pipeline_module = config.get("tool", {}).get("kptn", {}).get("pipeline")
    if not pipeline_module:
        raise ProjectConfigError(
            "Missing [tool.kptn] pipeline in pyproject.toml. "
            'Add: [tool.kptn]\npipeline = "your_package.pipeline"'
        )

    _prepare_project_imports(project_root)

    try:
        module = importlib.import_module(pipeline_module)
    except ImportError as exc:
        raise ProjectConfigError(
            f"Could not import pipeline module {pipeline_module!r} from {project_root}: {exc}. "
            "Check the [tool.kptn] pipeline path and the module's imports."
        ) from exc

    pipeline_attr = getattr(module, "pipeline", None)
    if isinstance(pipeline_attr, Pipeline):
        return pipeline_attr

    graph_attr = getattr(module, "graph", None)
    if isinstance(graph_attr, Pipeline):
        return graph_attr
    if isinstance(graph_attr, Graph):
        return Pipeline("default", graph_attr)

    raise ProjectConfigError(
        f"Module {pipeline_module!r} must expose a 'pipeline' (Pipeline) "
        "or 'graph' (Graph) attribute"
    )
