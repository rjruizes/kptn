from __future__ import annotations

import importlib
import sys
import tomllib
from pathlib import Path

from kptn.exceptions import ProjectConfigError
from kptn.graph.graph import Graph
from kptn.graph.pipeline import Pipeline


def load_pipeline(project_root: Path) -> Pipeline:
    """Load the configured pipeline from a project root."""
    with open(project_root / "pyproject.toml", "rb") as f:
        config = tomllib.load(f)

    pipeline_module = config.get("tool", {}).get("kptn", {}).get("pipeline")
    if not pipeline_module:
        raise ProjectConfigError(
            "Missing [tool.kptn] pipeline in pyproject.toml. "
            'Add: [tool.kptn]\npipeline = "your_package.pipeline"'
        )

    project_root_str = str(project_root)
    if sys.path[:1] != [project_root_str]:
        try:
            sys.path.remove(project_root_str)
        except ValueError:
            pass
        sys.path.insert(0, project_root_str)

    module = importlib.import_module(pipeline_module)

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
