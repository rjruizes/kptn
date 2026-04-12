from __future__ import annotations

import importlib
import tomllib
from pathlib import Path

import typer

from kptn.exceptions import TaskError
from kptn.graph.graph import Graph
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.executor import execute
from kptn.state_store.factory import init_state_store

app = typer.Typer()


def _load_graph_from_pyproject(project_root: Path) -> Graph:
    with open(project_root / "pyproject.toml", "rb") as f:
        config = tomllib.load(f)

    pipeline_module = config.get("tool", {}).get("kptn", {}).get("pipeline")
    if not pipeline_module:
        raise typer.BadParameter(
            "Missing [tool.kptn] pipeline in pyproject.toml. "
            "Add: [tool.kptn]\npipeline = \"your_package.pipeline\""
        )

    module = importlib.import_module(pipeline_module)
    graph = getattr(module, "graph", None)
    if not isinstance(graph, Graph):
        raise typer.BadParameter(
            f"Module {pipeline_module!r} must expose a module-level 'graph' (Graph instance)"
        )
    return graph


@app.command()
def run(
    profile: str | None = typer.Option(None, "--profile"),
) -> None:
    project_root = Path.cwd()
    graph = _load_graph_from_pyproject(project_root)
    resolved = ResolvedGraph(graph=graph, pipeline="default", storage_key="kptn")
    state_store = init_state_store()
    try:
        execute(resolved, state_store, cwd=project_root)
    except TaskError:
        raise typer.Exit(code=1)
