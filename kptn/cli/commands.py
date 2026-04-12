from __future__ import annotations

import importlib
import tomllib
from pathlib import Path

import typer

from kptn.exceptions import ProfileError, TaskError
from kptn.graph.graph import Graph
from kptn.graph.pipeline import Pipeline
from kptn.profiles.loader import ProfileLoader
from kptn.profiles.resolved import ResolvedGraph
from kptn.profiles.resolver import ProfileResolver
from kptn.runner.executor import execute
from kptn.state_store.factory import init_state_store
import kptn.runner.plan as runner_plan

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


def _load_pipeline_from_pyproject(project_root: Path) -> Pipeline:
    with open(project_root / "pyproject.toml", "rb") as f:
        config = tomllib.load(f)

    pipeline_module = config.get("tool", {}).get("kptn", {}).get("pipeline")
    if not pipeline_module:
        raise typer.BadParameter(
            "Missing [tool.kptn] pipeline in pyproject.toml. "
            "Add: [tool.kptn]\npipeline = \"your_package.pipeline\""
        )

    module = importlib.import_module(pipeline_module)

    pipeline_attr = getattr(module, "pipeline", None)
    if isinstance(pipeline_attr, Pipeline):
        return pipeline_attr

    graph_attr = getattr(module, "graph", None)
    if isinstance(graph_attr, Pipeline):
        return graph_attr
    if isinstance(graph_attr, Graph):
        return Pipeline("default", graph_attr)

    raise typer.BadParameter(
        f"Module {pipeline_module!r} must expose a 'pipeline' (Pipeline) "
        "or 'graph' (Graph) attribute"
    )


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


@app.command()
def plan(
    profile: str | None = typer.Option(None, "--profile"),
) -> None:
    project_root = Path.cwd()
    pipeline = _load_pipeline_from_pyproject(project_root)
    config = ProfileLoader.load(project_root / "kptn.yaml")

    if profile is not None:
        try:
            resolved = ProfileResolver(config).compile(pipeline, profile)
        except ProfileError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=1)
    else:
        resolved = ResolvedGraph(
            graph=pipeline,
            pipeline=pipeline.name,
            storage_key=config.settings.db_path or ".kptn/kptn.db",
        )

    state_store = init_state_store(config.settings)
    runner_plan.plan(resolved, state_store)
