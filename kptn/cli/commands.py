from __future__ import annotations

from pathlib import Path

import typer

from kptn.exceptions import ProfileError, ProjectConfigError
from kptn.project import load_pipeline
from kptn.runner.api import resolve_pipeline
from kptn.runner.api import run as _run_pipeline
import kptn.runner.plan as runner_plan

app = typer.Typer()


@app.command()
def run(
    profile: str | None = typer.Option(None, "--profile"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    project_root = Path.cwd()
    try:
        pipeline = load_pipeline(project_root)
    except ProjectConfigError as e:
        raise typer.BadParameter(str(e)) from e

    try:
        _run_pipeline(pipeline, profile=profile, force=force)
    except ProfileError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1)
    except Exception:
        raise typer.Exit(code=1)


@app.command()
def plan(
    profile: str | None = typer.Option(None, "--profile"),
) -> None:
    project_root = Path.cwd()
    try:
        pipeline = load_pipeline(project_root)
        resolved, state_store = resolve_pipeline(pipeline, project_root, profile)
    except ProjectConfigError as e:
        raise typer.BadParameter(str(e)) from e
    except ProfileError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1)

    runner_plan.plan(resolved, state_store)
