import os
from pathlib import Path
import typer
from typing import Any, Optional
import json
import yaml
from kapten.caching.TaskStateDbClient import TaskStateDbClient
from kapten.cli.infra_commands import register_infra_commands
from kapten.cli.config_validation import (
    SchemaValidationError,
    validate_kapten_config,
)
from kapten.codegen.codegen import generate_files
from kapten.read_config import read_config
from kapten.cli.task_validation import (
    _build_pipeline_config,
    _validate_python_tasks,
)

try:
    from botocore.exceptions import NoCredentialsError, NoRegionError
except ImportError:  # pragma: no cover - optional dependency
    NoCredentialsError = NoRegionError = None

app = typer.Typer()

# Register infrastructure commands
register_infra_commands(app)


def _infer_language(task_spec: dict[str, Any]) -> str:
    """Infer the task language from its configuration."""
    language = task_spec.get("language")
    if language:
        return str(language)

    file_entry = task_spec.get("file")
    if not file_entry:
        return "unknown"

    file_part = str(file_entry).split(":", 1)[0]
    suffix = Path(file_part).suffix.lower()

    if suffix == ".py":
        return "python"
    if suffix == ".r":
        return "r"
    if suffix:
        return suffix.lstrip(".")
    return "unknown"


def _choose_pipeline(kap_conf: dict[str, Any], requested: Optional[str]) -> str:
    graphs = kap_conf.get("graphs", {})
    if not graphs:
        raise ValueError("No graphs defined in kapten.yaml")

    if requested:
        if requested not in graphs:
            available = ", ".join(sorted(graphs))
            raise ValueError(
                f"Pipeline '{requested}' not found; available pipelines: {available}"
            )
        return requested

    if len(graphs) == 1:
        return next(iter(graphs))

    available = ", ".join(sorted(graphs))
    raise ValueError(
        f"Multiple pipelines found ({available}); please specify --pipeline"
    )


@app.command()
def codegen(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    """
    Generate Prefect flows (Python files) from the kapten.yaml file
    """
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            generate_files()
        finally:
            os.chdir(original_dir)
    else:
        generate_files()

@app.command()
def serve_docker(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    """
    Start a docker API server to allow the UI to trigger building and pushing docker images
    """
    from kapten.dockerbuild.dockerbuild import docker_api_server
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            docker_api_server()
        finally:
            os.chdir(original_dir)
    else:
        docker_api_server()

@app.command()
def watch_files(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    """
    Start a file watcher to monitor for changes in the code and send updates to the UI
    """
    from kapten.filewatcher.filewatcher import start_watching
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            start_watching()
        finally:
            os.chdir(original_dir)
    else:
        start_watching()

@app.command()
def backend(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    from kapten.watcher.app import start
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            start()
        finally:
            os.chdir(original_dir)
    else:
        start()

@app.command()
def config(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration"),
    format: str = typer.Option("yaml", "--format", "-f", help="Output format: yaml, json, or table")
):
    """
    Display the current kapten configuration
    """
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            kap_conf = read_config()
        finally:
            os.chdir(original_dir)
    else:
        kap_conf = read_config()
    
    if format.lower() == "json":
        typer.echo(json.dumps(kap_conf, indent=2))
    elif format.lower() == "table":
        typer.echo("=" * 40)
        for key, value in kap_conf.items():
            typer.echo(f"{key:<20}: {value}")
    else:  # default to yaml
        typer.echo(yaml.dump(kap_conf, default_flow_style=False))


@app.command()
def validate(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration"),
    graph: Optional[str] = typer.Option(None, "--graph", "-g", help="Graph name to validate"),
):
    """Validate kapten.yaml against kapten-schema.json."""

    base_dir = Path(project_dir).resolve() if project_dir else Path.cwd()
    if project_dir and not base_dir.exists():
        typer.echo(f"Provided project directory does not exist: {base_dir}")
        raise typer.Exit(1)

    config_path = base_dir / "kapten.yaml"
    module_root = Path(__file__).resolve()
    schema_path = module_root.parents[2] / "kapten-schema.json"
    if not schema_path.exists():
        schema_path = module_root.parents[1] / "kapten-schema.json"

    try:
        issues = validate_kapten_config(config_path, schema_path)
    except FileNotFoundError as exc:
        typer.echo(str(exc))
        raise typer.Exit(1)
    except yaml.YAMLError as exc:
        typer.echo(f"Failed to parse kapten.yaml: {exc}")
        raise typer.Exit(1) from exc
    except json.JSONDecodeError as exc:
        typer.echo(f"Failed to parse kapten-schema.json: {exc}")
        raise typer.Exit(1) from exc
    except SchemaValidationError as exc:  # pragma: no cover - schema should be valid
        typer.echo(str(exc))
        raise typer.Exit(1) from exc

    if issues:
        typer.echo("kapten.yaml does not conform to kapten-schema.json:")
        for issue in issues:
            typer.echo(f"- {issue.path}: {issue.message}")
        raise typer.Exit(1)

    try:
        with config_path.open("r", encoding="utf-8") as config_file:
            kap_conf = yaml.safe_load(config_file) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - already parsed during validation
        typer.echo(f"Failed to parse kapten.yaml: {exc}")
        raise typer.Exit(1) from exc

    if graph:
        graphs_block = kap_conf.get("graphs")
        if not isinstance(graphs_block, dict) or not graphs_block:
            typer.echo("No graphs defined in kapten.yaml.")
            raise typer.Exit(1)
        if graph not in graphs_block:
            available = ", ".join(sorted(graphs_block))
            typer.echo(
                f"Graph '{graph}' not found; available graphs: {available}"
            )
            raise typer.Exit(1)
        kap_conf = {**kap_conf, "graphs": {graph: graphs_block[graph]}}

    python_issues = _validate_python_tasks(base_dir, kap_conf)
    if python_issues:
        typer.echo("kapten.yaml contains task configuration errors:")
        for issue in python_issues:
            typer.echo(f"- {issue}")
        raise typer.Exit(1)

    typer.echo("kapten.yaml conforms to kapten-schema.json.")


@app.command()
def ls(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    """Display tasks from kapten.yaml alongside their language."""

    def _render_tasks_table() -> None:
        kap_conf = read_config()
        tasks = kap_conf.get("tasks", {})
        if not tasks:
            typer.echo("No tasks found in kapten.yaml.")
            return

        rows = []
        for task_name, task_spec in tasks.items():
            language = _infer_language(task_spec if isinstance(task_spec, dict) else {})
            rows.append((str(task_name), language))

        rows.sort(key=lambda item: item[0])

        header_task = "Task"
        header_lang = "Language"
        task_width = len(header_task)
        lang_width = len(header_lang)

        for task_name, language in rows:
            task_width = max(task_width, len(task_name))
            lang_width = max(lang_width, len(language))

        typer.echo(f"{header_task:<{task_width}}  {header_lang}")
        typer.echo(f"{'-' * task_width}  {'-' * lang_width}")
        for task_name, language in rows:
            typer.echo(f"{task_name:<{task_width}}  {language}")

    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            _render_tasks_table()
        finally:
            os.chdir(original_dir)
    else:
        _render_tasks_table()


@app.command()
def fetch(
    task_name: str = typer.Argument(..., help="Name of the task to fetch"),
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration"),
    pipeline: Optional[str] = typer.Option(None, "--pipeline", "-g", help="Pipeline (graph) name to use"),
    subset: bool = typer.Option(False, "--subset", help="Fetch subset data if subset mode is enabled"),
):
    """Retrieve cached data for a task from the configured database."""

    def _fetch_task() -> None:
        try:
            kap_conf = read_config()
        except FileNotFoundError as exc:
            typer.echo(str(exc))
            raise typer.Exit(1)

        try:
            pipeline_name = _choose_pipeline(kap_conf, pipeline)
        except ValueError as exc:
            typer.echo(str(exc))
            raise typer.Exit(1) from exc

        try:
            pipeline_config = _build_pipeline_config(kap_conf, pipeline_name, Path.cwd(), subset)
        except (RuntimeError, ValueError) as exc:
            typer.echo(f"Configuration error: {exc}")
            raise typer.Exit(1) from exc

        tscache = TaskStateDbClient(pipeline_config, tasks_config=kap_conf)
        try:
            state = tscache.fetch_state(task_name)
        except Exception as exc:
            if NoCredentialsError and isinstance(exc, NoCredentialsError):
                typer.echo("AWS credentials not found. Configure credentials or set settings.db to 'sqlite'.")
                raise typer.Exit(1) from exc
            if NoRegionError and isinstance(exc, NoRegionError):
                typer.echo("AWS region not configured. Set AWS_REGION or configure settings.db to 'sqlite'.")
                raise typer.Exit(1) from exc
            typer.echo(f"Failed to fetch data for task '{task_name}': {exc}")
            raise typer.Exit(1) from exc

        if not state:
            typer.echo(f"No cached state found for task '{task_name}'.")
            raise typer.Exit(1)

        if isinstance(state, (dict, list)) or hasattr(state, '__dict__'):
            try:
                # Convert state object to dict if it has attributes
                if hasattr(state, '__dict__'):
                    state_dict = state.__dict__
                else:
                    state_dict = state
                typer.echo(json.dumps(state_dict, indent=2, default=str))
            except (TypeError, ValueError):
                typer.echo(str(state))
        else:
            typer.echo(str(state))

    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            _fetch_task()
        finally:
            os.chdir(original_dir)
    else:
        _fetch_task()


if __name__ == "__main__":
    app()
