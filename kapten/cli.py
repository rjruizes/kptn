import os
from pathlib import Path
import typer
from typing import Any, Optional
import json
import yaml
from kapten.caching.TaskStateDbClient import TaskStateDbClient
from kapten.codegen.codegen import generate_files
from kapten.read_config import read_config
from kapten.util.pipeline_config import PipelineConfig, _module_path_from_dir

try:
    from botocore.exceptions import NoCredentialsError, NoRegionError
except ImportError:  # pragma: no cover - optional dependency
    NoCredentialsError = NoRegionError = None

app = typer.Typer()


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


def _build_pipeline_config(
    kap_conf: dict[str, Any],
    pipeline_name: str,
    project_root: Path,
    subset_mode: bool,
) -> PipelineConfig:
    settings = kap_conf.get("settings", {})
    py_tasks_dir = settings.get("py-tasks-dir")
    if not py_tasks_dir:
        raise RuntimeError("Missing 'py-tasks-dir' in kapten.yaml settings")

    module_path = _module_path_from_dir(py_tasks_dir)
    project_root = project_root.resolve()
    tasks_config_path = (project_root / "kapten.yaml").resolve()
    r_tasks_dir_setting = settings.get("r-tasks-dir", ".")
    r_tasks_dir_path = (project_root / Path(r_tasks_dir_setting)).resolve()

    pipeline_kwargs: dict[str, Any] = {
        "PIPELINE_NAME": pipeline_name,
        "PY_MODULE_PATH": module_path,
        "TASKS_CONFIG_PATH": str(tasks_config_path),
        "R_TASKS_DIR_PATH": str(r_tasks_dir_path),
        "SUBSET_MODE": subset_mode,
    }

    storage_key = settings.get("storage-key") or settings.get("storage_key")
    if storage_key:
        pipeline_kwargs["STORAGE_KEY"] = str(storage_key)

    branch = settings.get("branch")
    if branch:
        pipeline_kwargs["BRANCH"] = str(branch)

    return PipelineConfig(**pipeline_kwargs)


@app.command()
def codegen(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    """
    Generate Prefect flows (Python files) from the tasks.yaml file
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
