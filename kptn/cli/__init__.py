import os
from pathlib import Path
import typer
from typing import Any, Optional
import json
import yaml
from kptn.caching.TaskStateDbClient import TaskStateDbClient
from kptn.cli.decider_bundle import BundleDeciderError, bundle_decider_lambda
from kptn.cli.infra_commands import register_infra_commands
from kptn.cli.run_aws import (
    DirectRunConfig,
    StackInfoError,
    choose_state_machine_arn,
    create_boto_session,
    fetch_stack_info,
    parse_tasks_arg,
    resolve_stack_parameter_name,
    run_ecs_task,
    run_local,
    start_state_machine_execution,
    submit_batch_job,
)
from kptn.cli.config_validation import (
    SchemaValidationError,
    validate_kptn_config,
)
from kptn.codegen.codegen import generate_files
from kptn.read_config import read_config
from kptn.cli.task_validation import (
    _build_pipeline_config,
    _validate_python_tasks,
)
from kptn.lineage import SqlLineageAnalyzer, SqlLineageError, TableMetadata
from kptn.lineage.html_renderer import render_lineage_html

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


def _infer_lineage_dialect(config: dict[str, Any], requested: Optional[str]) -> str:
    """Infer the SQL dialect to use for lineage parsing."""

    if requested:
        return requested

    tasks = config.get("tasks", {})
    for task_spec in tasks.values():
        spec_dict = task_spec if isinstance(task_spec, dict) else {}
        outputs = spec_dict.get("outputs") or []
        for output in outputs:
            if isinstance(output, str) and "://" in output:
                scheme = output.split("://", 1)[0]
                if scheme:
                    return scheme

    db_setting = config.get("settings", {}).get("db")
    if isinstance(db_setting, str) and db_setting:
        return db_setting

    return "duckdb"


def _normalize_identifier(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value).strip().strip('"').lower()


def _candidate_table_keys(table_ref: str) -> list[str]:
    value = table_ref.strip()
    keys: list[str] = []
    normalized_full = _normalize_identifier(value)
    if normalized_full:
        keys.append(normalized_full)
    if "." in value:
        suffix = _normalize_identifier(value.split(".")[-1])
        if suffix and suffix not in keys:
            keys.append(suffix)
    return keys


def _task_order_from_graph(
    kap_conf: dict[str, Any],
    graph_name: Optional[str],
) -> Optional[list[str]]:
    graphs = kap_conf.get("graphs") or {}
    if not graphs:
        return None

    if graph_name:
        graph_spec = graphs.get(graph_name)
        if graph_spec is None:
            available = ", ".join(sorted(graphs))
            raise ValueError(
                f"Graph '{graph_name}' not found; available graphs: {available}"
            )
    else:
        graph_spec = next(iter(graphs.values()))

    tasks = (graph_spec or {}).get("tasks") or {}
    if not isinstance(tasks, dict):
        return None
    return list(tasks.keys())


lineage_app = typer.Typer(help="Inspect SQL lineage for tasks defined in kptn.yaml.")
app.add_typer(lineage_app, name="lineage")


def _build_lineage_payload(
    analyzer: SqlLineageAnalyzer,
    *,
    task_order: Optional[list[str]] | None = None,
    tasks_config: Optional[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Construct table/edge payloads for the lineage visualizer.

    ``task_order`` preserves the ordering of tables as they appear in kptn.yaml
    (or a supplied task list) while still adding upstream dependencies that are
    referenced but not produced by the project. ``tasks_config`` is accepted for
    future configurability and aligns with the test helper signature.
    """

    if not task_order and tasks_config:
        task_order = list(tasks_config.keys())

    metadata_entries = list(analyzer.tables().values())

    task_to_metadata: dict[str, list[TableMetadata]] = {}
    for metadata in metadata_entries:
        task_to_metadata.setdefault(metadata.task_name, []).append(metadata)

    if task_order:
        ordered_metadata: list[TableMetadata] = []
        remaining_metadata = {task: list(entries) for task, entries in task_to_metadata.items()}
        for task_name in task_order:
            ordered_metadata.extend(remaining_metadata.pop(task_name, []))

        if remaining_metadata:
            # Preserve original relative order for any tasks not listed in task_order.
            for metadata in metadata_entries:
                if metadata.task_name in remaining_metadata:
                    ordered_metadata.append(metadata)
                    remaining = remaining_metadata.get(metadata.task_name)
                    if remaining and metadata in remaining:
                        remaining.remove(metadata)
                    if not remaining:
                        remaining_metadata.pop(metadata.task_name, None)
    else:
        ordered_metadata = metadata_entries

    tables_payload: list[dict[str, Any]] = []
    table_lookup: dict[str, int] = {}

    def _register_table(name: str, columns: list[str]) -> int:
        primary_key = _normalize_identifier(name)
        if primary_key and primary_key in table_lookup:
            existing_index = table_lookup[primary_key]
            entry = tables_payload[existing_index]
            if (not entry.get("columns")) and columns:
                entry["columns"] = columns
            return existing_index

        candidates = {name, *_candidate_table_keys(name)}
        index = len(tables_payload)
        tables_payload.append({"name": name, "columns": columns})
        for candidate in candidates:
            key = _normalize_identifier(candidate)
            if key and key not in table_lookup:
                table_lookup[key] = index
        return index

    def _register_task_outputs(task_name: str) -> None:
        if not tasks_config:
            return
        task_spec = tasks_config.get(task_name) or {}
        outputs = task_spec.get("outputs") or []
        for output in outputs:
            table_name = SqlLineageAnalyzer._output_identifier(str(output))
            _register_table(table_name, [])

    # First, register tables produced by tasks (in the requested order).
    seen_tasks: set[str] = set()
    for task_name in task_order or []:
        seen_tasks.add(task_name)
        metadata_for_task = task_to_metadata.pop(task_name, [])
        if metadata_for_task:
            for metadata in metadata_for_task:
                _register_table(metadata.display_name, list(metadata.columns))
        else:
            _register_task_outputs(task_name)

    # Register any remaining SQL-backed tasks preserving their original order.
    for metadata in metadata_entries:
        if metadata.task_name in seen_tasks:
            continue
        _register_table(metadata.display_name, list(metadata.columns))
        seen_tasks.add(metadata.task_name)

    # Capture upstream tables that are referenced but not produced by tasks.
    external_columns: dict[str, set[str]] = {}
    for metadata in ordered_metadata:
        for sources in metadata.column_sources.values():
            for source in sources:
                if "." not in source:
                    continue
                table_part, column_name = source.rsplit(".", 1)
                if not table_part or not column_name:
                    continue
                existing_index = None
                for candidate in _candidate_table_keys(table_part):
                    if candidate in table_lookup:
                        existing_index = table_lookup[candidate]
                        break
                if existing_index is not None:
                    entry = tables_payload[existing_index]
                    current_columns = entry.get("columns") or []
                    if column_name not in current_columns:
                        entry["columns"] = [*current_columns, column_name]
                    continue
                external_columns.setdefault(table_part, set()).add(column_name)

    for table_name in sorted(external_columns):
        columns = sorted(external_columns[table_name]) or []
        _register_table(table_name, columns)

    # Build lineage edges now that all source/destination tables are registered.
    lineage_payload: list[dict[str, Any]] = []
    for metadata in ordered_metadata:
        destination_index = None
        for candidate in (_normalize_identifier(metadata.display_name), _normalize_identifier(metadata.table_key)):
            if candidate and candidate in table_lookup:
                destination_index = table_lookup[candidate]
                break
        if destination_index is None:
            continue

        for column in metadata.columns:
            for source in metadata.column_sources.get(column, []):
                if "." not in source:
                    continue
                table_part, column_name = source.rsplit(".", 1)
                if not table_part or not column_name:
                    continue

                source_index = None
                for candidate in _candidate_table_keys(table_part):
                    if candidate in table_lookup:
                        source_index = table_lookup[candidate]
                        break
                if source_index is None:
                    continue

                lineage_payload.append(
                    {
                        "from": [source_index, column_name],
                        "to": [destination_index, column],
                    }
                )

    return tables_payload, lineage_payload


def _choose_pipeline(kap_conf: dict[str, Any], requested: Optional[str]) -> str:
    graphs = kap_conf.get("graphs", {})
    if not graphs:
        raise ValueError("No graphs defined in kptn.yaml")

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
    project_dir: Optional[Path] = typer.Option(
        None, "--project-dir", "-p", help="Project directory containing kptn configuration"
    ),
    graph: Optional[str] = typer.Option(
        None, "--graph", "-g", help="Graph name to generate flows for"
    ),
    emit_vanilla_runner: Optional[bool] = typer.Option(
        None,
        "--emit-vanilla-runner/--no-emit-vanilla-runner",
        help="Also emit a vanilla Python runner alongside Step Functions artifacts (default: auto)",
    ),
):
    """
    Generate Prefect flows (Python files) from the kptn.yaml file
    """
    def _generate() -> None:
        try:
            generate_files(graph=graph, emit_vanilla_runner=emit_vanilla_runner)
        except ValueError as exc:
            typer.echo(str(exc))
            raise typer.Exit(1) from exc

    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            _generate()
        finally:
            os.chdir(original_dir)
    else:
        _generate()

@app.command()
def serve_docker(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration")
):
    """
    Start a docker API server to allow the UI to trigger building and pushing docker images
    """
    from kptn.dockerbuild.dockerbuild import docker_api_server
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
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration")
):
    """
    Start a file watcher to monitor for changes in the code and send updates to the UI
    """
    from kptn.filewatcher.filewatcher import start_watching
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
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration")
):
    from kptn.watcher.app import start
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
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration"),
    format: str = typer.Option("yaml", "--format", "-f", help="Output format: yaml, json, or table")
):
    """
    Display the current kptn configuration
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
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration"),
    graph: Optional[str] = typer.Option(None, "--graph", "-g", help="Graph name to validate"),
):
    """Validate kptn.yaml against kptn-schema.json."""

    base_dir = Path(project_dir).resolve() if project_dir else Path.cwd()
    if project_dir and not base_dir.exists():
        typer.echo(f"Provided project directory does not exist: {base_dir}")
        raise typer.Exit(1)

    config_path = base_dir / "kptn.yaml"
    module_root = Path(__file__).resolve()
    schema_path = module_root.parents[2] / "kptn-schema.json"
    if not schema_path.exists():
        schema_path = module_root.parents[1] / "kptn-schema.json"

    try:
        issues = validate_kptn_config(config_path, schema_path)
    except FileNotFoundError as exc:
        typer.echo(str(exc))
        raise typer.Exit(1)
    except yaml.YAMLError as exc:
        typer.echo(f"Failed to parse kptn.yaml: {exc}")
        raise typer.Exit(1) from exc
    except json.JSONDecodeError as exc:
        typer.echo(f"Failed to parse kptn-schema.json: {exc}")
        raise typer.Exit(1) from exc
    except SchemaValidationError as exc:  # pragma: no cover - schema should be valid
        typer.echo(str(exc))
        raise typer.Exit(1) from exc

    if issues:
        typer.echo("kptn.yaml does not conform to kptn-schema.json:")
        for issue in issues:
            typer.echo(f"- {issue.path}: {issue.message}")
        raise typer.Exit(1)

    try:
        with config_path.open("r", encoding="utf-8") as config_file:
            kap_conf = yaml.safe_load(config_file) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - already parsed during validation
        typer.echo(f"Failed to parse kptn.yaml: {exc}")
        raise typer.Exit(1) from exc

    if graph:
        graphs_block = kap_conf.get("graphs")
        if not isinstance(graphs_block, dict) or not graphs_block:
            typer.echo("No graphs defined in kptn.yaml.")
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
        typer.echo("kptn.yaml contains task configuration errors:")
        for issue in python_issues:
            typer.echo(f"- {issue}")
        raise typer.Exit(1)

    typer.echo("kptn.yaml conforms to kptn-schema.json.")


@app.command()
def run(
    pipeline: str = typer.Argument(..., help="Pipeline (graph) name associated with the run"),
    tasks: str | None = typer.Argument(
        None, help="Comma-separated task names to execute (omit to run all eligible tasks)"
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Force run even if another execution is active"),
    local: bool = typer.Option(False, "--local", help="Run locally instead of using AWS Step Functions"),
    stack_param_name: Optional[str] = typer.Option(
        None,
        "--stack-param-name",
        help="Override the SSM parameter name holding stack metadata",
    ),
    state_machine: Optional[str] = typer.Option(
        None,
        "--state-machine",
        "-s",
        help="State machine key or ARN to invoke when multiple are available",
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="AWS profile to use for cloud runs",
    ),
    region: Optional[str] = typer.Option(
        None,
        "--region",
        help="AWS region override for cloud runs",
    ),
    launch_type: Optional[str] = typer.Option(
        None,
        "--launch-type",
        help="Launch type override for direct ECS runs (e.g., FARGATE or EC2)",
    ),
    subnet_id: list[str] = typer.Option(
        [],
        "--subnet-id",
        help="Subnet ID for direct ECS runs (repeatable)",
    ),
    security_group_id: list[str] = typer.Option(
        [],
        "--security-group-id",
        help="Security group ID for direct ECS runs (repeatable)",
    ),
):
    """
    Run tasks for a pipeline via Step Functions (default) or directly via ECS/Batch when a single task is provided.
    """
    try:
        task_list = parse_tasks_arg(tasks)
    except ValueError as exc:
        typer.echo(str(exc))
        raise typer.Exit(1) from exc

    if local:
        try:
            run_local(pipeline, task_list, force)
        except StackInfoError as exc:
            typer.echo(str(exc))
            raise typer.Exit(1) from exc
        return

    try:
        session = create_boto_session(profile, region)
    except StackInfoError as exc:
        typer.echo(str(exc))
        raise typer.Exit(1) from exc

    parameter_name = resolve_stack_parameter_name(pipeline, stack_param_name)
    try:
        stack_info = fetch_stack_info(session=session, parameter_name=parameter_name)
    except StackInfoError as exc:
        typer.echo(str(exc))
        raise typer.Exit(1) from exc

    direct_run_config = DirectRunConfig(
        launch_type=launch_type,
        subnet_ids=subnet_id,
        security_group_ids=security_group_id,
    )

    if len(task_list) == 1:
        single_task = task_list[0]

        if stack_info.get("batch_job_queue_arn") and stack_info.get("batch_job_definition_arn"):
            try:
                response = submit_batch_job(
                    session=session,
                    stack_info=stack_info,
                    pipeline=pipeline,
                    task=single_task,
                )
                typer.echo(f"Submitted Batch job {response.get('jobName')} ({response.get('jobId')})")
                return
            except StackInfoError as exc:
                typer.echo(f"Batch submission skipped: {exc}", err=True)
            except Exception as exc:  # pragma: no cover - boto3 runtime failures
                typer.echo(f"Failed to submit Batch job: {exc}", err=True)

        if stack_info.get("cluster_arn") and stack_info.get("task_definition_arn"):
            try:
                response = run_ecs_task(
                    session=session,
                    stack_info=stack_info,
                    pipeline=pipeline,
                    task=single_task,
                    config=direct_run_config,
                )
                tasks_started = [task.get("taskArn") for task in response.get("tasks", []) if task.get("taskArn")]
                if tasks_started:
                    typer.echo(f"Started ECS task: {tasks_started[0]}")
                else:
                    typer.echo("Started ECS task")
                failures = response.get("failures")
                if failures:
                    typer.echo(f"ECS run returned failures: {failures}", err=True)
                return
            except StackInfoError as exc:
                typer.echo(f"ECS run skipped: {exc}", err=True)
            except Exception as exc:  # pragma: no cover - boto3 runtime failures
                typer.echo(f"Failed to start ECS task: {exc}", err=True)

    state_machine_arn = choose_state_machine_arn(
        stack_info,
        preferred_key=state_machine,
        pipeline=pipeline,
    )
    if not state_machine_arn:
        typer.echo("No state machine ARN found in stack metadata; specify --state-machine or fix the stack info.")
        raise typer.Exit(1)

    try:
        execution_arn = start_state_machine_execution(
            session=session,
            state_machine_arn=state_machine_arn,
            pipeline=pipeline,
            tasks=task_list,
            force=force,
        )
    except Exception as exc:  # pragma: no cover - boto3 runtime failures
        typer.echo(f"Failed to start Step Functions execution: {exc}")
        raise typer.Exit(1) from exc

    typer.echo(f"Started state machine execution: {execution_arn}")


@app.command(name="bundle-decider")
def bundle_decider(
    project_dir: Optional[Path] = typer.Option(
        None,
        "--project-dir",
        "-p",
        help="Project directory containing kptn configuration",
    ),
    output_dir: Path = typer.Option(
        Path("infra/lambda_decider"),
        "--output-dir",
        "-o",
        help="Directory where the decider bundle will be written",
    ),
    pipeline: Optional[str] = typer.Option(
        None,
        "--pipeline",
        "-n",
        help="Target pipeline (graph) name. Required when multiple graphs exist.",
    ),
    kptn_source: Optional[Path] = typer.Option(
        None,
        "--kptn-source",
        help="Path to kptn source to include in the bundle (defaults to PyPI release).",
    ),
    project_source: Optional[Path] = typer.Option(
        None,
        "--project-source",
        help="Path to install the project package from (defaults to the project directory).",
    ),
    python_version: str = typer.Option(
        "3.11",
        "--python-version",
        help="Python version used when installing dependencies with uv.",
    ),
    python_platform: str = typer.Option(
        "x86_64-manylinux2014",
        "--python-platform",
        help="Platform tag used when installing dependencies with uv.",
    ),
    install_project: bool = typer.Option(
        False,
        "--install-project/--no-install-project",
        help="Also install the project package into the bundle (defaults to copying sources only).",
    ),
    prefer_local_kptn: bool = typer.Option(
        True,
        "--prefer-local-kptn/--no-prefer-local-kptn",
        help="Install the locally checked-out kptn source when available (default: enabled).",
    ),
):
    """Build the kptn decider Lambda bundle for the current project."""

    base_dir = Path(project_dir).resolve() if project_dir else Path.cwd()
    if project_dir and not base_dir.exists():
        typer.secho(f"Provided project directory does not exist: {base_dir}", fg=typer.colors.RED)
        raise typer.Exit(1)

    bundle_output_dir = output_dir
    if not bundle_output_dir.is_absolute():
        bundle_output_dir = (base_dir / bundle_output_dir).resolve()
    else:
        bundle_output_dir = bundle_output_dir.resolve()

    kptn_src = kptn_source.resolve() if kptn_source else None
    if kptn_src and not kptn_src.exists():
        typer.secho(f"kptn source path does not exist: {kptn_src}", fg=typer.colors.RED)
        raise typer.Exit(1)

    project_src = project_source.resolve() if project_source else None
    if project_src and not project_src.exists():
        typer.secho(f"Project source path does not exist: {project_src}", fg=typer.colors.RED)
        raise typer.Exit(1)

    try:
        result = bundle_decider_lambda(
            project_root=base_dir,
            output_dir=bundle_output_dir,
            pipeline=pipeline,
            kptn_source=str(kptn_src) if kptn_src else None,
            project_source=str(project_src) if project_src else None,
            python_version=python_version,
            python_platform=python_platform,
            install_project=install_project,
            prefer_local_kptn=prefer_local_kptn,
        )
    except BundleDeciderError as exc:
        typer.secho(f"Failed to build decider bundle: {exc}", fg=typer.colors.RED)
        raise typer.Exit(1) from exc

    suffix = f" (pipeline {result.pipeline_name})" if result.pipeline_name else ""
    typer.echo(
        f"Decider bundle written to {result.bundle_dir}{suffix}"
    )


@app.command()
def ls(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration")
):
    """Display tasks from kptn.yaml alongside their language."""

    def _render_tasks_table() -> None:
        kap_conf = read_config()
        tasks = kap_conf.get("tasks", {})
        if not tasks:
            typer.echo("No tasks found in kptn.yaml.")
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
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration"),
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


@lineage_app.command("columns")
def lineage_columns(
    table: str = typer.Argument(..., help="Table name to inspect (e.g., fruit_metrics)"),
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration"),
    dialect: Optional[str] = typer.Option(None, "--dialect", help="SQL dialect to use for parsing (defaults to inferred value)"),
):
    """List the projected columns and upstream dependencies for a SQL-backed table."""

    def _render_columns() -> None:
        try:
            kap_conf = read_config()
        except FileNotFoundError as exc:
            typer.echo(str(exc))
            raise typer.Exit(1)

        resolved_dialect = _infer_lineage_dialect(kap_conf, dialect)
        analyzer = SqlLineageAnalyzer(kap_conf, Path.cwd(), dialect=resolved_dialect)

        try:
            analyzer.build()
        except (SqlLineageError, FileNotFoundError) as exc:
            typer.echo(f"Failed to build SQL lineage: {exc}")
            raise typer.Exit(1)

        try:
            metadata = analyzer.describe_table(table)
        except KeyError as exc:
            typer.echo(str(exc))
            raise typer.Exit(1)

        dependencies = analyzer.depends_on(metadata.display_name)

        typer.echo(f"Table: {metadata.display_name}")
        typer.echo(f"Task: {metadata.task_name}")
        typer.echo(f"SQL: {metadata.file_path}")

        if metadata.columns:
            typer.echo("Columns:")
            for column in metadata.columns:
                sources = metadata.column_sources.get(column)
                if sources:
                    typer.echo(f"  - {column}: {', '.join(sources)}")
                else:
                    typer.echo(f"  - {column}")
        else:
            typer.echo("Columns: <unknown>")

        if dependencies:
            typer.echo("Depends on:")
            for dep in dependencies:
                typer.echo(f"  - {dep}")
        else:
            typer.echo("Depends on: <none>")

    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            _render_columns()
        finally:
            os.chdir(original_dir)
    else:
        _render_columns()


@lineage_app.command("visualize")
def lineage_visualize(
    output: Path = typer.Option(Path("lineage.html"), "--output", "-o", help="Destination HTML file for the lineage visualizer"),
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kptn configuration"),
    dialect: Optional[str] = typer.Option(None, "--dialect", help="SQL dialect to use for parsing (defaults to inferred value)"),
    graph: Optional[str] = typer.Option(None, "--graph", "-g", help="Graph name used to order tasks in the visualization (defaults to first graph)"),
):
    """Generate an interactive column-lineage HTML visualization."""

    def _generate_visualizer() -> None:
        try:
            kap_conf = read_config()
        except FileNotFoundError as exc:
            typer.echo(str(exc))
            raise typer.Exit(1)

        resolved_dialect = _infer_lineage_dialect(kap_conf, dialect)
        analyzer = SqlLineageAnalyzer(kap_conf, Path.cwd(), dialect=resolved_dialect)

        try:
            analyzer.build()
        except (SqlLineageError, FileNotFoundError) as exc:
            typer.echo(f"Failed to build SQL lineage: {exc}")
            raise typer.Exit(1)

        tasks_conf = kap_conf.get("tasks", {}) if isinstance(kap_conf, dict) else {}
        task_order = None
        try:
            task_order = _task_order_from_graph(kap_conf, graph)
        except ValueError as exc:
            typer.echo(str(exc))
            raise typer.Exit(1)
        if not task_order and isinstance(tasks_conf, dict):
            task_order = list(tasks_conf.keys())
        tables_payload, lineage_payload = _build_lineage_payload(
            analyzer,
            task_order=task_order,
            tasks_config=tasks_conf if isinstance(tasks_conf, dict) else None,
        )

        html = render_lineage_html(tables_payload, lineage_payload)
        output_path = output if output.is_absolute() else Path.cwd() / output
        output_path.write_text(html, encoding="utf-8")
        typer.echo(f"Lineage visualization written to {output_path}")

    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            _generate_visualizer()
        finally:
            os.chdir(original_dir)
    else:
        _generate_visualizer()


if __name__ == "__main__":
    app()
