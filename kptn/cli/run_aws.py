from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
import time
from typing import Any, Iterable, Mapping, Optional, Sequence

import typer

try:  # pragma: no cover - optional dependency
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, NoRegionError
except ImportError:  # pragma: no cover - boto3 is optional until this command is used
    boto3 = None
    ClientError = NoCredentialsError = NoRegionError = BotoCoreError = None  # type: ignore[misc,assignment]

from kptn.read_config import read_config


class StackInfoError(Exception):
    """Raised when stack metadata cannot be retrieved or parsed."""


def parse_tasks_arg(tasks_arg: str | None) -> list[str]:
    if tasks_arg is None or tasks_arg.strip() == "":
        return []
    tasks = [task.strip() for task in tasks_arg.split(",") if task.strip()]
    if not tasks:
        raise ValueError("At least one task name is required (comma-separated when multiple).")
    return tasks


def resolve_stack_parameter_name(pipeline: str, override: Optional[str] = None) -> str:
    env_override = os.getenv("KPTN_STACK_INFO_PARAMETER")
    if override:
        return override
    if env_override:
        return env_override
    return f"/kptn/stack/{pipeline}/info"


def create_boto_session(profile: Optional[str], region: Optional[str]):
    if boto3 is None:  # pragma: no cover - exercised only when boto3 missing
        raise StackInfoError("boto3 is required for cloud runs. Install kptn with AWS extras.")
    try:
        return boto3.Session(profile_name=profile, region_name=region)
    except Exception as exc:  # pragma: no cover - Session rarely fails
        raise StackInfoError(f"Failed to create boto3 session: {exc}") from exc


def fetch_stack_info(
    *,
    session: Any,
    parameter_name: str,
) -> dict[str, Any]:
    try:
        ssm = session.client("ssm")
        response = ssm.get_parameter(Name=parameter_name)
    except (ClientError, NoCredentialsError, NoRegionError, BotoCoreError) as exc:
        raise StackInfoError(f"Unable to read SSM parameter '{parameter_name}': {exc}") from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise StackInfoError(f"Unexpected error reading SSM '{parameter_name}': {exc}") from exc

    try:
        value = response["Parameter"]["Value"]
    except KeyError as exc:
        raise StackInfoError(f"SSM parameter '{parameter_name}' missing value") from exc

    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise StackInfoError(f"SSM parameter '{parameter_name}' does not contain valid JSON") from exc


def choose_state_machine_arn(
    stack_info: dict[str, Any],
    preferred_key: Optional[str] = None,
    pipeline: Optional[str] = None,
) -> Optional[str]:
    arns = stack_info.get("state_machine_arns") or {}
    preferred_arn = stack_info.get("state_machine_arn")

    if preferred_key:
        if preferred_key.startswith("arn:"):
            return preferred_key
        if preferred_key in arns:
            return arns[preferred_key]
        typer.echo(f"State machine '{preferred_key}' not found in stack metadata; falling back.", err=True)

    if pipeline and pipeline in arns:
        return arns[pipeline]

    if preferred_arn:
        return preferred_arn

    if isinstance(arns, dict) and arns:
        return arns.get(sorted(arns)[0])

    return None


def _load_task_compute(task_name: str) -> Mapping[str, Any] | None:
    """Return the compute block for a task from kptn.yaml, if available."""
    try:
        kap_conf = read_config()
    except FileNotFoundError:
        return None
    except Exception:
        return None

    tasks = kap_conf.get("tasks") or {}
    task_spec = tasks.get(task_name)
    if isinstance(task_spec, Mapping):
        compute_cfg = task_spec.get("compute")
        if isinstance(compute_cfg, Mapping):
            return compute_cfg
    return None


def task_execution_mode(task_name: str) -> str | None:
    """
    Return the preferred execution mode for a task using the same defaults as the
    Step Functions generator.

    Defaults to ECS when execution metadata is present but unspecified, and
    returns None if task configuration cannot be read.
    """
    try:
        kap_conf = read_config()
    except FileNotFoundError:
        return None
    except Exception:
        return None

    tasks = kap_conf.get("tasks")
    if not isinstance(tasks, Mapping):
        return None
    task_spec = tasks.get(task_name)
    if not isinstance(task_spec, Mapping):
        return None

    execution_cfg = task_spec.get("execution")
    if isinstance(execution_cfg, Mapping):
        mode = execution_cfg.get("mode")
        if isinstance(mode, str) and mode.strip():
            return mode.strip()

    if task_spec.get("map_over"):
        return "batch_array"

    return "ecs"


def start_state_machine_execution(
    *,
    session: Any,
    state_machine_arn: str,
    pipeline: str,
    tasks: Sequence[str],
    force: bool,
) -> str:
    payload = {"pipeline": pipeline, "tasks": list(tasks), "force": bool(force)}
    sfn = session.client("stepfunctions")
    response = sfn.start_execution(stateMachineArn=state_machine_arn, input=json.dumps(payload))
    return response["executionArn"]


@dataclass
class DirectRunConfig:
    launch_type: Optional[str] = None
    subnet_ids: Iterable[str] = ()
    security_group_ids: Iterable[str] = ()


def _effective_launch_type(stack_info: dict[str, Any], override: Optional[str]) -> Optional[str]:
    if override:
        return override
    launch_type = stack_info.get("ecs_launch_type")
    if isinstance(launch_type, str) and launch_type:
        return launch_type
    return None


def ecs_task_console_url(task_arn: str, cluster_arn: str | None = None) -> str | None:
    """Render an AWS console URL for a given ECS task ARN."""
    try:
        arn_parts = task_arn.split(":")
        if len(arn_parts) < 6:
            return None

        region = arn_parts[3]
        resource = arn_parts[5]
        resource_parts = resource.split("/")
        task_id = resource_parts[-1] if resource_parts else ""

        cluster_name = None
        if len(resource_parts) >= 3:
            cluster_name = resource_parts[-2]
        elif cluster_arn:
            cluster_name = cluster_arn.split("/")[-1]

        if not (region and cluster_name and task_id):
            return None

        return (
            f"https://{region}.console.aws.amazon.com/ecs/v2/clusters/"
            f"{cluster_name}/tasks/{task_id}/configuration"
        )
    except Exception:
        return None


def _extract_task_id(task_arn: str) -> str | None:
    try:
        return task_arn.split("/")[-1]
    except Exception:
        return None


def _ecs_log_configuration(
    session: Any,
    stack_info: Mapping[str, Any],
    container_name: str | None = None,
) -> tuple[str, str, str] | None:
    task_definition_arn = stack_info.get("task_definition_arn")
    if not task_definition_arn:
        return None

    ecs = session.client("ecs")
    response = ecs.describe_task_definition(taskDefinition=task_definition_arn)
    task_def = response.get("taskDefinition") or {}
    containers = task_def.get("containerDefinitions") or []
    candidate_container = container_name or stack_info.get("task_definition_container_name")

    for container in containers:
        if candidate_container and container.get("name") != candidate_container:
            continue
        log_config = container.get("logConfiguration") or {}
        if log_config.get("logDriver") != "awslogs":
            continue
        options = log_config.get("options") or {}
        log_group = options.get("awslogs-group")
        stream_prefix = options.get("awslogs-stream-prefix")
        if log_group and stream_prefix:
            return log_group, stream_prefix, container.get("name") or candidate_container or ""

    # Fallback: pick first awslogs-enabled container if a name match was not found
    for container in containers:
        log_config = container.get("logConfiguration") or {}
        if log_config.get("logDriver") != "awslogs":
            continue
        options = log_config.get("options") or {}
        log_group = options.get("awslogs-group")
        stream_prefix = options.get("awslogs-stream-prefix")
        if log_group and stream_prefix:
            return log_group, stream_prefix, container.get("name") or candidate_container or ""

    return None


def follow_ecs_task_logs(
    *,
    session: Any,
    task_arn: str,
    stack_info: Mapping[str, Any],
    poll_interval: float = 2.0,
    max_polls: int | None = None,
) -> None:
    task_id = _extract_task_id(task_arn)
    try:
        log_config = _ecs_log_configuration(session, stack_info)
    except Exception as exc:  # pragma: no cover - boto failures exercised at runtime
        typer.echo(f"Log streaming not available: {exc}", err=True)
        return
    if not task_id or not log_config:
        typer.echo("Log streaming not available: ECS task lacks awslogs configuration.", err=True)
        return

    log_group, stream_prefix, container_name = log_config
    log_stream_name = "/".join([stream_prefix, container_name, task_id])
    logs_client = session.client("logs")
    ecs_client = session.client("ecs")

    cluster_arn = stack_info.get("cluster_arn")
    cluster_identifier = cluster_arn if isinstance(cluster_arn, str) else None

    next_token: str | None = None
    stopped = False
    seen_events = False
    polls = 0

    def _is_resource_not_found(exc: Exception) -> bool:
        code = None
        try:
            code = exc.response.get("Error", {}).get("Code")
        except Exception:
            pass
        if not code:
            try:
                code = exc.args[0].get("Error", {}).get("Code")  # type: ignore[index]
            except Exception:
                pass
        return code == "ResourceNotFoundException"

    def _task_stopped() -> bool:
        if not cluster_identifier:
            return False
        try:
            resp = ecs_client.describe_tasks(cluster=cluster_identifier, tasks=[task_arn])
            tasks = resp.get("tasks") or []
            if tasks:
                status = tasks[0].get("lastStatus")
                return status == "STOPPED"
        except Exception:
            return False
        return False

    while True:
        polls += 1
        params: dict[str, Any] = {
            "logGroupName": log_group,
            "logStreamName": log_stream_name,
            "startFromHead": True,
        }
        if next_token:
            params["nextToken"] = next_token

        try:
            response = logs_client.get_log_events(**params)
        except Exception as exc:
            if _is_resource_not_found(exc):
                if not stopped:
                    stopped = _task_stopped()
                if stopped:
                    typer.echo("Log stream not found for task; task may have stopped before producing logs.", err=True)
                    break
                time.sleep(poll_interval)
                continue
            typer.echo(f"Failed to fetch log events: {exc}", err=True)
            return

        events = response.get("events") or []
        for event in events:
            message = event.get("message")
            if message is not None:
                typer.echo(message)
                seen_events = True

        new_token = response.get("nextForwardToken")
        token_stable = new_token == next_token
        next_token = new_token or next_token

        if not stopped:
            stopped = _task_stopped()

        if (stopped and token_stable) or (max_polls is not None and polls >= max_polls):
            break

        time.sleep(poll_interval)

    if not seen_events:
        typer.echo("No log events were available for this task.", err=True)


def run_ecs_task(
    *,
    session: Any,
    stack_info: dict[str, Any],
    pipeline: str,
    task: str,
    config: DirectRunConfig,
    compute: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cluster_arn = stack_info.get("cluster_arn")
    task_definition_arn = stack_info.get("task_definition_arn")
    container_name = stack_info.get("task_definition_container_name")

    if not cluster_arn or not task_definition_arn:
        raise StackInfoError("Stack metadata is missing ECS cluster or task definition ARN.")

    subnets = list(config.subnet_ids) or stack_info.get("subnet_ids") or []
    security_groups = list(config.security_group_ids) or stack_info.get("security_group_ids") or []
    assign_public_ip = "ENABLED" if stack_info.get("assign_public_ip") else "DISABLED"
    launch_type = _effective_launch_type(stack_info, config.launch_type)

    network_config = {
        "awsvpcConfiguration": {
            "subnets": subnets,
            "securityGroups": security_groups,
            "assignPublicIp": assign_public_ip,
        }
    }

    env_overrides = [
        {"name": "KAPTEN_PIPELINE", "value": pipeline},
        {"name": "KAPTEN_TASK", "value": task},
    ]
    if stack_info.get("dynamodb_table_name"):
        env_overrides.append({"name": "DYNAMODB_TABLE_NAME", "value": stack_info["dynamodb_table_name"]})

    def _coerce_int(value: Any) -> int | None:
        try:
            return int(str(value).strip())
        except Exception:
            return None

    overrides: dict[str, Any] = {}

    if isinstance(compute, Mapping):
        cpu = _coerce_int(compute.get("cpu"))
        memory = _coerce_int(compute.get("memory"))
        if cpu is not None:
            overrides["cpu"] = str(cpu)
        if memory is not None:
            overrides["memory"] = str(memory)

    container_overrides = []
    if container_name:
        container_override: dict[str, Any] = {
            "name": container_name,
            "environment": env_overrides,
        }
        if isinstance(compute, Mapping):
            cpu = _coerce_int(compute.get("cpu"))
            memory = _coerce_int(compute.get("memory"))
            if cpu is not None:
                container_override["cpu"] = cpu
            if memory is not None:
                container_override["memory"] = memory
        container_overrides.append(container_override)

    ecs = session.client("ecs")
    response = ecs.run_task(
        cluster=cluster_arn,
        taskDefinition=task_definition_arn,
        count=1,
        launchType=launch_type,
        networkConfiguration=network_config if subnets or security_groups else {},
        overrides={**overrides, **({"containerOverrides": container_overrides} if container_overrides else {})}
        if overrides or container_overrides
        else {},
        enableExecuteCommand=True,
    )
    return response


def submit_batch_job(
    *,
    session: Any,
    stack_info: dict[str, Any],
    pipeline: str,
    task: str,
    resource_requirements: Sequence[Mapping[str, str]] | None = None,
    array_size: int | None = None,
    decision_reason: str | None = None,
) -> dict[str, Any]:
    queue_arn = stack_info.get("batch_job_queue_arn")
    job_definition_arn = stack_info.get("batch_job_definition_arn")
    if not queue_arn or not job_definition_arn:
        raise StackInfoError("Stack metadata is missing Batch queue or job definition ARN.")

    job_name = f"{pipeline}-{task}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    environment = [
        {"name": "KAPTEN_PIPELINE", "value": pipeline},
        {"name": "KAPTEN_TASK", "value": task},
    ]
    if stack_info.get("dynamodb_table_name"):
        environment.append({"name": "DYNAMODB_TABLE_NAME", "value": stack_info["dynamodb_table_name"]})
    if array_size:
        try:
            coerced = int(array_size)
            if coerced > 0:
                environment.append({"name": "ARRAY_SIZE", "value": str(coerced)})
                array_size = coerced
        except Exception:
            pass
    if decision_reason:
        environment.append({"name": "KAPTEN_DECISION_REASON", "value": decision_reason})

    container_overrides: dict[str, Any] = {"environment": environment}
    if resource_requirements:
        container_overrides["resourceRequirements"] = [
            {"type": requirement["type"], "value": requirement["value"]} for requirement in resource_requirements
        ]

    batch = session.client("batch")
    params: dict[str, Any] = {
        "jobName": job_name,
        "jobQueue": queue_arn,
        "jobDefinition": job_definition_arn,
        "containerOverrides": container_overrides,
        "propagateTags": True,
    }
    if array_size:
        params["arrayProperties"] = {"size": array_size}

    response = batch.submit_job(**params)
    return response


def run_local(pipeline: str, tasks: Sequence[str], force: bool) -> None:
    # Placeholder for future local execution wiring. Keep an explicit failure for now.
    raise StackInfoError(
        "Local execution is not implemented yet for 'kptn run --local'. "
        "Use cloud mode or implement a local runner hook."
    )
