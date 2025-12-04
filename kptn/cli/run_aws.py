from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
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
