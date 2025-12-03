from __future__ import annotations

import json
from collections import defaultdict, deque
from typing import Any, Iterable, Mapping

DEFAULT_STEP_FUNCTION_RESOURCE_ARN = "arn:aws:states:::ecs:runTask.sync"
DEFAULT_BATCH_RESOURCE_ARN = "arn:aws:states:::batch:submitJob.sync"


def _normalize_dependencies(dependencies: Any) -> list[str]:
    """Normalize task dependency declarations into a list of task names."""
    if dependencies is None:
        return []
    if isinstance(dependencies, str):
        value = dependencies.strip()
        return [value] if value else []
    return [dep for dep in dependencies if dep]


def topological_sort(task_order: Iterable[str], deps_lookup: Mapping[str, Any]) -> list[str]:
    """Return a stable topological ordering for the provided task graph."""
    order_list = list(task_order)
    indegree: dict[str, int] = {task: 0 for task in order_list}
    adjacency: dict[str, list[str]] = defaultdict(list)

    for task in order_list:
        for dep in _normalize_dependencies(deps_lookup.get(task)):
            if dep not in indegree:
                raise ValueError(f"Task '{task}' depends on unknown task '{dep}'")
            indegree[task] += 1
            adjacency[dep].append(task)

    ready = deque([task for task in order_list if indegree[task] == 0])
    ordered: list[str] = []

    while ready:
        current = ready.popleft()
        ordered.append(current)
        for child in adjacency.get(current, []):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)

    if len(ordered) != len(order_list):
        missing = [task for task in order_list if task not in ordered]
        raise ValueError(
            "Cyclic dependency detected while ordering tasks: "
            + ", ".join(missing)
        )

    return ordered


def _construct_execution_lanes(
    ordered_tasks: list[str],
    deps_lookup: Mapping[str, Any],
) -> list[list[str]]:
    """
    Partition tasks into execution lanes based on dependency readiness.

    Each lane contains the tasks whose dependencies are fully satisfied by the
    lanes computed before it. Tasks that land in the same lane can execute in
    parallel because they have no unmet prerequisites within the lane.
    """
    processed: set[str] = set()
    total_tasks = len(ordered_tasks)
    lanes: list[list[str]] = []

    while len(processed) < total_tasks:
        lane: list[str] = []
        for task in ordered_tasks:
            if task in processed:
                continue
            deps = _normalize_dependencies(deps_lookup.get(task))
            if all(dep in processed for dep in deps):
                lane.append(task)

        if not lane:
            unresolved = [task for task in ordered_tasks if task not in processed]
            raise ValueError(
                "Unable to build execution lanes; unresolved dependencies for: "
                + ", ".join(unresolved)
            )

        lanes.append(lane)
        processed.update(lane)

    return lanes


def _state_name(task_name: str, suffix: str) -> str:
    """Generate a unique Step Functions state name for a task and suffix."""
    return f"{task_name}_{suffix}"


def _task_execution_mode(task_config: Mapping[str, Any]) -> str:
    """
    Determine the default execution mode for a task.

    Prefers explicit configuration via `execution.mode`, otherwise falls back to
    inferred rules (mapped tasks default to batch array jobs).
    """
    execution_cfg = task_config.get("execution")
    if isinstance(execution_cfg, Mapping):
        mode = execution_cfg.get("mode")
        if isinstance(mode, str) and mode:
            return mode
    if task_config.get("map_over"):
        return "batch_array"
    return "ecs"


def _build_ecs_parameters(pipeline_name: str, task_name: str) -> dict[str, Any]:
    """Construct the ECS task parameters shared across generated states."""
    environment = [
        {"Name": "KAPTEN_PIPELINE", "Value": pipeline_name},
        {"Name": "KAPTEN_TASK", "Value": task_name},
        {"Name": "DYNAMODB_TABLE_NAME", "Value": "${dynamodb_table_name}"},
        {"Name": "KAPTEN_DECISION_REASON", "Value.$": "States.Format('{}', $.last_decision.Payload.reason)"},
    ]

    return {
        "Cluster": "${ecs_cluster_arn}",
        "TaskDefinition": "${ecs_task_definition_arn}",
        "LaunchType": "${launch_type}",
        "NetworkConfiguration": {
            "AwsvpcConfiguration": {
                "AssignPublicIp": "${assign_public_ip}",
                "Subnets": "${subnet_ids}",
                "SecurityGroups": "${security_group_ids}",
            }
        },
        "Overrides": {
            "ContainerOverrides": [
                {
                    "Name": "${container_name}",
                    "Environment": environment,
                }
            ]
        },
        "EnableExecuteCommand": True,
        "Tags": [
            {"Key": "KaptenPipeline", "Value": pipeline_name},
            {"Key": "KaptenTask", "Value": task_name},
        ],
    }


def _build_batch_parameters(pipeline_name: str, task_name: str) -> dict[str, Any]:
    """Construct AWS Batch task parameters for mapped tasks."""
    environment = [
        {"Name": "KAPTEN_PIPELINE", "Value": pipeline_name},
        {"Name": "KAPTEN_TASK", "Value": task_name},
        {"Name": "DYNAMODB_TABLE_NAME", "Value": "${dynamodb_table_name}"},
        {"Name": "ARRAY_SIZE", "Value.$": "States.Format('{}', $.last_decision.Payload.array_size)"},
        {"Name": "KAPTEN_DECISION_REASON", "Value.$": "States.Format('{}', $.last_decision.Payload.reason)"},
    ]

    return {
        "JobName.$": f"States.Format('{pipeline_name}-{task_name}-{{}}', $$.Execution.Name)",
        "JobQueue": "${batch_job_queue_arn}",
        "JobDefinition": "${batch_job_definition_arn}",
        "ArrayProperties": {
            "Size.$": "$.last_decision.Payload.array_size",
        },
        "ContainerOverrides": {
            "Environment": environment,
        },
        "Tags": {
            "KaptenPipeline": pipeline_name,
            "KaptenTask": task_name,
        },
    }


def _build_task_state_chain(
    pipeline_name: str,
    task_name: str,
    task_config: Mapping[str, Any],
    resource_arn: str,
    next_state: str | None,
    *,
    end: bool,
    decider_lambda_arn: str,
    tasks_config_path: str,
) -> tuple[str, dict[str, Any]]:
    """
    Build the collection of states needed to evaluate and execute a pipeline task.

    Returns a tuple of (start_state_name, states_dict).
    """
    execution_mode_default = _task_execution_mode(task_config)
    supports_batch = execution_mode_default == "batch_array"

    decide_state_name = _state_name(task_name, "Decide")
    choice_state_name = _state_name(task_name, "Choice")
    skip_state_name = _state_name(task_name, "Skip")
    ecs_state_name = _state_name(task_name, "RunEcs")
    batch_state_name = _state_name(task_name, "RunBatch")

    states: dict[str, Any] = {}

    states[decide_state_name] = {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
            "FunctionName": decider_lambda_arn,
            "Payload": {
                "state.$": "$",
                "task_name": task_name,
                "task_list.$": "$.tasks",
                "ignore_cache.$": "$.force",
                "execution_mode": execution_mode_default,
                "TASKS_CONFIG_PATH": tasks_config_path,
                "PIPELINE_NAME": pipeline_name,
            },
        },
        "ResultSelector": {
            "Payload.$": "$.Payload",
        },
        "ResultPath": "$.last_decision",
        "OutputPath": "$",
        "Next": choice_state_name,
    }

    choice_state: dict[str, Any] = {
        "Type": "Choice",
        "Default": skip_state_name,
        "Choices": [],
    }

    if supports_batch:
        choice_state["Choices"].append(
            {
                "And": [
                    {"Variable": "$.last_decision.Payload.should_run", "BooleanEquals": True},
                    {"Variable": "$.last_decision.Payload.execution_mode", "StringEquals": "batch_array"},
                    {"Variable": "$.last_decision.Payload.array_size", "NumericGreaterThan": 0},
                ],
                "Next": batch_state_name,
            }
        )

    if not supports_batch:
        ecs_condition: dict[str, Any] = {
            "And": [
                {"Variable": "$.last_decision.Payload.should_run", "BooleanEquals": True},
                {
                    "Or": [
                        {"Variable": "$.last_decision.Payload.execution_mode", "StringEquals": "ecs"},
                        {"Not": {"Variable": "$.last_decision.Payload.execution_mode", "IsPresent": True}},
                    ]
                },
            ],
            "Next": ecs_state_name,
        }
        choice_state["Choices"].append(ecs_condition)
    states[choice_state_name] = choice_state

    skip_state: dict[str, Any] = {
        "Type": "Pass",
    }
    if next_state:
        skip_state["Next"] = next_state
    elif end:
        skip_state["End"] = True
    else:
        skip_state["End"] = True
    states[skip_state_name] = skip_state

    if supports_batch:
        batch_state: dict[str, Any] = {
            "Type": "Task",
            "Resource": DEFAULT_BATCH_RESOURCE_ARN,
            "Parameters": _build_batch_parameters(pipeline_name, task_name),
            "ResultPath": None,
        }
        if next_state:
            batch_state["Next"] = next_state
        elif end:
            batch_state["End"] = True
        else:
            batch_state["End"] = True
        states[batch_state_name] = batch_state
    else:
        ecs_state: dict[str, Any] = {
            "Type": "Task",
            "Resource": resource_arn,
            "Parameters": _build_ecs_parameters(pipeline_name, task_name),
            "ResultPath": None,
        }
        if next_state:
            ecs_state["Next"] = next_state
        elif end:
            ecs_state["End"] = True
        else:
            ecs_state["End"] = True
        states[ecs_state_name] = ecs_state

    return decide_state_name, states


def build_state_machine_definition(
    pipeline_name: str,
    deps_lookup: Mapping[str, Any],
    *,
    tasks: Mapping[str, Any],
    task_order: Iterable[str],
    resource_arn: str | None = None,
    decider_lambda_arn: str | None = None,
    tasks_config_path: str = "kptn.yaml",
) -> dict[str, Any]:
    """Produce a Step Functions state machine definition for the pipeline graph."""
    ordered_tasks = topological_sort(task_order, deps_lookup)
    effective_resource = resource_arn or DEFAULT_STEP_FUNCTION_RESOURCE_ARN
    decider_resource = decider_lambda_arn or "${decider_lambda_arn}"
    start_name_lookup = {task: _state_name(task, "Decide") for task in ordered_tasks}
    lanes = _construct_execution_lanes(ordered_tasks, deps_lookup)

    if not lanes:
        raise ValueError("Cannot build state machine definition for empty task list")

    stage_metadata: list[dict[str, Any]] = []
    for lane_index, lane_tasks in enumerate(lanes):
        if not lane_tasks:
            continue
        if len(lane_tasks) == 1:
            stage_metadata.append(
                {
                    "type": "serial",
                    "tasks": lane_tasks,
                    "start_state": start_name_lookup[lane_tasks[0]],
                }
            )
            continue

        stage_metadata.append(
            {
                "type": "parallel",
                "tasks": lane_tasks,
                "state_name": f"Lane{lane_index}Parallel",
                "start_state": f"Lane{lane_index}Parallel",
            }
        )

    states: dict[str, Any] = {}

    for index, stage in enumerate(stage_metadata):
        if index + 1 < len(stage_metadata):
            next_stage_start = stage_metadata[index + 1]["start_state"]
        else:
            next_stage_start = None

        if stage["type"] == "parallel":
            parallel_branches = []
            for task in stage["tasks"]:
                branch_start, branch_states = _build_task_state_chain(
                    pipeline_name,
                    task,
                    tasks.get(task, {}),
                    effective_resource,
                    None,
                    end=True,
                    decider_lambda_arn=decider_resource,
                    tasks_config_path=tasks_config_path,
                )
                parallel_branches.append(
                    {
                        "StartAt": branch_start,
                        "States": branch_states,
                    }
                )

            parallel_state: dict[str, Any] = {
                "Type": "Parallel",
                "Branches": parallel_branches,
                # Keep the overall state as an object by nesting the parallel output.
                "ResultPath": f"$.Lane{lane_index}Parallel",
            }
            if next_stage_start:
                parallel_state["Next"] = next_stage_start
            else:
                parallel_state["End"] = True

            states[stage["state_name"]] = parallel_state
            continue

        task = stage["tasks"][0]
        start_name, task_states = _build_task_state_chain(
            pipeline_name,
            task,
            tasks.get(task, {}),
            effective_resource,
            next_stage_start,
            end=next_stage_start is None,
            decider_lambda_arn=decider_resource,
            tasks_config_path=tasks_config_path,
        )
        states.update(task_states)

    state_machine = {
        "Comment": f"kptn generated state machine for {pipeline_name}",
        "StartAt": stage_metadata[0]["start_state"],
        "States": states,
    }
    return state_machine


def build_stepfunctions_flow_context(
    *,
    pipeline_name: str,
    task_names: list[str],
    deps_lookup: Mapping[str, Any],
    tasks_dict: Mapping[str, Any],
    kap_conf: Mapping[str, Any],
) -> dict[str, Any]:
    """Assemble template context overrides for Step Functions flows."""
    resource_arn = kap_conf.get("stepfunctions_resource_arn")
    decider_lambda_arn = kap_conf.get("decider_lambda_arn", "${decider_lambda_arn}")
    tasks_config_path = "kptn.yaml"

    state_machine = build_state_machine_definition(
        pipeline_name,
        deps_lookup,
        tasks=tasks_dict,
        task_order=task_names,
        resource_arn=resource_arn,
        decider_lambda_arn=decider_lambda_arn,
        tasks_config_path=tasks_config_path,
    )
    json_definition = json.dumps(state_machine, indent=2)
    json_definition = json_definition.replace('"${subnet_ids}"', '${subnet_ids}')
    json_definition = json_definition.replace('"${security_group_ids}"', '${security_group_ids}')
    return {
        "state_machine": state_machine,
        "step_function_resource_arn": resource_arn or DEFAULT_STEP_FUNCTION_RESOURCE_ARN,
        "state_machine_json": json_definition,
        "lambda_tasks_config_path": tasks_config_path,
    }
