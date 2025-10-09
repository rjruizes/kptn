from __future__ import annotations

import json
from collections import defaultdict, deque
from typing import Any, Iterable, Mapping

DEFAULT_STEP_FUNCTION_RESOURCE_ARN = "arn:aws:states:::ecs:runTask.sync"


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


def _build_branch_sequences(
    ordered_tasks: list[str],
    deps_lookup: Mapping[str, Any],
) -> tuple[list[list[str]], set[str]]:
    """Group root-to-leaf chains that qualify for parallel execution."""
    branches: list[list[str]] = []
    assigned: set[str] = set()

    for task in ordered_tasks:
        deps = _normalize_dependencies(deps_lookup.get(task))
        if deps:
            continue

        branch = [task]
        assigned.add(task)
        changed = True
        while changed:
            changed = False
            for candidate in ordered_tasks:
                if candidate in assigned:
                    continue
                candidate_deps = _normalize_dependencies(deps_lookup.get(candidate))
                if candidate_deps and all(dep in branch for dep in candidate_deps):
                    branch.append(candidate)
                    assigned.add(candidate)
                    changed = True
        branches.append(branch)

    return branches, assigned


def _make_task_state(
    pipeline_name: str,
    task_name: str,
    resource_arn: str,
    next_state: str | None,
    *,
    end: bool,
) -> dict[str, Any]:
    parameters: dict[str, Any] = {
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
                    "Environment": [
                        {"Name": "KAPTEN_PIPELINE", "Value": pipeline_name},
                        {"Name": "KAPTEN_TASK", "Value": task_name},
                        {"Name": "DYNAMODB_TABLE_NAME", "Value": "${dynamodb_table_name}"},
                    ],
                }
            ]
        },
        "EnableExecuteCommand": True,
        "Tags": [
            {"Key": "KaptenPipeline", "Value": pipeline_name},
            {"Key": "KaptenTask", "Value": task_name},
        ],
    }

    state: dict[str, Any] = {
        "Type": "Task",
        "Resource": resource_arn,
        "Parameters": parameters,
        "ResultPath": None,
    }
    if next_state:
        state["Next"] = next_state
    elif end:
        state["End"] = True
    else:
        state["End"] = True
    return state


def build_state_machine_definition(
    pipeline_name: str,
    deps_lookup: Mapping[str, Any],
    *,
    task_order: Iterable[str],
    resource_arn: str | None = None,
) -> dict[str, Any]:
    """Produce a Step Functions state machine definition for the pipeline graph."""
    ordered_tasks = topological_sort(task_order, deps_lookup)
    branches, assigned = _build_branch_sequences(ordered_tasks, deps_lookup)
    unassigned = [task for task in ordered_tasks if task not in assigned]
    active_branches = [branch for branch in branches if branch]
    has_parallel = len(active_branches) > 1

    effective_resource = resource_arn or DEFAULT_STEP_FUNCTION_RESOURCE_ARN

    states: dict[str, Any] = {}

    if has_parallel:
        parallel_branches = []
        for branch in active_branches:
            branch_states: dict[str, Any] = {}
            for index, task in enumerate(branch):
                next_state = branch[index + 1] if index + 1 < len(branch) else None
                branch_states[task] = _make_task_state(
                    pipeline_name,
                    task,
                    effective_resource,
                    next_state,
                    end=next_state is None,
                )
            parallel_branches.append(
                {
                    "StartAt": branch[0],
                    "States": branch_states,
                }
            )

        states["ParallelRoot"] = {
            "Type": "Parallel",
            "Branches": parallel_branches,
        }
        if unassigned:
            states["ParallelRoot"]["Next"] = unassigned[0]
        else:
            states["ParallelRoot"]["End"] = True
    elif active_branches:
        branch = active_branches[0]
        for index, task in enumerate(branch):
            if index + 1 < len(branch):
                next_state = branch[index + 1]
            elif unassigned:
                next_state = unassigned[0]
            else:
                next_state = None
            states[task] = _make_task_state(
                pipeline_name,
                task,
                effective_resource,
                next_state,
                end=next_state is None,
            )

    for index, task in enumerate(unassigned):
        next_state = unassigned[index + 1] if index + 1 < len(unassigned) else None
        states[task] = _make_task_state(
            pipeline_name,
            task,
            effective_resource,
            next_state,
            end=next_state is None,
        )

    if has_parallel:
        start_state = "ParallelRoot"
    elif active_branches:
        start_state = active_branches[0][0]
    elif unassigned:
        start_state = unassigned[0]
    elif ordered_tasks:
        start_state = ordered_tasks[0]
    else:
        raise ValueError("Cannot build state machine definition for empty task list")

    state_machine = {
        "Comment": f"Kapten generated state machine for {pipeline_name}",
        "StartAt": start_state,
        "States": states,
    }
    return state_machine


def build_stepfunctions_flow_context(
    *,
    pipeline_name: str,
    task_names: list[str],
    deps_lookup: Mapping[str, Any],
    kap_conf: Mapping[str, Any],
) -> dict[str, Any]:
    """Assemble template context overrides for Step Functions flows."""
    resource_arn = kap_conf.get("stepfunctions-resource-arn")
    state_machine = build_state_machine_definition(
        pipeline_name,
        deps_lookup,
        task_order=task_names,
        resource_arn=resource_arn,
    )
    json_definition = json.dumps(state_machine, indent=2)
    json_definition = json_definition.replace('"${subnet_ids}"', '${subnet_ids}')
    json_definition = json_definition.replace('"${security_group_ids}"', '${security_group_ids}')
    return {
        "state_machine": state_machine,
        "step_function_resource_arn": resource_arn or DEFAULT_STEP_FUNCTION_RESOURCE_ARN,
        "state_machine_json": json_definition,
    }
