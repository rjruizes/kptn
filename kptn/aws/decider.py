from __future__ import annotations

from typing import Any, Mapping

from kptn.caching.TaskStateCache import TaskStateCache, TaskSubmissionDecision
from kptn.util.pipeline_config import PipelineConfig


def _normalise_task_list(task_list: Any) -> set[str]:
    """Convert the incoming task list payload to a set of task names."""
    if task_list is None or task_list == "":
        return set()
    if isinstance(task_list, str):
        items = [item.strip() for item in task_list.split(",") if item and item.strip()]
        return set(items)
    if isinstance(task_list, Mapping):
        return {str(key) for key, selected in task_list.items() if selected}
    try:
        return {str(item) for item in task_list}
    except TypeError:
        raise TypeError(f"Unsupported task_list type: {type(task_list)!r}")


def _build_pipeline_config(event: Mapping[str, Any]) -> PipelineConfig:
    """Construct a PipelineConfig from the Lambda event payload."""
    base_kwargs: dict[str, Any] = {
        "TASKS_CONFIG_PATH": event.get("TASKS_CONFIG_PATH"),
        "PIPELINE_NAME": event.get("PIPELINE_NAME"),
    }
    if not base_kwargs["TASKS_CONFIG_PATH"]:
        raise ValueError("TASKS_CONFIG_PATH is required")
    if not base_kwargs["PIPELINE_NAME"]:
        raise ValueError("PIPELINE_NAME is required")

    overrides = event.get("pipeline_config") or {}
    if not isinstance(overrides, Mapping):
        raise TypeError("pipeline_config must be a mapping if provided")

    base_kwargs.update(overrides)
    return PipelineConfig(**base_kwargs)


def decide_task_execution(
    *,
    event: Mapping[str, Any],
    db_client=None,
) -> dict[str, Any]:
    """
    Shared decision engine for the Decider Lambda and local runners.

    Returns a JSON-serialisable dictionary containing `task_name`, `should_run`,
    and optional metadata such as `reason` and `array_size`.
    """
    event_dict = dict(event)
    state_payload = event_dict.pop("state", None)
    if isinstance(state_payload, Mapping):
        merged_event = {**state_payload, **event_dict}
    else:
        merged_event = event_dict

    task_name = merged_event.get("task_name")
    if not task_name:
        raise ValueError("task_name is required")

    task_list = _normalise_task_list(merged_event.get("task_list"))
    if task_list and task_name not in task_list:
        return {
            "task_name": task_name,
            "should_run": False,
            "reason": "Task not selected",
        }

    pipeline_config = _build_pipeline_config(merged_event)
    parameters = merged_event.get("parameters") or {}
    ignore_cache = bool(merged_event.get("ignore_cache"))

    tscache = TaskStateCache(
        pipeline_config,
        db_client=db_client,
    )

    decision: TaskSubmissionDecision = tscache.evaluate_submission(
        task_name,
        parameters,
        ignore_cache,
    )

    response: dict[str, Any] = {
        "task_name": task_name,
        "should_run": decision.should_run,
    }

    if decision.reason:
        response["reason"] = decision.reason

    if decision.should_run and tscache.is_mapped_task(task_name):
        map_over_count = tscache.get_map_over_count(task_name)
        if map_over_count is not None:
            response["array_size"] = map_over_count

    execution_mode = merged_event.get("execution_mode")
    if execution_mode:
        response["execution_mode"] = execution_mode

    return response


def handler(event, context=None):
    """AWS Lambda handler."""
    return decide_task_execution(event=event)
