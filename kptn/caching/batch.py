"""
AWS Batch helpers for mapped tasks.

This module isolates batch-specific behavior so vanilla runners remain focused on
local/sequential execution.
"""

from __future__ import annotations

import os
from typing import Dict, List

from kptn.caching.TaskStateCache import TaskStateCache
from kptn.caching.TSCacheUtils import fetch_cached_dep_data, get_task_partial
from kptn.caching.models import Subtask
from kptn.util.hash import hash_obj
from kptn.util.pipeline_config import PipelineConfig


def _parse_batch_index() -> int:
    """Return AWS batch array index as an int or raise a helpful error."""
    idx_value = os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")
    if idx_value is None:
        raise ValueError("AWS_BATCH_JOB_ARRAY_INDEX is not set; not running as a batch array task")
    try:
        return int(idx_value)
    except ValueError as exc:
        raise ValueError(f"Invalid AWS_BATCH_JOB_ARRAY_INDEX value: {idx_value}") from exc


def _fetch_and_hash_subtasks(tscache: TaskStateCache, task_name: str) -> str:
    """Fetch subtasks, get subtask.outputHash for each."""
    subtasks: List[Subtask] = tscache.db_client.get_subtasks(task_name)
    output_hashes = [subtask.outputHash for subtask in subtasks]
    return hash_obj(output_hashes) if output_hashes else None


def run_batch_array_subtask(
    pipeline_config: PipelineConfig,
    task_name: str,
):
    """
    Execute a single mapped task element inside an AWS Batch array job.

    Uses the AWS_BATCH_JOB_ARRAY_INDEX environment variable to select the
    corresponding element from the map_over input, executes just that subtask,
    and when all subtasks have finished marks the task as SUCCESS.
    """
    tscache = TaskStateCache(pipeline_config)
    if not tscache.is_mapped_task(task_name):
        raise ValueError(f"Task {task_name} is not a mapped task and cannot be run as a batch array subtask")

    array_index = _parse_batch_index()
    array_size_env = os.getenv("ARRAY_SIZE")

    data_args, value_list, map_over_count = fetch_cached_dep_data(tscache, task_name)
    task_size = len(value_list)
    if task_size == 0:
        raise ValueError(f"Task {task_name} has no items to map over")

    if array_index < 0 or array_index >= task_size:
        raise IndexError(f"Batch array index {array_index} out of bounds for task_size {task_size}")

    if array_size_env:
        try:
            expected_size = int(array_size_env)
            if expected_size != task_size:
                tscache.logger.warning(
                    f"ARRAY_SIZE ({expected_size}) does not match computed task_size ({task_size}) for {task_name}"
                )
        except ValueError:
            tscache.logger.warning(f"ARRAY_SIZE is not an int: {array_size_env}")

    if map_over_count and map_over_count != task_size:
        tscache.logger.warning(
            f"map_over_count ({map_over_count}) does not match task_size ({task_size}) for {task_name}"
        )

    existing_state = tscache.db_client.get_task(task_name, include_data=False, subset_mode=pipeline_config.SUBSET_MODE)
    if not existing_state:
        tscache.logger.info(f"Creating initial task state for {task_name} (batch array)")
        tscache.set_initial_state(task_name)

    subtasks = tscache.db_client.get_subtasks(task_name)
    if not subtasks:
        tscache.logger.info(f"Creating {task_size} subtasks for {task_name}")
        tscache.db_client.create_subtasks(task_name, value_list)

    # Build kwargs for this specific index; only index lists to avoid string slicing
    task_kwargs: Dict[str, object] = {}
    for key, value in data_args.items():
        if isinstance(value, list):
            task_kwargs[key] = value[array_index]
        else:
            task_kwargs[key] = value
    task_kwargs["idx"] = array_index

    tscache.logger.info(f"Running batch array subtask {array_index} of {task_size} for {task_name}")

    func = get_task_partial(tscache, pipeline_config, task_name)
    try:
        func(**task_kwargs)
    except Exception as exc:
        tscache.logger.error(f"Batch subtask {array_index} for {task_name} failed: {exc}")
        # Mark the overall task as failed so decider can rerun or surface failure
        tscache.db_client.set_task_ended(task_name, status="FAILURE")
        tscache.set_final_state(task_name, status="FAILURE")
        raise

    # If all subtasks have completed, mark the task as successful and finalize state
    updated_subtasks = tscache.db_client.get_subtasks(task_name)
    if updated_subtasks and all(subtask.endTime for subtask in updated_subtasks):
        outputs_version = _fetch_and_hash_subtasks(tscache, task_name)
        tscache.db_client.set_task_ended(task_name, status="SUCCESS", outputs_version=outputs_version)
        tscache.set_final_state(task_name, status="SUCCESS")
        tscache.logger.info(f"All {task_size} subtasks completed for {task_name}; marked SUCCESS")
    else:
        tscache.logger.info(f"Subtask {array_index} complete for {task_name}; waiting for remaining subtasks")
