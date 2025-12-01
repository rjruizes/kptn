"""
Vanilla Python implementation for task execution in kptn pipelines.

This module provides sequential task execution capabilities without relying on Prefect.
All tasks are executed sequentially without parallelism or retries.

Key functions:
- run_task_vanilla: Main entry point for running tasks in vanilla mode
- map_task_vanilla: Handles mapped tasks sequentially
"""

from kptn.caching.models import Subtask
from kptn.caching.TaskStateCache import TaskStateCache
from kptn.caching.TSCacheUtils import fetch_cached_dep_data, get_task_partial, run_single_task
from kptn.util.logger import get_logger
from kptn.util.pipeline_config import PipelineConfig
from kptn.util.hash import hash_obj
import functools
import os
import time
from typing import List, Dict, Any


def check_overall_status(statuses: List[str]) -> str:
    """Determine the overall status of a list of statuses"""
    total = len(statuses)
    success = sum(status == "SUCCESS" for status in statuses)
    if success == total:
        return "SUCCESS"
    elif success == 0:
        return "FAILURE"
    else:
        return "INCOMPLETE"


def check_results_success(results: List[bool]) -> str:
    """Determines if all, some, or no elements in a list of results are successful"""
    total = len(results)
    success = sum(results)
    if success == total:
        return "SUCCESS"
    elif success == 0:
        return "FAILURE"
    else:
        return "INCOMPLETE"


def fetch_and_hash_subtasks(tscache: TaskStateCache, task_name: str) -> str:
    """Fetch subtasks, get subtask.outputHash for each"""
    subtasks: List[Subtask] = tscache.db_client.get_subtasks(task_name)
    start = time.time()
    output_hashes = [subtask.outputHash for subtask in subtasks]
    outputs_version = hash_obj(output_hashes) if output_hashes else None
    tscache.logger.info(f"Composite hash took {time.time() - start} seconds")
    return outputs_version


def map_task_vanilla(
    pipeline_config: PipelineConfig, 
    task_name: str, 
    **kwargs
):
    """Maps a script/function over its data_args (dependency data) sequentially"""
    tscache = TaskStateCache(pipeline_config)
    task_obj = tscache.get_task(task_name)
    data_args, value_list, _ = fetch_cached_dep_data(tscache, task_name)
    tscache.set_initial_state(task_name)
    index = list(range(len(value_list)))
    data_args["idx"] = index
    
    if pipeline_config.SUBSET_MODE:
        tscache.db_client.reset_subset_of_subtasks(task_name, value_list)
    else:
        subtasks = tscache.db_client.get_subtasks(task_name)
        if subtasks:
            # Reduce the list of subtasks to only the ones that haven't finished
            incomplete_subtasks = [subtask for subtask in subtasks if not subtask.endTime]
            tscache.logger.info(f"Subtasks found for {task_name}; Incomplete subtasks: {len(incomplete_subtasks)}")
            data_args["idx"] = [subtask.i for subtask in incomplete_subtasks]
            map_over_key = tscache.get_map_over_key(task_name)
            # If the task is mapped over multiple keys, assign each to data_args
            if "," in map_over_key:
                keys = map_over_key.split(",")
                tscache.logger.info(f"map_over_keys: {keys}")
                for i, key in enumerate(keys):
                    data_args[key] = [subtask.key.split(",")[i] for subtask in incomplete_subtasks]
                    tscache.logger.info(f"Setting {key} to {data_args[key]}")
            else:
                data_args[map_over_key] = [subtask.key for subtask in incomplete_subtasks]
                tscache.logger.info(f"Setting {map_over_key} to {data_args[map_over_key]}")
        else:
            tscache.logger.info(f"Creating fresh subtasks for {task_name}")
            tscache.db_client.create_subtasks(task_name, value_list)

    def split_data_args(data_args: Dict[str, List], size: int = 10) -> List[Dict[str, List]]:
        """
        Transform a data_args dictionary into a list of dictionaries with `size` elements
        e.g. if size = 5
        {"idx": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]} -> [{"idx": [1, 2, 3, 4, 5]}, {"idx": [6, 7, 8, 9, 10]}]
        """
        return [{key: data_args[key][i:i+size] for key in data_args} for i in range(0, len(data_args["idx"]), size)]

    def split_data_args_groups(data_args: Dict[str, List], size: int = 10) -> List[Dict[str, List]]:
        """
        If input is a regular data_args dictionary, split it into groups of size elements;
        If input is a bundled data_args dictionary, group the bundles into groups of size elements
        """
        if "idx" in data_args:
            return split_data_args(data_args, size)
        else:
            bundles = data_args["data_args_bundle"]
            return [{"data_args_bundle": bundles[i:i+size]} for i in range(0, len(bundles), size)]

    def loop_over_bundled_data_args(data_args_bundle: Dict[str, List], inner_func):
        """
        Loop over a data_args bundle and call the inner function with the data_args
        """
        logger = get_logger()
        logger.info(f"Looping over task {task_name} with data_args_bundle {data_args_bundle}")
        errors = []
        for i in range(len(data_args_bundle["idx"])):
            data_args = {key: data_args_bundle[key][i] for key in data_args_bundle}
            try:
                inner_func(**data_args)
            except Exception as e:
                errors.append(str(e))
        if errors:
            raise Exception("\n".join(errors))

    def execute_task_wrapper(func, task_args):
        """Wrapper to execute a task function with error handling"""
        try:
            if isinstance(task_args, dict) and "data_args_bundle" in task_args:
                loop_over_bundled_data_args(task_args["data_args_bundle"], func)
            else:
                func(**task_args, **kwargs)
            return True
        except Exception as e:
            tscache.logger.error(f"Task execution failed: {e}")
            return False

    if "bundle_size" in task_obj:
        data_args = {"data_args_bundle": split_data_args(data_args, task_obj["bundle_size"])}
        inner_func = get_task_partial(tscache, pipeline_config, task_name)
        func = functools.partial(loop_over_bundled_data_args, inner_func=inner_func)
    else:
        func = get_task_partial(tscache, pipeline_config, task_name)

    if "group_size" in task_obj:
        data_args_groups = split_data_args_groups(data_args, task_obj["group_size"])
        statuses = []
        
        for data_args_group in data_args_groups:
            # Process each group sequentially
            if "data_args_bundle" in data_args_group:
                # Handle bundled data args
                task_args_list = data_args_group["data_args_bundle"]
            else:
                # Convert grouped data_args to individual task arguments
                group_size = len(data_args_group["idx"])
                task_args_list = [{key: data_args_group[key][i] for key in data_args_group} for i in range(group_size)]
            
            # Execute tasks sequentially
            results = []
            for task_args in task_args_list:
                result = execute_task_wrapper(func, task_args)
                results.append(result)
                
            status = check_results_success(results)
            statuses.append(status)
            
        overall_status = check_overall_status(statuses)
        if overall_status == "SUCCESS":
            outputs_version = fetch_and_hash_subtasks(tscache, task_name)
            tscache.db_client.set_task_ended(task_name, status=overall_status, outputs_version=outputs_version)
        else:
            tscache.db_client.set_task_ended(task_name, status=overall_status)
    else:
        # Handle single group execution
        tscache.logger.info(f"Mapping task {task_name} with kwargs {kwargs} and data_args {data_args}")
        
        if "data_args_bundle" in data_args:
            # Handle bundled data args
            task_args_list = data_args["data_args_bundle"]
        else:
            # Convert data_args to individual task arguments
            task_size = len(data_args["idx"])
            task_args_list = [{key: data_args[key][i] for key in data_args} for i in range(task_size)]
        
        # Execute tasks sequentially
        results = []
        for task_args in task_args_list:
            result = execute_task_wrapper(func, task_args)
            results.append(result)
            
        status = check_results_success(results)
        if status == "SUCCESS":
            outputs_version = fetch_and_hash_subtasks(tscache, task_name)
            tscache.db_client.set_task_ended(task_name, status=status, outputs_version=outputs_version)
        else:
            if pipeline_config.SUBSET_MODE:
                tscache.db_client.set_task_ended(task_name) # Subset mode subtasks don't mark the task as INCOMPLETE or FAILURE
            else:
                tscache.db_client.set_task_ended(task_name, status=status)


def run_task_vanilla(
    pipeline_config: PipelineConfig, 
    task_name: str, 
    reason: str = ""
):
    """Runs a pipeline task using vanilla Python (no Prefect, sequential execution)"""
    tscache = TaskStateCache(pipeline_config)
    task_obj = tscache.get_task(task_name)
    is_batch_array_worker = os.getenv("AWS_BATCH_JOB_ARRAY_INDEX") is not None

    # When running inside an AWS Batch array job, execute only the targeted subtask
    if is_batch_array_worker and tscache.is_mapped_task(task_name):
        tscache.logger.info(f"Detected AWS batch array worker; running subtask for {task_name}")
        from kptn.caching.batch import run_batch_array_subtask
        run_batch_array_subtask(pipeline_config, task_name)
        return
    
    # Keep the cache if subset mode or the task is an incomplete mapped task
    if pipeline_config.SUBSET_MODE:
        if "py_script" in task_obj:
            tscache.logger.info(f"Clearing subset before running task {task_name}")
            tscache.db_client.delete_subsetdata(task_name) # Delete any existing subset before we create it
        else:
            pass # Keep the cache for R tasks in subset mode
    elif reason == "INCOMPLETE" and tscache.is_mapped_task(task_name):
        pass
    else:
        tscache.logger.info(f"Clearing cache before running task {task_name}")
        tscache.delete_state(task_name)
    
    kwargs = {} # empty kwargs for now

    if tscache.is_mapped_task(task_name):
        tscache.logger.info(f"Running mapped task {task_name}")
        try:
            map_task_vanilla(pipeline_config, task_name, **kwargs)
        except Exception as e:
            # If INCOMPLETE, status is saved in the map_task_vanilla function to allow re-running only the failed subtasks
            # We also save code and data hashes for the task in case they change and we should re-run all subtasks
            tscache.set_final_state(task_name)
            raise e
    else:
        run_single_task(pipeline_config, task_name, **kwargs)

    tscache.set_final_state(task_name, status="SUCCESS")
