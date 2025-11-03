from kptn.caching.models import Subtask
from kptn.caching.TaskStateCache import TaskStateCache
from kptn.caching.TSCacheUtils import fetch_cached_dep_data, get_task_partial, run_single_task
from kptn.util.logger import get_logger
from kptn.util.pipeline_config import PipelineConfig
from kptn.util.hash import hash_obj
from datetime import datetime
import functools
import os
import prefect
import time
from tasks.settings import load_custom_flow_params

def check_overall_status(statuses):
    """Determine the overall status of a list of statuses"""
    total = len(statuses)
    success = sum(status == "SUCCESS" for status in statuses)
    if success == total:
        return "SUCCESS"
    elif success == 0:
        return "FAILURE"
    else:
        return "INCOMPLETE"

def check_futures_success(futures):
    """Determines if all, some, or no elements in a list of futures are successful"""
    total = len(futures)
    success = sum(future.get_state().is_completed() for future in futures)
    if success == total:
        return "SUCCESS"
    elif success == 0:
        return "FAILURE"
    else:
        return "INCOMPLETE"

def fetch_and_hash_subtasks(tscache: TaskStateCache, task_name: str) -> str:
    # Fetch subtasks, get subtask.outputHash for each
    subtasks: list[Subtask] = tscache.db_client.get_subtasks(task_name)
    start = time.time()
    output_hashes = [subtask.outputHash for subtask in subtasks]
    outputs_version = hash_obj(output_hashes) if output_hashes else None
    tscache.logger.info(f"Composite hash took {time.time() - start} seconds")
    return outputs_version

@prefect.flow(log_prints=True)
def map_flow(
    pipeline_config: PipelineConfig, task_name: str, **kwargs
):
    """Maps an R script over its data_args (dependency data)"""
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

    def split_data_args(data_args: dict[str, list], size:int=10) -> list[dict[str, list]]:
        """
        Transform a data_args dictionary into a list of dictionaries with `size` elements
        e.g. if size = 5
        {"idx": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]} -> [{"idx": [1, 2, 3, 4, 5]}, {"idx": [6, 7, 8, 9, 10]}]
        """
        return [{key: data_args[key][i:i+size] for key in data_args} for i in range(0, len(data_args["idx"]), size)]

    def split_data_args_groups(data_args: dict[str, list], size:int=10) -> list[dict[str, list]]|list[list[dict[str, list]]]:
        """
        If input is a regular data_args dictionary, split it into groups of size elements;
        If input is a bundled data_args dictionary, group the bundles into groups of size elements
        """
        if "idx" in data_args:
            return split_data_args(data_args, size)
        else:
            bundles = data_args["data_args_bundle"]
            return [{"data_args_bundle": bundles[i:i+size]} for i in range(0, len(bundles), size)]

    def loop_over_bundled_data_args(data_args_bundle: dict[str, list], inner_func):
        """
        Loop over a data_args bundle and call the inner function with the data_args
        """
        logger = get_logger()
        logger.info(f"Looping over task {task_name} with data_args_bundle {data_args_bundle}")
        errors = []
        for i in range(len(data_args_bundle["idx"])):
            data_args = {key: data_args_bundle[key][i] for key in data_args_bundle}
            # logger.info(f"Calling {task_name} with data_args {data_args}")
            try:
                inner_func(**data_args)
            except Exception as e:
                errors.append(str(e))
        if errors:
            raise Exception("\n".join(errors))

    if "bundle_size" in task_obj:
        data_args = {"data_args_bundle": split_data_args(data_args, task_obj["bundle_size"])}
        inner_func = get_task_partial(tscache, pipeline_config, task_name)
        func = functools.partial(loop_over_bundled_data_args, inner_func=inner_func)
        # The dask scheduler will distribute the bundles to workers
        # A dask worker will call the loop function to iterate over the bundle, calling the inner function for each element
    else:
        func = get_task_partial(tscache, pipeline_config, task_name)

    tags = task_obj.get("tags", [])
    if "group_size" in task_obj:
        data_args_groups = split_data_args_groups(data_args, task_obj["group_size"])
        statuses = []
        for data_args_group in data_args_groups:
            futures = prefect.task(func, tags=tags).map(**data_args_group, **kwargs)
            for future in futures:
                future.wait()
            status = check_futures_success(futures)
            statuses.append(status)
        overall_status = check_overall_status(statuses)
        if overall_status == "SUCCESS":
            outputs_version = fetch_and_hash_subtasks(tscache, task_name)
            tscache.db_client.set_task_ended(task_name, status=overall_status, outputs_version=outputs_version)
        else:
            tscache.db_client.set_task_ended(task_name, status=overall_status)
    else:
        tscache.logger.info(f"Mapping task {task_name} with kwargs {kwargs} and data_args {data_args}")
        futures = prefect.task(func, tags=tags).map(**data_args, **kwargs)
        for future in futures:
            future.wait()
        status = check_futures_success(futures)
        if status == "SUCCESS":
            outputs_version = fetch_and_hash_subtasks(tscache, task_name)
            tscache.db_client.set_task_ended(task_name, status=status, outputs_version=outputs_version)
        else:
            if pipeline_config.SUBSET_MODE:
                tscache.db_client.set_task_ended(task_name) # Subset mode subtasks don't mark the task as INCOMPLETE or FAILURE
            else:
                tscache.db_client.set_task_ended(task_name, status=status)
    

@prefect.flow(log_prints=True)
def run_task_prefect(
    pipeline_config: PipelineConfig, task_name: str, reason: str = ""
):
    """Runs a pipeline task in its own container"""
    tscache = TaskStateCache(pipeline_config)
    task_obj = tscache.get_task(task_name)
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
        # Subflow runs in current container with distributed task runner
        dask_params = {
            "image": pipeline_config.IMAGE,
        }
        dask_params.update(tscache.get_task_dask_worker_vars(task_name))
        custom_flow_params = load_custom_flow_params(**dask_params)
        map_flow_run_name = f"{prefect.runtime.flow_run.name}-MapFlow-{datetime.now().strftime('%H:%M:%S')}"
        subflow = map_flow.with_options(**custom_flow_params, flow_run_name=map_flow_run_name)
        try:
            subflow(pipeline_config, task_name, **kwargs)
        except Exception:
            # If INCOMPLETE, status is saved in the subflow to allow re-running only the failed subtasks
            # We also save code and data hashes for the task in case they change and we should re-run all subtasks
            tscache.set_final_state(task_name)
            raise
    else:
        run_single_task(pipeline_config, task_name, **kwargs)

    tscache.set_final_state(task_name, status="SUCCESS")

def run_deployment_task(deployment_name: str, task_name: str, pipeline_config: PipelineConfig, task: dict, reason: str, logger):
    """Run a Prefect deployment for a task"""
    from prefect.deployments import run_deployment
    from prefect.states import raise_state_exception

    logger.info(f"Running deployment for '{task_name}'")
    job_variables = task["compute"] if "compute" in task else {}
    flow_run = run_deployment(
        name=deployment_name,
        # task_name-flow_name-hh:mm:ss
        flow_run_name=f"{task_name}-{prefect.runtime.flow_run.name}-{datetime.now().strftime('%H:%M:%S')}",
        as_subflow=False,
        parameters={
            "pipeline_config": pipeline_config.model_dump(),
            "task_name": task_name,
            "reason": reason,
            # "kwargs": parameters,
        },
        job_variables=job_variables,
    )
    logger.info(
        f"Finished deployment run '{task_name}', state: {flow_run.state} http://{os.getenv('AWS_EC2_EIP')}/flow-runs/flow-run/{flow_run.id}"
    )
    raise_state_exception(flow_run.state)
