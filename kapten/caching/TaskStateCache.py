from datetime import datetime
import functools
import importlib
import logging
import os
import json
from os import path
from typing import Optional, Union
import requests
import time
from pathlib import Path

from kapten.caching.Hasher import Hasher
from kapten.caching.models import Subtask, TaskState
from kapten.caching.client.DbClientBase import DbClientBase, init_db_client
from kapten.util.flow_type import is_flow_prefect
from kapten.util.logger import get_logger
from kapten.util.pipeline_config import PipelineConfig, get_storage_key
from kapten.util.rscript import r_script
from kapten.util.read_tasks_config import read_tasks_config
from kapten.util.hash import hash_obj

class TaskStateCache():
    """
    Given a branch (used in the cache key), a dictionary of task configurations, a directory of R tasks,
    and a scratch directory (where output files are stored) this class serves as a proxy that submits
    Prefect tasks only if:
    1. The python function or R code of the task has changed
    2. The input files of the task have changed
    4. The input (Python-generated) data of the task has changed
    4. The override cache flag is set
    """

    pipeline_config: PipelineConfig
    db_client: DbClientBase
    hasher: Hasher
    logger: Union[logging.Logger, logging.LoggerAdapter]

    _instance = None

    # Singleton pattern to avoid multiple cache clients and reads of the tasks config
    def __new__(
        cls,
        pipeline_config: PipelineConfig,
        db_client: Optional[DbClientBase] = None,
        tasks_config = None,
    ):
        self = cls._instance
        if self is None:
            self = super(TaskStateCache, cls).__new__(cls)
            self.pipeline_config = pipeline_config
            storage_key = get_storage_key(pipeline_config)
            self.pipeline_name = pipeline_config.PIPELINE_NAME
            self.tasks_config = tasks_config or read_tasks_config(pipeline_config.TASKS_CONFIG_PATH)
            self.db_client = db_client or init_db_client(table_name=os.getenv("DYNAMODB_TABLE_NAME", "tasks"), storage_key=storage_key, pipeline=pipeline_config.PIPELINE_NAME, tasks_config=self.tasks_config)
            self.r_tasks_dir = pipeline_config.R_TASKS_DIR_PATH
            py_tasks_dir = Path(pipeline_config.TASKS_CONFIG_PATH).parent / pipeline_config.PY_MODULE_PATH.replace(".", os.sep)
            self.hasher = Hasher(
                py_dirs=[py_tasks_dir],
                r_dirs=[self.r_tasks_dir],
                output_dir=pipeline_config.scratch_dir,
                tasks_config=self.tasks_config
            )
            self.logger = get_logger()
        return self

    def __str__(self):
        storage_key = get_storage_key(self.pipeline_config)
        return f"TaskStateCache(storage_key={storage_key}, client={self.db_client}, tasks_config={self.tasks_config})"

    def is_flow_prefect(self) -> str:
        """Check if the workflow type is Prefect"""
        return self.tasks_config.get("settings", {}).get("flow-type") == "prefect"

    def get_dep_list(self, task_name: str) -> list[str]:
        """Return the names of the dependencies of a task."""
        if task_name not in self.tasks_config["graphs"][self.pipeline_name]["tasks"]:
            pipeline_keys_str = json.dumps(list(self.tasks_config["graphs"][self.pipeline_name]["tasks"].keys()))
            raise KeyError(f"Task ({task_name}) not found in list of tasks; pipeline: {self.pipeline_name}; pipeline_keys: {pipeline_keys_str}")
        deps = self.tasks_config["graphs"][self.pipeline_name]["tasks"][task_name]
        if type(deps) == list:
            return deps
        elif type(deps) == str:
            return [deps]
        else:
            return []
    
    def get_dep_states(self, task_name: str) -> list[tuple[str, TaskState]]:
        """Return the states of the dependencies of a task."""
        deps = self.get_dep_list(task_name)
        if not deps:
            return []
        return [(dep, self.fetch_state(dep)) for dep in deps]

    def get_task(self, name: str):
        """Return the task configuration."""
        try:
            task = self.tasks_config["tasks"][name]
        except KeyError:
            taskname_keys = self.tasks_config["tasks"].keys()
            raise KeyError(f"Task '{name}' not found in list of tasks, {taskname_keys}")
        return task

    def get_task_dask_worker_vars(self, name: str) -> dict:
        """Return task-specific worker_cpu, worker_mem kwargs to the Dask"""
        task = self.get_task(name)
        if "dask_worker" not in task:
            return {}
        cpu = task["dask_worker"]["cpu"]
        mem = task["dask_worker"]["memory"]
        return {"worker_cpu": cpu, "worker_mem": mem}

    def get_task_rscript_path(self, name: str):
        task = self.get_task(name)
        if "r_script" not in task:
            task["r_script"] = path.join(name, "run.R")
        r_script_path = path.join(self.r_tasks_dir, task["r_script"])
        return r_script_path

    def should_cache_result(self, task_name: str) -> bool:
        """Check if the task should cache its results."""
        task = self.get_task(task_name)
        return task.get("cache_result") == True

    def should_call_on_main_flow(self, task_name: str) -> bool:
        """Check if the task should be called on the main flow."""
        task = self.get_task(task_name)
        return task.get("main_flow") == True

    def is_rscript(self, task_name: str) -> bool:
        """Check if the task is an R script."""
        return "r_script" in self.get_task(task_name)

    def task_returns_list(self, task_name: str) -> bool:
        """Check if the task is a mapped task."""
        return "iterable_item" in self.get_task(task_name)

    def has_mapped_task_deps(self, task_name: str) -> bool:
        """Check if the task has mapped task dependencies."""
        deps = self.get_dep_list(task_name)
        for dep in deps:
            if self.task_returns_list(dep):
                return True

    def is_mapped_task(self, task_name: str) -> bool:
        """Check if the task is a mapped task."""
        return "map_over" in self.get_task(task_name)

    def get_map_over_key(self, task_name: str) -> str|None:
        """Return the key name of a task."""
        task = self.get_task(task_name)
        return task.get("map_over")

    def get_key_value(self, task_name: str, kwargs) -> str:
        """Return the key value of a task."""
        key_name = self.get_map_over_key(task_name)
        if key_name:
            if "," in key_name:
                keys = key_name.split(",")
                if all(key in kwargs for key in keys):
                    return ",".join([str(kwargs[key]) for key in keys])
            elif key_name in kwargs:
                return kwargs[key_name]
        return None

    def get_custom_log_path(self, task_name: str) -> str|None:
        task = self.get_task(task_name)
        return task.get("logs")

    def get_cli_args(self, task_name: str) -> str:
        """Return the args of a task."""
        task = self.get_task(task_name)
        if "cli_args" in task:
            if "prefix_args" in task:
                return task["prefix_args"], task["cli_args"]
            else:
                return "", task["cli_args"]
        elif "prefix_args" in task:
            return task["prefix_args"], ""
        else:
            return "", ""

    def get_py_func_name(self, task_name: str) -> str:
        """Return the Python function name of a task."""
        task = self.get_task(task_name)
        if type(task["py_script"]) == str:
            return task["py_script"].split(".")[0]
        else:
            return task_name

    def get_py_func_args(self, task_name: str) -> dict|None:
        """Return the args of a task."""
        task = self.get_task(task_name)
        return task.get("args")

    def py_code_changed(self, py_code_hashes: str, cached_state: TaskState = None) -> bool:
        """Check if the Python code of a task has changed."""
        if cached_state:
            return hash_obj(py_code_hashes) != cached_state.py_code_version
        else:
            return True

    def r_code_changed(self, r_code_hashes: str, cached_state: TaskState = None) -> bool:
        """Check if the R code of a task has changed."""
        if cached_state:
            local_version = hash_obj(r_code_hashes)
            code_changed = local_version != cached_state.r_code_version
            if code_changed:
                self.logger.info(f"R code changed: {r_code_hashes} != {cached_state.r_code_hashes}\n local_version: {local_version} != cached_version: {cached_state.r_code_version}")
            return code_changed
        else:
            self.logger.info("No cached state")
            return True

    def inputs_changed(
        self, input_hashes: dict[str, str], cached_state: TaskState = None
    ) -> bool:
        """Check if the inputs of a task have changed."""
        if cached_state:
            return cached_state.inputs_version != hash_obj(input_hashes)
        else:
            return True

    def data_changed(
        self, data_hashes: dict[str, str], cached_state: TaskState = None
    ) -> bool:
        """Check if the data of a task has changed."""
        if cached_state:
            return cached_state.input_data_version != hash_obj(data_hashes)
        else:
            return True

    def get_input_hashes(self, name: str, dep_states: list[tuple[str, TaskState]]) -> dict[str, str]:
        """Return the output file hashes of the inputs of a task."""
        inputs_version_tree = {}
        for dep, dep_state in dep_states:
            dep_outputs_version = dep_state.outputs_version if dep_state else None
            if dep_outputs_version:
                inputs_version_tree[dep] = dep_outputs_version
        self.logger.info(f"{name} inputs_version_tree: {inputs_version_tree}")
        if not inputs_version_tree:
            return None
        return inputs_version_tree

    def get_data_hashes(self, name: str, dep_states: list[tuple[str, TaskState]] = None) -> dict[str, str]:
        """Return the output data hashes of the inputs of a task."""
        if not dep_states:
            dep_states = self.get_dep_states(name)
        data_version_tree = {}
        for dep, dep_state in dep_states:
            if dep_state and dep_state.output_data_version:
                data_version_tree[dep] = dep_state.output_data_version
        self.logger.info(f"task={name} data_version_tree={data_version_tree}")
        if not data_version_tree:
            return None
        return data_version_tree

    def fetch_state(self, task_name) -> Optional[TaskState]:
        """Get cache for a flow or task; return None if not found or outdated."""
        cached_state = self.db_client.get_task(task_name, include_data=True, subset_mode=self.pipeline_config.SUBSET_MODE)
        if not cached_state:
            return None
        return TaskState.model_validate(cached_state)

    def delete_state(self, task_name: str):
        """Delete cache for a task"""
        self.db_client.delete_task(task_name)

    def submit(self, task_name: str, parameters, ignore_cache: bool):
        """Submit Prefect task if task state is out-of-date (code or inputs changed)."""
        self.logger.debug(f"tscache.submit({task_name}, {parameters}, ignore_cache={ignore_cache}) called")
        storage_key = get_storage_key(self.pipeline_config)
        task = self.get_task(task_name)
        cached_state = self.fetch_state(task_name)
        r_code_hashes = self.hasher.build_r_code_hashes(task_name, task) if "r_script" in task else None
        py_code_hashes = self.hasher.build_py_code_hashes(task_name, task) if "py_script" in task else None
        deployment_name = (
            f"{run_task.__name__.replace('_', '-')}/{self.pipeline_config.PIPELINE_NAME}-RunTask-{storage_key}"
        )
        reason = None
        if not cached_state:
            reason = "No cached state"
        elif ignore_cache:
            reason = "ignore_cache is set"
        elif self.pipeline_config.SUBSET_MODE:
            reason = "Subset mode"
        elif cached_state and cached_state.status == "FAILURE":
            reason = "Task previously failed all subtasks"
        elif self.is_rscript(task_name) and self.r_code_changed(r_code_hashes, cached_state):
            reason = "R code changed"
        elif self.py_code_changed(py_code_hashes, cached_state):
            reason = f"Python code changed: {py_code_hashes} != {cached_state.py_code_hashes}"
        else:
            dep_states = self.get_dep_states(task_name)
            if self.inputs_changed(self.get_input_hashes(task_name, dep_states), cached_state):
                reason = f"Inputs changed" #: {self.get_input_hashes(task_name)} != {cached_state.input_hashes}"
            elif self.data_changed(self.get_data_hashes(task_name, dep_states), cached_state):
                reason = f"Data changed"
            # All the above reasons delete the task's cache and re-run the task, but one below submits the task to fill out the cache
            elif cached_state and cached_state.status == "INCOMPLETE":
                reason = "INCOMPLETE"
            elif cached_state and not cached_state.end_time:
                reason = "Not finished"

        if reason:
            self.logger.info(f"Submitting task {task_name} because {reason}")
            # Run as separate flow container in prod
            if self.is_flow_prefect():
                if not os.getenv("DEPLOY_AS_INLINE_SUBFLOWS") == "1":
                    from kapten.caching.prefect import run_deployment_task
                    run_deployment_task(deployment_name, task_name, self.pipeline_config, task, reason, self.logger)
                else:  # Run as subflow locally
                    import prefect
                    parameters["task_name"] = task_name
                    parameters["reason"] = reason
                    flow_run_name = f"{task_name}-{prefect.runtime.flow_run.name}-{datetime.now().strftime('%H:%M:%S')}"
                    run_task.with_options(flow_run_name=flow_run_name)(
                        self.pipeline_config, **parameters
                    )
            else:
                kwargs = {}
                run_task(self.pipeline_config, task_name, **kwargs)
        else:
            self.logger.info(f"Skipping task {task_name}")
            return None

    def log_ecs_task_id(self) -> str:
        """Log the ECS Task ID and memory graph URL"""
        ecs_task_id = fetch_ecs_task_id()
        metrics_url = build_metrics_url()
        if os.getenv("IS_PROD") == "1":
            self.logger.info(f"Task running as ECS Task {ecs_task_id}; memory graph: {metrics_url}")
        return ecs_task_id

    def set_initial_state(self, task_name: str) -> TaskState:
        """Set initial state for a task before execution."""
        ecs_task_id = self.log_ecs_task_id()
        initial_state = TaskState(
            ecs_task_id=ecs_task_id,
            start_time=datetime.now().isoformat(),
        )
        if "py_script" in self.get_task(task_name) and self.pipeline_config.SUBSET_MODE:
            # When in subset mode, only create the task if it doesn't exist
            if not self.db_client.get_task(task_name):
                self.db_client.create_task(task_name, initial_state)
        else:
            self.db_client.create_task(task_name, initial_state)
        return initial_state

    def set_final_state(self, task_name: str, status: str = None):
        """Set final state for a task"""
        
        task = self.get_task(task_name)
        dep_states = self.get_dep_states(task_name)
        input_file_hashes = self.get_input_hashes(task_name, dep_states)
        input_data_hashes = self.get_data_hashes(task_name, dep_states)
        output_hashes = self.hasher.hash_task_outputs(task_name)

        # Since this function is called by RunTask, a separate flow from the main flow,
        # recompute the hashes to ensure they are up-to-date
        r_code_hashes = self.hasher.build_r_code_hashes(task_name, task) if self.is_rscript(task_name) else None
        py_code_hashes = self.hasher.build_py_code_hashes(task_name, task) if "py_script" in task else None

        final_state = TaskState(
            r_code_hashes=str(r_code_hashes) if r_code_hashes else None,
            py_code_hashes=str(py_code_hashes) if py_code_hashes else None,
            outputs_version=str(output_hashes) if output_hashes else None,
            input_hashes=str(input_file_hashes) if input_file_hashes else None,
            input_data_hashes=str(input_data_hashes) if input_data_hashes else None,
            updated_at=datetime.now().isoformat(),
        )
        if status:
            final_state.status = status
        # FYI output_data_version has already been set in the set_task_ended function
        self.db_client.update_task(task_name, final_state)


def fetch_ecs_task_id():
    """Fetch the ECS Task ID from the ECS metadata endpoint"""
    if os.getenv("IS_PROD") == "1":
        resp = requests.get(f"{os.getenv('ECS_CONTAINER_METADATA_URI_V4')}/task")
        ecs_task_id = resp.json()["TaskARN"].split("/")[-1]
        return ecs_task_id
    else:
        return "local"

def build_metrics_url():
    """Build the URL to the CloudWatch metrics for the ECS Task"""
    ecs_task_id = fetch_ecs_task_id()
    REGION = os.getenv("AWS_REGION")
    METRIC_NS = "bravo"
    METRIC_NAME = "task-memory"
    return f"https://{REGION}.console.aws.amazon.com/cloudwatch/home?region={REGION}#metricsV2?graph=~(view~'timeSeries~stacked~false~metrics~(~(~'{METRIC_NS}~'{METRIC_NAME}~'TaskId~'{ecs_task_id}))~region~'{REGION}~stat~'Maximum~period~60)"

def rscript_task(pipeline_config: PipelineConfig, task_name: str, **kwargs):
    """Call a task's R script (this function is called by single and mapped tasks)"""
    tscache = TaskStateCache(pipeline_config)
    key = tscache.get_key_value(task_name, kwargs)
    idx = kwargs.pop("idx", None)
    if key:
        tscache.db_client.set_subtask_started(task_name, idx)
    else:
        tscache.set_initial_state(task_name)
    rscript_path = tscache.get_task_rscript_path(task_name)
    env = { **kwargs }
    prefix_args, cli_args = tscache.get_cli_args(task_name)
    tscache.logger.info(f"Calling R script {rscript_path} with env {env}")
    custom_log_path = tscache.get_custom_log_path(task_name)
    r_script(task_name, key, pipeline_config, rscript_path, env, prefix_args, cli_args, custom_log_path)
    if key:
        start = time.time()
        hash = tscache.hasher.hash_subtask_outputs(task_name, env)
        tscache.logger.info(f"Hashing output files took {time.time() - start} seconds")
        tscache.db_client.set_subtask_ended(task_name, idx, hash)
    else:
        tscache.db_client.set_task_ended(task_name)

def py_task(pipeline_config: PipelineConfig, task_name: str, **kwargs):
    """Call a Python function (this function is called by single and mapped tasks)"""
    if is_flow_prefect():
        import prefect
        if isinstance(pipeline_config, prefect.unmapped):
            pipeline_config = pipeline_config.value
    tscache = TaskStateCache(pipeline_config)
    key = tscache.get_key_value(task_name, kwargs)
    idx = kwargs.pop("idx", None)
    if key:
        tscache.db_client.set_subtask_started(task_name, idx)
    else:
        tscache.set_initial_state(task_name)
    # Add any constant arguments to the kwargs (data_args are already present in kwargs)
    func_args = tscache.get_py_func_args(task_name)
    if func_args:
        for arg_name, arg_value in func_args.items():
            if arg_name not in kwargs:
                kwargs[arg_name] = arg_value
    module = importlib.import_module(pipeline_config.PY_MODULE_PATH)
    func_name = tscache.get_py_func_name(task_name)
    task = getattr(module, func_name)
    result = task(pipeline_config, **kwargs)
    if key:
        tscache.db_client.set_subtask_ended(task_name, idx)
    else:
        tscache.db_client.set_task_ended(task_name, result=result, result_hash=hash_obj(result), subset_mode=pipeline_config.SUBSET_MODE)
    

def run_task(
    pipeline_config: PipelineConfig, task_name: str, reason: str = ""
):
    """Wrapper to call run_task flow"""
    if is_flow_prefect():
        import kapten.caching.prefect
        return kapten.caching.prefect.run_task_prefect(pipeline_config, task_name, reason)
    else:
        import kapten.caching.vanilla
        return kapten.caching.vanilla.run_task_vanilla(pipeline_config, task_name, reason)