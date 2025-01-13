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
import prefect
from prefect.deployments import run_deployment
from prefect.states import raise_state_exception
from kapten.caching.Hasher import Hasher
from kapten.caching.models import Subtask, TaskState
from kapten.caching.client.DbClientBase import DbClientBase, init_db_client
from kapten.util.logger import get_logger
from kapten.util.pipeline_config import PipelineConfig, get_storage_key
from kapten.util.rscript import r_script
from kapten.util.read_tasks_config import read_tasks_config
from kapten.util.hash import hash_obj
from tasks.settings import load_custom_flow_params


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
            self.db_client = db_client or init_db_client(table_name=os.getenv("DYNAMODB_TABLE_NAME", "tasks"), storage_key=storage_key, pipeline=pipeline_config.PIPELINE_NAME)
            self.pipeline_name = pipeline_config.PIPELINE_NAME
            self.tasks_config = tasks_config or read_tasks_config(pipeline_config.TASKS_CONFIG_PATH)
            self.r_tasks_dir = pipeline_config.R_TASKS_DIR_PATH
            self.hasher = Hasher(
                py_dirs=[pipeline_config.PY_MODULE_PATH.replace(".", os.sep)],
                r_dirs=[self.r_tasks_dir],
                output_dir=pipeline_config.scratch_dir,
                tasks_config=self.tasks_config
            )
            self.logger = get_logger()
        return self

    def __str__(self):
        storage_key = get_storage_key(self.pipeline_config)
        return f"TaskStateCache(storage_key={storage_key}, client={self.db_client}, tasks_config={self.tasks_config})"

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
        self.logger.info(f"tscache.submit({task_name}, {parameters}, ignore_cache={ignore_cache}) called")
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
            if not os.getenv("DEPLOY_AS_INLINE_SUBFLOWS") == "1":
                self.logger.info(f"Running deployment for '{task_name}'")
                job_variables = task["aws_vars"] if "aws_vars" in task else {}
                flow_run = run_deployment(
                    name=deployment_name,
                    # task_name-flow_name-hh:mm:ss
                    flow_run_name=f"{task_name}-{prefect.runtime.flow_run.name}-{datetime.now().strftime('%H:%M:%S')}",
                    as_subflow=False,
                    parameters={
                        "pipeline_config": self.pipeline_config.model_dump(),
                        "task_name": task_name,
                        "reason": reason,
                        # "kwargs": parameters,
                    },
                    job_variables=job_variables,
                )
                self.logger.info(
                    f"Finished deployment run '{task_name}', state: {flow_run.state} http://{os.getenv('AWS_EC2_EIP')}/flow-runs/flow-run/{flow_run.id}"
                )
                raise_state_exception(flow_run.state)
            else:  # Run as subflow locally
                parameters["task_name"] = task_name
                parameters["reason"] = reason
                flow_run_name = f"{task_name}-{prefect.runtime.flow_run.name}-{datetime.now().strftime('%H:%M:%S')}"
                run_task.with_options(flow_run_name=flow_run_name)(
                    self.pipeline_config, **parameters
                )
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
            UpdatedAt=datetime.now().isoformat(),
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
    

def rscript_partial(pipeline_config: PipelineConfig, task_name: str):
    """Return a partial function to call an R script with the pipeline config and task name"""
    return functools.partial(rscript_task, pipeline_config, task_name)

def pyfunc_partial(pipeline_config: PipelineConfig, task_name: str):
    """Return a partial function to call a Python function with the pipeline config and task name"""
    return functools.partial(py_task, prefect.unmapped(pipeline_config), task_name)

def fetch_cached_dep_data(tscache: TaskStateCache, task_name: str):
    """
    Fetch cached data for dependencies of a task
    data_args: a dictionary of the data for each dependency
    value_list: a list of values for the keys of the dependencies; used for mapping tasks
    """
    deps = tscache.get_dep_list(task_name)
    data_args = {}
    arg_lookup = {}
    value_list = []
    # Build a lookup table to map ref keys (task names) to arg names
    task = tscache.get_task(task_name)
    if "args" in task:
        for arg_name, arg_value in tscache.get_task(task_name)["args"].items():
            if type(arg_value) == dict and "ref" in arg_value:
                arg_lookup[arg_value["ref"]] = arg_name
    for dep_name in deps:
        if tscache.should_cache_result(dep_name):
            resp = tscache.fetch_state(dep_name)
            if resp != None and resp.data != "":
                dep = tscache.get_task(dep_name)
                if "map_over" in task and "iterable_item" in dep:
                    key = dep["iterable_item"]
                elif dep_name in arg_lookup:
                    key = arg_lookup[dep_name]
                else:
                    key = dep_name
                if "map_over" in task and "," in key:
                    keys = key.split(",")
                    data: list[tuple] = resp.data
                    # Unpack the tuples into separate lists
                    # e.g. if key = "a,b" and data = [(1, 2), (3, 4)]
                    # then data_args["a"] = [1, 3] and data_args["b"] = [2, 4]
                    for i, key in enumerate(keys):
                        data_args[key] = [data[j][i] for j in range(len(data))]
                    # Save the list of values for the keys
                    # e.g. if data = [(1, 2), (3, 4)]
                    # then value_list = ["1,2", "3,4"]
                    value_list = [",".join([str(x) for x in tup]) for tup in data]
                else:
                    data_args[key] = resp.data
                    value_list = resp.data
    return data_args, value_list

def run_single_task(pipeline_config: PipelineConfig, task_name: str, db_client=None, **kwargs):
    """Execute either an R script or a Python function"""
    tscache = TaskStateCache(pipeline_config, db_client)
    data_args = fetch_cached_dep_data(tscache, task_name)[0]

    if tscache.is_rscript(task_name):
        return rscript_task(pipeline_config, task_name, **data_args, **kwargs)
    else:
        return py_task(pipeline_config, task_name, **data_args, **kwargs)

def get_task_partial(tscache: TaskStateCache, pipeline_config: PipelineConfig, task_name: str):
    """Return a partial function to run a task with the pipeline config and task name"""
    if tscache.is_rscript(task_name):
        return rscript_partial(pipeline_config, task_name)
    else:
        return pyfunc_partial(pipeline_config, task_name)

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
    data_args, value_list = fetch_cached_dep_data(tscache, task_name)
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
def run_task(
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
        tscache.logger.info("Running mapped task {task_name}")
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
