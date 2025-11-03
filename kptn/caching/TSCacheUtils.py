import functools

from kptn.caching.TaskStateCache import TaskStateCache, rscript_task, py_task
from kptn.util.pipeline_config import PipelineConfig
from kptn.util.flow_type import is_flow_prefect
from kptn.util.task_args import build_task_argument_plan, resolve_dependency_key


def fetch_cached_dep_data(tscache: TaskStateCache, task_name: str):
    """
    Fetch cached data for dependencies of a task
    data_args: a dictionary of the data for each dependency
    value_list: a list of values for the keys of the dependencies; used for mapping tasks
    map_over_count: number of items that will be mapped over (if applicable)
    """
    deps = tscache.get_dep_list(task_name)
    task = tscache.get_task(task_name)
    tasks_def = tscache.tasks_config.get("tasks", {})
    plan = build_task_argument_plan(task_name, task, deps, tasks_def)
    if plan.errors:
        for message in plan.errors:
            tscache.logger.warning(
                "Task %s configuration issue during argument resolution: %s",
                task_name,
                message,
            )

    data_args = {}
    value_list = []
    map_over_count = None

    for dep_name in deps:
        if tscache.should_cache_result(dep_name):
            resp = tscache.fetch_state(dep_name)
            if resp != None and resp.data != "":
                dep = tscache.get_task(dep_name)
                key = resolve_dependency_key(task, dep_name, dep, plan.alias_lookup)
                if not key:
                    continue
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
                    map_over_count = len(value_list)
                else:
                    data_args[key] = resp.data
                    value_list = resp.data
                    if "map_over" in task and isinstance(value_list, list):
                        map_over_count = len(value_list)
    return data_args, value_list, map_over_count

def run_single_task(pipeline_config: PipelineConfig, task_name: str, db_client=None, **kwargs):
    """Execute either an R script or a Python function"""
    tscache = TaskStateCache(pipeline_config, db_client)
    data_args, _, _ = fetch_cached_dep_data(tscache, task_name)

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

def rscript_partial(pipeline_config: PipelineConfig, task_name: str):
    """Return a partial function to call an R script with the pipeline config and task name"""
    return functools.partial(rscript_task, pipeline_config, task_name)

def pyfunc_partial(pipeline_config: PipelineConfig, task_name: str):
    """Return a partial function to call a Python function with the pipeline config and task name"""
    if is_flow_prefect():
        import prefect
        return functools.partial(py_task, prefect.unmapped(pipeline_config), task_name)
    return functools.partial(py_task, pipeline_config, task_name)
