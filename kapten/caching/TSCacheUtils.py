
import functools

from kapten.caching.TaskStateCache import TaskStateCache, rscript_task, py_task
from kapten.util.pipeline_config import PipelineConfig
from kapten.util.flow_type import is_flow_prefect


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

def rscript_partial(pipeline_config: PipelineConfig, task_name: str):
    """Return a partial function to call an R script with the pipeline config and task name"""
    return functools.partial(rscript_task, pipeline_config, task_name)

def pyfunc_partial(pipeline_config: PipelineConfig, task_name: str):
    """Return a partial function to call a Python function with the pipeline config and task name"""
    if is_flow_prefect():
        import prefect
        return functools.partial(py_task, prefect.unmapped(pipeline_config), task_name)
    return functools.partial(py_task, pipeline_config, task_name)
