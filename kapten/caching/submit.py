
from kapten.caching.TaskStateCache import TaskStateCache, is_flow_prefect
from kapten.util.pipeline_config import PipelineConfig
from typing import Union


def check_cache(
    pipeline_config: PipelineConfig,
    task_name: str,
    func: callable,
    ignore_cache: bool = False,
    parameters: dict = {},
    wait_for: list = [],
):
    tscache = TaskStateCache(pipeline_config)
    if is_flow_prefect():
        from prefect import task
        @task
        def prefect_check_cache(
            pipeline_config: PipelineConfig,
            task_name: str,
            func: callable,
            ignore_cache: bool,
            parameters: dict,
        ):
            """Checks if task is cached and triggers flow deployment if not cached"""
            tscache = TaskStateCache(pipeline_config)

            if tscache.should_call_on_main_flow(task_name):
                return func(pipeline_config, **parameters)

            return tscache.with_options(task_run_name=task_name).submit(task_name, parameters, ignore_cache)
        """Checks if task is cached and triggers flow deployment if not cached"""
        return prefect_check_cache.with_options(task_run_name=f"check_cache-{task_name}").submit(
            pipeline_config,
            task_name,
            func,
            ignore_cache,
            parameters=parameters,
            wait_for=wait_for,
        )
    else:
        return tscache.submit(task_name, parameters, ignore_cache)

def submit(
    task_name: str,
    pipeline_config: PipelineConfig,
    task_list: list[str] = [],
    ignore_cache: bool = False,
    task: callable = None,
    parameters: dict = {},
    wait_for: list = [],
):
    """Submit check_cache task to the ConcurrentTaskRunner for a pipeline task"""
    if not task_list or task_name in task_list:
        return check_cache(
            pipeline_config,
            task_name,
            task,
            ignore_cache,
            parameters=parameters,
            wait_for=wait_for,
        )