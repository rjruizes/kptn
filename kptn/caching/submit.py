
from kptn.caching.TaskStateCache import TaskStateCache, is_flow_prefect
from kptn.util.pipeline_config import PipelineConfig
from typing import Union, Tuple

# Type alias for submit configuration tuple: (pipeline_config, task_list, ignore_cache)
SubmitConfig = Tuple[PipelineConfig, set[str], bool]


def _prefect_check_cache(
    pipeline_config: PipelineConfig,
    task_name: str,
    func: callable,
    ignore_cache: bool = False,
    parameters: dict = {},
    wait_for: list = [],
):
    """Handle cache checking for Prefect flows"""
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
    
    return prefect_check_cache.with_options(task_run_name=f"check_cache-{task_name}").submit(
        pipeline_config,
        task_name,
        func,
        ignore_cache,
        parameters=parameters,
        wait_for=wait_for,
    )


def check_cache(
    pipeline_config: PipelineConfig,
    task_name: str,
    func: callable,
    ignore_cache: bool = False,
    parameters: dict = {},
    wait_for: list = [],
):
    if is_flow_prefect():
        return _prefect_check_cache(
            pipeline_config,
            task_name,
            func,
            ignore_cache,
            parameters,
            wait_for,
        )
    else:
        tscache = TaskStateCache(pipeline_config)
        return tscache.submit(task_name, parameters, ignore_cache)

def _submit(
    task_name: str,
    pipeline_config: PipelineConfig,
    task_list: set[str] = [],
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

def submit(task_name: str, config: SubmitConfig):
    pipeline_config, task_list, ignore_cache = config
    return _submit(
        task_name,
        pipeline_config,
        task_list,
        ignore_cache,
    )