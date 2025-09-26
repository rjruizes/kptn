
import sys
from pathlib import Path
from typing import Literal, Never

from kapten.caching.submit import submit
from kapten.deploy.storage_key import read_branch_storage_key
from kapten.util.pipeline_config import PipelineConfig, get_storage_key

# Add the tasks directory to sys.path to enable imports
tasks_path = Path(__file__).parent / "../py_tasks"
if tasks_path.parent not in [Path(p) for p in sys.path]:
    sys.path.insert(0, str(tasks_path.parent))
import py_tasks as tasks

from kapten.caching.TaskStateCache import run_task
from kapten.deploy.push import docker_push
from kapten.watcher.stacks import get_stack_endpoints

TaskListChoices = list[Literal["combo50_list","combo_process_bundle",]]|list[Never]

def bundling(pipeline_config: PipelineConfig, task_list: TaskListChoices = [], ignore_cache: bool = False):
    
    _combo50_list = submit(
        "combo50_list",
        pipeline_config,
        task_list,
        ignore_cache,
        tasks.combo50_list
    )
    _combo_process_bundle = submit(
        "combo_process_bundle",
        pipeline_config,
        task_list,
        ignore_cache,
        tasks.combo_process
    )


if __name__ == "__main__":
    
    tasks_config_path = Path(__file__).parent / "../kapten.yaml"
    pipeline_config = PipelineConfig(
        TASKS_CONFIG_PATH=str(tasks_config_path),
        PIPELINE_NAME="bundling",
        PY_MODULE_PATH=tasks.__name__,
        R_TASKS_DIR_PATH=str(Path(__file__).parent / "../r_tasks"),
    )
    bundling(pipeline_config)
    
    