
import argparse
import sys
from pathlib import Path
from typing import Literal, Never

from kptn.caching.submit import submit
from kptn.deploy.storage_key import read_branch_storage_key
from kptn.util.pipeline_config import PipelineConfig, get_storage_key

# Add the tasks directory to sys.path to enable imports
tasks_path = Path(__file__).parent / "../py_tasks"
if tasks_path.parent not in [Path(p) for p in sys.path]:
    sys.path.insert(0, str(tasks_path.parent))
import py_tasks as tasks

from kptn.caching.TaskStateCache import run_task
from kptn.deploy.push import docker_push
from kptn.watcher.stacks import get_stack_endpoints

TaskListChoices = list[Literal["combo50_list","combo_process_group",]]|list[Never]

def grouping(pipeline_config: PipelineConfig, task_list: TaskListChoices = [], ignore_cache: bool = False):
    
    _combo50_list = submit(
        "combo50_list",
        pipeline_config,
        task_list,
        ignore_cache,
        tasks.combo50_list
    )
    _combo_process_group = submit(
        "combo_process_group",
        pipeline_config,
        task_list,
        ignore_cache,
        tasks.combo_process_group
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the pipeline")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        dest="force",
        help="Ignore cached results and force all tasks to run",
    )
    args, _ = parser.parse_known_args()
    
    tasks_config_path = Path(__file__).parent / "../kptn.yaml"
    pipeline_config = PipelineConfig(
        TASKS_CONFIG_PATH=str(tasks_config_path),
        PIPELINE_NAME="grouping",
        PY_MODULE_PATH=tasks.__name__,
        R_TASKS_DIRS=(str(Path(__file__).parent / "../r_tasks"),),
    )
    grouping(pipeline_config, ignore_cache=args.force)
    
    
