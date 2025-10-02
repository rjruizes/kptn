
import argparse
import sys
from pathlib import Path
from typing import Literal, Never

from kapten.caching.submit import submit
from kapten.deploy.storage_key import read_branch_storage_key
from kapten.util.pipeline_config import PipelineConfig, get_storage_key

# Add the tasks directory to sys.path to enable imports
tasks_path = Path(__file__).parent / "src"
if tasks_path.parent not in [Path(p) for p in sys.path]:
    sys.path.insert(0, str(tasks_path.parent))
import src as tasks

from kapten.caching.TaskStateCache import run_task
from kapten.deploy.push import docker_push
from kapten.watcher.stacks import get_stack_endpoints

TaskListChoices = list[Literal["raw_numbers_fn","fruit_metrics_fn","fruit_summary",]]|list[Never]

def duckdb_example(pipeline_config: PipelineConfig, task_list: TaskListChoices = [], ignore_cache: bool = False):
    
    _raw_numbers_fn = submit(
        "raw_numbers_fn",
        pipeline_config,
        task_list,
        ignore_cache,
        tasks.raw_numbers_fn
    )
    _fruit_metrics_fn = submit(
        "fruit_metrics_fn",
        pipeline_config,
        task_list,
        ignore_cache,
        tasks.fruit_metrics_fn
    )
    _fruit_summary = submit(
        "fruit_summary",
        pipeline_config,
        task_list,
        ignore_cache,
        tasks.fruit_summary
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
    
    tasks_config_path = Path(__file__).parent / "kapten.yaml"
    pipeline_config = PipelineConfig(
        TASKS_CONFIG_PATH=str(tasks_config_path),
        PIPELINE_NAME="duckdb_example",
        PY_MODULE_PATH=tasks.__name__,
    )
    duckdb_example(pipeline_config, ignore_cache=args.force)
    
    