
import argparse
import sys
from pathlib import Path
from typing import Literal, Never

from kapten.caching.submit import submit

from kapten.util.pipeline_config import PipelineConfig, get_storage_key

# Add the tasks directory to sys.path to enable imports
tasks_path = Path(__file__).parent / "src"
if tasks_path.parent not in [Path(p) for p in sys.path]:
    sys.path.insert(0, str(tasks_path.parent))
import src as tasks

TaskListChoices = list[Literal["raw_numbers","fruit_metrics","fruit_summary",]]|list[Never]
VALID_TASKS: set[str] = { "raw_numbers", "fruit_metrics", "fruit_summary" }

def duckdb_example(pipeline_config: PipelineConfig, task_list: TaskListChoices = [], ignore_cache: bool = False):
    
    _raw_numbers = submit(
        "raw_numbers",
        pipeline_config,
        task_list,
        ignore_cache
    )
    _fruit_metrics = submit(
        "fruit_metrics",
        pipeline_config,
        task_list,
        ignore_cache
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
    parser.add_argument(
        "tasks",
        nargs="?",
        default="",
        help=(
            "Optional comma-separated list of task names to run; "
            "runs all tasks when omitted"
        ),
    )
    args, _ = parser.parse_known_args()
    
    tasks_config_path = Path(__file__).parent / "kapten.yaml"
    pipeline_config = PipelineConfig(
        TASKS_CONFIG_PATH=str(tasks_config_path),
        PIPELINE_NAME="duckdb_example",
    )
    raw_task_list = args.tasks
    task_list = (
        [task.strip() for task in raw_task_list.split(",") if task.strip()]
        if raw_task_list
        else []
    )
    if task_list:
        invalid_tasks = [task for task in task_list if task not in VALID_TASKS]
        if invalid_tasks:
            parser.error(
                "Invalid task(s): " + ", ".join(invalid_tasks) + ". "
                "Expected one of: " + ", ".join(sorted(VALID_TASKS))
            )
    duckdb_example(
        pipeline_config,
        task_list=task_list,
        ignore_cache=args.force,
    )