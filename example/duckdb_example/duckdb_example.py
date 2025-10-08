
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
    
    _raw_numbers = submit( # file://./src/raw_numbers.sql
        "raw_numbers",
        pipeline_config,
        task_list,
        ignore_cache
    )
    _fruit_metrics = submit( # file://./src/fruit_metrics.sql
        "fruit_metrics",
        pipeline_config,
        task_list,
        ignore_cache
    )
    _fruit_summary = submit( # file://./src/fruit_tasks.py
        "fruit_summary",
        pipeline_config,
        task_list,
        ignore_cache,
        tasks.fruit_summary
    )


def run_pipeline(task_names: str | list[str] = None, force: bool = False, config_path: str = None) -> None:
    """Run the pipeline programmatically

    Args:
        task_names: Specific task(s) to run. Can be:
            - Single task name: "fruit_summary"
            - Comma-separated string: "fruit_metrics,fruit_summary"
            - List of task names: ["fruit_metrics", "fruit_summary"]
            - None: Run all tasks (default)
        force: Ignore cached results and force all tasks to run (default: False)
        config_path: Path to kapten.yaml config file. If None, uses default relative path.

    Example:
        run_pipeline("fruit_summary")
        run_pipeline(["fruit_metrics", "fruit_summary"], force=True)
    """
    if config_path is None:
        tasks_config_path = Path(__file__).parent / "kapten.yaml"
    else:
        tasks_config_path = Path(config_path)

    pipeline_config = PipelineConfig(
        TASKS_CONFIG_PATH=str(tasks_config_path),
        PIPELINE_NAME="duckdb_example",
    )

    # Parse task_names into a list
    if task_names is None:
        task_list = []
    elif isinstance(task_names, str):
        task_list = [task.strip() for task in task_names.split(",") if task.strip()]
    else:
        task_list = list(task_names)

    # Validate task names
    if task_list:
        invalid_tasks = [task for task in task_list if task not in VALID_TASKS]
        if invalid_tasks:
            raise ValueError(
                f"Invalid task(s): {', '.join(invalid_tasks)}. "
                f"Expected one of: {', '.join(sorted(VALID_TASKS))}"
            )

    duckdb_example(
        pipeline_config,
        task_list=task_list,
        ignore_cache=force,
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

    run_pipeline(
        task_names=args.tasks or None,
        force=args.force,
    )