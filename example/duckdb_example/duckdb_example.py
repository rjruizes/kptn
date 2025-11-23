
from pathlib import Path
import os
from kptn.caching.submit import submit
from kptn.runner import cli_parser, parse_and_validate_tasks
from kptn.util.pipeline_config import PipelineConfig

os.environ["KPTN_FLOW_TYPE"] = "vanilla"
os.environ.setdefault("KPTN_DB_TYPE", "sqlite")

VALID_TASKS: set[str] = { "raw_numbers", "fruit_metrics", "fruit_summary", "wide_table", "python_source", "python_consumer" }

def duckdb_example(task_list: list[str] = [], ignore_cache: bool = False):
    pipeline_config = PipelineConfig(
        TASKS_CONFIG_PATH=str(Path(__file__).parent / "kptn.yaml"),
        PIPELINE_NAME="duckdb_example",
    )

    task_list = parse_and_validate_tasks(task_list, VALID_TASKS)

    opts = (
        pipeline_config,
        set(task_list),
        ignore_cache,
    )

    submit("raw_numbers", opts)  # file://./src/raw_numbers.sql
    submit("fruit_metrics", opts)  # file://./src/fruit_metrics.sql
    submit("fruit_summary", opts)  # file://./src/fruit_tasks.py
    submit("wide_table", opts)  # file://./src/wide_table.sql
    submit("python_source", opts)  # file://./src/python_source.py
    submit("python_consumer", opts)  # file://./src/python_consumer.sql


if __name__ == "__main__":
    args, _ = cli_parser().parse_known_args()

    duckdb_example(
        task_list=args.tasks or None,
        ignore_cache=args.force,
    )