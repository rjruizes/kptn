
from pathlib import Path
import os
from kptn.caching.submit import submit
from kptn.runner import cli_parser, parse_and_validate_tasks
from kptn.util.pipeline_config import PipelineConfig

os.environ["KPTN_FLOW_TYPE"] = "vanilla"
os.environ.setdefault("KPTN_DB_TYPE", "sqlite")

VALID_TASKS: set[str] = { "a", "b" }

def basic2(task_list: list[str] = [], ignore_cache: bool = False):
    pipeline_config = PipelineConfig(
        TASKS_CONFIG_PATH=str(Path(__file__).parent / "kptn.yaml"),
        PIPELINE_NAME="basic2",
    )

    task_list = parse_and_validate_tasks(task_list, VALID_TASKS)

    opts = (
        pipeline_config,
        set(task_list),
        ignore_cache,
    )

    submit("a", opts)  # file://./src/a.py
    submit("b", opts)  # file://./src/b.py


if __name__ == "__main__":
    args, _ = cli_parser().parse_known_args()

    basic2(
        task_list=args.tasks or None,
        ignore_cache=args.force,
    )