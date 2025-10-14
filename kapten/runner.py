"""Pipeline runner utilities for task parsing and validation."""

import argparse


def parse_and_validate_tasks(
    task_names: str | list[str] | None,
    valid_tasks: set[str],
) -> list[str]:
    """Parse and validate task names for pipeline execution.

    Args:
        task_names: Task(s) to run. Can be:
            - None: Run all tasks
            - Single task name: "fruit_summary"
            - Comma-separated string: "fruit_metrics,fruit_summary"
            - List of task names: ["fruit_metrics", "fruit_summary"]
        valid_tasks: Set of valid task names to validate against

    Returns:
        List of validated task names. Empty list means run all tasks.

    Raises:
        ValueError: If any task name is not in valid_tasks

    Example:
        >>> valid = {"task1", "task2", "task3"}
        >>> parse_and_validate_tasks("task1,task2", valid)
        ['task1', 'task2']
        >>> parse_and_validate_tasks(None, valid)
        []
    """
    # Parse task_names into a list
    if task_names is None:
        task_list = []
    elif isinstance(task_names, str):
        task_list = [task.strip() for task in task_names.split(",") if task.strip()]
    else:
        task_list = list(task_names)

    # Validate task names
    if task_list:
        invalid_tasks = [task for task in task_list if task not in valid_tasks]
        if invalid_tasks:
            raise ValueError(
                f"Invalid task(s): {', '.join(invalid_tasks)}. "
                f"Expected one of: {', '.join(sorted(valid_tasks))}"
            )

    return task_list


def cli_parser(description: str = "Run the pipeline") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
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
    return parser
