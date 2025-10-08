# Kapten package
__version__ = "0.1.0"

import importlib.util
import yaml
from pathlib import Path
from typing import Union


def run(
    task_names: Union[str, list[str], None] = None,
    project_dir: str = ".",
    force: bool = False
) -> None:
    """Run a Kapten pipeline task programmatically

    This function provides a Python API to execute Kapten pipeline tasks
    without using the CLI. It imports and executes the generated flow file.

    Args:
        task_names: Specific task(s) to run. Can be:
            - Single task name: "fruit_summary"
            - Comma-separated string: "fruit_metrics,fruit_summary"
            - List of task names: ["fruit_metrics", "fruit_summary"]
            - None: Run all tasks (default)
        project_dir: Path to directory containing kapten.yaml (default: current directory)
        force: Ignore cached results and force all tasks to run (default: False)

    Raises:
        FileNotFoundError: If kapten.yaml or generated flow file not found
        ValueError: If invalid task names provided or multiple graphs found
        ImportError: If generated flow module cannot be imported

    Example:
        >>> import kapten
        >>> # Run a single task
        >>> kapten.run("fruit_summary", project_dir="example/duckdb_example")
        >>>
        >>> # Run multiple tasks, ignoring cache
        >>> kapten.run(["fruit_metrics", "fruit_summary"], force=True)
        >>>
        >>> # Run all tasks
        >>> kapten.run(project_dir="example/duckdb_example")

    Note:
        You must run `kapten codegen` first to generate the flow file before
        using this function.
    """
    project_path = Path(project_dir).resolve()
    config_path = project_path / "kapten.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"kapten.yaml not found at {config_path}. "
            f"Make sure project_dir points to a valid Kapten project."
        )

    # Read kapten.yaml to get pipeline name and flows-dir
    with open(config_path) as f:
        config = yaml.safe_load(f)

    graphs = config.get('graphs', {})
    if not graphs:
        raise ValueError(f"No graphs found in {config_path}")
    if len(graphs) > 1:
        graph_names = ", ".join(sorted(graphs.keys()))
        raise ValueError(
            f"Multiple graphs found ({graph_names}) in {config_path}. "
            "This function only supports single-graph projects. "
            "Please use the generated flow file directly for multi-graph projects."
        )

    pipeline_name = next(iter(graphs.keys()))
    flows_dir = config.get('settings', {}).get('flows-dir', '.')

    # Import the generated flow module
    flow_file = project_path / flows_dir / f"{pipeline_name}.py"
    if not flow_file.exists():
        raise FileNotFoundError(
            f"Generated flow file not found at {flow_file}. "
            f"Run 'kapten codegen -p {project_dir}' first."
        )

    spec = importlib.util.spec_from_file_location(pipeline_name, flow_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to load module from {flow_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Execute the run_pipeline function
    if not hasattr(module, 'run_pipeline'):
        raise ImportError(
            f"Generated flow file {flow_file} does not have a run_pipeline function. "
            "Please regenerate it with 'kapten codegen'."
        )

    run_pipeline_func = getattr(module, 'run_pipeline')
    run_pipeline_func(
        task_names=task_names,
        force=force,
        config_path=str(config_path)
    )