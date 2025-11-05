# kptn package
__version__ = "0.1.0"

import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Union

import yaml

from kptn.util.pipeline_config import (
    PipelineConfig,
    _module_path_from_dir,
    normalise_dir_setting,
)


def _coerce_task_list(task_names: Union[str, list[str], None]) -> list[str]:
    """Normalise task input into a list for flow execution."""
    if task_names is None:
        return []
    if isinstance(task_names, str):
        return [task.strip() for task in task_names.split(",") if task.strip()]
    return list(task_names)


def _build_pipeline_config_for_run(
    config: dict,
    pipeline_name: str,
    project_path: Path,
) -> PipelineConfig:
    """Construct a PipelineConfig from kptn.yaml for flow execution."""
    settings = config.get("settings", {})
    py_tasks_dir_setting = normalise_dir_setting(
        settings.get("py_tasks_dir"),
        setting_name="py_tasks_dir",
    )
    if not py_tasks_dir_setting:
        raise RuntimeError("Missing 'py_tasks_dir' in kptn.yaml settings")

    module_path = _module_path_from_dir(py_tasks_dir_setting[0])
    tasks_config_path = (project_path / "kptn.yaml").resolve()
    resolved_py_dirs = []
    for entry in py_tasks_dir_setting:
        entry_path = Path(entry)
        resolved_py_dirs.append(str((project_path / entry_path).resolve() if not entry_path.is_absolute() else entry_path.resolve()))

    r_tasks_dir_setting_raw = settings.get("r_tasks_dir", ".")
    r_tasks_dir_setting = normalise_dir_setting(
        r_tasks_dir_setting_raw,
        setting_name="r_tasks_dir",
    )
    if not r_tasks_dir_setting:
        r_tasks_dir_setting = ["."]

    resolved_r_dirs = []
    for entry in r_tasks_dir_setting:
        entry_path = Path(entry)
        resolved_r_dirs.append(str((project_path / entry_path).resolve() if not entry_path.is_absolute() else entry_path.resolve()))

    pipeline_kwargs: dict[str, Union[str, bool, list[str]]] = {
        "PIPELINE_NAME": pipeline_name,
        "PY_MODULE_PATH": module_path,
        "TASKS_CONFIG_PATH": str(tasks_config_path),
        "SUBSET_MODE": False,
    }

    pipeline_kwargs["PY_TASKS_DIRS"] = list(resolved_py_dirs)
    pipeline_kwargs["R_TASKS_DIRS"] = list(resolved_r_dirs)

    storage_key = settings.get("storage_key")
    if storage_key:
        pipeline_kwargs["STORAGE_KEY"] = str(storage_key)

    branch = settings.get("branch")
    if branch:
        pipeline_kwargs["BRANCH"] = str(branch)

    return PipelineConfig(**pipeline_kwargs)


def run(
    task_names: Union[str, list[str], None] = None,
    project_dir: str = ".",
    force: bool = False
) -> None:
    """Run a kptn pipeline task programmatically

    This function provides a Python API to execute kptn pipeline tasks
    without using the CLI. It imports and executes the generated flow file.

    Args:
        task_names: Specific task(s) to run. Can be:
            - Single task name: "fruit_summary"
            - Comma-separated string: "fruit_metrics,fruit_summary"
            - List of task names: ["fruit_metrics", "fruit_summary"]
            - None: Run all tasks (default)
        project_dir: Path to directory containing kptn.yaml (default: current directory)
        force: Ignore cached results and force all tasks to run (default: False)

    Raises:
        FileNotFoundError: If kptn.yaml or generated flow file not found
        ValueError: If invalid task names provided or multiple graphs found
        ImportError: If generated flow module cannot be imported

    Example:
        >>> import kptn
        >>> # Run a single task
        >>> kptn.run("fruit_summary", project_dir="example/duckdb_example")
        >>>
        >>> # Run multiple tasks, ignoring cache
        >>> kptn.run(["fruit_metrics", "fruit_summary"], force=True)
        >>>
        >>> # Run all tasks
        >>> kptn.run(project_dir="example/duckdb_example")

    Note:
        You must run `kptn codegen` first to generate the flow file before
        using this function.
    """
    project_path = Path(project_dir).resolve()
    config_path = project_path / "kptn.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"kptn.yaml not found at {config_path}. "
            f"Make sure project_dir points to a valid kptn project."
        )

    # Read kptn.yaml to get pipeline name and flows_dir
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
    flows_dir = config.get('settings', {}).get('flows_dir', '.')

    # Import the generated flow module
    flow_file = project_path / flows_dir / f"{pipeline_name}.py"
    if not flow_file.exists():
        raise FileNotFoundError(
            f"Generated flow file not found at {flow_file}. "
            f"Run 'kptn codegen -p {project_dir}' first."
        )

    spec = importlib.util.spec_from_file_location(pipeline_name, flow_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to load module from {flow_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    flow_func = getattr(module, pipeline_name, None)
    if flow_func is None and hasattr(module, "run_pipeline"):
        run_pipeline_func = getattr(module, "run_pipeline")
        run_pipeline_func(
            task_names=task_names,
            force=force,
            config_path=str(config_path)
        )
        return

    if flow_func is None:
        raise ImportError(
            f"Generated flow file {flow_file} does not define a '{pipeline_name}' function "
            "and no legacy run_pipeline fallback was found. "
            "Please regenerate it with 'kptn codegen'."
        )

    task_list = _coerce_task_list(task_names)
    signature = inspect.signature(flow_func)
    kwargs = {}

    if "task_list" in signature.parameters:
        kwargs["task_list"] = task_list
    elif "tasks" in signature.parameters:
        kwargs["tasks"] = task_list

    if "ignore_cache" in signature.parameters:
        kwargs["ignore_cache"] = force
    elif "force" in signature.parameters:
        kwargs["force"] = force

    if "pipeline_config" in signature.parameters:
        pipeline_config = _build_pipeline_config_for_run(config, pipeline_name, project_path)
        kwargs["pipeline_config"] = pipeline_config

    if "config_path" in signature.parameters:
        kwargs["config_path"] = str(config_path)

    sys_path_added = False
    project_dir_str = str(project_path)
    if project_dir_str not in sys.path:
        sys.path.insert(0, project_dir_str)
        sys_path_added = True

    try:
        flow_func(**kwargs)
    finally:
        if sys_path_added:
            try:
                sys.path.remove(project_dir_str)
            except ValueError:
                pass
