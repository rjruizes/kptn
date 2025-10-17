from __future__ import annotations

import ast
import inspect
from pathlib import Path
from typing import Any

from kapten.util.pipeline_config import PipelineConfig, _module_path_from_dir
from kapten.util.runtime_config import RuntimeConfig, ensure_pythonpath
from kapten.util.task_args import (
    build_task_argument_plan,
    normalise_dependency_spec,
    parse_task_file_spec,
    plan_python_call,
)

_AST_DEFAULT = object()


def _build_pipeline_config(
    kap_conf: dict[str, Any],
    pipeline_name: str,
    project_root: Path,
    subset_mode: bool,
) -> PipelineConfig:
    settings = kap_conf.get("settings", {})
    py_tasks_dir = settings.get("py-tasks-dir")
    if not py_tasks_dir:
        raise RuntimeError("Missing 'py-tasks-dir' in kapten.yaml settings")

    module_path = _module_path_from_dir(py_tasks_dir)
    project_root = project_root.resolve()
    tasks_config_path = (project_root / "kapten.yaml").resolve()
    r_tasks_dir_setting = settings.get("r-tasks-dir", ".")
    r_tasks_dir_path = (project_root / Path(r_tasks_dir_setting)).resolve()

    pipeline_kwargs: dict[str, Any] = {
        "PIPELINE_NAME": pipeline_name,
        "PY_MODULE_PATH": module_path,
        "TASKS_CONFIG_PATH": str(tasks_config_path),
        "R_TASKS_DIR_PATH": str(r_tasks_dir_path),
        "SUBSET_MODE": subset_mode,
    }

    storage_key = settings.get("storage-key") or settings.get("storage_key")
    if storage_key:
        pipeline_kwargs["STORAGE_KEY"] = str(storage_key)

    branch = settings.get("branch")
    if branch:
        pipeline_kwargs["BRANCH"] = str(branch)

    return PipelineConfig(**pipeline_kwargs)


def _load_python_function_signature(
    script_path: Path, function_name: str
) -> inspect.Signature:
    """Parse a Python module and return the signature for ``function_name``."""
    try:
        source = script_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:  # pragma: no cover - validated earlier
        raise FileNotFoundError(
            f"Python file '{script_path}' not found"
        ) from exc
    except OSError as exc:  # pragma: no cover - unlikely, defensive
        raise OSError(f"Unable to read '{script_path}': {exc}") from exc

    try:
        tree = ast.parse(source, filename=str(script_path))
    except SyntaxError as exc:
        raise ValueError(f"Unable to parse '{script_path}': {exc}") from exc

    target_node: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            target_node = node
            break
    if target_node is None:
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
                target_node = node
                break
    if target_node is None:
        raise ValueError(f"Function '{function_name}' not found in '{script_path}'")

    func_args = target_node.args
    parameters: list[inspect.Parameter] = []

    positional_nodes = list(func_args.posonlyargs) + list(func_args.args)
    num_defaults = len(func_args.defaults)
    default_start = len(positional_nodes) - num_defaults if num_defaults else len(positional_nodes)

    for index, arg in enumerate(func_args.posonlyargs):
        default = inspect._empty
        if index >= default_start:
            default = _AST_DEFAULT
        parameters.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.POSITIONAL_ONLY,
                default=default,
            )
        )

    for offset, arg in enumerate(func_args.args):
        index = len(func_args.posonlyargs) + offset
        default = inspect._empty
        if index >= default_start:
            default = _AST_DEFAULT
        parameters.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=default,
            )
        )

    if func_args.vararg is not None:
        parameters.append(
            inspect.Parameter(
                func_args.vararg.arg,
                inspect.Parameter.VAR_POSITIONAL,
            )
        )

    if func_args.kwonlyargs:
        kw_defaults = func_args.kw_defaults or []
        for idx, kw_arg in enumerate(func_args.kwonlyargs):
            default_value = kw_defaults[idx] if idx < len(kw_defaults) else None
            default = inspect._empty if default_value is None else _AST_DEFAULT
            parameters.append(
                inspect.Parameter(
                    kw_arg.arg,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=default,
                )
            )

    if func_args.kwarg is not None:
        parameters.append(
            inspect.Parameter(
                func_args.kwarg.arg,
                inspect.Parameter.VAR_KEYWORD,
            )
        )

    return inspect.Signature(parameters)


def _validate_python_tasks(base_dir: Path, kap_conf: dict[str, Any]) -> list[str]:
    """Validate that Python tasks accept the arguments Kapten will provide."""
    settings = kap_conf.get("settings") or {}
    py_tasks_dir_entry = settings.get("py-tasks-dir")
    tasks_def = kap_conf.get("tasks") or {}
    graphs = kap_conf.get("graphs") or {}

    if not py_tasks_dir_entry or not tasks_def or not graphs:
        return []

    py_tasks_dir = Path(py_tasks_dir_entry)
    if not py_tasks_dir.is_absolute():
        py_tasks_dir = (base_dir / py_tasks_dir).resolve()

    errors: list[str] = []
    if not py_tasks_dir.exists():
        errors.append(
            f"Python tasks directory '{py_tasks_dir_entry}' not found (resolved to {py_tasks_dir})"
        )

    runtime_configs: dict[str, RuntimeConfig] = {}
    pipelines_to_skip: set[str] = set()

    for pipeline_name in graphs:
        try:
            pipeline_config = _build_pipeline_config(
                kap_conf, pipeline_name, base_dir, subset_mode=False
            )
        except (RuntimeError, ValueError) as exc:
            errors.append(
                f"Graph '{pipeline_name}': unable to build pipeline configuration: {exc}"
            )
            pipelines_to_skip.add(pipeline_name)
            continue

        try:
            ensure_pythonpath(base_dir, pipeline_config.PY_MODULE_PATH or None)
            runtime_config = RuntimeConfig.from_tasks_config(
                kap_conf,
                base_dir=base_dir,
                fallback=pipeline_config,
            )
        except Exception as exc:
            errors.append(
                f"Graph '{pipeline_name}': unable to build runtime configuration: {exc}"
            )
            pipelines_to_skip.add(pipeline_name)
            continue

        runtime_configs[pipeline_name] = runtime_config

    for pipeline_name, pipeline_data in graphs.items():
        if pipeline_name in pipelines_to_skip:
            continue

        tasks_block = pipeline_data.get("tasks") if isinstance(pipeline_data, dict) else None
        if not isinstance(tasks_block, dict):
            continue
        runtime_config = runtime_configs.get(pipeline_name)
        for task_name, dependency_spec in tasks_block.items():
            task_config = tasks_def.get(task_name)
            if not isinstance(task_config, dict):
                errors.append(
                    f"Graph '{pipeline_name}' references unknown task '{task_name}'"
                )
                continue

            file_entry = task_config.get("file")
            if not isinstance(file_entry, str):
                continue
            file_path_str, func_name = parse_task_file_spec(file_entry)
            if Path(file_path_str).suffix.lower() != ".py":
                continue
            function_name = func_name or task_name

            script_path = Path(file_path_str)
            if not script_path.is_absolute():
                script_path = (py_tasks_dir / script_path).resolve()
            else:
                script_path = script_path.resolve()

            if not script_path.exists():
                errors.append(
                    f"Graph '{pipeline_name}' task '{task_name}': Python file '{file_path_str}' not found (expected at {script_path})"
                )
                continue

            try:
                signature = _load_python_function_signature(script_path, function_name)
            except (FileNotFoundError, OSError, ValueError) as exc:
                errors.append(
                    f"Graph '{pipeline_name}' task '{task_name}': {exc}"
                )
                continue
            dependencies = normalise_dependency_spec(dependency_spec)
            plan = build_task_argument_plan(
                task_name, task_config, dependencies, tasks_def
            )
            for msg in plan.errors:
                errors.append(f"Graph '{pipeline_name}' task '{task_name}': {msg}")

            provided_kwargs = {name: object() for name in plan.expected_kwargs}
            _, _, missing_params = plan_python_call(
                signature,
                provided_kwargs,
                runtime_config,
            )

            if missing_params:
                provided_names = sorted(plan.expected_kwargs)
                joined = ", ".join(sorted(missing_params))
                errors.append(
                    f"Graph '{pipeline_name}' task '{task_name}': function '{function_name}' requires parameter(s) {joined}, but Kapten only provides {provided_names} from its dependencies"
                )

            has_var_keywords = any(
                param.kind == inspect.Parameter.VAR_KEYWORD
                for param in signature.parameters.values()
            )

            if not has_var_keywords:
                accepted_names = {
                    param.name
                    for param in signature.parameters.values()
                    if param.kind
                    in (
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.KEYWORD_ONLY,
                    )
                }
                unexpected = [
                    name for name in plan.expected_kwargs if name not in accepted_names
                ]
                if unexpected:
                    joined = ", ".join(sorted(unexpected))
                    errors.append(
                        f"Graph '{pipeline_name}' task '{task_name}': function '{function_name}' does not accept argument(s) {joined}"
                    )

    return errors


__all__ = [
    "_build_pipeline_config",
    "_validate_python_tasks",
]
