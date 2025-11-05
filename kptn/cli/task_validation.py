from __future__ import annotations

import ast
import inspect
import glob
import re
from pathlib import Path
from typing import Any

from kptn.util.pipeline_config import (
    PipelineConfig,
    _module_path_from_dir,
    normalise_dir_setting,
)
from kptn.util.runtime_config import RuntimeConfig, ensure_pythonpath
from kptn.util.task_args import (
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
    module_path: str | None = None
    resolved_py_dirs: list[str] = []
    py_tasks_dir_setting = settings.get("py_tasks_dir")
    if py_tasks_dir_setting is not None:
        py_tasks_dir_values = normalise_dir_setting(
            py_tasks_dir_setting,
            setting_name="py_tasks_dir",
        )
        if py_tasks_dir_values:
            module_path = _module_path_from_dir(py_tasks_dir_values[0])
            for entry in py_tasks_dir_values:
                entry_path = Path(entry)
                resolved = (
                    (project_root / entry_path).resolve()
                    if not entry_path.is_absolute()
                    else entry_path.resolve()
                )
                resolved_py_dirs.append(str(resolved))

    project_root = project_root.resolve()
    tasks_config_path = (project_root / "kptn.yaml").resolve()

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
        resolved = (
            (project_root / entry_path).resolve()
            if not entry_path.is_absolute()
            else entry_path.resolve()
        )
        resolved_r_dirs.append(str(resolved))

    pipeline_kwargs: dict[str, Any] = {
        "PIPELINE_NAME": pipeline_name,
        "TASKS_CONFIG_PATH": str(tasks_config_path),
        "SUBSET_MODE": subset_mode,
    }

    if module_path:
        pipeline_kwargs["PY_MODULE_PATH"] = module_path
    if resolved_py_dirs:
        pipeline_kwargs["PY_TASKS_DIRS"] = list(resolved_py_dirs)
    pipeline_kwargs["R_TASKS_DIRS"] = list(resolved_r_dirs)

    storage_key = settings.get("storage_key")
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
    """Validate kptn tasks, including Python signatures and required files."""
    base_dir = base_dir.resolve()
    settings = kap_conf.get("settings") or {}
    tasks_def = kap_conf.get("tasks") or {}
    graphs = kap_conf.get("graphs") or {}

    if not tasks_def or not graphs:
        return []

    py_tasks_dir_values: list[str] = []
    resolved_py_dirs: list[Path] = []
    py_tasks_dir_entry = settings.get("py_tasks_dir")
    if py_tasks_dir_entry is not None:
        try:
            py_tasks_dir_values = normalise_dir_setting(
                py_tasks_dir_entry,
                setting_name="py_tasks_dir",
            )
        except (TypeError, ValueError) as exc:
            return [f"Invalid 'py_tasks_dir' setting: {exc}"]

        for entry in py_tasks_dir_values:
            entry_path = Path(entry)
            resolved = (
                (base_dir / entry_path).resolve()
                if not entry_path.is_absolute()
                else entry_path.resolve()
            )
            resolved_py_dirs.append(resolved)

    errors: list[str] = []
    if resolved_py_dirs and not any(path.exists() for path in resolved_py_dirs):
        joined_dirs = ", ".join(str(path) for path in resolved_py_dirs)
        errors.append(
            f"Python tasks directories {py_tasks_dir_values} not found (checked: {joined_dirs})"
        )

    runtime_configs: dict[str, RuntimeConfig] = {}
    r_task_roots: dict[str, list[Path]] = {}
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

        r_dirs = [
            Path(dir_path).resolve()
            for dir_path in getattr(pipeline_config, "R_TASKS_DIRS", ())
            if dir_path
        ]
        if not r_dirs:
            r_dirs = [base_dir.resolve()]
        r_task_roots[pipeline_name] = r_dirs

        try:
            ensure_pythonpath(
                base_dir,
                pipeline_config.PY_MODULE_PATH or None,
                getattr(pipeline_config, "PY_TASKS_DIRS", None),
            )
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
            file_suffix = Path(file_path_str).suffix.lower()

            if file_suffix == ".r":
                if "${" in file_path_str and "}" in file_path_str:
                    glob_pattern = re.sub(r"\$\{[^}/]+}", "*", file_path_str)
                    matches: list[Path] = []
                    script_path = Path(file_path_str)
                    if script_path.is_absolute():
                        matches = [Path(match).resolve() for match in glob.glob(glob_pattern)]
                    else:
                        r_roots = r_task_roots.get(pipeline_name) or [base_dir]
                        for root in r_roots:
                            root_path = Path(root)
                            root_matches = [
                                candidate.resolve()
                                for candidate in root_path.glob(glob_pattern)
                            ]
                            if root_matches:
                                matches = root_matches
                                break
                    if matches:
                        continue
                    # Treat unresolved template values as wildcards during validation.
                    continue

                script_path = Path(file_path_str)
                if not script_path.is_absolute():
                    r_roots = r_task_roots.get(pipeline_name) or []
                    candidate = None
                    for root in r_roots:
                        potential = (root / script_path).resolve()
                        if potential.exists():
                            candidate = potential
                            break
                    if candidate is None:
                        fallback_root = r_roots[0] if r_roots else base_dir
                        script_path = (fallback_root / script_path).resolve()
                    else:
                        script_path = candidate
                else:
                    script_path = script_path.resolve()

                if not script_path.exists():
                    errors.append(
                        f"Graph '{pipeline_name}' task '{task_name}': R file '{file_path_str}' not found (expected at {script_path})"
                    )
                continue

            if file_suffix != ".py":
                continue
            function_name = func_name or task_name

            script_path = Path(file_path_str)
            if script_path.is_absolute():
                script_path = script_path.resolve()
            else:
                search_roots = [base_dir, *resolved_py_dirs]
                candidate = None
                for root in search_roots:
                    potential = (root / script_path).resolve()
                    if potential.exists():
                        candidate = potential
                        break
                if candidate is None:
                    fallback_root = search_roots[0]
                    script_path = (fallback_root / script_path).resolve()
                else:
                    script_path = candidate

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
                    f"Graph '{pipeline_name}' task '{task_name}': function '{function_name}' requires parameter(s) {joined}, but kptn only provides {provided_names} from its dependencies"
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
