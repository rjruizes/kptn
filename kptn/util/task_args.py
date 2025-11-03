from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class TaskArgumentPlan:
    """Details about the arguments kptn will supply to a task."""

    expected_kwargs: set[str]
    alias_lookup: dict[str, str]
    errors: list[str]


def parse_task_file_spec(file_spec: str) -> tuple[str, str | None]:
    """Split a ``file`` entry into a path and optional function name."""
    spec = file_spec.strip()
    if ":" not in spec:
        return spec, None
    file_part, func_part = spec.rsplit(":", 1)
    func_part = func_part.strip() or None
    return file_part.strip(), func_part


def normalise_dependency_spec(spec: Any) -> list[str]:
    """Convert a dependency specification into a list of task names."""
    if spec is None:
        return []
    if isinstance(spec, list):
        return [item for item in spec if isinstance(item, str) and item.strip()]
    if isinstance(spec, str):
        stripped = spec.strip()
        return [stripped] if stripped else []
    return []


def resolve_dependency_key(
    task_spec: Mapping[str, Any],
    dep_name: str,
    dep_spec: Mapping[str, Any] | None,
    alias_lookup: Mapping[str, str],
) -> str | None:
    """Determine the keyword kptn will use for a dependency's cached data."""

    if dep_spec is None or dep_spec.get("cache_result") is not True:
        return None

    if "map_over" in task_spec:
        iterable_item = dep_spec.get("iterable_item")
        if isinstance(iterable_item, str) and iterable_item:
            return iterable_item

    alias = alias_lookup.get(dep_name)
    if isinstance(alias, str) and alias:
        return alias

    return dep_name


def build_task_argument_plan(
    task_name: str,
    task_spec: Mapping[str, Any],
    dependencies: Iterable[str],
    tasks_def: Mapping[str, Any],
) -> TaskArgumentPlan:
    """Infer the keyword arguments kptn will provide to a Python task."""

    expected: set[str] = set()
    alias_lookup: dict[str, str] = {}
    errors: list[str] = []

    args_spec = task_spec.get("args")
    if isinstance(args_spec, Mapping):
        for arg_name, arg_value in args_spec.items():
            expected.add(arg_name)
            if isinstance(arg_value, Mapping) and "ref" in arg_value:
                ref_target = arg_value["ref"]
                if isinstance(ref_target, str):
                    alias_lookup[ref_target] = arg_name
                else:
                    errors.append(
                        f"args.{arg_name} has unsupported ref target {ref_target!r}"
                    )

    dependency_names = list(dependencies)
    dependency_set = set(dependency_names)
    for ref_target, arg_name in alias_lookup.items():
        if ref_target not in dependency_set:
            errors.append(
                f"args.{arg_name} references '{ref_target}', but it is not listed as a dependency"
            )

    map_over_value = task_spec.get("map_over")
    if isinstance(map_over_value, str):
        parts = [part.strip() for part in map_over_value.split(",") if part.strip()]
        expected.update(parts)

    for dep_name in dependency_names:
        dep_spec = tasks_def.get(dep_name)
        if not isinstance(dep_spec, Mapping):
            continue
        key = resolve_dependency_key(task_spec, dep_name, dep_spec, alias_lookup)
        if not key:
            continue
        parts = [part.strip() for part in key.split(",") if part.strip()]
        expected.update(parts)

    return TaskArgumentPlan(expected, alias_lookup, errors)


_MISSING = object()


def _runtime_lookup(runtime_config: Any | None, name: str) -> Any:
    if runtime_config is None:
        return _MISSING
    if name == "runtime_config":
        return runtime_config
    try:
        return getattr(runtime_config, name)
    except AttributeError:
        return _MISSING


def plan_python_call(
    signature: inspect.Signature,
    provided_kwargs: Mapping[str, Any],
    runtime_config: Any | None,
) -> tuple[list[Any], dict[str, Any], list[str]]:
    """Plan positional and keyword arguments for a Python task call.

    Returns the positional arguments, keyword arguments, and a list of missing
    parameter names that could not be satisfied.
    """

    kwargs = dict(provided_kwargs)
    call_args: list[Any] = []
    missing: list[str] = []

    for param in signature.parameters.values():
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            continue
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            continue

        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            if param.name in kwargs or param.default is not inspect._empty:
                continue
            missing.append(param.name)
            continue

            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                if param.name in kwargs:
                    call_args.append(kwargs.pop(param.name))
                    continue

            value = _runtime_lookup(runtime_config, param.name)
            if value is _MISSING:
                if param.default is inspect._empty:
                    missing.append(param.name)
                continue
            call_args.append(value)
            continue

        if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            if param.name in kwargs:
                continue

            value = _runtime_lookup(runtime_config, param.name)
            if value is _MISSING:
                if param.default is inspect._empty:
                    missing.append(param.name)
                continue

            kwargs[param.name] = value

    return call_args, kwargs, missing
