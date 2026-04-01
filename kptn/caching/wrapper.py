"""Discover subtasks inside wrapper task functions via AST analysis.

A *wrapper task* is a Python function marked with ``wrapper: true`` in
``kptn.yaml`` that orchestrates several lower-level tasks.  This module
inspects the wrapper's source code to find which known tasks it calls
(direct calls only, not transitive) and returns them in source order.
"""

from __future__ import annotations

import ast
import logging
from collections import OrderedDict
from pathlib import Path
from typing import Any, Mapping

from kptn.caching.Hasher import FunctionRef, PythonFunctionAnalyzer
from kptn.codegen.codegen import parse_python_task_spec
from kptn.util.logger import get_logger

logger = get_logger()


def _build_task_function_lookup(
    tasks_dict: Mapping[str, Any],
) -> dict[tuple[str, str], str]:
    """Build a mapping from (module_name, function_name) → task_name.

    This lets us match an AST-resolved ``FunctionRef`` back to the kptn
    task that owns it.
    """
    lookup: dict[tuple[str, str], str] = {}
    for task_name, task_config in tasks_dict.items():
        if not isinstance(task_config, Mapping):
            continue
        spec = parse_python_task_spec(task_name, task_config)
        if spec is None:
            continue
        key = (spec["module"], spec["function"])
        lookup[key] = task_name
    return lookup


def _iter_direct_call_targets(
    analyzer: PythonFunctionAnalyzer,
    wrapper_file: Path,
    wrapper_function: str,
) -> list[FunctionRef]:
    """Return the *direct* call targets of a wrapper function in source order.

    Unlike ``_collect_closure`` which does a full transitive DFS, this
    only looks at the immediate body of the wrapper function.
    """
    summary = analyzer._load_module_from_path(wrapper_file)
    if summary is None:
        raise FileNotFoundError(f"Unable to parse module at {wrapper_file}")
    node = summary.get_function(wrapper_function)
    if node is None:
        raise KeyError(f"Function '{wrapper_function}' not found in {wrapper_file}")

    # Collect call targets preserving first-occurrence source order.
    seen: set[FunctionRef] = set()
    ordered: list[FunctionRef] = []

    # Walk the function body looking for Call nodes, but skip nested defs.
    stack: list[ast.AST] = list(ast.iter_child_nodes(node))
    while stack:
        current = stack.pop(0)  # BFS to preserve rough source order
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for child in ast.iter_child_nodes(current):
            stack.append(child)
        if isinstance(current, ast.Call):
            func = current.func
            if isinstance(func, ast.Name):
                kind, payload = "name", func.id
            elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                kind, payload = "attr", (func.value.id, func.attr)
            else:
                continue
            ref = analyzer._resolve_call_target(summary, kind, payload)
            if ref is not None and ref not in seen:
                seen.add(ref)
                ordered.append(ref)
    return ordered


def discover_wrapper_subtasks(
    wrapper_task_name: str,
    tasks_dict: Mapping[str, Any],
    *,
    py_dirs: list[str] | None = None,
) -> list[str]:
    """Discover the ordered list of known-task subtasks inside a wrapper.

    Parameters
    ----------
    wrapper_task_name:
        Name of the wrapper task (must exist in *tasks_dict* with
        ``wrapper: true``).
    tasks_dict:
        The full ``tasks`` block from ``kptn.yaml``.
    py_dirs:
        Python search directories forwarded to ``PythonFunctionAnalyzer``.

    Returns
    -------
    list[str]
        Task names (keys in *tasks_dict*) that the wrapper function calls,
        in the order they appear in the source code.

    Raises
    ------
    ValueError
        If the task is not marked as a wrapper or is not a Python task.
    """
    wrapper_config = tasks_dict.get(wrapper_task_name)
    if not isinstance(wrapper_config, Mapping):
        raise ValueError(f"Task '{wrapper_task_name}' not found in tasks dict")
    if not wrapper_config.get("wrapper"):
        raise ValueError(f"Task '{wrapper_task_name}' is not marked as a wrapper")

    wrapper_spec = parse_python_task_spec(wrapper_task_name, wrapper_config)
    if wrapper_spec is None:
        raise ValueError(f"Wrapper task '{wrapper_task_name}' must be a Python task")

    # Build the lookup of (module, function) → task_name for all tasks
    # except the wrapper itself.
    func_lookup = _build_task_function_lookup(tasks_dict)
    # Remove the wrapper from the lookup so it doesn't match itself
    wrapper_key = (wrapper_spec["module"], wrapper_spec["function"])
    func_lookup.pop(wrapper_key, None)

    # Also build a function-name-only lookup as a fallback for functions
    # defined in the same module or imported without a module qualifier.
    func_name_lookup: dict[str, str] = {}
    for (mod, func), task_name in func_lookup.items():
        # Only use name-only lookup when unambiguous
        if func not in func_name_lookup:
            func_name_lookup[func] = task_name
        else:
            # Ambiguous – two tasks share a function name; skip name-only match
            func_name_lookup[func] = None  # type: ignore[assignment]

    analyzer = PythonFunctionAnalyzer(py_dirs)

    # Resolve the wrapper's file path
    wrapper_file = Path(wrapper_spec["file_path"])
    if not wrapper_file.is_absolute():
        for d in (py_dirs or []):
            candidate = Path(d) / wrapper_file
            if candidate.exists():
                wrapper_file = candidate.resolve()
                break
        else:
            cwd_candidate = Path.cwd() / wrapper_file
            if cwd_candidate.exists():
                wrapper_file = cwd_candidate.resolve()

    direct_refs = _iter_direct_call_targets(analyzer, wrapper_file, wrapper_spec["function"])

    # Match refs against known tasks
    subtasks: list[str] = []
    seen_tasks: set[str] = set()
    for ref in direct_refs:
        key = (ref.module, ref.name)
        task_name = func_lookup.get(key)
        if task_name is None:
            # Fallback: match by function name only
            candidate = func_name_lookup.get(ref.name)
            if candidate is not None:
                task_name = candidate
        if task_name and task_name not in seen_tasks:
            seen_tasks.add(task_name)
            subtasks.append(task_name)

    logger.info(
        "Wrapper '%s' discovered %d subtask(s): %s",
        wrapper_task_name,
        len(subtasks),
        subtasks,
    )
    return subtasks
