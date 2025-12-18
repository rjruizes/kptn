import subprocess
from os import path
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional
import jinja2
from jinja2 import TemplateNotFound

from kptn.read_config import read_config
from kptn.codegen.lib.setup_jinja_env import debug
from kptn.codegen.lib.stepfunctions import build_stepfunctions_flow_context
from kptn.util.filepaths import codegen_dir
from kptn.util.pipeline_config import normalise_dir_setting
from kptn.util.read_tasks_config import read_tasks_config

PYTHON_FILE_SUFFIXES = {".py", ".pyw"}

DEFAULT_FLOW_CONFIG: dict[str, Any] = {
    "flow_template": "flows.py.jinja",
    "tasks_init_template": "tasks_init.py.jinja",
    "flow_extension": ".py",
}

FLOW_TYPE_CONFIG: dict[str, dict[str, Any]] = {
    "prefect": {
        "flow_template": "flows.py.jinja",
        "tasks_init_template": "tasks_init.py.jinja",
        "flow_extension": ".py",
    },
    "vanilla": {
        "flow_template": "flows.py.jinja",
        "tasks_init_template": "tasks_init.py.jinja",
        "flow_extension": ".py",
    },
    "stepfunctions": {
        "flow_template": None,
        "tasks_init_template": "tasks_init.py.jinja",
        "run_template": "run.py.jinja",
        "flow_extension": ".json.tpl",
        "context_builder": build_stepfunctions_flow_context,
    },
}


def is_python_task(task_config: dict) -> bool:
    file_value = task_config.get("file")
    if not file_value:
        return False
    file_path, _, _ = file_value.partition(":")
    return Path(file_path).suffix.lower() in PYTHON_FILE_SUFFIXES


def parse_python_task_spec(task_name: str, task_config: dict) -> dict | None:
    file_value = task_config.get("file")
    if not file_value:
        return None
    if ":" in file_value:
        file_path, func_name = file_value.rsplit(":", 1)
    else:
        file_path, func_name = file_value, None
    file_path = file_path.strip()
    if not file_path:
        return None
    suffix = Path(file_path).suffix.lower()
    if suffix not in PYTHON_FILE_SUFFIXES:
        return None
    func_name = func_name.strip() if func_name and func_name.strip() else task_name
    module_path = file_path
    if suffix:
        module_path = module_path[: -len(suffix)]
    module_path = module_path.replace("/", ".").replace("\\", ".")
    return {
        "module": module_path,
        "function": func_name,
        "file_path": file_path,
    }


def _normalize_dependencies(dependencies: Any) -> list[str]:
    """Normalize task dependency declarations into a list of task names."""
    if dependencies is None:
        return []
    if isinstance(dependencies, str):
        value = dependencies.strip()
        return [value] if value else []
    return [dep for dep in dependencies if dep]


def _normalize_extends(value: Any, *, graph_name: str) -> list[dict[str, Any]]:
    """Coerce the extends field into a list of graph entries with optional arg overrides."""
    if value is None:
        return []

    def _normalise_entry(raw: Any) -> dict[str, Any]:
        if isinstance(raw, str):
            cleaned = raw.strip()
            if not cleaned:
                raise ValueError(f"Graph '{graph_name}' has an empty extends entry")
            return {"graph": cleaned, "args": None}
        if isinstance(raw, Mapping):
            target = raw.get("graph")
            if not isinstance(target, str) or not target.strip():
                raise TypeError(
                    f"Graph '{graph_name}' extends entry must include non-empty 'graph'"
                )
            args = raw.get("args")
            if args is not None and not isinstance(args, Mapping):
                raise TypeError(
                    f"Graph '{graph_name}' extends entry args must be a mapping if provided"
                )
            if isinstance(args, Mapping):
                for task_name, task_args in args.items():
                    if not isinstance(task_name, str) or not task_name.strip():
                        raise TypeError(
                            f"Graph '{graph_name}' extends args keys must be non-empty strings"
                        )
                    if task_args is not None and not isinstance(task_args, Mapping):
                        raise TypeError(
                            f"Graph '{graph_name}' extends args for task '{task_name}' must be a mapping"
                        )
            return {"graph": target.strip(), "args": args}
        raise TypeError(
            f"Graph '{graph_name}' has invalid extends entry; expected a string or object with 'graph'"
        )

    if isinstance(value, list):
        entries: list[dict[str, Any]] = []
        for entry in value:
            entries.append(_normalise_entry(entry))
        return entries

    return [_normalise_entry(value)]


def _validate_graph_dependencies(graph_name: str, tasks_lookup: Mapping[str, Any]) -> None:
    """Ensure that all declared dependencies exist within the flattened graph."""
    for task, entry in tasks_lookup.items():
        deps = entry.get("deps") if isinstance(entry, Mapping) else entry
        for dep in _normalize_dependencies(deps):
            if dep not in tasks_lookup:
                raise ValueError(
                    f"Graph '{graph_name}' task '{task}' depends on unknown task '{dep}'"
                )


def _flatten_graph(
    graph_name: str,
    graphs_block: Mapping[str, Any],
    *,
    memo: dict[str, dict[str, Any]],
    stack: list[str],
) -> dict[str, Any]:
    """
    Resolve a graph's tasks, expanding any inherited graphs defined via 'extends'.

    Parents are processed in order; tasks defined later in the resolution chain override
    earlier ones. Cycles and missing parents are rejected with clear errors.
    """
    if graph_name in memo:
        return memo[graph_name]
    if graph_name in stack:
        cycle = " -> ".join([*stack, graph_name])
        raise ValueError(f"Cycle detected in graph inheritance: {cycle}")

    graph_def = graphs_block.get(graph_name)
    if graph_def is None:
        raise ValueError(f"Graph '{graph_name}' is not defined but is referenced in extends")
    if not isinstance(graph_def, Mapping):
        raise ValueError(f"Graph '{graph_name}' must be a mapping")

    extends = _normalize_extends(graph_def.get("extends"), graph_name=graph_name)
    tasks_block = graph_def.get("tasks")
    if tasks_block is None:
        if not extends:
            raise ValueError(
                f"Graph '{graph_name}' must define a mapping of tasks or extend another graph"
            )
        tasks_block = {}
    if not isinstance(tasks_block, Mapping):
        raise ValueError(f"Graph '{graph_name}' must define a mapping of tasks")

    stack.append(graph_name)
    merged_tasks: dict[str, Any] = {}
    for parent_spec in extends:
        parent_name = parent_spec.get("graph")
        parent_tasks = _flatten_graph(parent_name, graphs_block, memo=memo, stack=stack)
        overrides = parent_spec.get("args") if isinstance(parent_spec, Mapping) else None
        overrides = overrides or {}
        for task_name, entry in parent_tasks.items():
            if task_name in merged_tasks:
                continue
            merged_entry = entry if isinstance(entry, Mapping) else {"deps": entry}
            override_args = overrides.get(task_name)
            if override_args:
                base_args = merged_entry.get("args") if isinstance(merged_entry, Mapping) else None
                combined_args = {}
                if isinstance(base_args, Mapping):
                    combined_args.update(base_args)
                combined_args.update(override_args)
                merged_entry = dict(merged_entry)
                merged_entry["args"] = combined_args
            merged_tasks[task_name] = merged_entry

    for task_name, deps in tasks_block.items():
        if task_name not in merged_tasks:
            merged_tasks[task_name] = deps
    stack.pop()

    _validate_graph_dependencies(graph_name, merged_tasks)
    memo[graph_name] = merged_tasks
    return merged_tasks


def _flatten_graphs(graphs_block: Mapping[str, Any], graph_names: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Flatten a collection of graphs, resolving inheritance for each requested graph."""
    memo: dict[str, dict[str, Any]] = {}
    for graph_name in graph_names:
        _flatten_graph(graph_name, graphs_block, memo=memo, stack=[])
    return {name: memo[name] for name in graph_names}


def relative_path_from_flows_dir_to_tasks_conf_path(kap_conf):
    """
    Get the relative path from the flows dir to the kptn.yaml file
    so that the generated flows can find the kptn.yaml file
    (e.g. "../../kptn.yaml")
    """
    flows_dir = Path(kap_conf['flows_dir'])
    tasks_conf_path = "kptn.yaml"
    return path.relpath(tasks_conf_path, flows_dir)

# def relative_path_from_flows_dir_to_py_tasks_dir(kap_conf, py_tasks_dir_entry: str):
#     """
#     Get the relative path from the flows dir to the py_tasks dir
#     so that the generated flows can import the tasks
#     (e.g. "../../py_tasks")
#     """
#     flows_dir = Path(kap_conf['flows_dir'])
#     py_tasks_dir = Path(py_tasks_dir_entry)
#     return path.relpath(py_tasks_dir, flows_dir)

def relative_path_from_flows_dir_to_r_tasks_dir(kap_conf, r_tasks_dir_entry: str | None):
    """
    Get the relative path from the flows dir to the r_tasks dir
    so that the generated flows can find the R tasks
    (e.g. "../../r_tasks")
    """
    if not r_tasks_dir_entry:
        return None
    flows_dir = Path(kap_conf['flows_dir'])
    r_tasks_dir = Path(r_tasks_dir_entry)
    return path.relpath(r_tasks_dir, flows_dir)

def _create_environment(flow_type: str) -> jinja2.Environment:
    templates_path = path.join(codegen_dir, "templates", flow_type)
    environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(templates_path),
    )
    environment.filters["debug"] = debug
    return environment


def generate_files(graph: str = None, emit_vanilla_runner: Optional[bool] = None):
    kap_conf = read_config()["settings"]
    root_dir = Path('.')
    flows_dir = root_dir / kap_conf['flows_dir']
    flows_dir.mkdir(parents=True, exist_ok=True)
    flow_type = kap_conf.get('flow_type', 'vanilla')
    if flow_type not in FLOW_TYPE_CONFIG:
        flow_type = 'vanilla'
    flow_config = FLOW_TYPE_CONFIG.get(
        flow_type,
        DEFAULT_FLOW_CONFIG,
    )
    environment = _create_environment(flow_type)
    tasks_conf_path = "kptn.yaml"
    conf = read_tasks_config(root_dir / tasks_conf_path)
    tasks_dict = conf['tasks']

    r_tasks_dir_values: list[str] = []
    if 'r_tasks_dir' in kap_conf:
        r_tasks_dir_values = normalise_dir_setting(
            kap_conf['r_tasks_dir'],
            setting_name='r_tasks_dir',
        )
    primary_r_tasks_dir = r_tasks_dir_values[0] if r_tasks_dir_values else None

    python_task_specs = {}
    for name, task in tasks_dict.items():
        spec = parse_python_task_spec(name, task)
        if spec:
            python_task_specs[name] = spec
    python_task_names = list(python_task_specs.keys())
    graphs_block = conf.get('graphs')
    if not isinstance(graphs_block, dict) or not graphs_block:
        raise ValueError("No graphs defined in kptn.yaml.")
    if graph:
        if graph not in graphs_block:
            available = ", ".join(sorted(graphs_block))
            raise ValueError(f"Graph '{graph}' not found; available graphs: {available}")
        graph_names = [graph]
    else:
        graph_names = list(graphs_block.keys())

    flattened_graphs = _flatten_graphs(graphs_block, graph_names)
    render_contexts: dict[str, dict[str, Any]] = {}
    # Write flows/*.py files
    for graph_name in graph_names:
        deps_lookup = flattened_graphs[graph_name]
        task_names = list(deps_lookup.keys())
        render_context: dict[str, Any] = {
            "pipeline_name": graph_name,
            "task_names": task_names,
            "tasks_dict": tasks_dict,
            "deps_lookup": deps_lookup,
            # "py_tasks_dir": py_tasks_module,
            "r_tasks_dir": primary_r_tasks_dir,
            "rel_tasks_conf_path": relative_path_from_flows_dir_to_tasks_conf_path(kap_conf),
            # "rel_py_tasks_dir": relative_path_from_flows_dir_to_py_tasks_dir(kap_conf, primary_py_tasks_dir),
            "rel_r_tasks_dir": relative_path_from_flows_dir_to_r_tasks_dir(kap_conf, primary_r_tasks_dir),
            "python_task_names": python_task_names,
            "python_task_specs": python_task_specs,
            "settings": kap_conf,
            "imports_slot": kap_conf.get("imports_slot"),
        }

        context_builder: Callable[..., dict[str, Any]] | None = flow_config.get('context_builder')
        if context_builder:
            extra_context = context_builder(
                pipeline_name=graph_name,
                task_names=task_names,
                deps_lookup=deps_lookup,
                tasks_dict=tasks_dict,
                kap_conf=kap_conf,
            )
            render_context.update(extra_context)

        flow_template_name = flow_config.get('flow_template')
        if flow_template_name:
            rendered = environment.get_template(flow_template_name).render(
                **render_context
            )
        else:
            state_machine_json = render_context.get('state_machine_json')
            if state_machine_json is None:
                raise ValueError(
                    "Step Functions context builder did not supply 'state_machine_json'"
                )
            rendered = f"{state_machine_json}\n"
        flow_extension = flow_config.get('flow_extension', '.py')
        output_file = path.join(flows_dir, f'{graph_name}{flow_extension}')
        with open(output_file, 'w') as f:
            f.write(rendered)
        render_contexts[graph_name] = render_context

    # Write run.py file for stepfunctions (once, not per graph)
    run_template_name = flow_config.get('run_template')
    if run_template_name:
        run_context = {
            "rel_tasks_conf_path": relative_path_from_flows_dir_to_tasks_conf_path(kap_conf),
            "imports_slot": kap_conf.get("imports_slot"),
        }
        run_rendered = environment.get_template(run_template_name).render(
            **run_context
        )
        run_output_file = path.join(flows_dir, 'run.py')
        with open(run_output_file, 'w') as f:
            f.write(run_rendered)

    # Write tasks/__init__.py file
    task_names = list(tasks_dict.keys())
    tasks_init_template_name = flow_config.get('tasks_init_template')
    if tasks_init_template_name:
        try:
            rendered = environment.get_template(tasks_init_template_name).render(
                task_names=task_names,
                tasks_dict=tasks_dict,
                python_task_names=python_task_names,
                python_task_specs=python_task_specs,
            )
        except TemplateNotFound:
            rendered = None
        # if rendered is not None:
        #     output_file = root_dir / Path(primary_py_tasks_dir) / '__init__.py'
        #     with open(output_file, 'w') as f:
        #         f.write(rendered)

    # Emit vanilla runner for stepfunctions projects
    if emit_vanilla_runner is None:
        emit_vanilla_runner = flow_type == "stepfunctions"

    if emit_vanilla_runner:
        vanilla_config = FLOW_TYPE_CONFIG["vanilla"]
        vanilla_environment = _create_environment("vanilla")
        vanilla_flow_template = vanilla_config.get("flow_template")
        vanilla_extension = vanilla_config.get("flow_extension", ".py")
        for graph_name, render_context in render_contexts.items():
            if not vanilla_flow_template:
                continue
            vanilla_rendered = vanilla_environment.get_template(
                vanilla_flow_template
            ).render(**render_context)
            vanilla_output_file = path.join(
                flows_dir, f"{graph_name}{vanilla_extension}"
            )
            with open(vanilla_output_file, "w") as f:
                f.write(vanilla_rendered)
