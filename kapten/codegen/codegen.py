import subprocess
from os import path
from pathlib import Path
import jinja2

from kapten.read_config import read_config
from kapten.codegen.lib.setup_jinja_env import debug
from kapten.util.filepaths import py_dir, codegen_dir
from kapten.util.read_tasks_config import read_tasks_config

def relative_path_from_flows_dir_to_tasks_conf_path(kap_conf):
    """
    Get the relative path from the flows dir to the tasks.yaml file
    so that the generated flows can find the tasks.yaml file
    (e.g. "../../tasks.yaml")
    """
    flows_dir = Path(kap_conf['flows-dir'])
    tasks_conf_path = "kapten.yaml"
    return path.relpath(tasks_conf_path, flows_dir)

def relative_path_from_flows_dir_to_py_tasks_dir(kap_conf):
    """
    Get the relative path from the flows dir to the py_tasks dir
    so that the generated flows can import the tasks
    (e.g. "../../py_tasks")
    """
    flows_dir = Path(kap_conf['flows-dir'])
    py_tasks_dir = Path(kap_conf['py-tasks-dir'])
    return path.relpath(py_tasks_dir, flows_dir)

def relative_path_from_flows_dir_to_r_tasks_dir(kap_conf):
    """
    Get the relative path from the flows dir to the r_tasks dir
    so that the generated flows can find the R tasks
    (e.g. "../../r_tasks")
    """
    flows_dir = Path(kap_conf['flows-dir'])
    r_tasks_dir = Path(kap_conf['r-tasks-dir'])
    return path.relpath(r_tasks_dir, flows_dir)

def generate_files(graph: str = None):
    kap_conf = read_config()["settings"]
    # flows_dir = path.join(py_dir, 'flows')
    root_dir = Path('.')
    flows_dir = root_dir / kap_conf['flows-dir']
    flow_type = kap_conf.get('flow-type')
    templates_path = path.join(codegen_dir, 'templates', flow_type)
    environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(templates_path),
    )
    tasks_conf_path = "kapten.yaml"
    conf = read_tasks_config(root_dir / tasks_conf_path)
    environment.filters['debug'] = debug
    tasks_dict = conf['tasks']
    if graph:
        graphs = {graph: conf['graphs'][graph]}
    else:
        graphs = conf['graphs']
    # Write flows/*.py files
    for graph_name in graphs:
        deps_lookup = graphs[graph_name]["tasks"]
        task_names = list(deps_lookup.keys())
        rendered = environment.get_template('flows.py.jinja').render(
            pipeline_name=graph_name,
            task_names=task_names,
            tasks_dict=tasks_dict,
            deps_lookup=deps_lookup,
            py_tasks_dir=kap_conf['py-tasks-dir'],
            r_tasks_dir=kap_conf['r-tasks-dir'],
            rel_tasks_conf_path=relative_path_from_flows_dir_to_tasks_conf_path(kap_conf),
            rel_py_tasks_dir=relative_path_from_flows_dir_to_py_tasks_dir(kap_conf),
            rel_r_tasks_dir=relative_path_from_flows_dir_to_r_tasks_dir(kap_conf)
        )
        output_file = path.join(flows_dir, f'{graph_name}.py')
        with open(output_file, 'w') as f:
            f.write(rendered)

    # Write tasks/__init__.py file
    task_names = list(tasks_dict.keys())
    rendered = environment.get_template('tasks_init.py.jinja').render(task_names=task_names, tasks_dict=tasks_dict)
    # output_file = path.join(py_dir, 'tasks', '__init__.py')
    output_file = root_dir / kap_conf['py-tasks-dir'] / '__init__.py'
    with open(output_file, 'w') as f:
        f.write(rendered)

    # print("Formatting code...")
    # subprocess.run(["black", "-q", "."], cwd=flows_dir)
