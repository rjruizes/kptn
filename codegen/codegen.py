import subprocess
from os import path
from pathlib import Path
import jinja2

from kapten.read_config import read_config
from kapten.codegen.lib.setup_jinja_env import debug
from kapten.util.filepaths import py_dir, codegen_dir
from kapten.util.read_tasks_config import read_tasks_config

def generate_files(graph: str = None):
    kap_conf = read_config()
    # flows_dir = path.join(py_dir, 'flows')
    root_dir = Path('.')
    flows_dir = root_dir / kap_conf['flows-dir']
    templates_path = path.join(codegen_dir, 'templates')
    environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(templates_path),
    )
    tasks_conf_path = kap_conf['tasks-conf-path']
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
        rendered = environment.get_template('flows.py.jinja').render(pipeline_name=graph_name, task_names=task_names, tasks_dict=tasks_dict, deps_lookup=deps_lookup)
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
