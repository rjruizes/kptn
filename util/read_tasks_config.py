from pathlib import Path
from kapten.read_config import read_config
from kapten.util.filepaths import py_dir, project_root
from os import path
import yaml
from kapten.util.logger import get_logger

def read_tasks_config(tasks_yaml_path: str):
    logger = get_logger()
    logger.info(f"Reading tasks config from {tasks_yaml_path}")
    with open(tasks_yaml_path) as f:
        return yaml.load(f, Loader=yaml.FullLoader)

# Source: https://stackoverflow.com/a/7205107
def merge(a: dict, b: dict, path=[]):
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] != b[key]:
                raise Exception('Conflict at ' + '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def read_tasks_configs(tasks_yaml_paths: list[str]):
    superset = {}
    for tasks_yaml_path in tasks_yaml_paths:
        superset = merge(superset, read_tasks_config(tasks_yaml_path))
    return superset

def all_tasks_configs():
    kap_conf = read_config()
    tasks_conf_path = kap_conf['tasks-conf-path']
    config1 = read_tasks_config(tasks_conf_path)
    config2 = read_tasks_config(Path(project_root) / "tests" / "mock_pipeline" / "tasks.yaml")
    return merge(config1, config2)