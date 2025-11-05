from pathlib import Path
from kptn.read_config import read_config
from kptn.util.filepaths import py_dir, project_root
from os import path
import yaml
from kptn.util.logger import get_logger

def read_tasks_config(tasks_yaml_path: str):
    logger = get_logger()
    logger.debug(f"Reading tasks config from {tasks_yaml_path}")
    if not Path(tasks_yaml_path).exists():
        raise FileNotFoundError(f"Tasks config file {tasks_yaml_path} not found; cwd={Path.cwd()}")
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
def all_tasks_configs_with_paths():
    kap_conf = read_config()
    tasks_conf_path = Path(kap_conf['tasks_conf_path'])
    config_paths = [
        tasks_conf_path,
        Path(project_root) / "tests" / "mock_pipeline" / "kptn.yaml",
    ]
    superset: dict = {}
    for config_path in config_paths:
        superset = merge(superset, read_tasks_config(config_path))
    return superset, config_paths

def all_tasks_configs():
    configs, _ = all_tasks_configs_with_paths()
    return configs
