import os
from watchfiles import DefaultFilter, watch, Change
import requests
from kptn.caching.Hasher import Hasher
from kptn.util.filepaths import project_root
from kptn.util.read_tasks_config import all_tasks_configs_with_paths
from kptn.util.filepaths import test_r_tasks_dir
from kptn.util.logger import get_logger

class RehashFilter(DefaultFilter):
    """
    Modifying Python, R, or YAML files triggers a rehash event of that code and sending the update to the UI
    """
    allowed_extensions = '.py', '.yaml', '.R'
    def __call__(self, change: Change, path: str) -> bool:
        ignore_paths = ['kptn/caching', 'kptn/deploy', 'kptn/util', 'kptn/watcher']
        # print(f"Change: {change}, Path: {path}")
        return (
            super().__call__(change, path) and
            not any(ignore_path in path for ignore_path in ignore_paths) and
            path.endswith(self.allowed_extensions)
        )

def file_to_task_name(file_path: str) -> str:
    # If filepath begins with "tests/mock_pipeline", then it's a test task
    if file_path.startswith("tests/mock_pipeline"):
        # If ends with .py
        if file_path.endswith(".py"):
            file_path = file_path.replace("tests/mock_pipeline/py_tasks", "")

        # If ends with .R
        elif file_path.endswith(".R"):
            return file_path.replace("tests/mock_pipeline/r_tasks", "").replace(".R", "")

def watch_files(index):
    logger = get_logger()
    logger.info("Watching files...")
    for changes in watch('.', watch_filter=RehashFilter()):
        for change, path in changes:
            rel_path = path.replace(f"{project_root}/", '')
            logger.info(f"Event type: {Change(change)}, Path: {rel_path}")
            if rel_path in index:
                logger.info(f"Tasks: {index[rel_path]}")
                # POST a request to /api/codechange with the task name
                requests.post("http://localhost:8000/api/codechange", json={
                    "file": rel_path,
                    "updated_tasks": index[rel_path]
                })
            else:
                logger.info(f"Task not found {rel_path}")


def build_reverse_index():
    logger = get_logger()
    tasks_config, config_paths = all_tasks_configs_with_paths()
    config_path_strings = [str(path) for path in config_paths]
    index = {}
    h = Hasher(
        py_dirs=["py_src/tasks", "tests/mock_pipeline/py_tasks"],
        r_dirs=[".", test_r_tasks_dir],
        tasks_config=tasks_config,
        tasks_config_paths=config_path_strings,
    )
    for name, task in tasks_config["tasks"].items():
        # print(name)
        if "py_script" in task:
            filename = task["py_script"] if type(task["py_script"]) == str else f"{name}.py"
            try:
                script_path = h.get_full_py_script_path(name, filename)
                rel_path = os.path.relpath(script_path, project_root)
                index.setdefault(rel_path, []).append(name)
            except FileNotFoundError:
                logger.warning(f"Task {name} has no associated Python script at {filename}")
        if "r_script" in task:
            try:
                filelist = h.get_task_filelist(name, task)
            except FileNotFoundError:
                logger.warning(f"Task {name} has no associated R script at {task['r_script']}")
                continue
            normalized = []
            for file_path in filelist:
                rel_path = os.path.relpath(file_path, project_root)
                normalized.append(rel_path)
            for file in normalized:
                index.setdefault(file, []).append(name)
                
            # elif os.path.exists(f"tests/mock_pipeline/r_tasks/{task["r_script"]}"):
            #     index.setdefault(f"tests/mock_pipeline/r_tasks/{task["r_script"]}", []).append(name)
            # print(f"ERROR: Task {name} has no associated R script")
    # for file_path, task_name in index.items():
    #     print(f"{file_path} -> {task_name}")
    return index

def start_watching():
    index = build_reverse_index()
    watch_files(index)

if __name__ == "__main__":
    start_watching()
