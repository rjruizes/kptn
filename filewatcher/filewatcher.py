import os
from watchfiles import DefaultFilter, watch, Change
import requests
from kapten.caching.Hasher import Hasher
from kapten.util.filepaths import project_root
from kapten.util.read_tasks_config import all_tasks_configs
from kapten.util.filepaths import test_r_tasks_dir
from kapten.util.logger import get_logger

class RehashFilter(DefaultFilter):
    """
    Modifying Python, R, or YAML files triggers a rehash event of that code and sending the update to the UI
    """
    allowed_extensions = '.py', '.yaml', '.R'
    def __call__(self, change: Change, path: str) -> bool:
        ignore_paths = ['kapten/caching', 'kapten/deploy', 'kapten/util', 'kapten/watcher']
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
    tasks_config = all_tasks_configs()
    index = {}
    h = Hasher(py_dirs=["py_src/tasks", "tests/mock_pipeline/py_tasks"], r_dirs=[".", test_r_tasks_dir], tasks_config=tasks_config)
    for name, task in tasks_config["tasks"].items():
        # print(name)
        if "py_script" in task:
            filename = task["py_script"] if type(task["py_script"]) == str else f"{name}.py"
            # Check if file exists at py_src/tasks/{filename}
            if os.path.exists(f"py_src/tasks/{filename}"):
                # Using dict.setdefault()
                index.setdefault(f"py_src/tasks/{filename}", []).append(name)
            elif os.path.exists(f"tests/mock_pipeline/py_tasks/{filename}"):
                index.setdefault(f"tests/mock_pipeline/py_tasks/{filename}", []).append(name)
            else:
                logger.warning(f"Task {name} has no associated Python script")
        if "r_script" in task:
            filelist = h.get_task_filelist(task)
            filelist = [f.replace(f"{project_root}/", '') for f in filelist]
            for file in filelist:
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