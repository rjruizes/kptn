import logging
from pathlib import Path
import glob
import re
from string import Template
from kapten.caching.r_imports import get_file_list, hash_r_files
from kapten.util.hash import hash_file, hash_obj
from kapten.util.read_tasks_config import read_tasks_configs

logger = logging.getLogger(__name__)
var_pattern = re.compile(r"\$\{([a-zA-Z0-9\-_\.]+)\}")

class Hasher:
    def __init__(self, py_dirs: list[str]=None, r_dirs: list[str]=None, output_dir=None, tasks_config=None, tasks_config_paths=None):
        self.r_dirs = r_dirs
        self.py_dirs = py_dirs
        self.output_dir = output_dir
        self.tasks_config = tasks_config or read_tasks_configs(tasks_config_paths)

    def get_task(self, name: str):
        """Return the task configuration."""
        try:
            task = self.tasks_config["tasks"][name]
        except KeyError:
            taskname_keys = self.tasks_config["tasks"].keys()
            raise KeyError(f"Task '{name}' not found in list of tasks, {taskname_keys}")
        return task

    def get_full_r_script_paths(self, filename: str) -> tuple[list[Path], list[str]]:
        """Search r_dirs for matching R script(s)."""
        matching_r_scripts = []
        matching_r_dir = None
        for r_dir in self.r_dirs:
            if "$" in filename:
                # Regex replace any variables ${.*} -> '*'
                r_script_pattern = Path(r_dir) / re.sub(var_pattern, "*", filename)
                matching_r_scripts.extend(glob.glob(str(r_script_pattern)))
                matching_r_dir = r_dir
            else:
                r_script_path = Path(r_dir) / filename
                if r_script_path.exists():
                    matching_r_scripts.append(r_script_path)
                    matching_r_dir = r_dir
        if len(matching_r_scripts) == 0:
            raise FileNotFoundError(f"No R script {filename} not found in {self.r_dirs}")
        return matching_r_scripts, matching_r_dir

    def get_task_filelist(self, task: dict) -> list[str]:
        """Return a list of all files associated with a task."""
        filename = task["r_script"]
        full_paths, r_tasks_dir = self.get_full_r_script_paths(filename)
        abs_file_list = get_file_list(full_paths)
        return abs_file_list

    def build_r_code_hashes(self, name: str, task: dict = None) -> list[dict[str, str]]:
        """Hash the code of a task to determine if it has changed."""
        if task is None:
            task = self.get_task(name)
        filename = task["r_script"]
        full_paths, r_tasks_dir = self.get_full_r_script_paths(filename)
        logger.info(f"Building R code hashes for {name}, paths: {full_paths}")
        return hash_r_files(full_paths, r_tasks_dir)

    def get_full_py_script_path(self, filename: str) -> Path:
        """Search py_dirs for the Python script."""
        for py_dir in self.py_dirs:
            py_script_path = Path(py_dir) / filename
            if py_script_path.exists():
                return py_script_path
        raise FileNotFoundError(f"Python script {filename} not found in {self.py_dirs}")

    def build_py_code_hashes(self, name: str, task: dict = None) -> str:
        """Hash the code of a task to determine if it has changed."""
        if task is None:
            task = self.get_task(name)
        # If task["py_script"] is a string, use it as the filename
        filename = task["py_script"] if type(task["py_script"]) == str else name + ".py"
        full_path = self.get_full_py_script_path(filename)
        logger.info(f"Building Python code hashes for {name}, path: {full_path}")
        return hash_file(full_path)

    def hash_code_for_task(self, name: str):
        task = self.get_task(name)
        if "r_script" in task:
            code_hashes = self.build_r_code_hashes(name, task)
        elif "py_script" in task:
            code_hashes = self.build_py_code_hashes(name, task)
        else:
            raise KeyError(f"Task '{name}' has no R or Python script")
        return code_hashes

    def hash_task_outputs(self, name: str) -> str:
        """Hash output files of a task to determine if they have changed."""
        task = self.get_task(name)
        if self.output_dir is None:
            raise ValueError("Output directory not set")
        if "outputs" not in task:
            return ""
        output_filepaths: list[str] = task["outputs"]
        # Search for all output files in the scratch directory
        file_list = set()
        for output_filepath in output_filepaths:
            if "$" in output_filepath:
                # Check if all variables in the pattern are in the environment
                # Replace any unknown variables with '*'
                all_vars = var_pattern.findall(output_filepath)
                for var in all_vars:
                    output_filepath = output_filepath.replace(f"${{{var}}}", "*")
                glob_pattern = str(Path(self.output_dir) / output_filepath)
                matching_files = glob.glob(glob_pattern)
                if len(matching_files) > 0:
                    file_list.update(matching_files)
                else:
                    print(f"Warning: File {glob_pattern} not found")
            else:
                if (Path(self.output_dir) / output_filepath).exists():
                    file_list.add(output_filepath)
                else:
                    print(f"Warning: File {output_filepath} not found")
        # Sort the files by name
        sorted_file_list = sorted(file_list)

        if len(sorted_file_list) == 0:
            return
        # Hash the contents of the files
        hashed_output_files = [
            {file: hash_file(Path(self.output_dir) / file)} for file in sorted_file_list
        ]
        return hash_obj(hashed_output_files)

    def hash_subtask_outputs(self, name:str, env: dict) -> str:
        """Hash the outputs of a subtask to determine if they have changed."""
        task = self.get_task(name)
        if self.output_dir is None:
            raise ValueError("Output directory not set")
        if "outputs" not in task:
            return ""
        filename_patterns: list[str] = task["outputs"]
        file_list = set()
        for pattern in filename_patterns:
            # If the pattern contains a variable, replace it with the value from the environment
            if "$" in pattern:
                # Check if all variables in the pattern are in the environment
                # Replace any unknown variables with '*'
                all_vars = var_pattern.findall(pattern)
                for var in all_vars:
                    if var in env:
                        pattern = pattern.replace(f"${{{var}}}", str(env[var]))
                    else:
                        # replace with '*' to match any file
                        pattern = pattern.replace(f"${{{var}}}", "*")
                
                glob_pattern = str(Path(self.output_dir) / pattern)
                matching_files = glob.glob(glob_pattern)
                if len(matching_files) == 0:
                    print(f"Warning: File {glob_pattern} not found")
                else:
                    file_list.update(matching_files)
        # Sort the files by name
        sorted_file_list = sorted(file_list)
        if len(file_list) == 0:
            return
        # Hash the contents of the files
        hashed_output_files = [
            {file: hash_file(str(Path(self.output_dir) / file))} for file in sorted_file_list
        ]
        return hash_obj(hashed_output_files)
