from pathlib import Path
from os import path

mock_dir = Path(__file__).parent.parent / "example/mock_pipeline"
r_tasks_dir = str(Path(mock_dir) / "r_tasks")
tasks_yaml_path = str(Path(mock_dir) / "kptn.yaml")
list_py_file = Path(mock_dir) / "py_tasks" / "subtask_list.py"
process_py_file = Path(mock_dir) / "py_tasks" / "subtask_process.py"
PY_COMMENT = "\n# This is a comment\n"
