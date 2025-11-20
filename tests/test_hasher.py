import os
from pathlib import Path
import shutil
import pytest
from kptn.caching.Hasher import Hasher
from tests.fixture_constants import mock_dir, tasks_yaml_path

@pytest.fixture
def cleanup():
    yield
    scratch_dir = Path(mock_dir) / "scratch"
    if scratch_dir.exists():
        shutil.rmtree(scratch_dir)

def test_Hasher_py():
    h = Hasher(tasks_config_paths=["example/basic/kptn.yaml"])
    hashes = h.hash_code_for_task("a")
    assert isinstance(hashes, list)
    assert hashes and all("function" in item and "hash" in item for item in hashes)

def test_Hasher_r():
    h = Hasher(tasks_config_paths=["example/mock_pipeline/kptn.yaml"])
    assert isinstance(h.hash_code_for_task("A"), list)

def test_Hasher_r_glob():
    task_name = "srs_indicators_estimated_tables_part1_preprocessing"
    h = Hasher(tasks_config_paths=["example/nibrs/kptn.yaml"])
    task = h.get_task(task_name)
    script_spec = task["file"] if isinstance(task.get("file"), str) else task.get("r_script", "")
    script_path = script_spec.split(":", 1)[0]
    assert isinstance(h.get_full_r_script_paths(task_name, script_path), tuple)
    assert isinstance(h.hash_code_for_task(task_name), list)

def test_Hasher_weights_computed():
    h = Hasher(tasks_config_paths=["example/mock_pipeline/kptn.yaml"])
    hashes = h.hash_code_for_task("A")
    assert type(hashes) == list
    # Assert no duplicate keys in list of objects
    seen = set()
    for h in hashes:
        key = list(h.keys())[0]
        assert key not in seen
        seen.add(key)

def test_Hasher_get_task_filelist():
    h = Hasher(r_dirs=["example/mock_pipeline/r_tasks"], tasks_config_paths=["example/mock_pipeline/kptn.yaml"])
    task = h.get_task("A")
    assert type(h.get_task_filelist("A", task)) == list

def test_Hasher_indicators():
    task_name = "srs_indicators_estimated_tables_part1_preprocessing"
    h = Hasher(tasks_config_paths=["example/nibrs/kptn.yaml"])
    task = h.get_task(task_name)
    assert isinstance(h.get_task_filelist(task_name, task), list)

def test_Hasher_outputs(cleanup):
    scratch_dir = Path(mock_dir) / "scratch"
    os.environ["SCRATCH_DIR"] = str(scratch_dir)
    # blah represents an unknown variable in the Rscript, will be replaced by a wildcard
    sample_output_file1 = scratch_dir / "my_output" / "dummy1-blah.txt"
    sample_output_file2 = scratch_dir / "my_output" / "dummy2-blah.txt"
    # Create a sample output file
    sample_output_file1.parent.mkdir(parents=True, exist_ok=True)
    sample_output_file1.write_text("Hello, world!")
    sample_output_file2.write_text("Hello, world!")
    h = Hasher(output_dir=scratch_dir, tasks_config_paths=[tasks_yaml_path])
    assert h.hash_task_outputs("static_params") == "98d3e3f89b95ed2b9a16e934f151d502a9e56fe8"

def test_Hasher_subtask_outputs(cleanup):
    scratch_dir = Path(mock_dir) / "scratch"
    os.environ["SCRATCH_DIR"] = str(scratch_dir)
    sample_output_file1 = scratch_dir / "my_output" / "T1-blah.txt"
    sample_output_file2 = scratch_dir / "my_output" / "T2-blah.txt"
    # Create a sample output file
    sample_output_file1.parent.mkdir(parents=True, exist_ok=True)
    sample_output_file1.write_text("Hello, world!")
    sample_output_file2.write_text("Hello, world!")
    h = Hasher(output_dir=scratch_dir, tasks_config_paths=[tasks_yaml_path])
    assert h.hash_subtask_outputs("write_param", { "item": "T1" }) == "14b33eb50ce0d754e9b272729cc50215794cce27"
    assert h.hash_subtask_outputs("write_param", { "item": "T2" }) == "ec52df80076c26f62593312e43c9f533fdc1c6a6"


def test_py_function_dependency_hashing(tmp_path):
    pkg_root = tmp_path / "tasks"
    pkg_root.mkdir()
    (pkg_root / "__init__.py").write_text("")
    (pkg_root / "helper.py").write_text(
        "def helper() -> int:\n    return 41\n"
    )
    (pkg_root / "task.py").write_text(
        "from .helper import helper\n\n"
        "def task() -> int:\n    return helper() + 1\n"
    )
    tasks_config = {
        "tasks": {
            "task": {
                "py_script": "task.py",
            }
        }
    }
    hasher = Hasher(py_dirs=[str(pkg_root)], tasks_config=tasks_config)
    hashes = hasher.build_py_code_hashes("task", tasks_config["tasks"]["task"])
    functions = {item["function"] for item in hashes}
    assert "task.task" in functions
    assert "helper.helper" in functions
