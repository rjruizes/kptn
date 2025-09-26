import os
from pathlib import Path
import shutil
import pytest
from kapten.caching.Hasher import Hasher
from tests.fixture_constants import mock_dir, tasks_yaml_path

@pytest.fixture
def cleanup():
    yield
    scratch_dir = Path(mock_dir) / "scratch"
    if scratch_dir.exists():
        shutil.rmtree(scratch_dir)

def test_Hasher_py():
    h = Hasher(py_dirs=["example/basic/src"], tasks_config_paths=["example/basic/kapten.yaml"])
    assert type(h.hash_code_for_task("A")) == str

def test_Hasher_r():
    h = Hasher(r_dirs=["example/mock_pipeline/r_tasks"], tasks_config_paths=["example/mock_pipeline/kapten.yaml"])
    assert type(h.hash_code_for_task("A")) == list

def test_Hasher_r_glob():
    h = Hasher(r_dirs=["example/mock_pipeline/r_tasks"], tasks_config_paths=["example/mock_pipeline/kapten.yaml"])
    assert type(h.get_full_r_script_paths("srs/generate_estimates/Tables_Core/${TABLE_NAME}/Part1_prepare_datasets.R")) == tuple
    assert type(h.hash_code_for_task("srs_indicators_estimated_tables_part1_preprocessing")) == list

def test_Hasher_weights_computed():
    h = Hasher(r_dirs=["example/mock_pipeline/r_tasks"], tasks_config_paths=["example/mock_pipeline/kapten.yaml"])
    hashes = h.hash_code_for_task("weights_computed")
    assert type(hashes) == list
    # Assert no duplicate keys in list of objects
    seen = set()
    for h in hashes:
        key = list(h.keys())[0]
        assert key not in seen
        seen.add(key)

def test_Hasher_get_task_filelist():
    h = Hasher(r_dirs=["example/mock_pipeline/r_tasks"], tasks_config_paths=["example/mock_pipeline/kapten.yaml"])
    task = h.get_task("A")
    assert type(h.get_task_filelist(task)) == list

def test_Hasher_indicators():
    h = Hasher(r_dirs=["example/mock_pipeline/r_tasks"], tasks_config_paths=["example/mock_pipeline/kapten.yaml"])
    task = h.get_task("indicators_estimated_tables_part1_preprocessing")
    assert type(h.get_task_filelist(task)) == list

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
    assert h.hash_task_outputs("static_params") == "3b1cd281ce33909a35e284806b42a0418d7eedc2"

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
    assert h.hash_subtask_outputs("write_param", { "item": "T1" }) == "80c0944ca3d675834e1b3c660601c8cc2ac34154"
    assert h.hash_subtask_outputs("write_param", { "item": "T2" }) == "f93d7eeb4966c50ce20e6774363e594821ef1757"