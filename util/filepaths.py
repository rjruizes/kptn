from os import path

py_dir = path.dirname(path.dirname(path.realpath(__file__)))
project_root = path.dirname(py_dir)
codegen_dir = path.join(py_dir, 'codegen')
r_tasks_dir = path.join(project_root, 'tasks')
test_r_tasks_dir = path.join(project_root, 'tests', 'mock_pipeline', 'r_tasks')
test_py_module_path = "tests.mock_pipeline.py_tasks"