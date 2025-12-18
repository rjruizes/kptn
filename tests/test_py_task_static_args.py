from types import SimpleNamespace
import textwrap

from kptn.caching.TaskStateCache import TaskStateCache, py_task


def test_static_args_from_tasks_config(tmp_path, monkeypatch):
    monkeypatch.setattr("kptn.caching.TaskStateCache.is_flow_prefect", lambda: False)
    TaskStateCache._instance = None

    task_file = tmp_path / "task_mod.py"
    task_file.write_text(
        textwrap.dedent(
            """
            def run(x, y):
                return x + y
            """
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "kptn.yaml"
    config_path.write_text("graphs: {}\ntasks: {}\n", encoding="utf-8")

    class DummyHasher:
        def __init__(self, *args, **kwargs):
            pass

    class DummyDb:
        def __init__(self):
            self.created = []
            self.task_ended = []

        def get_task(self, task_name, include_data=False, subset_mode=False):
            return None

        def create_task(self, task_name, state):
            self.created.append((task_name, state))

        def set_task_ended(self, task_name, result=None, result_hash=None, subset_mode=False, status=None):
            self.task_ended.append((task_name, result, result_hash, subset_mode, status))

    monkeypatch.setattr("kptn.caching.TaskStateCache.Hasher", DummyHasher)
    monkeypatch.setattr("kptn.caching.TaskStateCache.init_db_client", lambda *a, **k: DummyDb())

    pipeline_config = SimpleNamespace(
        PIPELINE_NAME="pipe",
        SUBSET_MODE=False,
        scratch_dir=str(tmp_path / "scratch"),
        TASKS_CONFIG_PATH=str(config_path),
        R_TASKS_DIRS=(),
        PY_MODULE_PATH=None,
        STORAGE_KEY="storage",
        BRANCH="branch",
    )

    tasks_config = {
        "graphs": {
            "pipe": {
                "tasks": {
                    "my_task": {
                        "deps": None,
                        "args": {"y": 7},
                    }
                }
            }
        },
        "tasks": {
            "my_task": {
                "file": str(task_file),
                "args": {"x": 10, "y": 5},
            }
        },
    }

    cache = TaskStateCache(
        pipeline_config,
        db_client=None,
        tasks_config=tasks_config,
        tasks_config_paths=[str(config_path)],
    )
    cache.get_python_callable = lambda task_name: (lambda x, y: x + y)
    TaskStateCache._instance = cache

    try:
        py_task(pipeline_config, "my_task")
    finally:
        TaskStateCache._instance = None

    assert cache.db_client.task_ended, "Task should record completion"
    ended = cache.db_client.task_ended[0]
    assert ended[0] == "my_task"
    assert ended[1] == 17
