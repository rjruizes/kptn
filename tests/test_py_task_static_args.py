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


def test_py_task_checkpoint_saves_after_success(tmp_path, monkeypatch):
    monkeypatch.setattr("kptn.caching.TaskStateCache.is_flow_prefect", lambda: False)
    TaskStateCache._instance = None

    task_file = tmp_path / "task_mod.py"
    task_file.write_text(
        textwrap.dedent(
            """
            def run():
                return "ok"
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
        "graphs": {"pipe": {"tasks": {"my_task": None}}},
        "tasks": {
            "my_task": {
                "file": str(task_file),
                "duckdb_checkpoint": True,
            }
        },
    }

    db_path = tmp_path / "example.ddb"
    backup_path = tmp_path / "example.my_task.backup.ddb"
    db_path.write_text("dirty live db", encoding="utf-8")
    backup_path.write_text("checkpoint seed", encoding="utf-8")

    class StubConn:
        def __init__(self):
            self.closed = False
            self._last_sql = None

        def execute(self, sql, params=None):
            self._last_sql = sql
            return self

        def fetchall(self):
            return [(592, "main", str(db_path))] if self._last_sql == "PRAGMA database_list" else []

        def close(self):
            self.closed = True

    build_calls = {"count": 0}

    cache = TaskStateCache(
        pipeline_config,
        db_client=None,
        tasks_config=tasks_config,
        tasks_config_paths=[str(config_path)],
    )

    def build_runtime_config(task_name=None):
        build_calls["count"] += 1
        return SimpleNamespace(duckdb=StubConn())

    def task_callable():
        db_path.write_text("task output", encoding="utf-8")
        return None

    cache.build_runtime_config = build_runtime_config
    cache.get_python_callable = lambda task_name: task_callable
    TaskStateCache._instance = cache

    try:
        py_task(pipeline_config, "my_task")
    finally:
        TaskStateCache._instance = None

    # py_task now calls build_runtime_config twice: once to build the per-task
    # config before running the callable, and once to re-establish the bootstrap
    # config after the task so that subsequent tasks have a valid DuckDB connection.
    assert build_calls["count"] == 2
    assert db_path.read_text(encoding="utf-8") == "task output"
    assert backup_path.read_text(encoding="utf-8") == "task output"
    assert cache.db_client.task_ended[0][1] is None
