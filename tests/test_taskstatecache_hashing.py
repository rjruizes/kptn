from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Dict

from kptn.caching.TaskStateCache import TaskStateCache
from kptn.caching.models import TaskState


class DummyHasher:
    def __init__(self):
        self.called: list[str] = []

    def hash_task_outputs(self, task_name: str) -> str:
        self.called.append(task_name)
        return "hashed-output"


class DummyDbClient:
    def __init__(self):
        self.state: Dict[str, TaskState] = {}

    def get_task(self, task_name: str, include_data: bool = False, subset_mode: bool = False):
        return self.state.get(task_name)

    def create_task(self, task_name: str, value: TaskState, data=None):
        self.state[task_name] = value

    def update_task(self, task_name: str, task: TaskState):
        self.state[task_name] = task


def _make_cache(tmp_path) -> TaskStateCache:
    cache = object.__new__(TaskStateCache)
    cache.pipeline_name = "demo"
    cache.pipeline_config = SimpleNamespace(
        PIPELINE_NAME="demo",
        SUBSET_MODE=False,
        scratch_dir=str(tmp_path),
    )
    cache.tasks_root_dir = tmp_path
    cache.duckdb_tasks_dir = tmp_path
    cache.tasks_config_paths = []
    cache.tasks_config = {
        "graphs": {"demo": {"tasks": {"alpha": []}}},
        "tasks": {"alpha": {"file": "alpha.py", "cache_result": True, "outputs": ["foo.txt"]}},
    }
    cache.db_client = DummyDbClient()
    cache.hasher = DummyHasher()
    cache.logger = logging.getLogger("test")
    cache._duckdb_sql_functions = {}
    cache._python_module_cache = {}
    cache._task_has_prior_runs = {}

    def _noop_build(*args, **kwargs):
        return None, None

    cache.build_task_code_hashes = _noop_build.__get__(cache, TaskStateCache)
    return cache


def test_hashing_waits_for_prior_run(tmp_path):
    TaskStateCache._instance = None
    cache = _make_cache(tmp_path)

    cache.set_initial_state("alpha")
    cache.set_final_state("alpha")
    assert cache.hasher.called == []

    cache.set_initial_state("alpha")
    cache.set_final_state("alpha")
    assert cache.hasher.called == ["alpha"]
