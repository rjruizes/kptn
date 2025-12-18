from types import SimpleNamespace

import pytest

from kptn.caching.TaskStateCache import TaskStateCache


def test_graph_config_overrides_base(tmp_path, monkeypatch):
    captured = {}

    def fake_from_tasks_config(tasks_config, *, base_dir=None, fallback=None, task_info=None):
        captured["config"] = tasks_config.get("config")
        captured["base_dir"] = base_dir
        captured["task_info"] = task_info
        return "runtime"

    monkeypatch.setattr("kptn.caching.TaskStateCache.RuntimeConfig.from_tasks_config", fake_from_tasks_config)
    TaskStateCache._instance = None

    config_path = tmp_path / "kptn.yaml"
    config_path.write_text("graphs: {}\ntasks: {}\n", encoding="utf-8")

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
        "config": {"foo": "base", "bar": 1},
        "graphs": {
            "pipe": {
                "config": {"foo": "graph"},
                "tasks": {"a": None},
            }
        },
        "tasks": {"a": {"file": "a.py"}},
    }

    cache = TaskStateCache(
        pipeline_config,
        db_client=None,
        tasks_config=tasks_config,
        tasks_config_paths=[str(config_path)],
    )

    runtime = cache.build_runtime_config(task_name="a", task_lang="python")

    assert runtime == "runtime"
    assert captured["config"] == {"foo": "graph", "bar": 1}
    assert captured["task_info"]["task_name"] == "a"
