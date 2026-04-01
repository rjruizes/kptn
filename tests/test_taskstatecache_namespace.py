from types import SimpleNamespace

from kptn.caching.TaskStateCache import TaskStateCache


def test_cache_namespace_overrides_pipeline(monkeypatch, tmp_path):
    TaskStateCache._instance = None

    captured = {}

    class DummyDb:
        pass

    def fake_init_db_client(table_name=None, storage_key=None, pipeline=None, tasks_config=None, tasks_config_path=None):
        captured["pipeline"] = pipeline
        return DummyDb()

    class DummyHasher:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr("kptn.caching.TaskStateCache.init_db_client", fake_init_db_client)
    monkeypatch.setattr("kptn.caching.TaskStateCache.Hasher", DummyHasher)

    config_path = tmp_path / "kptn.yaml"
    config_path.write_text("settings: {}\n")

    pipeline_config = SimpleNamespace(
        PIPELINE_NAME="child",
        SUBSET_MODE=False,
        scratch_dir=str(tmp_path / "scratch"),
        TASKS_CONFIG_PATH=str(config_path),
        R_TASKS_DIRS=(),
        PY_MODULE_PATH=None,
        STORAGE_KEY="",
        BRANCH="test",
    )

    tasks_config = {
        "settings": {"cache_namespace": "shared"},
        "graphs": {"child": {"tasks": {"a": None}}},
        "tasks": {"a": {"file": "a.py"}},
    }

    cache = TaskStateCache(
        pipeline_config,
        tasks_config=tasks_config,
        tasks_config_paths=[str(config_path)],
        db_client=None,
    )

    assert cache.cache_namespace == "shared"
    assert captured["pipeline"] == "shared"


def test_taskstatecache_reuses_singleton_instance(monkeypatch, tmp_path):
    TaskStateCache._instance = None

    build_calls = {"count": 0}
    restore_calls = {"count": 0}

    class DummyDb:
        pass

    class DummyHasher:
        def __init__(self, *args, **kwargs):
            pass

    def fake_build_runtime_config(self, task_name=None, task_lang=None):
        build_calls["count"] += 1
        return SimpleNamespace()

    def fake_restore_initial_duckdb_checkpoint(self, runtime_config):
        restore_calls["count"] += 1
        return None

    monkeypatch.setattr("kptn.caching.TaskStateCache.init_db_client", lambda *a, **k: DummyDb())
    monkeypatch.setattr("kptn.caching.TaskStateCache.Hasher", DummyHasher)
    monkeypatch.setattr(TaskStateCache, "build_runtime_config", fake_build_runtime_config)
    monkeypatch.setattr(TaskStateCache, "restore_initial_duckdb_checkpoint", fake_restore_initial_duckdb_checkpoint)

    config_path = tmp_path / "kptn.yaml"
    config_path.write_text("settings: {}\n", encoding="utf-8")

    pipeline_config = SimpleNamespace(
        PIPELINE_NAME="child",
        SUBSET_MODE=False,
        scratch_dir=str(tmp_path / "scratch"),
        TASKS_CONFIG_PATH=str(config_path),
        R_TASKS_DIRS=(),
        PY_MODULE_PATH=None,
        STORAGE_KEY="",
        BRANCH="test",
    )

    tasks_config = {
        "settings": {"cache_namespace": "shared"},
        "graphs": {"child": {"tasks": {"a": None}}},
        "tasks": {"a": {"file": "a.py"}},
    }

    first = TaskStateCache(
        pipeline_config,
        tasks_config=tasks_config,
        tasks_config_paths=[str(config_path)],
        db_client=None,
    )
    second = TaskStateCache(
        pipeline_config,
        tasks_config=tasks_config,
        tasks_config_paths=[str(config_path)],
        db_client=None,
    )

    assert first is second
    assert TaskStateCache._instance is first
    assert build_calls["count"] == 1
    assert restore_calls["count"] == 1
