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
