import json
from types import SimpleNamespace

import pytest
import yaml

from kptn.util.runtime_config import RuntimeConfig, RuntimeConfigError


def test_runtime_config_evaluates_callables():
    config = {"engine": "tests.runtime_config_fixtures:build_value"}

    runtime = RuntimeConfig.from_config(config)

    assert runtime["engine"] == 99


def test_runtime_config_merges_includes(tmp_path):
    include_payload = {
        "from_include": "tests.runtime_config_fixtures:build_value",
        "nested": {"dialect": "sqlite"},
    }
    include_path = tmp_path / "config.json"
    include_path.write_text(json.dumps(include_payload), encoding="utf-8")

    config = {
        "include": "config.json",
        "my_global": 42,
        "engine": "tests.runtime_config_fixtures:build_engine",
        "nested": {"schema": "public"},
    }

    runtime = RuntimeConfig.from_config(config, base_dir=tmp_path)

    assert runtime["my_global"] == 42
    assert runtime["from_include"] == 99
    assert runtime["engine"] == {"url": "sqlite://example"}
    assert runtime["nested"] == {"dialect": "sqlite", "schema": "public"}


def test_runtime_config_requires_mapping_from_include(tmp_path):
    include_path = tmp_path / "list.json"
    include_path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")

    with pytest.raises(RuntimeConfigError):
        RuntimeConfig.from_config({"include": "list.json"}, base_dir=tmp_path)


def test_runtime_config_supports_multiple_include_files(tmp_path):
    first = tmp_path / "config.json"
    first.write_text(json.dumps({"foo": "bar"}), encoding="utf-8")

    second = tmp_path / "extras.yaml"
    second.write_text(yaml.safe_dump({"bar": 123}), encoding="utf-8")

    config = {
        "include": ["config.json", "extras.yaml"],
        "baz": "tests.runtime_config_fixtures:build_value",
    }

    runtime = RuntimeConfig.from_config(config, base_dir=tmp_path)

    assert runtime.as_dict() == {"foo": "bar", "bar": 123, "baz": 99}


def test_runtime_config_delegates_to_fallback():
    fallback = SimpleNamespace(answer=42)
    runtime = RuntimeConfig.from_config({}, fallback=fallback)

    assert runtime.answer == 42


def test_runtime_config_passes_task_info_to_callable():
    config = {"meta": "tests.runtime_config_fixtures:build_value_with_task_info"}
    runtime = RuntimeConfig.from_config(
        config,
        task_info={"task_name": "fruits", "task_lang": "python"},
    )

    assert runtime["meta"] == {"task_name": "fruits", "task_lang": "python"}


def test_runtime_config_defaults_task_info_when_not_provided():
    config = {"meta": "tests.runtime_config_fixtures:build_value_with_task_info"}
    runtime = RuntimeConfig.from_config(config)

    assert runtime["meta"] == {"task_name": None, "task_lang": None}


def test_runtime_config_supports_duckdb_mapping_alias():
    config = {
        "duckdb": {
            "function": "tests.runtime_config_fixtures:build_engine",
            "parameter_name": "engine",
        }
    }

    runtime = RuntimeConfig.from_config(config)

    assert runtime.duckdb == {"url": "sqlite://example"}
    assert runtime.engine == {"url": "sqlite://example"}


def test_runtime_config_rejects_invalid_duckdb_alias():
    config = {
        "duckdb": {
            "function": "tests.runtime_config_fixtures:build_engine",
            "parameter_name": "not valid",
        }
    }

    with pytest.raises(RuntimeConfigError):
        RuntimeConfig.from_config(config)


def test_runtime_config_requires_duckdb_function_when_mapping():
    config = {
        "duckdb": {
            "parameter_name": "engine",
        }
    }

    with pytest.raises(RuntimeConfigError):
        RuntimeConfig.from_config(config)


def test_runtime_config_supports_nested_include(tmp_path):
    nested_path = tmp_path / "config.json"
    nested_path.write_text(json.dumps({"hello": "world"}), encoding="utf-8")

    config = {
        "nested": {
            "include": "config.json",
            "extra": 99,
        }
    }

    runtime = RuntimeConfig.from_config(config, base_dir=tmp_path)

    assert runtime.nested == {"hello": "world", "extra": 99}


def test_runtime_config_nested_include_requires_mapping(tmp_path):
    nested_path = tmp_path / "config.json"
    nested_path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")

    config = {"nested": {"include": "config.json"}}

    with pytest.raises(RuntimeConfigError):
        RuntimeConfig.from_config(config, base_dir=tmp_path)
