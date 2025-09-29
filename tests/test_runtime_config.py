import json
from types import SimpleNamespace

import pytest
import yaml

from kapten.util.runtime_config import RuntimeConfig, RuntimeConfigError


def test_runtime_config_evaluates_callables():
    config = {"engine": "tests.runtime_config_fixtures:build_value()"}

    runtime = RuntimeConfig.from_config(config)

    assert runtime["engine"] == 99


def test_runtime_config_merges_includes(tmp_path):
    include_payload = {
        "from_include": "tests.runtime_config_fixtures:build_value()",
        "nested": {"dialect": "sqlite"},
    }
    include_path = tmp_path / "config.json"
    include_path.write_text(json.dumps(include_payload), encoding="utf-8")

    config = {
        "include": "config.json",
        "my_global": 42,
        "engine": "tests.runtime_config_fixtures:build_engine()",
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
        "baz": "tests.runtime_config_fixtures:build_value()",
    }

    runtime = RuntimeConfig.from_config(config, base_dir=tmp_path)

    assert runtime.as_dict() == {"foo": "bar", "bar": 123, "baz": 99}


def test_runtime_config_delegates_to_fallback():
    fallback = SimpleNamespace(answer=42)
    runtime = RuntimeConfig.from_config({}, fallback=fallback)

    assert runtime.answer == 42
