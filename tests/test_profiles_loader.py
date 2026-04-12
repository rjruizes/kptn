import pytest

from kptn.exceptions import ProfileError
from kptn.profiles.loader import ProfileLoader
from kptn.profiles.schema import KptnConfig


def test_load_returns_kptn_config(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("settings:\n  db: sqlite\nprofiles: {}\n")
    config = ProfileLoader.load(kptn_yaml)
    assert isinstance(config, KptnConfig)


def test_load_settings_db_and_cache_namespace(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("settings:\n  db: duckdb\n  cache_namespace: my_ns\n")
    config = ProfileLoader.load(kptn_yaml)
    assert config.settings.db == "duckdb"
    assert config.settings.cache_namespace == "my_ns"


def test_load_profile_stage_selections(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("profiles:\n  dev:\n    data_sources:\n      - A\n      - B\n")
    config = ProfileLoader.load(kptn_yaml)
    assert config.profiles["dev"].stage_selections["data_sources"] == ["A", "B"]


def test_load_profile_optional_groups(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text('profiles:\n  dev:\n    "*.validate": true\n')
    config = ProfileLoader.load(kptn_yaml)
    assert config.profiles["dev"].optional_groups["*.validate"] is True


def test_load_profile_args(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text(
        "profiles:\n  dev:\n    args:\n      task_name:\n        param: val\n"
    )
    config = ProfileLoader.load(kptn_yaml)
    assert config.profiles["dev"].args["task_name"]["param"] == "val"


def test_load_invalid_yaml_raises_profile_error(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("settings: {db: duckdb\nprofiles: [\n")
    with pytest.raises(ProfileError) as exc_info:
        ProfileLoader.load(kptn_yaml)
    assert str(kptn_yaml) in str(exc_info.value)


def test_load_schema_violation_raises_profile_error(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("profiles:\n  dev:\n    args: not-a-dict\n")
    with pytest.raises(ProfileError) as exc_info:
        ProfileLoader.load(kptn_yaml)
    assert str(kptn_yaml) in str(exc_info.value)
    assert "profiles.dev" in str(exc_info.value)


def test_load_missing_file_returns_default_config(tmp_path):
    config = ProfileLoader.load(tmp_path / "nonexistent_kptn.yaml")
    assert isinstance(config, KptnConfig)
    assert config.settings.db == "sqlite"
    assert config.profiles == {}


def test_load_empty_file_returns_default_config(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("")
    config = ProfileLoader.load(kptn_yaml)
    assert isinstance(config, KptnConfig)


def test_load_multiple_profiles(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("profiles:\n  dev: {}\n  ci: {}\n")
    config = ProfileLoader.load(kptn_yaml)
    assert len(config.profiles) == 2
    assert "dev" in config.profiles
    assert "ci" in config.profiles


def test_load_non_mapping_top_level_raises_profile_error(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("- item1\n- item2\n")
    with pytest.raises(ProfileError) as exc_info:
        ProfileLoader.load(kptn_yaml)
    assert str(kptn_yaml) in str(exc_info.value)


def test_load_non_dict_settings_raises_profile_error(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("settings: not-a-dict\n")
    with pytest.raises(ProfileError) as exc_info:
        ProfileLoader.load(kptn_yaml)
    assert "settings" in str(exc_info.value)


def test_load_non_dict_profiles_raises_profile_error(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("profiles: not-a-dict\n")
    with pytest.raises(ProfileError) as exc_info:
        ProfileLoader.load(kptn_yaml)
    assert "profiles" in str(exc_info.value)


def test_load_null_profile_entry_raises_profile_error(tmp_path):
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text("profiles:\n  dev:\n")
    with pytest.raises(ProfileError) as exc_info:
        ProfileLoader.load(kptn_yaml)
    assert "profiles.dev" in str(exc_info.value)
