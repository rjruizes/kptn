from kptn.profiles.schema import KptnConfig, KptnSettings, ProfileSpec


def test_kptn_config_defaults():
    config = KptnConfig()
    assert config.settings.db == "sqlite"
    assert config.profiles == {}


def test_kptn_settings_db_field():
    assert KptnSettings(db="duckdb").db == "duckdb"


def test_kptn_settings_cache_namespace():
    assert KptnSettings(cache_namespace="my_ns").cache_namespace == "my_ns"


def test_profile_spec_stage_selections():
    spec = ProfileSpec(stage_selections={"data_sources": ["A", "B"]})
    assert spec.stage_selections["data_sources"] == ["A", "B"]


def test_profile_spec_optional_groups():
    spec = ProfileSpec(optional_groups={"*.validate": True})
    assert spec.optional_groups["*.validate"] is True


def test_profile_spec_args():
    spec = ProfileSpec(args={"t": {"k": "v"}})
    assert spec.args["t"]["k"] == "v"


def test_profile_spec_extends_str():
    assert ProfileSpec(extends="base").extends == "base"


def test_profile_spec_extends_list():
    assert ProfileSpec(extends=["a", "b"]).extends == ["a", "b"]


def test_profile_spec_cursor_fields():
    spec = ProfileSpec(start_from="t1", stop_after="t2")
    assert spec.start_from == "t1"
    assert spec.stop_after == "t2"


def test_kptn_config_with_profile():
    config = KptnConfig(profiles={"dev": ProfileSpec()})
    assert "dev" in config.profiles
    assert isinstance(config.profiles["dev"], ProfileSpec)
