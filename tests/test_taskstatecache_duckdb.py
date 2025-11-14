from pathlib import Path
import logging

from kptn.caching.TaskStateCache import TaskStateCache
from kptn.util.runtime_config import RuntimeConfig


def _build_runtime_config(entries: dict) -> RuntimeConfig:
    return RuntimeConfig(entries, None)


def _make_cache(tmp_path: Path) -> TaskStateCache:
    cache = object.__new__(TaskStateCache)
    cache.tasks_root_dir = tmp_path
    cache.duckdb_tasks_dir = tmp_path
    cache.tasks_config = {"tasks": {}, "config": {}}
    cache._duckdb_sql_functions = {}
    cache._python_module_cache = {}
    cache.logger = logging.getLogger("test")
    return cache


def test_duckdb_parameters_use_include_path(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    cache = _make_cache(tmp_path)
    cache.tasks_config["config"] = {
        "duckdb": "memory",
        "config_path": {"include": "config.json"},
        "plain": 5,
    }
    runtime_config = _build_runtime_config(
        {
            "duckdb": object(),
            "config_path": {"should": "be overridden"},
            "plain": 5,
        }
    )

    params = cache._build_duckdb_sql_parameters(runtime_config)

    assert params["config_path"] == str(config_path.resolve())
    assert params["plain"] == 5
    assert "duckdb" not in params


def test_duckdb_parameters_support_multiple_include_entries(tmp_path):
    (tmp_path / "one.json").write_text("{}", encoding="utf-8")
    (tmp_path / "two.json").write_text("{}", encoding="utf-8")
    cache = _make_cache(tmp_path)
    cache.tasks_config["config"] = {
        "duckdb": "memory",
        "multi": {"include": ["one.json", "two.json"]},
    }
    runtime_config = _build_runtime_config(
        {
            "duckdb": object(),
            "multi": {"ignored": True},
        }
    )

    params = cache._build_duckdb_sql_parameters(runtime_config)

    assert params["multi"] == [
        str((tmp_path / "one.json").resolve()),
        str((tmp_path / "two.json").resolve()),
    ]


def test_duckdb_parameters_skip_connection_alias(tmp_path):
    conn = object()
    cache = _make_cache(tmp_path)
    cache.tasks_config["config"] = {}
    runtime_config = _build_runtime_config(
        {
            "duckdb": conn,
            "engine": conn,
            "other": "value",
        }
    )

    params = cache._build_duckdb_sql_parameters(runtime_config)

    assert "engine" not in params
    assert params["other"] == "value"


def test_split_duckdb_sql_respects_quotes(tmp_path):
    cache = _make_cache(tmp_path)
    sql = "INSERT INTO demo VALUES('a;b'); -- comment;\nSELECT 1; /* multi; */"

    statements = cache._split_duckdb_sql(sql)

    assert statements == [
        "INSERT INTO demo VALUES('a;b')",
        "SELECT 1",
    ]


def test_statement_parameters_ignore_casts_and_strings(tmp_path):
    cache = _make_cache(tmp_path)
    statement = "SELECT :foo, ':bar', value::text, :baz FROM demo"
    params = {"foo": 1, "baz": 2, "text": 3}

    filtered = cache._extract_statement_parameters(statement, params)

    assert filtered == {"foo": 1, "baz": 2}


class FakeConn:
    def __init__(self):
        self.calls: list[tuple[str, object | None]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self

    def fetchone(self):
        return None


def test_duckdb_sql_runner_executes_statements_individually(tmp_path):
    cache = _make_cache(tmp_path)
    cache.tasks_config["tasks"] = {
        "duck": {
            "file": "script.sql",
        }
    }
    cache.tasks_config["config"] = {
        "duckdb": "memory",
        "foo": "value",
        "bar": "value",
    }
    sql_text = "SELECT 1; SELECT :foo; SELECT ':bar'; SELECT :bar;"
    (tmp_path / "script.sql").write_text(sql_text, encoding="utf-8")
    conn = FakeConn()
    runtime_config = _build_runtime_config({"duckdb": conn, "foo": 10, "bar": 20})

    runner = cache._ensure_duckdb_sql_callable("duck")
    runner(runtime_config)

    executed = [call for call in conn.calls if call[0].startswith("SELECT") and "file_search_path" not in call[0]]
    assert executed[0][0] == "SELECT 1"
    assert executed[0][1] is None
    assert executed[1][0] == "SELECT :foo"
    assert executed[1][1] == {"foo": 10}
    assert executed[2][0] == "SELECT ':bar'"
    assert executed[2][1] is None
    assert executed[3][0] == "SELECT :bar"
    assert executed[3][1] == {"bar": 20}
