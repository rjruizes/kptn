from pathlib import Path
import logging
from types import SimpleNamespace

import tests.runtime_config_fixtures as fixtures

from kptn.caching.TaskStateCache import TaskStateCache
from kptn.util.runtime_config import RuntimeConfig


def _build_runtime_config(entries: dict) -> RuntimeConfig:
    return RuntimeConfig(entries, None)


def _make_cache(tmp_path: Path) -> TaskStateCache:
    cache = object.__new__(TaskStateCache)
    cache.pipeline_name = "pipe"
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


def test_statement_parameters_detect_dollar_notation(tmp_path):
    cache = _make_cache(tmp_path)
    statement = "set variable my_var = (select my.key from read_json_auto($config));"
    params = {"config": "/tmp/config.json", "other": 1}

    filtered = cache._extract_statement_parameters(statement, params)

    assert filtered == {"config": "/tmp/config.json"}


class FakeConn:
    def __init__(self, db_path: str | None = None):
        self.calls: list[tuple[str, object | None]] = []
        self.db_path = db_path
        self.closed = False
        self._last_sql = None

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self._last_sql = sql
        return self

    def fetchone(self):
        return None

    def fetchall(self):
        if self._last_sql == "PRAGMA database_list":
            return [(592, "memory" if self.db_path is None else "main", self.db_path)]
        return []

    def close(self):
        self.closed = True


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


def test_duckdb_sql_runner_uses_runtime_config_callable(tmp_path):
    cache = _make_cache(tmp_path)
    fixtures.TEST_CONFIG_PATH = str(tmp_path / "duck_config.json")
    (tmp_path / "duck_config.json").write_text("{}", encoding="utf-8")
    script_path = tmp_path / "duck.sql"
    script_path.write_text(
        "set variable diet_period_gateway = (select asa24.diet_period_gateway from read_json_auto($config));",
        encoding="utf-8",
    )
    cache.tasks_config = {
        "config": {
            "duckdb": "memory",
            "config": {"function": "tests.runtime_config_fixtures:get_config"},
        },
        "tasks": {"duck": {"file": "duck.sql"}},
    }
    cache.pipeline_config = SimpleNamespace(
        TASKS_CONFIG_PATH=str(tmp_path / "kptn.yaml"),
        PY_MODULE_PATH="",
        PY_TASKS_DIRS=(),
        R_TASKS_DIRS=(),
        scratch_dir=str(tmp_path / "scratch"),
        SUBSET_MODE=False,
    )
    (tmp_path / "kptn.yaml").write_text("{}", encoding="utf-8")
    runtime_config = cache.build_runtime_config(task_name="duck")
    expected_path = fixtures.TEST_CONFIG_PATH
    assert runtime_config["config"] == expected_path

    conn = FakeConn()
    runtime_values = runtime_config.as_dict()
    runtime_values["duckdb"] = conn
    runtime_with_conn = RuntimeConfig(runtime_values, None)

    runner = cache._ensure_duckdb_sql_callable("duck")
    try:
        runner(runtime_with_conn)
    finally:
        fixtures.TEST_CONFIG_PATH = None

    json_calls = [call for call in conn.calls if "read_json_auto" in call[0]]
    assert json_calls
    assert json_calls[0][1] == {"config": expected_path}


def test_flow_type_override_defaults_to_config(tmp_path):
    cache = _make_cache(tmp_path)
    cache.tasks_config["settings"] = {"flow_type": "prefect"}

    assert cache._effective_flow_type() == "prefect"
    assert cache.is_flow_prefect()
    assert not cache.is_flow_stepfunctions()


def test_flow_type_override_honors_env(monkeypatch, tmp_path):
    cache = _make_cache(tmp_path)
    cache.tasks_config["settings"] = {"flow_type": "stepfunctions"}
    assert cache.is_flow_stepfunctions()

    monkeypatch.setenv("KPTN_FLOW_TYPE", "vanilla")
    assert not cache.is_flow_stepfunctions()
    assert cache._effective_flow_type() == "vanilla"

    monkeypatch.setenv("KPTN_FLOW_TYPE", "prefect")
    assert cache.is_flow_prefect()


def test_get_duckdb_checkpoint_supports_boolean(tmp_path):
    cache = _make_cache(tmp_path)
    cache.tasks_config["tasks"] = {"duck": {"file": "duck.sql", "duckdb_checkpoint": True}}

    checkpoint = cache.get_duckdb_checkpoint("duck")

    assert checkpoint is True


def test_get_duckdb_checkpoint_rejects_non_boolean(tmp_path):
    cache = _make_cache(tmp_path)
    cache.tasks_config["tasks"] = {"duck": {"file": "duck.sql", "duckdb_checkpoint": "yes"}}

    try:
        cache.get_duckdb_checkpoint("duck")
    except TypeError as exc:
        assert "must be a boolean" in str(exc)
    else:
        raise AssertionError("Expected TypeError for non-boolean duckdb_checkpoint")


def test_duckdb_checkpoint_restore_and_save_skip_memory(tmp_path):
    cache = _make_cache(tmp_path)
    runtime_config = _build_runtime_config({"duckdb": FakeConn()})

    assert cache.restore_duckdb_checkpoint("duck", runtime_config) is None
    assert cache.save_duckdb_checkpoint("duck", runtime_config) is None


def test_duckdb_checkpoint_restore_and_save_for_file_backed_db(tmp_path):
    cache = _make_cache(tmp_path)
    db_path = tmp_path / "example.ddb"
    db_path.write_text("current-db", encoding="utf-8")
    wal_path = Path(f"{db_path}.wal")
    wal_path.write_text("stale wal", encoding="utf-8")
    conn = FakeConn(str(db_path))
    runtime_config = _build_runtime_config({"duckdb": conn})
    backup_path = tmp_path / "example.init_database.backup.ddb"
    backup_path.write_text("saved-db", encoding="utf-8")

    restored = cache.restore_duckdb_checkpoint("init_database", runtime_config)

    assert restored == backup_path.resolve()
    assert db_path.read_text(encoding="utf-8") == "saved-db"
    assert not wal_path.exists()
    assert conn.closed

    conn_after = FakeConn(str(db_path))
    runtime_after = _build_runtime_config({"duckdb": conn_after})
    db_path.write_text("fresh-db", encoding="utf-8")

    saved = cache.save_duckdb_checkpoint("init_database", runtime_after)

    assert saved == backup_path.resolve()
    assert backup_path.read_text(encoding="utf-8") == "fresh-db"
    assert ("checkpoint", None) in conn_after.calls
    assert conn_after.closed


def test_duckdb_checkpoint_default_path_includes_task_name_and_key(tmp_path):
    cache = _make_cache(tmp_path)
    db_path = tmp_path / "example.ddb"

    path = cache._default_duckdb_checkpoint_path(db_path, "init_database", key="a/b")

    assert path == tmp_path / "example.init_database-a-b.backup.ddb"
