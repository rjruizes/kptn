import sys
import types

from kptn.caching.client.DbClientBase import init_db_client


def _install_stub(monkeypatch, module_name: str, class_name: str):
    module = types.ModuleType(module_name)

    class StubClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    setattr(module, class_name, StubClient)
    monkeypatch.setitem(sys.modules, module_name, module)
    return StubClient


def _prepare_stubs(monkeypatch):
    ddb = _install_stub(monkeypatch, "kptn.caching.client.DbClientDDB", "DbClientDDB")
    sqlite = _install_stub(monkeypatch, "kptn.caching.client.DbClientSQLite", "DbClientSQLite")
    return ddb, sqlite


def test_init_db_client_defaults_to_dynamodb(monkeypatch):
    ddb_cls, _ = _prepare_stubs(monkeypatch)
    client = init_db_client("table", "sk", "pipe", tasks_config={"settings": {}})
    assert isinstance(client, ddb_cls)


def test_init_db_client_uses_configured_sqlite(monkeypatch):
    _, sqlite_cls = _prepare_stubs(monkeypatch)
    client = init_db_client("table", "sk", "pipe", tasks_config={"settings": {"db": "sqlite"}})
    assert isinstance(client, sqlite_cls)


def test_env_override_wins_over_config(monkeypatch):
    _, sqlite_cls = _prepare_stubs(monkeypatch)
    monkeypatch.setenv("KPTN_DB_TYPE", "sqlite")
    client = init_db_client("table", "sk", "pipe", tasks_config={"settings": {"db": "dynamodb"}})
    assert isinstance(client, sqlite_cls)


def test_env_override_is_case_insensitive(monkeypatch):
    ddb_cls, _ = _prepare_stubs(monkeypatch)
    monkeypatch.setenv("KPTN_DB_TYPE", "DYNAMODB")
    client = init_db_client("table", "sk", "pipe", tasks_config={"settings": {"db": "sqlite"}})
    assert isinstance(client, ddb_cls)
