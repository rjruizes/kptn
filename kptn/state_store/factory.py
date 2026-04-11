from typing import Any

from kptn.state_store.protocol import StateStoreBackend
from kptn.state_store.sqlite import SqliteBackend


def init_state_store(settings: Any = None) -> StateStoreBackend:
    db = (getattr(settings, "db", "sqlite") or "sqlite").lower()
    path = getattr(settings, "db_path", None) or ".kptn/kptn.db"

    if db == "sqlite":
        return SqliteBackend(path=path)
    elif db == "duckdb":
        from kptn.state_store.duckdb import DuckDbBackend  # deferred — duckdb is optional
        return DuckDbBackend(path=path)
    else:
        raise ValueError(f"Unknown state store backend {db!r}. Expected 'sqlite' or 'duckdb'.")
