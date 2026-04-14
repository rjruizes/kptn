from typing import Any, Callable

from kptn.state_store.protocol import StateStoreBackend
from kptn.state_store.sqlite import SqliteBackend


def init_state_store(
    settings: Any = None,
    *,
    duckdb_factory: Callable | None = None,
) -> StateStoreBackend:
    """Initialise the state store backend.

    When *duckdb_factory* is provided the DuckDB backend is initialised in
    factory mode — it re-calls the callable before every operation so it always
    holds the currently active connection.  This takes precedence over the
    ``settings.db`` value; the factory is assumed to point at the same file as
    ``settings.db_path``.
    """
    if duckdb_factory is not None:
        from kptn.state_store.duckdb import DuckDbBackend  # deferred — duckdb is optional
        return DuckDbBackend(factory=duckdb_factory)

    db = (getattr(settings, "db", "sqlite") or "sqlite").lower()
    path = getattr(settings, "db_path", None) or ".kptn/kptn.db"

    if db == "sqlite":
        return SqliteBackend(path=path)
    elif db == "duckdb":
        from kptn.state_store.duckdb import DuckDbBackend  # deferred — duckdb is optional
        return DuckDbBackend(path=path)
    else:
        raise ValueError(f"Unknown state store backend {db!r}. Expected 'sqlite' or 'duckdb'.")
