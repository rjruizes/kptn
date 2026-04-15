from kptn.state_store.protocol import StateStoreBackend
from kptn.state_store.sqlite import SqliteBackend
from kptn.state_store.factory import init_state_store
from kptn.state_store.noop import NoOpBackend

try:
    from kptn.state_store.duckdb import DuckDbBackend
    __all__ = ["StateStoreBackend", "SqliteBackend", "DuckDbBackend", "init_state_store", "NoOpBackend"]
except ModuleNotFoundError:
    __all__ = ["StateStoreBackend", "SqliteBackend", "init_state_store", "NoOpBackend"]
