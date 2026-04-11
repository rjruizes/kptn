"""In-memory fakes for testing cross-component boundaries.

These fakes implement the StateStoreBackend Protocol using in-memory data
structures, allowing tests to avoid real SQLite or DuckDB backends.
"""


class FakeStateStore:
    """In-memory StateStoreBackend for testing cross-component boundaries."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str, str], str] = {}

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None:
        return self._store.get((storage_key, pipeline, task))

    def write_hash(self, storage_key: str, pipeline: str, task: str, hash: str) -> None:
        self._store[(storage_key, pipeline, task)] = hash

    def delete(self, storage_key: str, pipeline: str, task: str) -> None:
        self._store.pop((storage_key, pipeline, task), None)

    def list_tasks(self, storage_key: str, pipeline: str) -> list[str]:
        return [t for (sk, p, t) in self._store if sk == storage_key and p == pipeline]
