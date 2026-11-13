"""No-op state store backend — used when caching is disabled."""

from typing import Mapping



class NoOpBackend:
    """A StateStoreBackend that never reads or writes anything.

    Used when no_cache=True to avoid creating database files on disk.
    """

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None:
        return None

    def read_hashes(self, storage_key: str, pipeline: str) -> dict[str, str | None]:
        return {}

    def write_hash(self, storage_key: str, pipeline: str, task: str, hash: str) -> None:
        pass

    def write_hashes(self, storage_key: str, pipeline: str, hashes: Mapping[str, str]) -> None:
        pass

    def delete(self, storage_key: str, pipeline: str, task: str) -> None:
        pass

    def list_tasks(self, storage_key: str, pipeline: str) -> list[str]:
        return []
