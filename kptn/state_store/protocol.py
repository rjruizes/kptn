"""StateStoreBackend Protocol for kptn state persistence.

This module only imports from the Python standard library (typing).
The state_store/ package only imports from kptn.exceptions.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class StateStoreBackend(Protocol):
    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None: ...
    def write_hash(self, storage_key: str, pipeline: str, task: str, hash: str) -> None: ...
    def delete(self, storage_key: str, pipeline: str, task: str) -> None: ...
    def list_tasks(self, storage_key: str, pipeline: str) -> list[str]: ...
