"""change_detector package — public API for deterministic hashing and staleness detection."""

from kptn.change_detector.hasher import (
    hash_duckdb_table,
    hash_file,
    hash_sqlite_table,
    hash_task_source,
)
from kptn.change_detector.detector import is_stale

__all__ = [
    "hash_duckdb_table",
    "hash_sqlite_table",
    "hash_file",
    "hash_task_source",
    "is_stale",
]
