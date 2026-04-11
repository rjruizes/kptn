import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from kptn.exceptions import StateStoreError


class SqliteBackend:
    def __init__(self, path: str = ".kptn/kptn.db") -> None:
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(path)
        except (OSError, sqlite3.Error) as exc:
            raise StateStoreError(f"Failed to open state store at {path!r}") from exc
        try:
            self._create_table()
        except sqlite3.Error as exc:
            self._conn.close()
            raise StateStoreError(f"Failed to open state store at {path!r}") from exc

    def _create_table(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS task_state (
                storage_key   TEXT NOT NULL,
                pipeline_name TEXT NOT NULL,
                task_name     TEXT NOT NULL,
                output_hash   TEXT,
                status        TEXT,
                ran_at        TEXT,
                PRIMARY KEY (storage_key, pipeline_name, task_name)
            )
        """)
        self._conn.commit()

    def write_hash(self, storage_key: str, pipeline: str, task: str, hash: str) -> None:
        try:
            self._conn.execute(
                "INSERT OR REPLACE INTO task_state "
                "(storage_key, pipeline_name, task_name, output_hash, status, ran_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (storage_key, pipeline, task, hash, "success", datetime.now(UTC).isoformat()),
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise StateStoreError("write_hash failed") from exc

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None:
        try:
            row = self._conn.execute(
                "SELECT output_hash FROM task_state "
                "WHERE storage_key=? AND pipeline_name=? AND task_name=?",
                (storage_key, pipeline, task),
            ).fetchone()
            return row[0] if row else None
        except sqlite3.Error as exc:
            raise StateStoreError("read_hash failed") from exc

    def delete(self, storage_key: str, pipeline: str, task: str) -> None:
        try:
            self._conn.execute(
                "DELETE FROM task_state "
                "WHERE storage_key=? AND pipeline_name=? AND task_name=?",
                (storage_key, pipeline, task),
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise StateStoreError("delete failed") from exc

    def list_tasks(self, storage_key: str, pipeline: str) -> list[str]:
        try:
            rows = self._conn.execute(
                "SELECT task_name FROM task_state "
                "WHERE storage_key=? AND pipeline_name=?",
                (storage_key, pipeline),
            ).fetchall()
            return [r[0] for r in rows]
        except sqlite3.Error as exc:
            raise StateStoreError("list_tasks failed") from exc
