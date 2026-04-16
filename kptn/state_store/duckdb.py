import duckdb
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from kptn.exceptions import StateStoreError


class DuckDbBackend:
    """DuckDB-backed state store.

    Two construction modes:

    * ``DuckDbBackend(path="...")`` — opens and owns a persistent connection to
      the given file.  Connection lifecycle is managed internally.

    * ``DuckDbBackend(factory=get_engine)`` — delegates connection acquisition to
      a user-supplied callable.  The factory is re-called before *every* operation
      so that the backend always uses the currently active connection, even if
      tasks have closed and replaced it during pipeline execution.  Ownership of
      the connection remains with the caller; ``close()`` delegates to the factory.
    """

    def __init__(
        self,
        path: str = ".kptn/kptn.db",
        *,
        factory: Callable[[], "duckdb.DuckDBPyConnection"] | None = None,
    ) -> None:
        if factory is not None:
            self._factory = factory
            self._path = None
        else:
            self._factory = None
            self._path = path
            try:
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                self.__owned_conn = duckdb.connect(path)
            except (OSError, duckdb.Error) as exc:
                raise StateStoreError(f"Failed to open state store at {path!r}") from exc

        try:
            self._create_table()
        except duckdb.Error as exc:
            if self._factory is None:
                self.__owned_conn.close()
            raise StateStoreError(
                f"Failed to initialise state store"
                + (f" at {path!r}" if path else "")
            ) from exc

    def _conn(self) -> "duckdb.DuckDBPyConnection":
        """Return the active connection — fresh from factory each call, or the owned one."""
        if self._factory is not None:
            return self._factory()
        return self.__owned_conn

    def close(self) -> None:
        """Close the underlying connection.

        In factory mode the factory is called to obtain the current connection and
        then closed, leaving lifecycle management consistent with path mode.
        """
        try:
            self._conn().close()
        except Exception:
            pass

    def _create_table(self) -> None:
        conn = self._conn()
        conn.execute("CREATE SCHEMA IF NOT EXISTS _kptn")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _kptn.task_state (
                storage_key   TEXT NOT NULL,
                pipeline_name TEXT NOT NULL,
                task_name     TEXT NOT NULL,
                output_hash   TEXT,
                status        TEXT,
                ran_at        TEXT,
                PRIMARY KEY (storage_key, pipeline_name, task_name)
            )
        """)
        conn.commit()

    def write_hash(self, storage_key: str, pipeline: str, task: str, hash: str) -> None:
        try:
            self._write_hash(storage_key, pipeline, task, hash)
        except duckdb.CatalogException:
            self._create_table()
            self._write_hash(storage_key, pipeline, task, hash)
        except duckdb.Error as exc:
            raise StateStoreError("write_hash failed") from exc

    def _write_hash(self, storage_key: str, pipeline: str, task: str, hash: str) -> None:
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO _kptn.task_state "
            "(storage_key, pipeline_name, task_name, output_hash, status, ran_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (storage_key, pipeline, task, hash, "success", datetime.now(UTC).isoformat()),
        )
        conn.commit()
        conn.execute("CHECKPOINT")

    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None:
        try:
            row = self._conn().execute(
                "SELECT output_hash FROM _kptn.task_state "
                "WHERE storage_key=? AND pipeline_name=? AND task_name=?",
                (storage_key, pipeline, task),
            ).fetchone()
            return row[0] if row else None
        except duckdb.CatalogException:
            return None
        except duckdb.Error as exc:
            raise StateStoreError("read_hash failed") from exc

    def delete(self, storage_key: str, pipeline: str, task: str) -> None:
        try:
            conn = self._conn()
            conn.execute(
                "DELETE FROM _kptn.task_state "
                "WHERE storage_key=? AND pipeline_name=? AND task_name=?",
                (storage_key, pipeline, task),
            )
            conn.commit()
        except duckdb.CatalogException:
            pass
        except duckdb.Error as exc:
            raise StateStoreError("delete failed") from exc

    def list_tasks(self, storage_key: str, pipeline: str) -> list[str]:
        try:
            rows = self._conn().execute(
                "SELECT task_name FROM _kptn.task_state "
                "WHERE storage_key=? AND pipeline_name=?",
                (storage_key, pipeline),
            ).fetchall()
            return [r[0] for r in rows]
        except duckdb.CatalogException:
            return []
        except duckdb.Error as exc:
            raise StateStoreError("list_tasks failed") from exc
