"""DuckDB-backed implementation of DbClientBase.

kptn state is stored under the ``kptn`` schema inside the pipeline's DuckDB
database file.  Because the file is the same one used by tasks, the DuckDB
checkpoint mechanism (``save_duckdb_checkpoint``) naturally snapshots both
the pipeline data *and* the kptn metadata in a single atomic file copy.

Usage
-----
Enabled by setting ``db: duckdb`` in the ``settings`` block of ``kptn.yaml``.
The ``wire_conn`` method must be called once the DuckDB connection is
available (after ``build_runtime_config`` runs in ``TaskStateCache``).
"""

from __future__ import annotations

import datetime
import json
from typing import Any, List, Optional

from kptn.caching.client.DbClientBase import DbClientBase
from kptn.caching.models import Subtask, TaskState, taskStateAdapter, subtasksAdapter

BIN_SIZE = 500

_SCHEMA_DDL = [
    "CREATE SCHEMA IF NOT EXISTS kptn",
    """
    CREATE TABLE IF NOT EXISTS kptn.tasks (
        storage_key  VARCHAR NOT NULL,
        pipeline     VARCHAR NOT NULL,
        task_id      VARCHAR NOT NULL,
        code_hashes  JSON,
        input_hashes JSON,
        input_data_hashes JSON,
        outputs_version     VARCHAR,
        output_data_version VARCHAR,
        status        VARCHAR,
        start_time    VARCHAR,
        end_time      VARCHAR,
        subtask_count  INTEGER DEFAULT 0,
        taskdata_count INTEGER DEFAULT 0,
        subset_count   INTEGER DEFAULT 0,
        created_at VARCHAR NOT NULL,
        updated_at VARCHAR NOT NULL,
        PRIMARY KEY (storage_key, pipeline, task_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS kptn.taskdata_bins (
        storage_key VARCHAR NOT NULL,
        pipeline    VARCHAR NOT NULL,
        task_id     VARCHAR NOT NULL,
        bin_type    VARCHAR NOT NULL,
        bin_id      VARCHAR NOT NULL,
        data        JSON NOT NULL,
        created_at  VARCHAR NOT NULL,
        updated_at  VARCHAR NOT NULL,
        PRIMARY KEY (storage_key, pipeline, task_id, bin_type, bin_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS kptn.subtask_bins (
        storage_key VARCHAR NOT NULL,
        pipeline    VARCHAR NOT NULL,
        task_id     VARCHAR NOT NULL,
        bin_id      VARCHAR NOT NULL,
        data        JSON NOT NULL,
        created_at  VARCHAR NOT NULL,
        updated_at  VARCHAR NOT NULL,
        PRIMARY KEY (storage_key, pipeline, task_id, bin_id)
    )
    """,
]


def _now() -> str:
    return datetime.datetime.now().isoformat()


def _to_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value)


def _from_json(value: Any) -> Any:
    if value is None or not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


class DbClientDuckDB(DbClientBase):
    """Store kptn task state in the pipeline's DuckDB file under the ``kptn`` schema."""

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(
        self,
        table_name: str | None = None,
        storage_key: str | None = None,
        pipeline: str | None = None,
        tasks_config_path: str | None = None,
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.storage_key = storage_key or ""
        self.pipeline = pipeline or ""
        self.tasks_config_path = tasks_config_path
        self._conn: Any = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def wire_conn(self, conn: Any) -> None:
        """Provide the DuckDB connection to use for all state operations.

        Must be called once after the DuckDB connection is available (i.e.
        after ``TaskStateCache`` has built its bootstrap ``RuntimeConfig``).
        Creates the ``kptn`` schema and tables if they do not already exist.
        """
        self._conn = conn
        self._ensure_schema()

    @property
    def conn(self) -> Any:
        if self._conn is None:
            raise RuntimeError(
                "DbClientDuckDB: DuckDB connection has not been wired. "
                "Call wire_conn(conn) before using the client."
            )
        return self._conn

    def _ensure_schema(self) -> None:
        for ddl in _SCHEMA_DDL:
            self._conn.execute(ddl)

    # ------------------------------------------------------------------
    # Task CRUD
    # ------------------------------------------------------------------

    def create_task(self, task_name: str, task: TaskState, data: Any = None) -> None:
        raw_task = task.model_dump(exclude_none=True)
        taskdata = raw_task.pop("data", None)
        data = data or taskdata

        if isinstance(data, list):
            raw_task["taskdata_count"] = len(data)

        ts = _now()
        self.conn.execute(
            """
            INSERT OR REPLACE INTO kptn.tasks (
                storage_key, pipeline, task_id,
                code_hashes, input_hashes, input_data_hashes,
                outputs_version, output_data_version,
                status, start_time, end_time,
                subtask_count, taskdata_count, subset_count,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                self.storage_key,
                self.pipeline,
                task_name,
                _to_json(raw_task.get("code_hashes")),
                _to_json(raw_task.get("input_hashes")),
                _to_json(raw_task.get("input_data_hashes")),
                raw_task.get("outputs_version"),
                raw_task.get("output_data_version"),
                raw_task.get("status"),
                raw_task.get("start_time"),
                raw_task.get("end_time"),
                raw_task.get("subtask_count", 0),
                raw_task.get("taskdata_count", 0),
                raw_task.get("subset_count", 0),
                ts,
                ts,
            ],
        )

        if data:
            self._create_taskdata(task_name, data, "TASKDATABIN")

    def create_taskdata(self, task_name: str, data: Any, bin_name: str = "TASKDATABIN") -> None:
        """Public alias for creating taskdata bins (used by BaseDbClientTest)."""
        self._create_taskdata(task_name, data, bin_name)

    def _create_taskdata(self, task_name: str, data: Any, bin_name: str) -> None:
        if isinstance(data, list):
            for i in range(0, len(data), BIN_SIZE):
                bin_id = str(i // BIN_SIZE)
                self._upsert_taskdata_bin(task_name, bin_name, bin_id, data[i : i + BIN_SIZE])
        else:
            self._upsert_taskdata_bin(task_name, bin_name, "0", data)

    def _upsert_taskdata_bin(
        self, task_name: str, bin_name: str, bin_id: str, data: Any
    ) -> None:
        ts = _now()
        self.conn.execute(
            """
            INSERT OR REPLACE INTO kptn.taskdata_bins
                (storage_key, pipeline, task_id, bin_type, bin_id, data, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [self.storage_key, self.pipeline, task_name, bin_name, bin_id, _to_json(data), ts, ts],
        )

    def get_task(
        self,
        task_name: str,
        include_data: bool = False,
        subset_mode: bool = False,
    ) -> Optional[TaskState]:
        row = self.conn.execute(
            """
            SELECT storage_key, pipeline, task_id,
                   code_hashes, input_hashes, input_data_hashes,
                   outputs_version, output_data_version,
                   status, start_time, end_time,
                   subtask_count, taskdata_count, subset_count
            FROM kptn.tasks
            WHERE storage_key = ? AND pipeline = ? AND task_id = ?
            """,
            [self.storage_key, self.pipeline, task_name],
        ).fetchone()

        if row is None:
            return None

        raw = self._row_to_dict(row)
        task = taskStateAdapter.validate_python(raw)

        if include_data:
            task.data = self.get_taskdata(task_name, subset_mode=subset_mode)

        return task

    def _row_to_dict(self, row: tuple) -> dict[str, Any]:
        keys = [
            "storage_key", "pipeline", "task_id",
            "code_hashes", "input_hashes", "input_data_hashes",
            "outputs_version", "output_data_version",
            "status", "start_time", "end_time",
            "subtask_count", "taskdata_count", "subset_count",
        ]
        raw: dict[str, Any] = {}
        for key, value in zip(keys, row):
            if key in {"code_hashes", "input_hashes", "input_data_hashes"}:
                value = _from_json(value)
            if value is not None:
                raw[key] = value
        raw.pop("storage_key", None)
        raw.pop("pipeline", None)
        task_id = raw.pop("task_id", "")
        raw["PK"] = task_id
        return raw

    def get_tasks(self, pipeline: str | None = None) -> List[TaskState]:
        pipeline = pipeline or self.pipeline
        rows = self.conn.execute(
            """
            SELECT storage_key, pipeline, task_id,
                   code_hashes, input_hashes, input_data_hashes,
                   outputs_version, output_data_version,
                   status, start_time, end_time,
                   subtask_count, taskdata_count, subset_count
            FROM kptn.tasks
            WHERE storage_key = ? AND pipeline = ?
            ORDER BY task_id
            """,
            [self.storage_key, pipeline],
        ).fetchall()

        tasks = []
        for row in rows:
            raw = self._row_to_dict(row)
            tasks.append(taskStateAdapter.validate_python(raw))
        return tasks

    def update_task(self, task_name: str, task: TaskState) -> None:
        raw_task = task.model_dump(
            exclude_none=True,
            exclude={"code_version", "inputs_version", "input_data_version"},
        )
        raw_task.pop("data", None)
        raw_task["updated_at"] = _now()

        set_clauses = []
        values = []
        for key, value in raw_task.items():
            if key in {"code_hashes", "input_hashes", "input_data_hashes"}:
                value = _to_json(value)
            set_clauses.append(f"{key} = ?")
            values.append(value)

        if not set_clauses:
            return

        values.extend([self.storage_key, self.pipeline, task_name])
        self.conn.execute(
            f"UPDATE kptn.tasks SET {', '.join(set_clauses)} "
            "WHERE storage_key = ? AND pipeline = ? AND task_id = ?",
            values,
        )

    def set_task_ended(
        self,
        task_name: str,
        result: Any = None,
        result_hash: Any = None,
        outputs_version: Any = None,
        status: Any = None,
        subset_mode: bool = False,
    ) -> None:
        ts = _now()

        if subset_mode and result:
            self.conn.execute(
                "UPDATE kptn.tasks SET updated_at = ?, subset_count = ? "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ?",
                [ts, len(result), self.storage_key, self.pipeline, task_name],
            )
            self._create_taskdata(task_name, result, "SUBSETBIN")
            return

        updates: dict[str, Any] = {"end_time": ts, "updated_at": ts}
        if result:
            updates["taskdata_count"] = len(result)
        if outputs_version:
            updates["outputs_version"] = outputs_version
        if result_hash:
            updates["output_data_version"] = result_hash
        if status:
            updates["status"] = status

        set_clauses = [f"{k} = ?" for k in updates]
        values = list(updates.values())
        values.extend([self.storage_key, self.pipeline, task_name])
        self.conn.execute(
            f"UPDATE kptn.tasks SET {', '.join(set_clauses)} "
            "WHERE storage_key = ? AND pipeline = ? AND task_id = ?",
            values,
        )

        if result:
            self._create_taskdata(task_name, result, "TASKDATABIN")

    def delete_task(self, task_name: str) -> None:
        for tbl in ("taskdata_bins", "subtask_bins"):
            self.conn.execute(
                f"DELETE FROM kptn.{tbl} "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ?",
                [self.storage_key, self.pipeline, task_name],
            )
        self.conn.execute(
            "DELETE FROM kptn.tasks "
            "WHERE storage_key = ? AND pipeline = ? AND task_id = ?",
            [self.storage_key, self.pipeline, task_name],
        )

    def batch_delete(self, keys: List[str]) -> None:
        for key in keys:
            self.delete_task(key)

    # ------------------------------------------------------------------
    # Taskdata
    # ------------------------------------------------------------------

    def get_taskdata(
        self,
        task_name: str,
        subset_mode: bool = False,
        bin_ids: List[str] | None = None,
    ) -> List[Any]:
        bin_name = "SUBSETBIN" if subset_mode else "TASKDATABIN"
        if bin_ids is None:
            rows = self.conn.execute(
                "SELECT bin_id FROM kptn.taskdata_bins "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_type = ? "
                "ORDER BY CAST(bin_id AS INTEGER)",
                [self.storage_key, self.pipeline, task_name, bin_name],
            ).fetchall()
            bin_ids = [r[0] for r in rows]

        if not bin_ids:
            return []

        result: list[Any] = []
        raw_bins: list[Any] = []
        for bin_id in bin_ids:
            row = self.conn.execute(
                "SELECT data FROM kptn.taskdata_bins "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ? "
                "  AND bin_type = ? AND bin_id = ?",
                [self.storage_key, self.pipeline, task_name, bin_name, bin_id],
            ).fetchone()
            if row:
                data = _from_json(row[0])
                raw_bins.append(data)
                if isinstance(data, list):
                    result.extend(data)
                else:
                    result.append(data)

        # If there's only one bin holding a single non-list item, return it
        # directly (mirrors SQLite get_taskdatabins behaviour).
        if len(bin_ids) == 1 and len(result) == 1 and not isinstance(raw_bins[0], list):
            return result[0]

        return result

    def delete_bins(
        self, task_id: str, bin_type: str, task: TaskState | None = None
    ) -> None:
        if bin_type == "SUBTASKBIN":
            self.conn.execute(
                "DELETE FROM kptn.subtask_bins "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ?",
                [self.storage_key, self.pipeline, task_id],
            )
        elif bin_type in {"TASKDATABIN", "SUBSETBIN"}:
            self.conn.execute(
                "DELETE FROM kptn.taskdata_bins "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_type = ?",
                [self.storage_key, self.pipeline, task_id, bin_type],
            )

    def delete_subsetdata(self, task_id: str) -> None:
        self.delete_bins(task_id, "SUBSETBIN")

    # ------------------------------------------------------------------
    # Subtasks
    # ------------------------------------------------------------------

    def create_subtasks(
        self, task_name: str, data: List[str], update_count: bool = True
    ) -> None:
        if update_count:
            self.conn.execute(
                "UPDATE kptn.tasks SET subtask_count = ? "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ?",
                [len(data), self.storage_key, self.pipeline, task_name],
            )

        ts = _now()
        for j in range(0, len(data), BIN_SIZE):
            bin_id = str(j // BIN_SIZE)
            items = [
                {"i": i, "key": data[i]}
                for i in range(j, min(j + BIN_SIZE, len(data)))
            ]
            self.conn.execute(
                """
                INSERT OR REPLACE INTO kptn.subtask_bins
                    (storage_key, pipeline, task_id, bin_id, data, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [self.storage_key, self.pipeline, task_name, bin_id, _to_json(items), ts, ts],
            )

    def get_subtasks(
        self, task_name: str, bin_ids: List[str] | None = None
    ) -> List[Subtask]:
        if bin_ids is None:
            rows = self.conn.execute(
                "SELECT bin_id FROM kptn.subtask_bins "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ? "
                "ORDER BY CAST(bin_id AS INTEGER)",
                [self.storage_key, self.pipeline, task_name],
            ).fetchall()
            bin_ids = [r[0] for r in rows]

        if not bin_ids:
            return []

        all_items: list[dict] = []
        for bin_id in bin_ids:
            row = self.conn.execute(
                "SELECT data FROM kptn.subtask_bins "
                "WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_id = ?",
                [self.storage_key, self.pipeline, task_name, bin_id],
            ).fetchone()
            if row:
                items = _from_json(row[0])
                if isinstance(items, list):
                    all_items.extend(items)

        return subtasksAdapter.validate_python(all_items)

    def set_subtask_started(self, task_name: str, index: str) -> None:
        self._update_subtask_time(task_name, int(index), "startTime", _now())

    def set_subtask_ended(
        self, task_name: str, index: str, output_hash: str | None = None
    ) -> None:
        self._update_subtask_time(
            task_name, int(index), "endTime", _now(), output_hash=output_hash
        )

    def _update_subtask_time(
        self,
        task_name: str,
        index: int,
        field: str,
        timestamp: str,
        output_hash: str | None = None,
    ) -> None:
        bin_id = str(index // BIN_SIZE)

        row = self.conn.execute(
            "SELECT data FROM kptn.subtask_bins "
            "WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_id = ?",
            [self.storage_key, self.pipeline, task_name, bin_id],
        ).fetchone()

        if row is None:
            return

        items: list[dict] = _from_json(row[0]) or []
        for item in items:
            if item.get("i") == index:
                item[field] = timestamp
                if output_hash is not None:
                    item["outputHash"] = output_hash
                break

        self.conn.execute(
            "UPDATE kptn.subtask_bins SET data = ?, updated_at = ? "
            "WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_id = ?",
            [_to_json(items), _now(), self.storage_key, self.pipeline, task_name, bin_id],
        )

    def reset_subset_of_subtasks(
        self, task_name: str, subset: List[str]
    ) -> None:
        """Replace subtask list with ``subset``, resetting all timing fields."""
        items = [{"i": i, "key": subset[i]} for i in range(len(subset))]
        # Delete existing bins and rewrite
        self.conn.execute(
            "DELETE FROM kptn.subtask_bins "
            "WHERE storage_key = ? AND pipeline = ? AND task_id = ?",
            [self.storage_key, self.pipeline, task_name],
        )
        ts = _now()
        for j in range(0, len(items), BIN_SIZE):
            bin_id = str(j // BIN_SIZE)
            chunk = items[j : j + BIN_SIZE]
            self.conn.execute(
                """
                INSERT INTO kptn.subtask_bins
                    (storage_key, pipeline, task_id, bin_id, data, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [self.storage_key, self.pipeline, task_name, bin_id, _to_json(chunk), ts, ts],
            )
