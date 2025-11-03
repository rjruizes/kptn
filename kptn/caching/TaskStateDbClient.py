"""Lightweight access layer for reading task state from the cache backend."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from kptn.caching.client.DbClientBase import DbClientBase, init_db_client
from kptn.caching.models import TaskState
from kptn.util.pipeline_config import PipelineConfig, get_storage_key
from kptn.util.read_tasks_config import read_tasks_config


class TaskStateDbClient:
    """Provide direct database access for task state without hashing/runtime setup."""

    def __init__(
        self,
        pipeline_config: PipelineConfig,
        db_client: Optional[DbClientBase] = None,
        tasks_config: Optional[dict[str, Any]] = None,
    ) -> None:
        self.pipeline_config = pipeline_config
        self.tasks_config = tasks_config or self._load_tasks_config(pipeline_config)
        storage_key = get_storage_key(pipeline_config)
        table_name = os.getenv("DYNAMODB_TABLE_NAME", "tasks")
        self.db_client = db_client or init_db_client(
            table_name=table_name,
            storage_key=storage_key,
            pipeline=pipeline_config.PIPELINE_NAME,
            tasks_config=self.tasks_config,
            tasks_config_path=pipeline_config.TASKS_CONFIG_PATH,
        )

    def _load_tasks_config(self, pipeline_config: PipelineConfig) -> dict[str, Any]:
        config_path = Path(pipeline_config.TASKS_CONFIG_PATH)
        return read_tasks_config(str(config_path))

    def fetch_state(self, task_name: str, include_data: bool = True) -> Optional[TaskState]:
        """Retrieve cached task state if available."""
        cached_state = self.db_client.get_task(
            task_name,
            include_data=include_data,
            subset_mode=self.pipeline_config.SUBSET_MODE,
        )
        if cached_state is None:
            return None
        if isinstance(cached_state, TaskState):
            return cached_state
        return TaskState.model_validate(cached_state)

    def list_tasks(self) -> dict[str, Any]:
        """Expose the raw tasks mapping from the loaded configuration."""
        return self.tasks_config.get("tasks", {})
