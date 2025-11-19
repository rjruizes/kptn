import os
from pydantic import BaseModel
from typing import Mapping

class DbClientBase(BaseModel):
    def create_task(self, task_name: str, value, data=None):
        pass

    def create_subtasks(self, task_name: str, subtask_name, value):
        pass

    def get_task(self, task_name: str, include_data: bool, subset_mode=False):
        pass

    def get_tasks(self, pipeline: str):
        pass

    def get_taskdata(self, task_name: str):
        pass

    def delete_task(self, task_name: str):
        pass

    def batch_delete(self, keys):
        pass

    def set_task_ended(self, task_name: str, result=None, result_hash=None, outputs_version=None, status=None, subset_mode=False):
        pass


def _normalize_db_type(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def init_db_client(table_name, storage_key, pipeline, tasks_config=None, tasks_config_path=None) -> DbClientBase:
    """Return a database client; choose between DynamoDB and SQLite based on db setting in config"""
    
    # Default to DynamoDB for backwards compatibility
    db_type = "dynamodb"

    env_override = _normalize_db_type(os.getenv("KPTN_DB_TYPE"))
    if env_override:
        db_type = env_override
    else:
        settings_block = None
        if isinstance(tasks_config, Mapping):
            settings_block = tasks_config.get("settings")
        if isinstance(settings_block, Mapping):
            configured = _normalize_db_type(settings_block.get("db"))
            if configured:
                db_type = configured
    
    if db_type == "dynamodb":
        from kptn.caching.client.DbClientDDB import DbClientDDB

        aws_auth={}
        if os.getenv("LOCAL_DYNAMODB") == "true":
            aws_auth = { "endpoint_url": "http://dynamodb:8001" }

        return DbClientDDB(table_name=table_name, storage_key=storage_key, pipeline=pipeline, aws_auth=aws_auth)
    elif db_type == "sqlite":
        from kptn.caching.client.DbClientSQLite import DbClientSQLite
        return DbClientSQLite(
            table_name=table_name,
            storage_key=storage_key,
            pipeline=pipeline,
            tasks_config_path=tasks_config_path,
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}. Supported types are: dynamodb, sqlite")
