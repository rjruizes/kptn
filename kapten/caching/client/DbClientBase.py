import os
from pydantic import BaseModel

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


def init_db_client(table_name, storage_key, pipeline, tasks_config=None) -> DbClientBase:
    """Return a database client; choose between DynamoDB and SQLite based on db setting in config"""
    
    # Default to DynamoDB for backwards compatibility
    db_type = "dynamodb"
    
    # Read db type from tasks config if available
    if tasks_config and "settings" in tasks_config:
        db_type = tasks_config["settings"].get("db", "dynamodb")
    
    if db_type == "dynamodb":
        from kapten.caching.client.DbClientDDB import DbClientDDB

        aws_auth={}
        if os.getenv("LOCAL_DYNAMODB") == "true":
            aws_auth = { "endpoint_url": "http://dynamodb:8001" }

        return DbClientDDB(table_name=table_name, storage_key=storage_key, pipeline=pipeline, aws_auth=aws_auth)
    elif db_type == "sqlite":
        from kapten.caching.client.DbClientSQLite import DbClientSQLite
        return DbClientSQLite(table_name=table_name, storage_key=storage_key, pipeline=pipeline)
    else:
        raise ValueError(f"Unsupported database type: {db_type}. Supported types are: dynamodb, sqlite")
