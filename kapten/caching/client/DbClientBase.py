import os
from pydantic import BaseModel


class DbClientBase(BaseModel):
    def create_task(self, storage_key: str, pipeline: str, task_name: str, value, data=None):
        pass

    def create_subtasks(self, storage_key: str, pipeline: str, task_name: str, subtask_name, value):
        pass

    def get_task(self, storage_key: str, pipeline: str, task_name: str, include_data: bool):
        pass

    def get_tasks(self, storage_key: str, pipeline: str):
        pass

    def get_taskdata(self, storage_key: str, pipeline: str, task_name: str):
        pass

    def delete_task(self, storage_key: str, pipeline: str, task_name: str):
        pass

    def batch_delete(self, keys):
        pass


def init_db_client(table_name, storage_key, pipeline) -> DbClientBase:
    """Return a database client; DynamoDB is the only option"""
    from kapten.caching.client.DbClientDDB import DbClientDDB

    aws_auth={}
    if os.getenv("LOCAL_DYNAMODB") == "true":
        aws_auth = { "endpoint_url": "http://dynamodb:8001" }

    return DbClientDDB(table_name=table_name, storage_key=storage_key, pipeline=pipeline, aws_auth=aws_auth)
