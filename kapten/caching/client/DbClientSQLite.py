
from kapten.caching.client.DbClientBase import DbClientBase


class DbClientSQLite(DbClientBase):
    def __init__(self):
        super().__init__()
        pass

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