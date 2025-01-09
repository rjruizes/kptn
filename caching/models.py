from pydantic import BaseModel, TypeAdapter, computed_field
from typing import Any, Optional
from kapten.util.hash import hash_obj

class TaskState(BaseModel):
    PK: str = None
    ecs_task_id: str = ""
    py_code_hashes: Optional[str] = None # We hash the file of the Python task
    r_code_hashes: Optional[str] = None # We hash the file tree of the R task
    input_hashes: Optional[str] = None # We fetch the output_version of each dependency
    input_data_hashes: Optional[str] = None # We fetch the output_version of each dependency
    outputs_version: Optional[str] = None # A task lists its outputs in YAML; we hash each file after running
    data: Optional[Any] = None # Python tasks can store data in the cache; it can be iterated over and/or passed into other tasks 
    output_data_version: Optional[str] = None # We fetch the output_data_version of each dependency
    status:Optional[str] = None
    start_time: str = None
    end_time: str = None
    UpdatedAt: str = None

    @computed_field
    def py_code_version(self) -> str | None:
        return hash_obj(self.py_code_hashes)
    
    @computed_field
    def r_code_version(self) -> str | None:
        return hash_obj(self.r_code_hashes)

    @computed_field
    def inputs_version(self) -> str | None:
        return hash_obj(self.input_hashes)

    @computed_field
    def input_data_version(self) -> str | None:
        return hash_obj(self.input_data_hashes)

taskStateAdapter = TypeAdapter(TaskState)


class Subtask(BaseModel):
    i: int
    key: str
    startTime: str = None
    endTime: str = None
    outputHash: str = None

subtaskAdapter = TypeAdapter(Subtask)
subtasksAdapter = TypeAdapter(list[Subtask])