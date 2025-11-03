from pydantic import BaseModel, TypeAdapter, computed_field
from typing import Any, Optional
from kptn.util.hash import hash_obj

class TaskState(BaseModel):
    PK: str = None
    code_hashes: Optional[Any] = None # Stores function/file-level hashes for Python, R, or SQL tasks
    input_hashes: Optional[str] = None # We fetch the output_version of each dependency
    input_data_hashes: Optional[str] = None # We fetch the output_version of each dependency
    outputs_version: Optional[str] = None # A task lists its outputs in YAML; we hash each file after running
    data: Optional[Any] = None # Python tasks can store data in the cache; it can be iterated over and/or passed into other tasks 
    output_data_version: Optional[str] = None # We fetch the output_data_version of each dependency
    status: Optional[str] = None
    start_time: str = None
    end_time: str = None
    subtask_count: int = None
    taskdata_count: int = None
    subset_count: int = None
    updated_at: str = None

    @computed_field
    def code_version(self) -> str | None:
        return hash_obj(self.code_hashes)

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
