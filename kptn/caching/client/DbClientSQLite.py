
import os
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from kptn.caching.client.DbClientBase import DbClientBase
from kptn.caching.client.sqlite import get_connection
from kptn.caching.client.sqlite.create_task import (
    create_task,
    get_single_task,
    update_task,
    get_tasks_for_pipeline,
    delete_task as delete_task_helper
)
from kptn.caching.client.sqlite.create_taskdatabin import (
    create_taskdatabin,
    get_taskdatabins,
    delete_taskdata_bins,
    get_taskdata_bin_ids
)
from kptn.caching.client.sqlite.create_subtaskbin import (
    create_subtaskbin,
    get_subtaskbins,
    set_time_in_subitem_in_bin,
    delete_subtask_bins,
    get_subtask_bin_ids,
    update_subtask_subset
)
from kptn.caching.models import Subtask, TaskState, taskStateAdapter, subtasksAdapter

# Use same bin size as DynamoDB for consistency
BIN_SIZE = 500

def calculate_bin_ids(subitem_count: int) -> List[str]:
    """Calculate bin IDs needed for the given number of subitems."""
    if not subitem_count:
        return ["0"]
    num_bins = int(subitem_count) // BIN_SIZE
    bin_ids = [str(i) for i in range(0, num_bins+1)]
    return bin_ids

def get_count_field(bin_name: str) -> str:
    """Get the count field name for a given bin type."""
    if bin_name == "SUBSETBIN":
        return "subset_count"
    elif bin_name == "TASKDATABIN":
        return "taskdata_count"
    elif bin_name == "SUBTASKBIN":
        return "subtask_count"
    else:
        raise ValueError(f"Invalid bin type: {bin_name}")


class DbClientSQLite(DbClientBase):
    model_config = {
        "arbitrary_types_allowed": True,
        "extra": "allow"
    }

    def __init__(self, table_name=None, storage_key=None, pipeline=None, db_path=None, tasks_config_path: Optional[str] = None):
        super().__init__()
        
        # Set instance attributes
        self.table_name = table_name
        self.storage_key = storage_key or ""
        self.pipeline = pipeline or ""
        self.tasks_config_path = tasks_config_path
        
        # Set up database path
        self.db_path = self._resolve_db_path(db_path)
        
        # Initialize database connection
        self.conn = get_connection(self.db_path)

    def _resolve_db_path(self, explicit_path: Optional[str]) -> str:
        """Determine the on-disk path for the sqlite database."""
        if explicit_path:
            return explicit_path

        default_dir = self._resolve_default_dir()
        default_dir.mkdir(parents=True, exist_ok=True)

        identifier_parts = [part for part in (self.storage_key, self.pipeline) if part]
        if identifier_parts:
            filename = "_".join(identifier_parts) + ".db"
        else:
            filename = "cache.db"

        return str(default_dir / filename)

    def _resolve_default_dir(self) -> Path:
        """Find the directory where the sqlite database should live by default."""
        candidates: List[Path] = []

        if self.tasks_config_path:
            candidates.append(Path(self.tasks_config_path))

        candidates.append(Path.cwd() / "kptn.yaml")

        for candidate in candidates:
            if candidate.is_file():
                return candidate.parent.resolve()

        return Path(os.path.expanduser("~/.kptn/cache"))

    def create_task(self, task_name: str, task: TaskState, data=None):
        """
        Create a task in the SQLite database.
        Task `data` may be on the task object itself or passed in as a separate argument.
        """
        raw_task = task.model_dump(exclude_none=True)
        taskdata = raw_task.pop("data", None)
        data = data or taskdata
        if isinstance(data, list):
            raw_task['taskdata_count'] = len(data)

        create_task(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name,
            raw_task,
        )
        
        if data:
            self.create_taskdata(task_name, data, "TASKDATABIN")

    def create_taskdata(self, task_name: str, data: Any, bin_name="TASKDATABIN"):
        """Create taskdata bins for storing task data."""
        if isinstance(data, list):
            # Break up the data into bins
            for i in range(0, len(data), BIN_SIZE):
                bin_id = f"{i // BIN_SIZE}"
                binned_items = data[i : i + BIN_SIZE]
                create_taskdatabin(
                    self.conn,
                    self.storage_key,
                    self.pipeline,
                    task_name,
                    bin_name,
                    bin_id,
                    binned_items,
                )
        else:
            bin_id = "0"
            create_taskdatabin(
                self.conn,
                self.storage_key,
                self.pipeline,
                task_name,
                bin_name,
                bin_id,
                data,
            )

    def create_subtasks(self, task_name: str, data: List[str], update_count=True):
        """Create subtask bins for tracking subtask progress."""
        if update_count:
            update_task(
                self.conn,
                self.storage_key,
                self.pipeline,
                task_name,
                {"subtask_count": len(data)}
            )
        
        assert isinstance(data, list)
        # Break up the data into bins
        for j in range(0, len(data), BIN_SIZE):
            bin_id = f"{j // BIN_SIZE}"
            binned_items = [{"i": i, "key": data[i]} for i in range(j, min(j + BIN_SIZE, len(data)))]
            create_subtaskbin(
                self.conn,
                self.storage_key,
                self.pipeline,
                task_name,
                bin_id,
                binned_items,
            )

    def set_subtask_started(self, task_name: str, index: str):
        """Mark a subtask as started."""
        timestamp = datetime.datetime.now().isoformat()
        index_int = int(index)
        bin_id = str(index_int // BIN_SIZE)
        bin_index = index_int % BIN_SIZE
        
        set_time_in_subitem_in_bin(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name,
            bin_id,
            bin_index,
            "startTime",
            timestamp
        )

    def set_subtask_ended(self, task_name: str, index: str, output_hash=None):
        """Mark a subtask as ended."""
        timestamp = datetime.datetime.now().isoformat()
        index_int = int(index)
        bin_id = str(index_int // BIN_SIZE)
        bin_index = index_int % BIN_SIZE
        
        set_time_in_subitem_in_bin(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name,
            bin_id,
            bin_index,
            "endTime",
            timestamp,
            output_hash
        )

    def set_task_ended(self, task_name: str, result=None, result_hash=None, outputs_version=None, status=None, subset_mode=False):
        """Mark a task as ended and optionally store result data."""
        timestamp = datetime.datetime.now().isoformat()
        
        if subset_mode and result:
            update_data = {"updated_at": timestamp}
            if result:
                update_data["subset_count"] = len(result)
            
            update_task(
                self.conn,
                self.storage_key,
                self.pipeline,
                task_name,
                update_data,
            )
            
            self.create_taskdata(task_name, result, "SUBSETBIN")
            return

        update_data = {"end_time": timestamp, "updated_at": timestamp}
        if result:
            update_data["taskdata_count"] = len(result)
        if outputs_version:
            update_data["outputs_version"] = outputs_version
        if result_hash:
            update_data["output_data_version"] = result_hash
        if status:
            update_data["status"] = status
        
        update_task(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name,
            update_data,
        )
        
        if result:
            self.create_taskdata(task_name, result, "TASKDATABIN")

    def update_task(self, task_name: str, task: TaskState):
        """Update a task with new data."""
        # Exclude computed fields since they don't exist as database columns
        raw_task = task.model_dump(
            exclude_none=True,
            exclude={'code_version', 'inputs_version', 'input_data_version'}
        )
        update_task(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name,
            raw_task
        )

    def get_task(self, task_name: str, include_data=False, subset_mode=False) -> Optional[TaskState]:
        """Retrieve a task by name."""
        raw_task = get_single_task(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name
        )
        
        if not raw_task:
            return None
        
        # Remove SQLite-specific fields and None values
        for field in ['id', 'created_at', 'updated_at']:
            raw_task.pop(field, None)
        
        # Remove None values to prevent Pydantic validation errors
        filtered_task = {k: v for k, v in raw_task.items() if v is not None}
        
        # Convert to TaskState
        task = taskStateAdapter.validate_python(filtered_task)
        
        if include_data:
            if subset_mode:
                task.data = self.get_taskdata(task_name, subset_mode=True)
            else:
                task.data = self.get_taskdata(task_name)
        
        return task

    def get_tasks(self, pipeline: str = None) -> List[TaskState]:
        """Get all tasks for the pipeline."""
        pipeline = pipeline or self.pipeline
        raw_tasks = get_tasks_for_pipeline(
            self.conn,
            self.storage_key,
            pipeline
        )
        
        tasks = []
        for raw_task in raw_tasks:
            # Remove SQLite-specific fields and None values
            for field in ['id', 'created_at', 'updated_at']:
                raw_task.pop(field, None)
            
            # Remove None values to prevent Pydantic validation errors
            filtered_task = {k: v for k, v in raw_task.items() if v is not None}
            
            task = taskStateAdapter.validate_python(filtered_task)
            tasks.append(task)
        
        return tasks

    def get_taskdata(self, task_name: str, subset_mode=False, bin_ids=None):
        """Retrieve task data from bins."""
        bin_name = "SUBSETBIN" if subset_mode else "TASKDATABIN"
        
        if bin_ids is None:
            # Get all bin IDs for this task and bin type
            bin_ids = get_taskdata_bin_ids(
                self.conn,
                self.storage_key,
                self.pipeline,
                task_name,
                bin_name
            )
        
        if not bin_ids:
            return []
        
        return get_taskdatabins(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name,
            bin_ids,
            bin_name
        )

    def get_subtasks(self, task_name: str, bin_ids=None) -> List[Subtask]:
        """Retrieve subtasks from bins."""
        if bin_ids is None:
            # Get all bin IDs for subtasks
            bin_ids = get_subtask_bin_ids(
                self.conn,
                self.storage_key,
                self.pipeline,
                task_name
            )
        
        if not bin_ids:
            return []
        
        raw_subtasks = get_subtaskbins(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name,
            bin_ids
        )
        
        return subtasksAdapter.validate_python(raw_subtasks)

    def reset_subset_of_subtasks(self, task_name: str, subset: List[str]):
        """Reset subtasks to only include those in the subset."""
        update_subtask_subset(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name,
            subset,
            reset_times=True
        )

    def delete_task(self, task_name: str):
        """Delete a task and all associated data."""
        delete_task_helper(
            self.conn,
            self.storage_key,
            self.pipeline,
            task_name
        )

    def batch_delete(self, keys):
        """Batch delete multiple tasks."""
        for key in keys:
            self.delete_task(key)

    def delete_bins(self, task_id: str, bin_type: str, task: TaskState = None):
        """Delete bins for a specific task and bin type."""
        if bin_type == "SUBTASKBIN":
            delete_subtask_bins(
                self.conn,
                self.storage_key,
                self.pipeline,
                task_id
            )
        elif bin_type in ["TASKDATABIN", "SUBSETBIN"]:
            delete_taskdata_bins(
                self.conn,
                self.storage_key,
                self.pipeline,
                task_id,
                bin_type
            )

    def delete_subsetdata(self, task_id: str):
        """Delete subset data for a task."""
        self.delete_bins(task_id, "SUBSETBIN")

    def __del__(self):
        """Clean up database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
