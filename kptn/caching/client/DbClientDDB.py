
import boto3
import os
import json
import datetime
from collections.abc import Sized
from typing import Any, Dict, List
from kptn.caching.client.DbClientBase import DbClientBase
from kptn.caching.client.dynamodb import (
    create_subtaskbin,
    create_task,
    create_taskdatabin,
    get_subtaskbins,
    get_single_task,
    get_taskdatabins,
    get_tasks_for_pipeline,
    set_time_in_subitem_in_bin,
    update_task
)
from kptn.caching.models import Subtask, TaskState, taskStateAdapter, subtasksAdapter

# Subitems are binned to workaround the low batch operation limit (25) of DynamoDB.
# A bin size of 500 is chosen to keep the app from getting throttled by a rate limit on the number of 
# updates to a partition per second.
BIN_SIZE = 500

def calculate_bin_ids(subitem_count: int) -> List[str]:
    if not subitem_count:
        return ["0"]
    num_bins = int(subitem_count) // BIN_SIZE
    bin_ids = [str(i) for i in range(0, num_bins+1)]
    return bin_ids

def get_count_field(bin_name: str):
    if bin_name == "SUBSETBIN":
        count_field = "subset_count"
    elif bin_name == "TASKDATABIN":
        count_field = "taskdata_count"
    elif bin_name == "SUBTASKBIN":
        count_field = "subtask_count"
    else:
        raise ValueError(f"Invalid bin type: {bin_name}")

    return count_field

class DbClientDDB(DbClientBase):
    client: boto3.client = None
    table_name: str = os.getenv("DYNAMODB_TABLE_NAME", "tasks")
    storage_key: str
    pipeline: str
    primary_key: str = "PK"
    sort_key: str = "SK"

    def __init__(
        self, table_name=None, storage_key=None, pipeline=None, region=None, aws_auth={}
    ):
        super().__init__(table_name=table_name, storage_key=storage_key, pipeline=pipeline)
        # aws auth if defined includes aws_access_key_id, aws_secret_access_key, aws_session_token

        self.client = boto3.client(
            "dynamodb", region_name=os.getenv("AWS_REGION", region), **aws_auth
        )

        # ecs_container_metadata_file = os.getenv("ECS_CONTAINER_METADATA_FILE")
        # if ecs_container_metadata_file:
        #     # Example output: { ...
        #     # "TaskARN": "arn:aws:ecs:us-east-2:576127390344:task/nibrsep-bravo/22b264e9930b43678540e0e8e69c82c1",
        #     # }
        #     metadata = subprocess.check_output(["cat", ecs_container_metadata_file]).decode("utf-8").strip()
        #     metadata = json.loads(metadata)
        #     region = metadata["TaskARN"].split(":")[3]
        #     self.dynamodb = boto3.resource("dynamodb", region_name=region)
        # else:
        #     self.dynamodb = boto3.resource("dynamodb", **aws_auth)

        if "endpoint_url" in aws_auth:
            self.create_table(self.table_name)
        # self.table = self.dynamodb.Table(self.table_name)

    def create_table(self, table_name):
        # Pass if the table already exists
        try:
            self.client.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "PK", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "SK", "KeyType": "RANGE"},  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "PK", "AttributeType": "S"},  # String
                    {"AttributeName": "SK", "AttributeType": "S"},  # String
                ],
                BillingMode="PAY_PER_REQUEST",
                StreamSpecification={
                    "StreamEnabled": True,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            )
        except self.client.exceptions.ResourceInUseException:
            pass

    def create_task(self, task_name, task: TaskState, data=None):
        """
        Create a task in the DynamoDB table.
        Task `data` may be on the task object itself or passed in as a separate argument.
        """
        raw_task = task.model_dump(exclude_none=True)
        taskdata = raw_task.pop("data", None)
        data = data or taskdata
        if isinstance(data, list):
            raw_task['taskdata_count'] = len(data)

        create_task(
            self.client,
            self.table_name,
            self.storage_key,
            self.pipeline,
            task_name,
            raw_task,
        )
        if data:
            self.create_taskdata(task_name, data, "TASKDATABIN")

    def create_taskdata(self, task_name, data, bin_name="TASKDATABIN"):
        if isinstance(data, list):
            # Break up the data into bins
            for i in range(0, len(data), BIN_SIZE):
                bin_id = f"{i // BIN_SIZE}"
                binned_items = data[i : i + BIN_SIZE]
                create_taskdatabin(
                    self.client,
                    self.table_name,
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
                self.client,
                self.table_name,
                self.storage_key,
                self.pipeline,
                task_name,
                bin_name,
                bin_id,
                data,
            )


    def create_subtasks(self, task_name, data, update_count=True):
        if update_count:
            update = {"subtask_count": len(data)}
            update_task(
                self.client,
                self.table_name,
                self.storage_key,
                self.pipeline,
                task_name,
                update,
            )
        assert isinstance(data, list)
        # Break up the data into bins
        for j in range(0, len(data), BIN_SIZE):
            bin_id = f"{j // BIN_SIZE}"
            binned_items = [{"i": i, "key": data[i]} for i in range(j, min(j + BIN_SIZE, len(data)))]
            create_subtaskbin(
                self.client,
                self.table_name,
                self.storage_key,
                self.pipeline,
                task_name,
                bin_id,
                binned_items,
            )

    def set_subtask_started(self, task_name: str, index: str):
        bin_id = f"{index // BIN_SIZE}"
        adjusted_index = index % BIN_SIZE
        time_value = datetime.datetime.now().isoformat()
        set_time_in_subitem_in_bin(
            self.client,
            self.table_name,
            self.storage_key,
            self.pipeline,
            task_name,
            bin_id,
            adjusted_index,
            "startTime",
            time_value,
        )

    def set_subtask_ended(self, task_name: str, index: str, output_hash=None):
        bin_id = f"{index // BIN_SIZE}"
        adjusted_index = index % BIN_SIZE
        end_time = datetime.datetime.now().isoformat()
        set_time_in_subitem_in_bin(
            self.client,
            self.table_name,
            self.storage_key,
            self.pipeline,
            task_name,
            bin_id,
            adjusted_index,
            "endTime",
            end_time,
            hash=output_hash,
        )
    
    def set_task_ended(self, task_name: str, result=None, result_hash=None, outputs_version=None, status=None, subset_mode=False):
        timestamp = datetime.datetime.now().isoformat()
        if subset_mode and result:
            update = {"UpdatedAt": timestamp}
            if isinstance(result, Sized):
                update["subset_count"] = len(result)
            update_task(
                self.client,
                self.table_name,
                self.storage_key,
                self.pipeline,
                task_name,
                update,
            )
            # print("set_task_ended: Creating subset data", result)
            self.create_taskdata(task_name, result, "SUBSETBIN")
            return

        update = {"end_time": timestamp, "UpdatedAt": timestamp}
        if isinstance(result, Sized):
            update["taskdata_count"] = len(result)
        elif result is not None:
            update["taskdata_count"] = 1
        if outputs_version:
            update["outputs_version"] = outputs_version
        if result_hash:
            update["output_data_version"] = result_hash
        if status:
            update["status"] = status
        update_task(
            self.client,
            self.table_name,
            self.storage_key,
            self.pipeline,
            task_name,
            update,
        )
        if result:
            self.create_taskdata(task_name, result, "TASKDATABIN")

    def update_task(self, task_name, task: TaskState):
        update_task(
            self.client,
            self.table_name,
            self.storage_key,
            self.pipeline,
            task_name,
            task.model_dump(exclude_none=True),
        )

    def get_task(self, task_name, include_data=False, subset_mode=False) -> TaskState:
        single_task = get_single_task(
            self.client, self.table_name, self.storage_key, self.pipeline, task_name
        )
        if single_task and include_data and "taskdata_count" in single_task:
            bin_ids = calculate_bin_ids(single_task["taskdata_count"])
            # if subset mode, try to get subset data; if it doesn't exist, get task data
            if subset_mode:
                subset = self.get_taskdata(task_name, subset_mode=True, bin_ids=bin_ids)
                if subset:
                    # print("get_task: Using subset data", subset)
                    single_task["data"] = subset
                else:
                    # print("get_task: No subset data, using task data")
                    single_task["data"] = self.get_taskdata(task_name, bin_ids=bin_ids)
            else:
                # print("get_task: Using task data")
                single_task["data"] = self.get_taskdata(task_name, bin_ids=bin_ids)

        if single_task is None:
            return None
        return taskStateAdapter.validate_python(single_task)

    def get_tasks(self):
        return get_tasks_for_pipeline(
            self.client, self.table_name, self.storage_key, self.pipeline
        )

    def get_taskdata(self, task_name, subset_mode=False, bin_ids=None):
        if bin_ids is None:
            t = self.get_task(task_name)
            if t is None:
                return []
            count = t.subset_count if subset_mode else t.taskdata_count
            bin_ids = calculate_bin_ids(count)

        bin_name = "SUBSETBIN" if subset_mode else "TASKDATABIN"
        databins = get_taskdatabins(
            self.client, self.table_name, self.storage_key, self.pipeline, task_name, bin_ids, bin_name
        )
        # If data isn't broken up into bins, return it as is
        if len(databins) == 1:
            # Try to parse the data as JSON
            try:
                return json.loads(databins[0]["data"])
            except json.JSONDecodeError:
                return databins[0]["data"]
        # Else concatenate the data from all bins
        data = []
        for bin in databins:
            data.extend(json.loads(bin["data"]))
        return data

    def get_subtasks(self, task_name, bin_ids=None) -> list[Subtask]:
        if bin_ids is None:
            t = self.get_task(task_name)
            if t is None:
                return []
            bin_ids = calculate_bin_ids(t.subtask_count)

        databins = get_taskdatabins(
            self.client, self.table_name, self.storage_key, self.pipeline, task_name, bin_ids, "SUBTASKBIN"
        )
        data = []
        for databin in databins:
            data.extend(databin["items"])
        return subtasksAdapter.validate_python(data)

    def reset_subset_of_subtasks(self, task_name: str, subset: List[str]):
        """
        Implementing this would allow tracking subtask state when a subset of subtasks are re-run.
        Without this implemented, the start and end times are updated as the subtasks are re-run. So in the event of subset subtask failure,
        you would know from Prefect but not from the cache, because the end time would still be set from the original non-subset run.
        We don't need this at the moment.
        The implementation would be:
        1. Query subtask bins for the task,
        2. Loop over the subset (subtask key list),
        3. Group keys by bin (requires finding each key in existing bins),
        4. Update each bin 50 subobjects at a time: remove start and end time on given keys
        """
        pass

    def _batch_delete_bins(
        self, storage_key: str, pipeline_id: str, task_id: str, bin_type: str, bin_ids: List[str]
    ):
        """Utility function to delete multiple subtask bins in a single batch."""
        request_items = {
            self.table_name: [
                {
                    "DeleteRequest": {
                        "Key": {
                            self.primary_key: {"S": f"BRANCH#{storage_key}#PIPELINE#{pipeline_id}#TASK#{task_id}#{bin_type}#{bin_id}"},
                            self.sort_key: { "S": f"BIN#{bin_id}" }
                        }
                    }
                }
                for bin_id in bin_ids
            ]
        }
        self.client.batch_write_item(RequestItems=request_items)

    def delete_bins(self, task_id: str, bin_type: str, task: TaskState = None):
        if task is None:
            task = self.get_task(task_id)
            if task is None:
                return
        count_field = get_count_field(bin_type)
        bin_ids = calculate_bin_ids(getattr(task, count_field))
        DDB_MAX_BATCH_SIZE = (
            25  # Max number of items that can be deleted in a single batch
        )

        # bins = get_taskdatabins(
        #     self.client, self.table_name, self.storage_key, self.pipeline, task_id, bin_ids, bin_type
        # )
        print("Deleting bins", bin_ids)
        for i in range(0, len(bin_ids), DDB_MAX_BATCH_SIZE):
            self._batch_delete_bins(
                self.storage_key,
                self.pipeline,
                task_id,
                bin_type,
                bin_ids[i : i + DDB_MAX_BATCH_SIZE],
            )
    def delete_subsetdata(self, task_id: str):
        self.delete_bins(task_id, "SUBSETBIN")

    def delete_task(self, task_id: str):
        """Delete a task and all associated databins from the DynamoDB table."""
        task = self.get_task(task_id)
        if task is None:
            return
        self.delete_bins(task_id, "SUBTASKBIN", task)
        self.delete_bins(task_id, "TASKDATABIN", task)
        self.delete_bins(task_id, "SUBSETBIN", task)

        # Delete the task itself
        self.client.delete_item(
            TableName=self.table_name,
            Key={
                self.primary_key: {"S": f"BRANCH#{self.storage_key}"},
                self.sort_key: {"S": f"PIPELINE#{self.pipeline}#TASK#{task_id}"},
            },
        )
