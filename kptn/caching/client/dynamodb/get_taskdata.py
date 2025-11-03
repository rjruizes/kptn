import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from typing import List, Dict, Any
from kptn.util.logger import get_logger

deserializer = TypeDeserializer()

# ':pk': {'S': f'BRANCH#{storage_key}#PIPELINE#{pipeline_id}#TASK#{task_id}#{bin_name}#'},

def get_taskdatabins(dynamodb: boto3.client, table_name: str, storage_key: str, pipeline_id: str, task_id: str, bin_ids: List[str], bin_name="TASKDATABIN") -> List[Dict[str, Any]]:
    """
    Retrieve all taskdata bins for a specific task in a pipeline from the DynamoDB table.

    :param table_name: The name of the DynamoDB table
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_name: The name of the bin
    :param bin_ids: A list of bin IDs to retrieve (e.g. ['1', '2', '3', ...])
    :return: A list of taskdata bin items
    """

    logger = get_logger()
    taskdatabins = []
    for bin_id in bin_ids:
        try:
            response = dynamodb.get_item(
                TableName=table_name,
                Key={
                    'PK': {'S': f'BRANCH#{storage_key}#PIPELINE#{pipeline_id}#TASK#{task_id}#{bin_name}#{bin_id}'},
                    'SK': {'S': f'BIN#{bin_id}'},
                }
            )


            # Check if the item was found
            if 'Item' in response:
                taskdatabin = {k: deserializer.deserialize(v) for k, v in response['Item'].items()}
                taskdatabins.append(taskdatabin)
            else:
                logger.info(f"Item {bin_id} not found in PK: BRANCH#{storage_key}#PIPELINE#{pipeline_id}#TASK#{task_id}#{bin_name}#")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(f'Item {bin_id} not found in PK: BRANCH#{storage_key}#PIPELINE#{pipeline_id}#TASK#{task_id}#{bin_name}#')
            else:
                raise e
    return taskdatabins