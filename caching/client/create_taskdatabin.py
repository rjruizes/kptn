import boto3
from botocore.exceptions import ClientError
import json
from typing import Dict, Any, List
import datetime


def create_taskdatabin(dynamodb: boto3.client, table_name: str, storage_key: str, pipeline_id: str, task_id: str, bin_name:str, bin_id: str, data: Any) -> Dict[str, Any]:
    """
    Create a new taskdata bin in the DynamoDB table using boto3.client.

    :param table_name: The name of the DynamoDB table
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_name: Either 'TASKDATABIN' or 'SUBSETBIN'
    :param bin_id: The taskdata bin ID
    :param data: A dictionary of taskdata bin attributes
    :return: The created taskdata bin
    """

    # Construct the item to be inserted
    timestamp = datetime.datetime.now().isoformat()
    item = {
        'PK': {'S': f'BRANCH#{storage_key}#PIPELINE#{pipeline_id}#TASK#{task_id}#{bin_name}#{bin_id}'},
        'SK': {'S': f'BIN#{bin_id}'}, # Unused. DynamoDB doesn't allow empty SK
        'BinId': {'S': bin_id},
        'CreatedAt': {'S': timestamp},
        'UpdatedAt': {'S': timestamp},
    }

    # Add task data to the item
    if isinstance(data, list):
        # Store list values as strings
        item['data'] = {'S': json.dumps(data)}
    elif isinstance(data, dict):
        # Store dictionary value as string
        item['data'] = {'S': json.dumps(data)}
    else:
        item['data'] = {'S': str(data)}

    try:
        response = dynamodb.put_item(
            TableName=table_name,
            Item=item,
            ReturnValues="ALL_OLD"  # This will return None for a new item
        )
        return {k: list(v.values())[0] if isinstance(v, dict) else v for k, v in item.items()}
    except ClientError as e:
        print(f"Error creating task: {e.response['Error']['Message']}")
        raise
