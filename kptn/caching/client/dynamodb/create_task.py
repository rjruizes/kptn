import boto3
from botocore.exceptions import ClientError
from typing import Dict, Any
import datetime

def create_task(dynamodb: boto3.client, table_name: str, storage_key: str, pipeline_id: str, task_id: str, task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new task in the DynamoDB table using boto3.client.

    :param table_name: The name of the DynamoDB table
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :param task_data: A dictionary of task attributes
    :return: The created task item
    """

    # Construct the item to be inserted
    timestamp = datetime.datetime.now().isoformat()
    item = {
        'PK': {'S': f'BRANCH#{storage_key}'},
        'SK': {'S': f'PIPELINE#{pipeline_id}#TASK#{task_id}'},
        'TaskId': {'S': task_id},
        'CreatedAt': {'S': timestamp},
        'UpdatedAt': {'S': timestamp},
    }

    # Add task data to the item
    for key, value in task_data.items():
        if isinstance(value, str):
            item[key] = {'S': value}
        elif isinstance(value, int):
            item[key] = {'N': str(value)}
        elif isinstance(value, float):
            item[key] = {'N': str(value)}
        elif isinstance(value, bool):
            item[key] = {'BOOL': value}
        elif isinstance(value, list):
            item[key] = {'L': [{'S': str(v)} for v in value]}  # Assuming list of strings
        elif isinstance(value, dict):
            item[key] = {'M': {k: {'S': str(v)} for k, v in value.items()}}  # Assuming dict of strings
        else:
            item[key] = {'S': str(value)}  # Default to string for unknown types

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

