import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional
from kptn.util.logger import get_logger


deserializer = TypeDeserializer()

def get_single_task(dynamodb: boto3.client, table_name: str, storage_key: str, pipeline_id: str, task_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve a single task from the DynamoDB table.

    :param table_name: The name of the DynamoDB table
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :return: The task item if found, None otherwise
    """
    logger = get_logger()

    # Construct the key for the GetItem operation
    key = {
        'PK': {'S': f'BRANCH#{storage_key}'},
        'SK': {'S': f'PIPELINE#{pipeline_id}#TASK#{task_id}'}
    }

    try:
        response = dynamodb.get_item(
            TableName=table_name,
            Key=key
        )

        # Check if the item was found
        if 'Item' not in response:
            logger.info(f"Task not found: storage_key={storage_key}, pipeline_id={pipeline_id}, task_id={task_id} table={table_name}")
            return None

        # Convert the DynamoDB item to a Python dictionary
        task = {k: deserializer.deserialize(v) for k, v in response['Item'].items()}
        logger.info(f"Task found: {task}")
        return task

    except ClientError as e:
        logger.error(f"Error retrieving task: {e.response['Error']['Message']}")
        raise

# Example usage
if __name__ == "__main__":
    try:
        task = get_single_task(
            table_name='DataPipeline',
            storage_key='main',
            pipeline_id='pipeline123',
            task_id='task456'
        )
        if task:
            print("Retrieved task:")
            print(f"Task ID: {task.get('TaskId')}")
            print(f"Status: {task.get('Status')}")
            print(f"Task Name: {task.get('TaskName')}")
            # Print other relevant task details
        else:
            print("Task not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")