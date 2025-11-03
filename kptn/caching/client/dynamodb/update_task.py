import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from typing import Dict, Any
from kptn.util.logger import get_logger

deserializer = TypeDeserializer()

def update_task(
    dynamodb: boto3.client,
    table_name: str,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    update: dict,
) -> Dict[str, Any]:
    """
    Update the task in the DynamoDB table using boto3.client.

    :param dynamodb: The DynamoDB client
    :param table_name: The name of the DynamoDB table
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param update: The update dictionary
    :return: The response from DynamoDB
    """
    logger = get_logger()

    # Construct the primary key
    key = {
        "PK": {"S": f"BRANCH#{storage_key}"},
        "SK": {"S": f"PIPELINE#{pipeline_id}#TASK#{task_id}"},
    }

    # Construct the update expression and attribute values
    # Assuming update is a dictionary with string keys and string values
    update_expression = "SET " + ", ".join([f"#{k} = :{k}" for k in update.keys()])
    expression_attribute_names = {f"#{k}": k for k in update.keys()}
    expression_attribute_values = {f":{k}": {"S": str(v)} for k, v in update.items()}
    logger.info(f"Update expression: {update_expression}, Attribute values: {expression_attribute_values}, Key: {key}")

    try:
        response = dynamodb.update_item(
            TableName=table_name,
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="ALL_NEW",
        )
        # Convert the DynamoDB item to a Python dictionary
        item = {k: deserializer.deserialize(v) for k, v in response["Attributes"].items()}

        return item
    except ClientError as e:
        print(f"Error updating task: {e.response['Error']['Message']}")
        raise
