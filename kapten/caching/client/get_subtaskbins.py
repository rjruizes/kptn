import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from typing import List, Dict, Any

deserializer = TypeDeserializer()

def get_subtaskbins(dynamodb: boto3.client, table_name: str, storage_key: str, pipeline_id: str, task_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve all subtask bins for a specific task in a pipeline from the DynamoDB table.

    :param table_name: The name of the DynamoDB table
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :return: A list of subtask bin items
    """

    # Construct the key condition expression
    key_condition_expression = "PK = :pk AND begins_with(SK, :sk_prefix)"
    expression_attribute_values = {
        ':pk': {'S': f'BRANCH#{storage_key}'},
        ':sk_prefix': {'S': f'PIPELINE#{pipeline_id}#TASK#{task_id}#SUBTASKBIN#'}
    }

    tasks = []
    last_evaluated_key = None

    try:
        while True:
            if last_evaluated_key:
                response = dynamodb.query(
                    TableName=table_name,
                    KeyConditionExpression=key_condition_expression,
                    ExpressionAttributeValues=expression_attribute_values,
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                response = dynamodb.query(
                    TableName=table_name,
                    KeyConditionExpression=key_condition_expression,
                    ExpressionAttributeValues=expression_attribute_values
                )

            # Process the items
            for item in response.get('Items', []):
                tasks.append({k: deserializer.deserialize(v) for k, v in item.items()})

            # Check if there are more items to fetch
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break

        return tasks

    except ClientError as e:
        print(f"Error querying tasks: {e.response['Error']['Message']}")
        raise
