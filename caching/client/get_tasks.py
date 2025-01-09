import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Any

def get_tasks_for_pipeline(dynamodb: boto3.client, table_name: str, storage_key: str, pipeline_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve all tasks for a specific pipeline in a storage_key from the DynamoDB table.

    :param table_name: The name of the DynamoDB table
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :return: A list of task items
    """
    # print(f"Retrieving tasks for pipeline {pipeline_id} in storage_key {storage_key}, table {table_name}, ddb endpoint {dynamodb._endpoint.host}")

    # Construct the key condition expression
    key_condition_expression = "PK = :pk AND begins_with(SK, :sk_prefix)"
    expression_attribute_values = {
        ':pk': {'S': f'BRANCH#{storage_key}'},
        ':sk_prefix': {'S': f'PIPELINE#{pipeline_id}#TASK#'}
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
                tasks.append({k: list(v.values())[0] for k, v in item.items()})

            # Check if there are more items to fetch
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break

        return tasks

    except ClientError as e:
        print(f"Error querying tasks: {e.response['Error']['Message']}")
        raise

# Example usage
if __name__ == "__main__":
    try:
        results = get_tasks_for_pipeline(
            table_name='DataPipeline',
            storage_key='main',
            pipeline_id='pipeline123'
        )
        print(f"Retrieved {len(results)} tasks:")
        for task in results:
            print(f"Task ID: {task.get('TaskId')}, Status: {task.get('Status')}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")