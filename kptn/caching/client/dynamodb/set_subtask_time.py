import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from typing import Dict, Any

deserializer = TypeDeserializer()

def set_time_in_subitem_in_bin(
    dynamodb: boto3.client,
    table_name: str,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_id: str,
    index: int,
    field_name: str,
    time_value: str,
    hash: str = None,
) -> Dict[str, Any]:
    """
    Update the bin_id item at the given index for the given field in the DynamoDB table using boto3.client.

    :param dynamodb: The DynamoDB client
    :param table_name: The name of the DynamoDB table
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_id: The bin_id ID
    :param index: The index of the subtask
    :param field_name: The field name to update
    :param time_value: The time value to set
    :param hash: Optional hash to set on field `outputHash` if provided

    :return: The response from DynamoDB
    """

    # Construct the primary key
    key = {
        "PK": {"S": f"BRANCH#{storage_key}#PIPELINE#{pipeline_id}#TASK#{task_id}#SUBTASKBIN#{bin_id}"},
        "SK": {"S": f"BIN#{bin_id}"},
    }

    # Construct the update expression and attribute values
    # Assuming update is a dictionary with string keys and string values
    update_expression = f"SET #items[{index}].{field_name} = :update"
    expression_attribute_names = {"#items": "items"}
    expression_attribute_values = {":update": {"S": time_value}}
    if hash:
        update_expression += f", #items[{index}].outputHash = :hash"
        expression_attribute_values[":hash"] = {"S": hash}

    print("Update expression:", update_expression, "Attribute values:", expression_attribute_values, "Key:", key)
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
        print(f"Error updating subtask in bin: {e.response['Error']['Message']}")
        raise
