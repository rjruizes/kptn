import os

import boto3
import pytest
from kptn.caching.client.DbClientDDB import DbClientDDB
from tests.base_db_client_test import BaseDbClientTest

TABLE = "tasks"

# Skip by default to avoid hanging when local DynamoDB isn't running.
pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_DDB_TESTS"),
    reason="requires local DynamoDB at http://localhost:8000; set RUN_DDB_TESTS=1 to run",
)


def cleanup():
    # Reset local dynamodb table
    dynamodb = boto3.resource("dynamodb", endpoint_url="http://localhost:8000")
    table = dynamodb.Table(TABLE)
    table.delete()
    table.wait_until_not_exists()

class TestDynamoDbClient(BaseDbClientTest):
    """Test class for DynamoDB client implementation."""
    
    @pytest.fixture
    def db(self):
        """Provides a DynamoDB client instance for testing."""
        os.environ["AWS_ACCESS_KEY_ID"] = 'DUMMYIDEXAMPLE'
        os.environ["AWS_SECRET_ACCESS_KEY"] = 'DUMMYEXAMPLEKEY'
        os.environ["LOCAL_DYNAMODB"] = "true"
        db = DbClientDDB(table_name=TABLE, storage_key="mybranch", pipeline="mygraph", aws_auth={ "endpoint_url": "http://localhost:8000" })
        yield db
        cleanup()
