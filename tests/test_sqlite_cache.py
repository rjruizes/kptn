import pytest
import os
import tempfile
from kapten.caching.client.DbClientSQLite import DbClientSQLite
from tests.base_db_client_test import BaseDbClientTest


class TestSQLiteClient(BaseDbClientTest):
    """Test class for SQLite client implementation."""
    
    @pytest.fixture
    def db(self):
        """Provides a SQLite client instance for testing."""
        # Use a temporary database file for testing
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
            db_path = tmp_file.name
        
        db = None
        try:
            db = DbClientSQLite(
                table_name="tasks", 
                storage_key="test_branch", 
                pipeline="test_pipeline",
                db_path=db_path
            )
            yield db
        finally:
            # Clean up - close connection and remove temp file
            if db and hasattr(db, 'conn') and db.conn:
                db.conn.close()
            if os.path.exists(db_path):
                os.unlink(db_path)