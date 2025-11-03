import pytest
import os
import tempfile
from pathlib import Path
from kptn.caching.client.DbClientSQLite import DbClientSQLite
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

    def test_default_db_path_uses_kptn_directory(self, tmp_path):
        """Ensure the sqlite file defaults to the kptn.yaml directory."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        config_path = project_dir / "kptn.yaml"
        config_path.write_text("settings: {}")

        db = DbClientSQLite(
            table_name="tasks",
            storage_key="branch",
            pipeline="pipeline",
            tasks_config_path=str(config_path),
        )

        try:
            assert Path(db.db_path).parent == project_dir.resolve()
        finally:
            if hasattr(db, "conn") and db.conn:
                db.conn.close()
            db_file = Path(db.db_path)
            if db_file.exists():
                db_file.unlink()
