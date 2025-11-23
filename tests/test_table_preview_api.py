"""Test kptn_server table-preview API with duckdb_example."""

import subprocess
from pathlib import Path

import pytest

from kptn_server.service import get_duckdb_preview


@pytest.fixture(scope="module")
def duckdb_example_dir():
    """Return the path to the duckdb_example directory."""
    return Path(__file__).parent.parent / "example" / "duckdb_example"


@pytest.fixture(scope="module")
def run_pipeline(duckdb_example_dir):
    """Run the duckdb_example pipeline to populate the database."""
    # Run the pipeline from the duckdb_example directory so it creates
    # example.ddb in the same place where the API will look for it
    result = subprocess.run(
        ["uv", "run", "duckdb_example.py", "--force"],
        cwd=duckdb_example_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"Failed to run duckdb_example: {result.stderr}")
    return result


def test_table_preview_raw_numbers(duckdb_example_dir, run_pipeline):
    """Test table-preview API for raw_numbers table."""
    config_path = duckdb_example_dir / "kptn.yaml"

    preview = get_duckdb_preview(config_path, "raw_numbers")

    assert "columns" in preview
    assert "row" in preview
    assert "rows" in preview
    assert preview["resolvedTable"] == "main.raw_numbers"
    assert "id" in preview["columns"]
    assert "fruit" in preview["columns"]
    assert len(preview["row"]) == len(preview["columns"])
    assert len(preview["rows"]) <= 5


def test_table_preview_fruit_metrics(duckdb_example_dir, run_pipeline):
    """Test table-preview API for fruit_metrics table."""
    config_path = duckdb_example_dir / "kptn.yaml"

    preview = get_duckdb_preview(config_path, "fruit_metrics")

    assert "columns" in preview
    assert "row" in preview
    assert "rows" in preview
    assert preview["resolvedTable"] == "main.fruit_metrics"
    assert "fruit" in preview["columns"]
    assert "score" in preview["columns"]
    assert len(preview["row"]) == len(preview["columns"])
    assert len(preview["rows"]) <= 5


def test_table_preview_fruit_summary(duckdb_example_dir, run_pipeline):
    """Test table-preview API for fruit_summary table."""
    config_path = duckdb_example_dir / "kptn.yaml"

    preview = get_duckdb_preview(config_path, "fruit_summary")

    assert "columns" in preview
    assert "row" in preview
    assert "rows" in preview
    assert preview["resolvedTable"] == "main.fruit_summary"
    expected_columns = ["fruit_count", "total_score", "avg_score", "max_score", "min_score"]
    for col in expected_columns:
        assert col in preview["columns"]
    assert len(preview["row"]) == len(preview["columns"])
    # Verify data values make sense
    fruit_count_idx = preview["columns"].index("fruit_count")
    assert preview["row"][fruit_count_idx] == 5


def test_table_preview_nonexistent_table(duckdb_example_dir, run_pipeline):
    """Test table-preview API with a table that doesn't exist in config."""
    config_path = duckdb_example_dir / "kptn.yaml"

    preview = get_duckdb_preview(config_path, "nonexistent_table")

    assert "message" in preview
    assert "not configured" in preview["message"].lower()


def test_table_preview_with_schema_prefix(duckdb_example_dir, run_pipeline):
    """Test table-preview API with schema.table notation."""
    config_path = duckdb_example_dir / "kptn.yaml"

    preview = get_duckdb_preview(config_path, "main.fruit_summary")

    assert "columns" in preview
    assert "row" in preview
    assert preview["resolvedTable"] == "main.fruit_summary"


def test_table_preview_client_sql_with_limit_injected(duckdb_example_dir, run_pipeline):
    """Client-supplied SQL should be executed with an auto-limit when missing."""
    config_path = duckdb_example_dir / "kptn.yaml"

    preview = get_duckdb_preview(
        config_path,
        sql="SELECT fruit, score FROM main.fruit_metrics ORDER BY score DESC",
    )

    assert "columns" in preview
    assert "row" in preview
    assert "rows" in preview
    assert preview.get("resolvedTable") is None
    assert preview.get("sql")
    assert "fruit" in preview["columns"]
    assert "score" in preview["columns"]


def test_table_preview_client_sql_rejects_multi_statement(duckdb_example_dir, run_pipeline):
    """Multiple statements should be rejected to avoid batch execution."""
    config_path = duckdb_example_dir / "kptn.yaml"

    preview = get_duckdb_preview(config_path, sql="SELECT 1; SELECT 2")

    assert "message" in preview
    assert "single" in preview["message"].lower()
