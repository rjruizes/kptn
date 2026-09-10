"""Test kptn_server table-preview API with duckdb_example.

No module-level skip. ``kptn_server.service`` imports the lineage analyzer,
which needs ``sqlglot`` -- and the ``web`` extra now declares it, so these
tests run rather than silently skipping and leaving ``get_duckdb_preview``
unexercised, which is what happened for as long as no extra shipped the parser.

The database these tests read is built by executing the example project's own
task code against its own ``get_engine`` factory. It used to be built by
running ``duckdb_example.py``, which is a kptn 0.1-era script
(``kptn.caching.submit``, ``kptn.runner.cli_parser``) that no longer imports at
all -- so once the module stopped skipping, every test here failed on the
harness rather than on the API under test. Driving the task files directly
keeps the fixture data identical and the subject of the test unchanged.
"""

import importlib.util
import os
import sys
from pathlib import Path

import pytest

from kptn_server.service import get_duckdb_preview


@pytest.fixture(autouse=True)
def unshadow_the_examples_src_package():
    """Drop any other project's ``src`` package from ``sys.modules``.

    ``example/duckdb_example/kptn.yaml`` names its connection factory
    ``src.utils:get_engine``, which ``RuntimeConfig`` resolves with a plain
    ``importlib.import_module`` after putting the project directory on
    ``sys.path``. ``src`` is a name several fixture projects in this suite also
    use, and an already-imported ``src`` wins over any ``sys.path`` entry -- so
    running after ``tests/test_cli_validate.py`` (whose tmp project has its own
    ``src/utils.py`` with no ``get_engine``) made every test here report
    "Runtime config has no DuckDB connection". The tests passed alone and
    failed in the suite, which is the least useful failure mode there is.

    Evicting the cached name, and restoring it afterwards, keeps this module
    order-independent in both directions.
    """
    shadowed = {
        name: module
        for name, module in sys.modules.items()
        if name == "src" or name.startswith("src.")
    }
    for name in shadowed:
        del sys.modules[name]

    yield

    for name in list(sys.modules):
        if name == "src" or name.startswith("src."):
            del sys.modules[name]
    sys.modules.update(shadowed)


@pytest.fixture(scope="module")
def duckdb_example_dir():
    """Return the path to the duckdb_example directory."""
    return Path(__file__).parent.parent / "example" / "duckdb_example"


def _load_module(path: Path, name: str):
    """Import a module from a file path without putting it on ``sys.path``."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        pytest.fail(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(name, None)
    return module


@pytest.fixture(scope="module")
def run_pipeline(duckdb_example_dir):
    """Populate ``example.ddb`` where ``get_duckdb_preview`` will look for it.

    The engine factory (``src/utils.py``) opens ``example.ddb`` *relative to
    the process's working directory*, and so does the preview's own connection
    -- both go through ``kptn.yaml``'s ``config.duckdb.function``. The database
    therefore has to be created with the example directory as the cwd, exactly
    as the old subprocess did.
    """
    utils = _load_module(duckdb_example_dir / "src" / "utils.py", "_ddb_example_utils")
    fruit_tasks = _load_module(
        duckdb_example_dir / "src" / "fruit_tasks.py", "_ddb_example_fruit_tasks"
    )

    original_cwd = Path.cwd()
    os.chdir(duckdb_example_dir)
    try:
        con = utils.get_engine()
        try:
            for sql_file in ("raw_numbers.sql", "fruit_metrics.sql"):
                con.execute((duckdb_example_dir / "src" / sql_file).read_text())
            fruit_tasks.fruit_summary(con, {})
        finally:
            con.close()
    finally:
        os.chdir(original_cwd)
    return duckdb_example_dir / "example.ddb"


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
    expected_columns = [
        "fruit_count",
        "total_score",
        "avg_score",
        "max_score",
        "min_score",
    ]
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


def test_table_preview_client_sql_rejects_multi_statement(
    duckdb_example_dir, run_pipeline
):
    """Multiple statements should be rejected to avoid batch execution."""
    config_path = duckdb_example_dir / "kptn.yaml"

    preview = get_duckdb_preview(config_path, sql="SELECT 1; SELECT 2")

    assert "message" in preview
    assert "single" in preview["message"].lower()
