import duckdb


def get_engine(database: str = "example.db") -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection configured for the example pipeline."""
    con = duckdb.connect(database=database, read_only=False)
    return con
