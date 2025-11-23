from pathlib import Path


def run(engine, output_path: str | None = None) -> None:
    """Example python task emitting a simple DuckDB table."""
    # Use an in-memory connection for the sample pipeline.
    engine.execute(
        """
        create or replace table main.python_source_table as
        select
            1 as id,
            'payload' as payload
        """
    )
    # Optionally export table for inspection if a path is provided.
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        engine.execute(f"copy main.python_source_table to '{path}' (format parquet)")


if __name__ == "__main__":
    run()
