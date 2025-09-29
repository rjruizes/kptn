"""Transform raw DuckDB data into a derived table."""

from kapten.util.runtime_config import RuntimeConfig


def b(runtime_config: RuntimeConfig) -> None:
    """Compute basic metrics from the seeded dataset."""
    con = runtime_config.engine
    con.execute(
        """
        create or replace table fruit_metrics as
        select
            id,
            fruit,
            length(fruit) as name_length,
            id * 10 as score
        from raw_numbers
        order by id
        """
    )
