"""Seed data for the DuckDB example pipeline."""

from kapten.util.runtime_config import RuntimeConfig


def raw_numbers(runtime_config: RuntimeConfig) -> None:
    """Create the raw dataset that downstream tasks will reference."""
    con = runtime_config.duckdb
    con.execute("drop table if exists raw_numbers")
    con.execute(
        """
        create table raw_numbers as
        select * from (
            values
                (1, 'apple'),
                (2, 'banana'),
                (3, 'cherry'),
                (4, 'dragonfruit'),
                (5, 'elderberry')
        ) as t(id, fruit)
        """
    )
