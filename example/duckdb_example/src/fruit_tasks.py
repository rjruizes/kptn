from kapten.util.runtime_config import RuntimeConfig


def raw_numbers_fn(runtime_config: RuntimeConfig) -> None:
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

def fruit_metrics_fn(runtime_config: RuntimeConfig) -> None:
    """Compute basic metrics from the seeded dataset."""
    con = runtime_config.duckdb
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

def fruit_summary(runtime_config: RuntimeConfig) -> None:
    """Create summary table from fruit_metrics."""
    con = runtime_config.duckdb
    con.execute(
        """
        create or replace table fruit_summary as
        select
            count(*) as fruit_count,
            sum(score) as total_score,
            avg(score) as avg_score,
            max(score) as max_score,
            min(score) as min_score
        from 'fruit_metrics'
        """
    )

    row = con.execute(
        "select fruit_count, total_score from fruit_summary"
    ).fetchone()

    if row is None:
        summary = {"fruit_count": 0, "total_score": 0}
    else:
        fruit_count, total_score = row
        summary = {"fruit_count": fruit_count, "total_score": total_score or 0}
    print(f"DuckDB summary: {summary}")
