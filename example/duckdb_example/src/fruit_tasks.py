from kptn.util.runtime_config import RuntimeConfig


def fruit_summary(engine, config) -> None:
    """Create summary table from fruit_metrics."""
    con = engine
    print(f"Running fruit_summary with config: {config}")
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
