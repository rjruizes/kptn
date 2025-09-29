"""Summarise results from the DuckDB pipeline."""

from kapten.util.runtime_config import RuntimeConfig


def c(runtime_config: RuntimeConfig) -> dict[str, int]:
    """Return aggregate metrics for downstream inspection or caching."""
    con = runtime_config.engine
    row = con.execute(
        """
        select
            count(*) as fruit_count,
            sum(score) as total_score
        from fruit_metrics
        """
    ).fetchone()

    if row is None:
        summary = {"fruit_count": 0, "total_score": 0}
    else:
        fruit_count, total_score = row
        summary = {"fruit_count": fruit_count, "total_score": total_score or 0}
    print(f"DuckDB summary: {summary}")
    return summary
