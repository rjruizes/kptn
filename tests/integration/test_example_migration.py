"""Integration tests for the reference pipeline — Story 4.3."""
from __future__ import annotations

import time

import pytest

pytest.importorskip("duckdb")  # skip entire module when duckdb not installed

import kptn
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.plan import plan
from kptn.state_store.duckdb import DuckDbBackend


def _make_large_pipeline(n: int = 90) -> kptn.Pipeline:
    """Build a synthetic n-task linear pipeline (representative of large compositions)."""
    tasks = []
    for i in range(n):
        def _fn(i=i) -> None:  # noqa: ANN001 — synthetic closure
            pass
        _fn.__name__ = f"task_{i:03d}"
        decorated = kptn.task(outputs=[])(_fn)
        tasks.append(decorated)

    graph = tasks[0]
    for t in tasks[1:]:
        graph = graph >> t

    return kptn.Pipeline("all_export", graph)


def test_large_pipeline_plan_under_one_second(tmp_path) -> None:
    """kptn plan on 90-task pipeline must complete in under 1 second (NFR-1)."""
    pipeline = _make_large_pipeline(90)
    resolved = ResolvedGraph(
        graph=pipeline,
        pipeline=pipeline.name,
        storage_key="kptn",
    )
    state_store = DuckDbBackend(path=str(tmp_path / "state.duckdb"))

    start = time.monotonic()
    plan(resolved, state_store)
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"kptn plan took {elapsed:.3f}s on 90-task pipeline (limit: 1.0s)"
