"""Tests for optional documentation metadata on graph node/task specs (Task 3).

Covers: task, sql_task, r_task, Stage, Pipeline — all new arguments are
keyword-only and optional, and a legacy (no-metadata) call site keeps working
unchanged.
"""

from __future__ import annotations

import kptn
from kptn.graph.decorators import RTaskSpec, SqlTaskSpec, TaskSpec, r_task, sql_task, task
from kptn.graph.nodes import PipelineNode, StageNode


@kptn.task(
    outputs=["duckdb://main.cleaned"],
    inputs=["duckdb://raw.source"],
    description="Normalize source rows.",
    docs="docs/source.md#normalization",
)
def clean() -> None:
    """Fallback text that explicit metadata overrides."""


def test_task_metadata_is_optional_and_preserved() -> None:
    spec = clean.__kptn__
    assert spec.description == "Normalize source rows."
    assert spec.inputs == ["duckdb://raw.source"]
    assert spec.docs == "docs/source.md#normalization"


def test_task_metadata_defaults_to_none_and_empty() -> None:
    @task(outputs=["duckdb://s.t"])
    def legacy_fn() -> None:
        pass

    spec = legacy_fn.__kptn__
    assert isinstance(spec, TaskSpec)
    assert spec.description is None
    assert spec.inputs == []
    assert spec.docs is None


def test_sql_task_metadata_is_optional_and_preserved() -> None:
    handle = sql_task(
        "queries/clean.sql",
        outputs=["duckdb://main.cleaned"],
        inputs=["duckdb://raw.source"],
        description="Clean raw rows in SQL.",
        docs="docs/source.md#sql-clean",
    )
    spec = handle.__kptn__
    assert isinstance(spec, SqlTaskSpec)
    assert spec.description == "Clean raw rows in SQL."
    assert spec.inputs == ["duckdb://raw.source"]
    assert spec.docs == "docs/source.md#sql-clean"


def test_sql_task_metadata_defaults_unchanged() -> None:
    handle = sql_task("queries/legacy.sql", outputs=["duckdb://s.t"])
    spec = handle.__kptn__
    assert spec.description is None
    assert spec.inputs == []
    assert spec.docs is None


def test_r_task_metadata_is_optional_and_preserved() -> None:
    handle = r_task(
        "scripts/analyze.R",
        outputs=["duckdb://main.analyzed"],
        inputs=["duckdb://raw.source"],
        description="Analyze rows in R.",
        docs="docs/source.md#r-analyze",
    )
    spec = handle.__kptn__
    assert isinstance(spec, RTaskSpec)
    assert spec.description == "Analyze rows in R."
    assert spec.inputs == ["duckdb://raw.source"]
    assert spec.docs == "docs/source.md#r-analyze"


def test_r_task_metadata_defaults_unchanged() -> None:
    handle = r_task("scripts/legacy.R", outputs=["duckdb://s.t"])
    spec = handle.__kptn__
    assert spec.description is None
    assert spec.inputs == []
    assert spec.docs is None


def test_stage_metadata_is_optional_and_preserved() -> None:
    graph = kptn.Stage(
        "data_sources",
        clean,
        description="Choose which raw source to ingest.",
        docs="docs/source.md#data-sources",
    )
    sentinel = next(n for n in graph.nodes if isinstance(n, StageNode))
    assert sentinel.description == "Choose which raw source to ingest."
    assert sentinel.docs == "docs/source.md#data-sources"


def test_stage_metadata_defaults_unchanged() -> None:
    graph = kptn.Stage("legacy_stage", clean)
    sentinel = next(n for n in graph.nodes if isinstance(n, StageNode))
    assert sentinel.description is None
    assert sentinel.docs is None


def test_pipeline_metadata_is_optional_and_preserved() -> None:
    pipeline = kptn.Pipeline(
        "ingest",
        clean,
        description="Ingest and clean the source feed.",
        docs="docs/source.md#ingest-pipeline",
    )
    sentinel = next(n for n in pipeline.nodes if isinstance(n, PipelineNode))
    assert sentinel.description == "Ingest and clean the source feed."
    assert sentinel.docs == "docs/source.md#ingest-pipeline"


def test_pipeline_metadata_defaults_unchanged() -> None:
    pipeline = kptn.Pipeline("legacy_pipeline", clean)
    sentinel = next(n for n in pipeline.nodes if isinstance(n, PipelineNode))
    assert sentinel.description is None
    assert sentinel.docs is None
