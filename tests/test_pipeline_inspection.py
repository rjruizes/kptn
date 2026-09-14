"""Tests for kptn/inspection.py — profile-aware, read-only pipeline inspection."""

from __future__ import annotations

from pathlib import Path

import pytest

import kptn
from kptn.inspection import (
    PipelineInspection,
    inspect_pipeline,
    resolve_docs_path,
)
from kptn.profiles.resolved import ResolvedGraph
from kptn.profiles.schema import KptnConfig, ProfileSpec
from kptn.runner.api import _gate


def _build_dexcom_like_pipeline() -> kptn.Pipeline:
    @kptn.task(outputs=["duckdb://main.init"], description="Initialize the database.")
    def init_database() -> None:
        pass

    @kptn.task(outputs=["duckdb://dexcom.raw"])
    def load_dexcom_reports() -> None:
        pass

    @kptn.task(outputs=["duckdb://other.raw"])
    def load_other_reports() -> None:
        pass

    @kptn.task(
        outputs=["duckdb://dexcom.qc"],
        inputs=["duckdb://dexcom.raw"],
        description="Check Dexcom interval quality.",
    )
    def qc_dexcom_detail() -> None:
        pass

    graph = (
        init_database
        >> kptn.Stage("data_sources", load_dexcom_reports, load_other_reports)
        >> qc_dexcom_detail
    )
    return kptn.Pipeline("dexcom_pipeline", graph)


def _dexcom_test_config() -> KptnConfig:
    return KptnConfig(
        profiles={
            "dexcom_test": ProfileSpec(
                stage_selections={"data_sources": ["load_dexcom_reports"]}
            )
        }
    )


def test_inspection_uses_resolved_order_and_marks_bypassed(tmp_path: Path) -> None:
    pipeline = _build_dexcom_like_pipeline()
    config = _dexcom_test_config()

    inspection = inspect_pipeline(pipeline, config, "dexcom_test", tmp_path)

    assert isinstance(inspection, PipelineInspection)
    assert inspection.pipeline == "dexcom_pipeline"
    assert inspection.profile == "dexcom_test"
    assert [item.name for item in inspection.items] == [
        n.name for n in inspection.items
    ]  # sanity: names present

    assert [item.name for item in inspection.items if item.executable] == [
        "init_database",
        "load_dexcom_reports",
        "qc_dexcom_detail",
    ]
    # The inactive stage branch is pruned entirely — never inferred as bypassed data.
    assert all(item.name != "load_other_reports" for item in inspection.items)

    assert inspection.items[-1].description == "Check Dexcom interval quality."
    assert inspection.items[-1].name == "qc_dexcom_detail"
    assert inspection.items[-1].inputs == ("duckdb://dexcom.raw",)
    assert inspection.items[-1].outputs == ("duckdb://dexcom.qc",)


def test_inspection_marks_start_from_cursor_nodes_bypassed(tmp_path: Path) -> None:
    pipeline = _build_dexcom_like_pipeline()
    config = KptnConfig(
        profiles={
            "dexcom_test": ProfileSpec(
                stage_selections={"data_sources": ["load_dexcom_reports"]},
                start_from="qc_dexcom_detail",
            )
        }
    )

    inspection = inspect_pipeline(pipeline, config, "dexcom_test", tmp_path)

    by_name = {item.name: item for item in inspection.items}
    assert by_name["init_database"].bypassed is True
    assert by_name["init_database"].executable is False
    assert by_name["qc_dexcom_detail"].bypassed is False
    assert by_name["qc_dexcom_detail"].executable is True


def test_inspection_populates_python_task_source_location(tmp_path: Path) -> None:
    pipeline = _build_dexcom_like_pipeline()
    config = _dexcom_test_config()

    inspection = inspect_pipeline(pipeline, config, "dexcom_test", tmp_path)

    init_item = next(item for item in inspection.items if item.name == "init_database")
    assert init_item.source_path == Path(__file__).resolve()
    assert isinstance(init_item.source_line, int)
    assert init_item.source_line > 0


def test_inspection_resolves_declared_docs_within_project_root(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    docs_file = tmp_path / "docs" / "dexcom.md"
    docs_file.write_text("# Dexcom\n")

    @kptn.task(outputs=["duckdb://dexcom.raw"], docs="docs/dexcom.md#loading")
    def load_dexcom_reports() -> None:
        pass

    pipeline = kptn.Pipeline("dexcom_only", load_dexcom_reports)
    config = KptnConfig(profiles={"dexcom_test": ProfileSpec()})

    inspection = inspect_pipeline(pipeline, config, "dexcom_test", tmp_path)

    item = next(i for i in inspection.items if i.name == "load_dexcom_reports")
    assert item.docs_path == docs_file.resolve()
    assert item.docs_anchor == "loading"


def test_inspection_without_profile_inspects_raw_graph(tmp_path: Path) -> None:
    pipeline = _build_dexcom_like_pipeline()
    config = _dexcom_test_config()

    inspection = inspect_pipeline(pipeline, config, None, tmp_path)

    assert inspection.profile is None
    names = [item.name for item in inspection.items if item.executable]
    # No profile resolution → both stage branches survive, none bypassed.
    assert set(names) == {
        "init_database",
        "load_dexcom_reports",
        "load_other_reports",
        "qc_dexcom_detail",
    }


def test_inspection_stage_and_pipeline_sentinels_carry_no_data_inputs(
    tmp_path: Path,
) -> None:
    pipeline = _build_dexcom_like_pipeline()
    config = _dexcom_test_config()

    inspection = inspect_pipeline(pipeline, config, "dexcom_test", tmp_path)

    stage_item = next(i for i in inspection.items if i.kind == "stage")
    pipeline_item = next(i for i in inspection.items if i.kind == "pipeline")
    assert stage_item.inputs == ()
    assert stage_item.outputs == ()
    assert stage_item.executable is False
    assert pipeline_item.inputs == ()
    assert pipeline_item.executable is False


def test_inspection_rejects_docs_outside_project(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must stay within project root"):
        resolve_docs_path(tmp_path, "../secret.md")


def test_inspection_reports_an_escaping_docs_reference_per_item(
    tmp_path: Path,
) -> None:
    """A bad ``docs=`` degrades that one item, and nothing else.

    Building the read model used to raise, which made a single bad reference
    anywhere in a pipeline cost the caller the entire inspection.
    """

    @kptn.task(outputs=["duckdb://main.a"], docs="../../outside.md")
    def escaping() -> None: ...

    @kptn.task(outputs=["duckdb://main.b"], docs="docs/fine.md")
    def innocent() -> None: ...

    pipeline = kptn.Pipeline("escaper", escaping >> innocent)

    inspection = inspect_pipeline(pipeline, KptnConfig(), None, tmp_path)

    by_name = {item.name: item for item in inspection.items}
    assert by_name["escaping"].docs_path is None
    assert by_name["escaping"].docs_anchor is None
    assert "must stay within project root" in (by_name["escaping"].docs_error or "")
    assert by_name["innocent"].docs_error is None
    assert by_name["innocent"].docs_path == (tmp_path / "docs" / "fine.md").resolve()


def test_resolve_docs_path_accepts_relative_path_within_root(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "notes.md").write_text("hello")
    resolved = resolve_docs_path(tmp_path, "docs/notes.md")
    assert resolved == (tmp_path / "docs" / "notes.md").resolve()


def test_inspection_reads_sql_task_and_r_task_declared_metadata(tmp_path: Path) -> None:
    """Regression: SqlTaskNode/RTaskNode carry their spec as node.spec, not
    node.__kptn__ (that attribute only exists on the pre-wrap handles). This
    must resolve description/docs/inputs/outputs for sql_task and r_task the
    same as it does for @kptn.task.
    """
    sql_handle = kptn.sql_task(
        "queries/clean.sql",
        outputs=["duckdb://main.cleaned"],
        inputs=["duckdb://raw.source"],
        description="Clean raw rows in SQL.",
        docs="docs/source.md#sql-clean",
    )
    r_handle = kptn.r_task(
        "scripts/analyze.R",
        outputs=["duckdb://main.analyzed"],
        inputs=["duckdb://main.cleaned"],
        description="Analyze rows in R.",
        docs="docs/source.md#r-analyze",
    )

    pipeline = kptn.Pipeline("sql_and_r", sql_handle >> r_handle)
    config = KptnConfig(profiles={"default": ProfileSpec()})

    inspection = inspect_pipeline(pipeline, config, "default", tmp_path)

    by_name = {item.name: item for item in inspection.items}

    sql_item = by_name["clean"]
    assert sql_item.description == "Clean raw rows in SQL."
    assert sql_item.docs_anchor == "sql-clean"
    assert sql_item.inputs == ("duckdb://raw.source",)
    assert sql_item.outputs == ("duckdb://main.cleaned",)

    r_item = by_name["analyze"]
    assert r_item.description == "Analyze rows in R."
    assert r_item.docs_anchor == "r-analyze"
    assert r_item.inputs == ("duckdb://main.cleaned",)
    assert r_item.outputs == ("duckdb://main.analyzed",)


def _build_any_of_pipeline() -> tuple[kptn.Pipeline, KptnConfig]:
    """A pipeline whose ``any_of`` member is never present in the graph.

    ``any_of`` never *pulls* a task in (see ``kptn/graph/requires.py``), so
    ``E``'s requirement is unsatisfiable here and the runner drops it. ``F``
    only follows ``E`` structurally, so it survives by bypass reconnection.
    """

    @kptn.task(outputs=["duckdb://absent"])
    def absent_provider() -> None: ...

    @kptn.task(outputs=["duckdb://e"], requires=[kptn.any_of(absent_provider)])
    def E() -> None: ...

    @kptn.task(outputs=["duckdb://f"])
    def F() -> None: ...

    @kptn.task(outputs=["duckdb://demo"])
    def demo() -> None: ...

    pipeline = kptn.Pipeline("gated_pipeline", demo >> E >> F)
    return pipeline, KptnConfig(profiles={"all": ProfileSpec()})


def test_inspection_gates_unsatisfied_any_of_like_the_runner(tmp_path: Path) -> None:
    """The walkthrough's active steps are exactly the runner's gated graph.

    Before this was fixed, ``inspect_pipeline`` skipped ``gate_disjunctive``
    and numbered ``E`` as an active step on a page whose stated contract is
    the effective execution order — while ``kptn run`` never executed it.
    """
    pipeline, config = _build_any_of_pipeline()

    resolved = _gate(
        ResolvedGraph(
            graph=pipeline, pipeline=pipeline.name, storage_key=".kptn/kptn.db"
        )
    )
    runner_names = {node.name for node in resolved.graph.nodes}
    assert "E" not in runner_names  # the premise: the runner drops it

    for profile in (None, "all"):
        inspection = inspect_pipeline(pipeline, config, profile, tmp_path)
        active = {item.name for item in inspection.items if item.executable}
        assert active == {name for name in runner_names if name in {"demo", "E", "F"}}
        assert all(item.name != "E" for item in inspection.items)
