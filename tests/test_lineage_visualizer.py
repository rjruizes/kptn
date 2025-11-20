import json
import re

from pathlib import Path

from kptn.cli import _build_lineage_payload
from kptn.lineage import SqlLineageAnalyzer
from kptn.lineage.html_renderer import render_lineage_html


def test_render_lineage_html_embeds_tables_and_lineage():
    tables = [
        {"name": "orders", "columns": ["id", "amount"]},
        {"name": "payments", "columns": ["order_id"]},
    ]
    lineage = [
        {"from": [0, "id"], "to": [1, "order_id"]},
    ]

    html = render_lineage_html(tables, lineage, title="Test Lineage")

    assert "<title>Test Lineage</title>" in html
    assert '"name": "orders"' in html
    assert '"columns": ["id", "amount"]' in html
    assert "const lineageData" in html
    assert '"from": [0, "id"]' in html


def _extract_const(html: str, name: str):
    pattern = re.compile(rf"const {name} = (.*?);\n", re.DOTALL)
    match = pattern.search(html)
    assert match, f"Unable to locate const {name}"
    payload = match.group(1).strip()
    return json.loads(payload)


def test_render_lineage_html_serializes_payload():
    tables = [
        {"name": "analytics.visit_dates", "columns": ["participant_id", "visit_start_date"]},
        {"name": "staging.visit_plan", "columns": ["planned_start"]},
        {"name": "staging.visit_adjustments", "columns": ["start_override"]},
    ]
    lineage = [
        {"from": [1, "planned_start"], "to": [0, "visit_start_date"]},
        {"from": [2, "start_override"], "to": [0, "visit_start_date"]},
    ]

    html = render_lineage_html(tables, lineage, title="Visit dates")

    tables_data = _extract_const(html, "tablesData")
    lineage_data = _extract_const(html, "lineageData")

    assert tables_data == tables
    assert lineage_data == lineage


def test_build_lineage_payload_preserves_order(tmp_path):
    sql_a = tmp_path / "first.sql"
    sql_a.write_text(
        """
        create or replace table mart.first as
        select id, value from staging.source_a;
        """,
        encoding="utf-8",
    )
    sql_b = tmp_path / "second.sql"
    sql_b.write_text(
        """
        create or replace table mart.second as
        select
            f.id,
            f.value + s.delta as adjusted
        from mart.first f
        join staging.source_b s on f.id = s.id;
        """,
        encoding="utf-8",
    )

    config = {
        "tasks": {
            "first": {
                "file": str(sql_a.relative_to(tmp_path)),
                "outputs": ["duckdb://mart.first"],
            },
            "second": {
                "file": str(sql_b.relative_to(tmp_path)),
                "outputs": ["duckdb://mart.second"],
            },
        }
    }

    analyzer = SqlLineageAnalyzer(config, project_root=tmp_path, dialect="duckdb")
    analyzer.build()
    task_order = list(config["tasks"].keys())
    tables_payload, lineage_payload = _build_lineage_payload(
        analyzer,
        task_order=task_order,
        tasks_config=config["tasks"],
    )

    names = [entry["name"] for entry in tables_payload]
    assert names[:2] == ["mart.first", "mart.second"]
    assert "staging.source_a" in names
    assert "staging.source_b" in names
    assert {"from": [0, "value"], "to": [1, "adjusted"]} in lineage_payload


def test_lineage_payload_matches_visit_assignments(tmp_path):
    sql_enrollments = tmp_path / "enrollments.sql"
    sql_enrollments.write_text(
        """
        create or replace table staging.enrollments as
        select 1 as participant_id, 'A' as cohort_code;
        """,
        encoding="utf-8",
    )
    sql_raw_visits = tmp_path / "raw_visits.sql"
    sql_raw_visits.write_text(
        """
        create or replace table staging.raw_visits as
        select 1 as participant_id, 10 as visit_id, 'X1' as concept_code;
        """,
        encoding="utf-8",
    )
    sql_concepts = tmp_path / "concepts.sql"
    sql_concepts.write_text(
        """
        create or replace table staging.concepts as
        select 'X1' as concept_code, 'NX1' as normalized_code;
        """,
        encoding="utf-8",
    )
    sql_assignments = tmp_path / "visit_assignments.sql"
    sql_assignments.write_text(
        """
        create or replace table analytics.visit_assignments as
        with enrollments as (
            select participant_id, cohort_code from staging.enrollments
        ),
        visits as (
            select participant_id, visit_id, concept_code from staging.raw_visits
        ),
        concepts as (
            select concept_code, normalized_code from staging.concepts
        )
        select
            v.participant_id,
            v.visit_id,
            coalesce(c.normalized_code, v.concept_code) as visit_concept_code
        from visits v
        join enrollments e on v.participant_id = e.participant_id
        join concepts c on v.concept_code = c.concept_code;
        """,
        encoding="utf-8",
    )

    config = {
        "tasks": {
            "enrollments": {
                "file": str(sql_enrollments.relative_to(tmp_path)),
                "outputs": ["duckdb://staging.enrollments"],
            },
            "raw_visits": {
                "file": str(sql_raw_visits.relative_to(tmp_path)),
                "outputs": ["duckdb://staging.raw_visits"],
            },
            "concepts": {
                "file": str(sql_concepts.relative_to(tmp_path)),
                "outputs": ["duckdb://staging.concepts"],
            },
            "visit_assignments": {
                "file": str(sql_assignments.relative_to(tmp_path)),
                "outputs": ["duckdb://analytics.visit_assignments"],
            }
        }
    }

    analyzer = SqlLineageAnalyzer(config, project_root=tmp_path, dialect="duckdb")
    analyzer.build()
    task_order = list(config["tasks"].keys())
    tables_payload, lineage_payload = _build_lineage_payload(
        analyzer,
        task_order=task_order,
        tasks_config=config["tasks"],
    )

    html = render_lineage_html(tables_payload, lineage_payload, title="Visit assignments")
    tables_data = _extract_const(html, "tablesData")
    lineage_data = _extract_const(html, "lineageData")

    # Tables appear in kptn.yaml order (tasks order).
    assert [entry["name"] for entry in tables_data] == [
        "staging.enrollments",
        "staging.raw_visits",
        "staging.concepts",
        "analytics.visit_assignments",
    ]

    visit_concept_edges = [
        edge for edge in lineage_data if edge["to"] == [3, "visit_concept_code"]
    ]
    assert len(visit_concept_edges) == 2
    assert {"from": [2, "normalized_code"], "to": [3, "visit_concept_code"]} in visit_concept_edges
    assert {"from": [1, "concept_code"], "to": [3, "visit_concept_code"]} in visit_concept_edges


def test_lineage_payload_keeps_table_columns(tmp_path):
    sql_metrics = tmp_path / "metrics.sql"
    sql_metrics.write_text(
        """
        create or replace table analytics.metrics as
        select id, value from staging.source_data;
        """,
        encoding="utf-8",
    )

    config = {
        "tasks": {
            "metrics": {
                "file": str(sql_metrics.relative_to(tmp_path)),
                "outputs": ["duckdb://analytics.metrics"],
            }
        }
    }

    analyzer = SqlLineageAnalyzer(config, project_root=tmp_path, dialect="duckdb")
    analyzer.build()
    task_order = list(config["tasks"].keys())
    tables_payload, _ = _build_lineage_payload(
        analyzer,
        task_order=task_order,
        tasks_config=config["tasks"],
    )

    metrics_entry = next(entry for entry in tables_payload if entry["name"] == "analytics.metrics")
    assert metrics_entry["columns"] == ["id", "value"]


def test_lineage_payload_skips_empty_table_names(tmp_path):
    sql_schedule = tmp_path / "schedule.sql"
    sql_schedule.write_text(
        """
        create or replace table analytics.schedule as
        with event_dates as (
            select * from (
                select record_id, redcap_event_name, visit_date from staging.events
            )
        )
        select
            record_id,
            max(case when redcap_event_name = 'baseline' then visit_date end)::date as baseline_date
        from event_dates
        group by record_id;
        """,
        encoding="utf-8",
    )

    config = {
        "tasks": {
            "schedule": {
                "file": str(sql_schedule.relative_to(tmp_path)),
                "outputs": ["duckdb://analytics.schedule"],
            }
        }
    }

    analyzer = SqlLineageAnalyzer(config, project_root=tmp_path, dialect="duckdb")
    analyzer.build()
    task_order = list(config["tasks"].keys())
    tables_payload, _ = _build_lineage_payload(
        analyzer,
        task_order=task_order,
        tasks_config=config["tasks"],
    )

    names = [entry["name"] for entry in tables_payload]
    assert "" not in names

    schedule_entry = next(entry for entry in tables_payload if entry["name"] == "analytics.schedule")
    assert schedule_entry["columns"] == ["record_id", "baseline_date"]
