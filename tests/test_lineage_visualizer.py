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
    tables_payload, lineage_payload = _build_lineage_payload(analyzer)

    assert [entry["name"] for entry in tables_payload] == ["mart.first", "mart.second"]
    assert {"from": [0, "value"], "to": [1, "adjusted"]} in lineage_payload
