from pathlib import Path

import yaml

from kptn.lineage import SqlLineageAnalyzer


def _load_example_analyzer() -> SqlLineageAnalyzer:
    config_path = Path("example/duckdb_example/kptn.yaml").resolve()
    with open(config_path, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return SqlLineageAnalyzer(config, project_root=config_path.parent, dialect="duckdb")


def test_lineage_lists_columns_for_example_tables():
    analyzer = _load_example_analyzer()
    analyzer.build()

    raw_numbers = analyzer.describe_table("raw_numbers")
    assert raw_numbers.display_name == "main.raw_numbers"
    assert raw_numbers.columns == ["id", "fruit"]
    assert raw_numbers.source_tables == []
    assert raw_numbers.column_sources["id"] == ["t.id"]
    assert raw_numbers.column_sources["fruit"] == ["t.fruit"]

    fruit_metrics = analyzer.describe_table("fruit_metrics")
    assert fruit_metrics.display_name == "main.fruit_metrics"
    assert fruit_metrics.columns == ["id", "fruit", "name_length", "score"]
    assert "raw_numbers" in fruit_metrics.source_tables
    assert fruit_metrics.column_sources["id"] == ["raw_numbers.id"]
    assert fruit_metrics.column_sources["fruit"] == ["raw_numbers.fruit"]
    assert set(fruit_metrics.column_sources["name_length"]) == {"raw_numbers.fruit"}
    assert set(fruit_metrics.column_sources["score"]) == {"raw_numbers.id"}


def test_dependency_tree_resolves_display_names():
    analyzer = _load_example_analyzer()
    analyzer.build()

    tree = analyzer.dependency_tree()
    assert "main.raw_numbers" in tree
    assert tree["main.raw_numbers"] == []
    assert tree["main.fruit_metrics"] == ["main.raw_numbers"]


def test_lineage_handles_multistatement_cte(tmp_path):
    sql_file = tmp_path / "schedule_builder.sql"
    sql_file.write_text(
        """
        set variable staging_input = (select pipeline_config.staging_input from read_json_auto($config));

        create or replace table analytics.daily_schedule as
        with source_events as (
            select
                record_id,
                redcap_event_name,
                coalesce(
                    try_cast(visit1_date as date),
                    try_strptime(visit1_date, '%m/%d/%Y')
                ) as visit_date
            from read_csv(
                getvariable('staging_input'),
                header = true,
                union_by_name = true
            )
            where visit_date is not null
        )
        select
            record_id,
            max(case when redcap_event_name = 'group_alpha_day0' then visit_date end)::date as group_alpha_day0,
            max(case when redcap_event_name = 'group_beta_day0' then visit_date end)::date as group_beta_day0
        from source_events
        group by record_id;
        """,
        encoding="utf-8",
    )

    config = {
        "tasks": {
            "daily_schedule": {
                "file": str(sql_file.relative_to(tmp_path)),
                "outputs": ["duckdb://analytics.daily_schedule"],
            }
        }
    }

    analyzer = SqlLineageAnalyzer(config, project_root=tmp_path, dialect="duckdb")
    analyzer.build()

    metadata = analyzer.describe_table("analytics.daily_schedule")
    assert metadata.display_name == "analytics.daily_schedule"
    assert metadata.columns == [
        "record_id",
        "group_alpha_day0",
        "group_beta_day0",
    ]
    assert metadata.source_tables == [
        "read_csv(getvariable('staging_input'), header = true, union_by_name = true)"
    ]
    assert metadata.column_sources["record_id"] == [
        "READ_CSV(GETVARIABLE('staging_input'), header = TRUE, union_by_name = TRUE).record_id"
    ]
    assert set(metadata.column_sources["group_alpha_day0"]) == {
        "READ_CSV(GETVARIABLE('staging_input'), header = TRUE, union_by_name = TRUE).redcap_event_name",
        "READ_CSV(GETVARIABLE('staging_input'), header = TRUE, union_by_name = TRUE).visit1_date",
    }
    assert set(metadata.column_sources["group_beta_day0"]) == {
        "READ_CSV(GETVARIABLE('staging_input'), header = TRUE, union_by_name = TRUE).redcap_event_name",
        "READ_CSV(GETVARIABLE('staging_input'), header = TRUE, union_by_name = TRUE).visit1_date",
    }


def test_lineage_resolves_visit_dates_dependencies(tmp_path):
    sql_file = tmp_path / "visit_dates.sql"
    sql_file.write_text(
        """
        create or replace table analytics.visit_dates as
        with schedule as (
            select
                participant_id,
                route_id,
                planned_start,
                planned_end
            from staging.visit_plan
        ),
        overrides as (
            select
                participant_id,
                start_override,
                end_override
            from staging.visit_adjustments
        )
        select
            s.participant_id,
            coalesce(o.start_override, s.planned_start) as visit_start_date,
            coalesce(o.end_override, s.planned_end) as visit_end_date
        from schedule s
        left join overrides o on s.participant_id = o.participant_id;
        """,
        encoding="utf-8",
    )

    config = {
        "tasks": {
            "visit_dates": {
                "file": str(sql_file.relative_to(tmp_path)),
                "outputs": ["duckdb://analytics.visit_dates"],
            }
        }
    }

    analyzer = SqlLineageAnalyzer(config, project_root=tmp_path, dialect="duckdb")
    analyzer.build()

    metadata = analyzer.describe_table("analytics.visit_dates")
    assert metadata.columns == ["participant_id", "visit_start_date", "visit_end_date"]
    assert set(metadata.source_tables) == {"visit_plan", "visit_adjustments"}
    assert set(metadata.column_sources["visit_start_date"]) == {
        "staging.visit_adjustments.start_override",
        "staging.visit_plan.planned_start",
    }
    assert set(metadata.column_sources["visit_end_date"]) == {
        "staging.visit_adjustments.end_override",
        "staging.visit_plan.planned_end",
    }
