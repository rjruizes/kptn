import pytest
pytest.skip("v0.1.x jsonschema dep removed in v0.2.0", allow_module_level=True)

from pathlib import Path

from kptn.cli.config_validation import validate_kptn_config


def test_validate_kptn_config_accepts_settings_logging_file(tmp_path):
    config_path = tmp_path / "kptn.yaml"
    schema_path = Path(__file__).resolve().parents[1] / "kptn-schema.json"
    config_path.write_text(
        "\n".join(
            [
                "settings:",
                "  flows_dir: .",
                "  flow_type: vanilla",
                "  logging:",
                "    file: log/kptn.log",
                "graphs:",
                "  demo:",
                "    tasks:",
                "      task_a:",
                "tasks:",
                "  task_a:",
                "    file: a.py",
                "",
            ]
        ),
        encoding="utf-8",
    )

    issues = validate_kptn_config(config_path, schema_path)

    assert issues == []
