from kptn.util.pipeline_config import PipelineConfig


def test_pipeline_config_resolves_runtime_log_file_relative_to_kptn_yaml(tmp_path):
    config_dir = tmp_path / "project"
    config_dir.mkdir()
    config_path = config_dir / "kptn.yaml"
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
                "    tasks: {}",
                "tasks: {}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    pipeline_config = PipelineConfig(TASKS_CONFIG_PATH=str(config_path), PIPELINE_NAME="demo")

    assert pipeline_config.runtime_log_file == str((config_dir / "log" / "kptn.log").resolve())


def test_pipeline_config_returns_none_when_runtime_log_file_is_unset(tmp_path):
    config_path = tmp_path / "kptn.yaml"
    config_path.write_text(
        "\n".join(
            [
                "settings:",
                "  flows_dir: .",
                "  flow_type: vanilla",
                "graphs:",
                "  demo:",
                "    tasks: {}",
                "tasks: {}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    pipeline_config = PipelineConfig(TASKS_CONFIG_PATH=str(config_path), PIPELINE_NAME="demo")

    assert pipeline_config.runtime_log_file is None
