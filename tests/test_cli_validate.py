from __future__ import annotations

from pathlib import Path

import textwrap
import yaml

from kapten.cli import _validate_python_tasks


def _write_task(path: Path, filename: str, source: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / filename).write_text(textwrap.dedent(source).strip() + "\n", encoding="utf-8")


def _base_config(
    tmp_path: Path,
    consumer_source: str,
    *,
    consumer_filename: str = "consumer.py",
    consumer_overrides: dict | None = None,
    config_block: dict | None = None,
    consumer_dependencies: list[str] | None = None,
) -> dict:
    if consumer_dependencies is None:
        consumer_dependencies = ["producer"]

    py_tasks_dir = tmp_path / "py_tasks"
    _write_task(py_tasks_dir, "__init__.py", "")
    _write_task(py_tasks_dir, consumer_filename, consumer_source)
    (tmp_path / "r_tasks").mkdir(exist_ok=True)

    tasks: dict[str, dict] = {
        "producer": {
            "file": "producer.R",
            "cache_result": True,
        },
        "consumer": {
            "file": consumer_filename,
        },
    }
    if consumer_overrides:
        tasks["consumer"].update(consumer_overrides)

    kap_conf = {
        "settings": {
            "py-tasks-dir": "py_tasks",
            "r-tasks-dir": "r_tasks",
            "flows-dir": ".",
        },
        "tasks": tasks,
        "graphs": {
            "demo": {
                "tasks": {
                    "producer": None,
                    "consumer": consumer_dependencies,
                }
            }
        },
    }

    if config_block:
        kap_conf["config"] = config_block

    (tmp_path / "kapten.yaml").write_text(
        yaml.safe_dump(kap_conf), encoding="utf-8"
    )

    return kap_conf


def test_validate_python_tasks_passes_with_matching_signature(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(runtime_config, producer):
            return producer
        """,
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert errors == []


def test_validate_python_tasks_defaults_to_task_name(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(runtime_config, producer):
            return producer
        """,
        consumer_filename="module.py",
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert errors == []


def test_validate_python_tasks_reports_missing_dependency_argument(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(runtime_config):
            return None
        """,
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert any(
        "does not accept argument(s) producer" in message for message in errors
    )


def test_validate_python_tasks_allows_keyword_only_parameters(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(*, producer):
            return producer
        """,
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert errors == []


def test_validate_python_tasks_flags_invalid_ref_alias(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(runtime_config, **kwargs):
            return kwargs
        """,
        consumer_overrides={
            "args": {
                "data": {"ref": "nonexistent"},
            }
        },
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert any("references 'nonexistent'" in message for message in errors)


def test_validate_python_tasks_uses_runtime_config_attributes(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(duckdb):
            return duckdb
        """,
        config_block={"duckdb": "memory"},
        consumer_dependencies=[],
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert errors == []


def test_validate_python_tasks_flags_missing_runtime_attribute(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(duckdb):
            return duckdb
        """,
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert any("requires parameter(s) duckdb" in message for message in errors)


def test_validate_python_tasks_imports_modules_from_project(tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "__init__.py").write_text("", encoding="utf-8")
    (src_dir / "utils.py").write_text(
        "def get_engine():\n    return 'duckdb://memory'\n",
        encoding="utf-8",
    )

    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(runtime_config):
            return runtime_config.duckdb
        """,
        config_block={"duckdb": "src.utils:get_engine()"},
        consumer_dependencies=[],
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert errors == []
