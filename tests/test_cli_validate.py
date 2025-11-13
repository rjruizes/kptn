from __future__ import annotations

from pathlib import Path

import textwrap
import yaml

from kptn.cli import _validate_python_tasks


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
    create_r_script: bool = True,
) -> dict:
    if consumer_dependencies is None:
        consumer_dependencies = ["producer"]

    py_tasks_dir = tmp_path / "py_tasks"
    _write_task(py_tasks_dir, "__init__.py", "")
    _write_task(py_tasks_dir, consumer_filename, consumer_source)
    r_tasks_dir = tmp_path / "r_tasks"
    r_tasks_dir.mkdir(exist_ok=True)
    if create_r_script:
        _write_task(
            r_tasks_dir,
            "producer.R",
            """
            # placeholder producer
            """,
        )

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
            "py_tasks_dir": "py_tasks",
            "r_tasks_dir": "r_tasks",
            "flows_dir": ".",
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

    (tmp_path / "kptn.yaml").write_text(
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


def test_validate_python_tasks_flags_missing_r_script(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(runtime_config, producer):
            return producer
        """,
        create_r_script=False,
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert any("R file 'producer.R' not found" in message for message in errors)


def test_validate_python_tasks_supports_multiple_python_dirs(tmp_path):
    primary_dir = tmp_path / "py_primary"
    secondary_dir = tmp_path / "py_shared"
    _write_task(primary_dir, "__init__.py", "")
    _write_task(primary_dir, "placeholder.py", "def placeholder():\n    return 1")
    _write_task(secondary_dir, "__init__.py", "")
    _write_task(
        secondary_dir,
        "consumer.py",
        """
        def consumer(runtime_config, producer):
            return producer
        """,
    )

    r_dir = tmp_path / "r_tasks"
    _write_task(
        r_dir,
        "producer.R",
        """
        # placeholder producer
        """,
    )

    kap_conf = {
        "settings": {
            "py_tasks_dir": ["py_primary", "py_shared"],
            "r_tasks_dir": "r_tasks",
            "flows_dir": ".",
        },
        "tasks": {
            "producer": {"file": "producer.R", "cache_result": True},
            "consumer": {"file": "consumer.py"},
        },
        "graphs": {
            "demo": {
                "tasks": {
                    "producer": None,
                    "consumer": ["producer"],
                }
            }
        },
    }

    (tmp_path / "kptn.yaml").write_text(
        yaml.safe_dump(kap_conf), encoding="utf-8"
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert errors == []


def test_validate_python_tasks_supports_multiple_r_dirs(tmp_path):
    py_dir = tmp_path / "py_tasks"
    _write_task(py_dir, "__init__.py", "")
    _write_task(
        py_dir,
        "consumer.py",
        """
        def consumer(runtime_config, producer):
            return producer
        """,
    )

    primary_r_dir = tmp_path / "r_missing"
    primary_r_dir.mkdir(parents=True, exist_ok=True)
    secondary_r_dir = tmp_path / "r_tasks"
    _write_task(
        secondary_r_dir,
        "producer.R",
        """
        # placeholder producer
        """,
    )

    kap_conf = {
        "settings": {
            "py_tasks_dir": "py_tasks",
            "r_tasks_dir": ["r_missing", "r_tasks"],
            "flows_dir": ".",
        },
        "tasks": {
            "producer": {"file": "producer.R", "cache_result": True},
            "consumer": {"file": "consumer.py"},
        },
        "graphs": {
            "demo": {
                "tasks": {
                    "producer": None,
                    "consumer": ["producer"],
                }
            }
        },
    }

    (tmp_path / "kptn.yaml").write_text(
        yaml.safe_dump(kap_conf), encoding="utf-8"
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
        config_block={"duckdb": "src.utils:get_engine"},
        consumer_dependencies=[],
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert errors == []


def test_validate_python_tasks_uses_duckdb_alias(tmp_path):
    kap_conf = _base_config(
        tmp_path,
        """
        def consumer(engine):
            return engine
        """,
        config_block={
            "duckdb": {
                "function": "tests.runtime_config_fixtures:build_engine",
                "parameter_name": "engine",
            }
        },
        consumer_dependencies=[],
    )

    errors = _validate_python_tasks(tmp_path, kap_conf)

    assert errors == []
