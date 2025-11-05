from __future__ import annotations

import json
from pathlib import Path

import kptn


def _create_basic_project(root: Path, *, include_pipeline_config: bool) -> Path:
    """Set up a minimal kptn project for testing kptn.run."""
    project_dir = root / ("with_config" if include_pipeline_config else "no_config")
    flows_dir = project_dir
    py_tasks_dir = project_dir / "py_tasks"
    py_tasks_dir.mkdir(parents=True, exist_ok=True)
    (py_tasks_dir / "__init__.py").write_text("", encoding="utf-8")
    (py_tasks_dir / "alpha.py").write_text("def alpha():\n    return 1\n", encoding="utf-8")

    kptn_yaml = {
        "settings": {
            "flows_dir": ".",
            "py_tasks_dir": "py_tasks",
            "r_tasks_dir": ".",
        },
        "graphs": {
            "demo": {"tasks": {"alpha": None}},
        },
        "tasks": {
            "alpha": {"file": "py_tasks/alpha.py"}
        },
    }

    (project_dir / "kptn.yaml").write_text(json.dumps(kptn_yaml), encoding="utf-8")

    call_file = flows_dir / "call.json"
    call_file_literal = json.dumps(str(call_file))

    if include_pipeline_config:
        flow_source = f"""
import json
from pathlib import Path
from kptn.util.pipeline_config import PipelineConfig

def demo(pipeline_config: PipelineConfig, task_list=None, ignore_cache=False):
    data = {{
        "has_pipeline_config": isinstance(pipeline_config, PipelineConfig),
        "task_list": list(task_list or []),
        "ignore_cache": ignore_cache,
    }}
    Path({call_file_literal}).write_text(json.dumps(data), encoding="utf-8")
"""
    else:
        flow_source = f"""
import json
from pathlib import Path

def demo(task_list=None, ignore_cache=False):
    data = {{
        "task_list": list(task_list or []),
        "ignore_cache": ignore_cache,
    }}
    Path({call_file_literal}).write_text(json.dumps(data), encoding="utf-8")
"""

    (flows_dir / "demo.py").write_text(flow_source.strip() + "\n", encoding="utf-8")

    return project_dir


def test_run_invokes_pipeline_with_pipeline_config(tmp_path):
    project_dir = _create_basic_project(tmp_path, include_pipeline_config=True)

    kptn.run("alpha", project_dir=str(project_dir), force=True)

    call_data = json.loads((project_dir / "call.json").read_text(encoding="utf-8"))
    assert call_data["has_pipeline_config"] is True
    assert call_data["task_list"] == ["alpha"]
    assert call_data["ignore_cache"] is True


def test_run_invokes_pipeline_without_pipeline_config(tmp_path):
    project_dir = _create_basic_project(tmp_path, include_pipeline_config=False)

    kptn.run(["alpha"], project_dir=str(project_dir))

    call_data = json.loads((project_dir / "call.json").read_text(encoding="utf-8"))
    assert call_data["task_list"] == ["alpha"]
    assert call_data["ignore_cache"] is False
