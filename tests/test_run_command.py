from __future__ import annotations

import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

import kptn
from kptn.graph.graph import Graph
from kptn.graph.pipeline import Pipeline
from kptn.runner.api import run
from kptn.cli.commands import _load_pipeline_from_pyproject


def _make_pipeline(name: str = "default") -> Pipeline:
    return Pipeline(name, Graph())


def test_run_v2_accepts_pipeline_object() -> None:
    """kptn.run() accepts a Pipeline object (v0.2.0 API)."""
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute"), \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        kptn.run(pipeline)  # should not raise


def test_pipeline_run_method() -> None:
    """pipeline.run() is equivalent to kptn.run(pipeline)."""
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute") as mock_exec, \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        pipeline.run()
        mock_exec.assert_called_once()


def test_pipeline_run_method_passes_profile() -> None:
    """pipeline.run(profile=...) forwards the profile to kptn.run()."""
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))
    mock_resolved = MagicMock()

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.ProfileResolver") as mock_resolver_cls, \
         patch("kptn.runner.api.execute"), \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        mock_resolver_cls.return_value.compile.return_value = mock_resolved
        pipeline.run(profile="prod")
        mock_resolver_cls.return_value.compile.assert_called_once_with(pipeline, "prod")


def test_run_v2_does_not_accept_old_kwargs() -> None:
    """kptn.run() no longer accepts v0.1.x kwargs (project_dir, force, task_names)."""
    pipeline = _make_pipeline("default")
    with pytest.raises(TypeError):
        kptn.run(pipeline, project_dir=".")  # type: ignore[call-arg]


def test_load_pipeline_inserts_project_root_into_sys_path(tmp_path: Path) -> None:
    """_load_pipeline_from_pyproject inserts project_root into sys.path before importing."""
    # Create a minimal pyproject.toml
    (tmp_path / "pyproject.toml").write_text('[tool.kptn]\npipeline = "my_pipeline"\n')

    # Create a simple pipeline module
    (tmp_path / "my_pipeline.py").write_text(
        "from kptn.graph.pipeline import Pipeline\n"
        "from kptn.graph.graph import Graph\n"
        "pipeline = Pipeline('test', Graph())\n"
    )

    # Ensure tmp_path is NOT already in sys.path
    original_path = sys.path.copy()
    if str(tmp_path) in sys.path:
        sys.path.remove(str(tmp_path))

    try:
        result = _load_pipeline_from_pyproject(tmp_path)
        assert isinstance(result, Pipeline)
        assert str(tmp_path) in sys.path
    finally:
        # Restore sys.path
        sys.path[:] = original_path

