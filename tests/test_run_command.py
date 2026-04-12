from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

import kptn
from kptn.graph.graph import Graph
from kptn.graph.pipeline import Pipeline
from kptn.runner.api import run


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


def test_run_v2_does_not_accept_old_kwargs() -> None:
    """kptn.run() no longer accepts v0.1.x kwargs (project_dir, force, task_names)."""
    pipeline = _make_pipeline("default")
    with pytest.raises(TypeError):
        kptn.run(pipeline, project_dir=".")  # type: ignore[call-arg]

