from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

import kptn
from kptn.exceptions import ProfileError, TaskError
from kptn.graph.graph import Graph
from kptn.graph.pipeline import Pipeline
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.api import run


def _make_pipeline(name: str = "default") -> Pipeline:
    return Pipeline(name, Graph())


def test_run_no_profile_uses_default_storage_key() -> None:
    """AC-1: minimal invocation uses default SQLite storage key."""
    pipeline = _make_pipeline("default")
    mock_settings = MagicMock(db="sqlite", db_path=None)
    mock_config = MagicMock(settings=mock_settings)

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute") as mock_exec, \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        run(pipeline)

    resolved = mock_exec.call_args[0][0]
    assert resolved.pipeline == "default"
    assert resolved.storage_key == ".kptn/kptn.db"


def test_run_sources_db_settings_from_config() -> None:
    """AC-2: db backend and db_path sourced from kptn.yaml settings."""
    pipeline = _make_pipeline("default")
    mock_settings = MagicMock(db="duckdb", db_path=".kptn/prod.db")
    mock_config = MagicMock(settings=mock_settings)

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute") as mock_exec, \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()) as mock_store:
        mock_loader.load.return_value = mock_config
        run(pipeline)

    mock_store.assert_called_once_with(mock_settings)
    resolved = mock_exec.call_args[0][0]
    assert resolved.storage_key == ".kptn/prod.db"


def test_run_applies_profile() -> None:
    """AC-3: profile argument routes through ProfileResolver.compile."""
    pipeline = _make_pipeline("default")
    mock_settings = MagicMock(db="sqlite", db_path=None)
    mock_config = MagicMock(settings=mock_settings)
    expected_resolved = ResolvedGraph(graph=pipeline, pipeline="default", storage_key=".kptn/kptn.db")

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.ProfileResolver") as mock_resolver_cls, \
         patch("kptn.runner.api.execute"), \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        mock_resolver_cls.return_value.compile.return_value = expected_resolved
        run(pipeline, profile="dev")

    mock_resolver_cls.return_value.compile.assert_called_once_with(pipeline, "dev")


def test_run_profile_error_propagates() -> None:
    """AC-4: ProfileError propagates to caller without re-wrapping."""
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.ProfileResolver") as mock_resolver_cls, \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        mock_resolver_cls.return_value.compile.side_effect = ProfileError("no such profile")

        with pytest.raises(ProfileError, match="no such profile"):
            run(pipeline, profile="nonexistent")


def test_run_task_error_propagates() -> None:
    """AC-5: TaskError propagates to caller without re-wrapping."""
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute") as mock_exec, \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        mock_exec.side_effect = TaskError("task a failed")

        with pytest.raises(TaskError, match="task a failed"):
            run(pipeline)


def test_run_exported_in_all() -> None:
    """AC-7: 'run' is present in kptn.__all__."""
    assert "run" in kptn.__all__


def test_run_is_v2_implementation() -> None:
    """AC-6: kptn.run has v0.2.0 signature (pipeline param, no task_names)."""
    sig = inspect.signature(kptn.run)
    assert "pipeline" in sig.parameters
    assert "task_names" not in sig.parameters
