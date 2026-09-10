from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from uuid import UUID

import kptn
from kptn.exceptions import ProfileError, TaskError
from kptn.graph.graph import Graph
from kptn.graph.pipeline import Pipeline
from kptn.profiles.resolved import ResolvedGraph
from kptn.runner.api import plan, resolve_pipeline, run
from kptn.runner.events import EventKind, RunEvent


def _make_pipeline(name: str = "default") -> Pipeline:
    return Pipeline(name, Graph())


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: RunEvent) -> None:
        self.events.append(event)


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

    mock_store.assert_called_once_with(mock_settings, duckdb_factory=None)
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


def test_run_no_cache_and_kwargs_forwarded_to_execute() -> None:
    """no_cache=True and runtime kwargs are forwarded to execute()."""
    pipeline = _make_pipeline()
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute") as mock_exec, \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        run(pipeline, no_cache=True, engine="eng", config="cfg")

    _, exec_kwargs = mock_exec.call_args
    assert exec_kwargs["no_cache"] is True
    assert exec_kwargs["extra_kwargs"] == {"engine": "eng", "config": "cfg"}


def test_run_no_cache_graceful_when_kptn_yaml_absent() -> None:
    """no_cache=True does not crash when kptn.yaml is absent, and still forwards params."""
    pipeline = _make_pipeline()

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute") as mock_exec, \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.side_effect = FileNotFoundError
        run(pipeline, no_cache=True)

    mock_exec.assert_called_once()
    _, exec_kwargs = mock_exec.call_args
    assert exec_kwargs["no_cache"] is True
    assert exec_kwargs["extra_kwargs"] is None


def test_run_missing_yaml_raises_when_cache_enabled() -> None:
    """FileNotFoundError propagates normally when no_cache is False."""
    pipeline = _make_pipeline()

    with patch("kptn.runner.api.ProfileLoader") as mock_loader:
        mock_loader.load.side_effect = FileNotFoundError
        with pytest.raises(FileNotFoundError):
            run(pipeline)


def test_run_no_cache_with_profile_still_requires_yaml() -> None:
    """no_cache=True does not suppress FileNotFoundError when a profile is requested."""
    pipeline = _make_pipeline()

    with patch("kptn.runner.api.ProfileLoader") as mock_loader:
        mock_loader.load.side_effect = FileNotFoundError
        with pytest.raises(FileNotFoundError):
            run(pipeline, no_cache=True, profile="prod")


def test_run_no_cache_does_not_create_db_file(tmp_path, monkeypatch) -> None:
    """pipeline() with no_cache=True must not create .kptn/kptn.db on disk."""
    monkeypatch.chdir(tmp_path)
    # No kptn.yaml here — this should be fine with no_cache=True
    pipeline = _make_pipeline("no_db")
    run(pipeline, no_cache=True)
    assert not (tmp_path / ".kptn").exists(), ".kptn/ directory must not be created"


def test_run_emits_run_lifecycle_and_passes_emitter_to_execute() -> None:
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))
    sink = RecordingSink()

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute") as mock_exec, \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        run(pipeline, event_sink=sink, run_id="run-123")

    assert "emitter" in mock_exec.call_args.kwargs
    assert [event.kind for event in sink.events] == [
        EventKind.RUN_STARTED,
        EventKind.RUN_FINISHED,
    ]
    assert [event.run_id for event in sink.events] == ["run-123", "run-123"]
    assert sink.events[-1].payload["status"] == "succeeded"


def test_run_emits_failed_run_finished_before_reraising() -> None:
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))
    sink = RecordingSink()

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute", side_effect=TaskError("boom")), \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()):
        mock_loader.load.return_value = mock_config
        with pytest.raises(TaskError, match="boom"):
            run(pipeline, event_sink=sink, run_id="run-123")

    assert [event.kind for event in sink.events] == [
        EventKind.RUN_STARTED,
        EventKind.RUN_FINISHED,
    ]
    assert sink.events[-1].payload["status"] == "failed"
    assert sink.events[-1].payload["error"] == "boom"


def test_run_generates_run_id_only_when_missing() -> None:
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))
    sink = RecordingSink()
    generated = UUID("12345678-1234-5678-1234-567812345678")

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute"), \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()), \
         patch("kptn.runner.api.uuid4", return_value=generated) as mock_uuid:
        mock_loader.load.return_value = mock_config
        run(pipeline, event_sink=sink)

    mock_uuid.assert_called_once_with()
    assert sink.events[0].run_id == str(generated)


def test_run_uses_supplied_run_id_without_generating_uuid() -> None:
    pipeline = _make_pipeline("default")
    mock_config = MagicMock(settings=MagicMock(db="sqlite", db_path=None))
    sink = RecordingSink()

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.execute"), \
         patch("kptn.runner.api.init_state_store", return_value=MagicMock()), \
         patch("kptn.runner.api.uuid4") as mock_uuid:
        mock_loader.load.return_value = mock_config
        run(pipeline, event_sink=sink, run_id="run-123")

    mock_uuid.assert_not_called()
    assert sink.events[0].run_id == "run-123"


def test_resolve_pipeline_returns_resolved_graph_and_state_store() -> None:
    """Shared plan resolution returns the resolved graph and initialized state store."""
    pipeline = _make_pipeline("default")
    mock_settings = MagicMock(db="duckdb", db_path=".kptn/prod.db")
    mock_config = MagicMock(settings=mock_settings)
    mock_state_store = MagicMock()

    with patch("kptn.runner.api.ProfileLoader") as mock_loader, \
         patch("kptn.runner.api.init_state_store", return_value=mock_state_store) as mock_store:
        mock_loader.load.return_value = mock_config
        resolved, state_store = resolve_pipeline(pipeline, Path("/tmp/project"), None)

    assert resolved.pipeline == "default"
    assert resolved.storage_key == ".kptn/prod.db"
    assert state_store is mock_state_store
    mock_store.assert_called_once_with(mock_settings, duckdb_factory=None)


def test_plan_uses_shared_resolution() -> None:
    """runner.api.plan() resolves through the shared helper before rendering."""
    pipeline = _make_pipeline("default")
    resolved = ResolvedGraph(graph=pipeline, pipeline="default", storage_key=".kptn/kptn.db")
    state_store = MagicMock()

    with patch("kptn.runner.api.resolve_pipeline", return_value=(resolved, state_store)) as mock_resolve, \
         patch("kptn.runner.api._plan") as mock_plan:
        plan(pipeline, profile="dev")

    mock_resolve.assert_called_once_with(pipeline, Path.cwd(), "dev")
    mock_plan.assert_called_once_with(resolved, state_store)
