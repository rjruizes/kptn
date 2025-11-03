from datetime import datetime
from pathlib import Path

import pytest

from kptn.aws.decider import decide_task_execution
from kptn.caching.TaskStateCache import TaskStateCache
from kptn.caching.models import TaskState


class FakeDbClient:
    """Lightweight DB client stub for TaskStateCache interactions."""

    def __init__(self, task_states: dict[str, TaskState] | None = None):
        self._states = {
            name: state.model_dump()
            for name, state in (task_states or {}).items()
        }

    def get_task(self, task_name: str, include_data: bool, subset_mode: bool = False):
        state = self._states.get(task_name)
        if state is None:
            return None
        return dict(state)

    # The remaining methods are unused in the decider workflow but are present
    # to satisfy TaskStateCache expectations.
    def create_task(self, task_name: str, value, data=None):
        self._states[task_name] = dict(value)

    def delete_task(self, task_name: str):
        self._states.pop(task_name, None)

    def get_subtasks(self, task_name: str):
        return []

    def create_subtasks(self, task_name: str, value):
        return None

    def set_task_ended(self, task_name: str, **kwargs):
        return None

    def set_subtask_ended(self, task_name: str, idx: int, hash_value: str):
        return None

    def set_subtask_started(self, task_name: str, idx: int):
        return None


@pytest.fixture(autouse=True)
def reset_task_state_cache():
    """Ensure TaskStateCache singleton does not leak between tests."""
    TaskStateCache._instance = None
    yield
    TaskStateCache._instance = None


@pytest.fixture
def mock_pipeline_config_path() -> str:
    return str(Path("example/mock_pipeline/kptn.yaml").resolve())


@pytest.fixture
def patch_code_hashes(monkeypatch):
    """Avoid hashing real task code during unit tests."""

    def _no_op_build(self, task_name, task, **kwargs):
        return None, None

    monkeypatch.setattr(TaskStateCache, "build_task_code_hashes", _no_op_build)


def build_task_state(**overrides) -> TaskState:
    base = {
        "PK": overrides.get("PK", "task#demo"),
        "start_time": overrides.get("start_time", datetime.now().isoformat()),
        "end_time": overrides.get("end_time", datetime.now().isoformat()),
        "status": overrides.get("status", "SUCCESS"),
        "subtask_count": overrides.get("subtask_count", 0),
        "taskdata_count": overrides.get("taskdata_count", 0),
        "subset_count": overrides.get("subset_count", 0),
        "updated_at": overrides.get("updated_at", datetime.now().isoformat()),
        "code_hashes": overrides.get("code_hashes"),
        "input_hashes": overrides.get("input_hashes"),
        "input_data_hashes": overrides.get("input_data_hashes"),
        "outputs_version": overrides.get("outputs_version"),
        "data": overrides.get("data"),
        "output_data_version": overrides.get("output_data_version"),
    }
    return TaskState(**base)


def test_decider_skips_when_task_not_in_list(mock_pipeline_config_path, patch_code_hashes):
    event = {
        "TASKS_CONFIG_PATH": mock_pipeline_config_path,
        "PIPELINE_NAME": "sample",
        "task_name": "A",
        "task_list": ["B", "C"],
    }
    response = decide_task_execution(event=event, db_client=FakeDbClient())
    assert response == {
        "task_name": "A",
        "should_run": False,
        "reason": "Task not selected",
    }


def test_decider_triggers_when_cache_missing(mock_pipeline_config_path, patch_code_hashes):
    event = {
        "TASKS_CONFIG_PATH": mock_pipeline_config_path,
        "PIPELINE_NAME": "sample",
        "task_name": "A",
    }
    response = decide_task_execution(event=event, db_client=FakeDbClient())
    assert response["task_name"] == "A"
    assert response["should_run"] is True
    assert response["reason"] == "No cached state"


def test_decider_skips_when_cache_fresh(mock_pipeline_config_path, patch_code_hashes):
    cached = build_task_state()
    event = {
        "TASKS_CONFIG_PATH": mock_pipeline_config_path,
        "PIPELINE_NAME": "sample",
        "task_name": "A",
    }
    response = decide_task_execution(
        event=event,
        db_client=FakeDbClient({"A": cached}),
    )
    assert response["task_name"] == "A"
    assert response["should_run"] is False
    assert "reason" not in response


def test_decider_respects_ignore_cache(mock_pipeline_config_path, patch_code_hashes):
    cached = build_task_state()
    event = {
        "TASKS_CONFIG_PATH": mock_pipeline_config_path,
        "PIPELINE_NAME": "sample",
        "task_name": "A",
        "ignore_cache": True,
    }
    response = decide_task_execution(
        event=event,
        db_client=FakeDbClient({"A": cached}),
    )
    assert response["task_name"] == "A"
    assert response["should_run"] is True
    assert response["reason"] == "ignore_cache is set"


def test_decider_returns_array_size_for_mapped_task(mock_pipeline_config_path, patch_code_hashes):
    combo_data = build_task_state(
        data=[("item-a", "item-b"), ("item-c", "item-d")],
    )
    event = {
        "TASKS_CONFIG_PATH": mock_pipeline_config_path,
        "PIPELINE_NAME": "combotest",
        "task_name": "combo_process",
    }
    response = decide_task_execution(
        event=event,
        db_client=FakeDbClient({"combo_list": combo_data}),
    )
    assert response["task_name"] == "combo_process"
    assert response["should_run"] is True
    assert response["reason"] == "No cached state"
    assert response["array_size"] == 2


def test_decider_merges_state_payload(mock_pipeline_config_path, patch_code_hashes):
    event = {
        "state": {
            "TASKS_CONFIG_PATH": mock_pipeline_config_path,
            "PIPELINE_NAME": "sample",
            "task_list": [],
        },
        "task_name": "A",
        "execution_mode": "ecs",
    }
    response = decide_task_execution(event=event, db_client=FakeDbClient())
    assert response["task_name"] == "A"
    assert response["should_run"] is True
