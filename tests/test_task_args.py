import inspect
from types import SimpleNamespace

from kptn.util.task_args import plan_python_call


def test_plan_python_call_injects_runtime_config_attribute():
    runtime_config = SimpleNamespace(foo="bar")

    def consumer(foo):  # noqa: ANN001 - signature under test
        return foo

    signature = inspect.signature(consumer)
    args, kwargs, missing = plan_python_call(signature, {}, runtime_config)

    assert args == []
    assert kwargs["foo"] == "bar"
    assert missing == []


def test_plan_python_call_passes_runtime_config_object():
    runtime_config = object()

    def consumer(runtime_config):  # noqa: ANN001 - signature under test
        return runtime_config

    signature = inspect.signature(consumer)
    args, kwargs, missing = plan_python_call(signature, {}, runtime_config)

    assert args == []
    assert kwargs["runtime_config"] is runtime_config
    assert missing == []


def test_plan_python_call_reports_missing_values():
    runtime_config = SimpleNamespace()

    def consumer(foo):  # noqa: ANN001 - signature under test
        return foo

    signature = inspect.signature(consumer)
    args, kwargs, missing = plan_python_call(signature, {}, runtime_config)

    assert args == []
    assert kwargs == {}
    assert missing == ["foo"]
