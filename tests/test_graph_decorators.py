import pytest
from kptn.graph.decorators import TaskSpec, task
from kptn.graph.nodes import TaskNode


def test_task_is_callable() -> None:
    """AC-1: decorated function remains callable."""

    @task(outputs=["duckdb://schema.table"])
    def my_fn() -> int:
        return 42

    assert callable(my_fn)


def test_task_kptn_attribute_attached() -> None:
    """AC-2: __kptn__ is a TaskSpec."""

    @task(outputs=["duckdb://schema.table"])
    def my_fn() -> None:
        pass

    assert hasattr(my_fn, "__kptn__")
    assert isinstance(my_fn.__kptn__, TaskSpec)


def test_task_spec_outputs() -> None:
    """TaskSpec.outputs holds the declared list."""

    @task(outputs=["duckdb://s.t", "duckdb://s.u"])
    def my_fn() -> None:
        pass

    assert my_fn.__kptn__.outputs == ["duckdb://s.t", "duckdb://s.u"]


def test_task_spec_optional_default_none() -> None:
    """optional defaults to None."""

    @task(outputs=["duckdb://s.t"])
    def my_fn() -> None:
        pass

    assert my_fn.__kptn__.optional is None


def test_task_spec_compute_default_none() -> None:
    """compute defaults to None."""

    @task(outputs=["duckdb://s.t"])
    def my_fn() -> None:
        pass

    assert my_fn.__kptn__.compute is None


def test_task_is_not_task_node() -> None:
    """AC-2: isinstance(fn, TaskNode) is False."""

    @task(outputs=["duckdb://s.t"])
    def my_fn() -> None:
        pass

    assert not isinstance(my_fn, TaskNode)


def test_task_return_value_unchanged() -> None:
    """AC-1: calling fn returns original return value."""

    @task(outputs=["duckdb://s.t"])
    def my_fn() -> int:
        return 99

    assert my_fn() == 99


def test_task_spec_optional_and_compute_set() -> None:
    """TaskSpec stores optional and compute when provided."""

    @task(outputs=["duckdb://s.t"], optional="skip", compute="large")
    def my_fn() -> None:
        pass

    assert my_fn.__kptn__.optional == "skip"
    assert my_fn.__kptn__.compute == "large"
