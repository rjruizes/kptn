from __future__ import annotations

import pytest

import kptn
from kptn.graph.decorators import RTaskSpec, SqlTaskSpec, TaskSpec, r_task, sql_task, task
from kptn.graph.requires import AnyOf, any_of


@task(outputs=["duckdb://a"])
def a() -> None: ...


@task(outputs=["duckdb://b"])
def b() -> None: ...


def test_task_requires_default_none() -> None:
    @task(outputs=["duckdb://x"])
    def x() -> None: ...

    assert x.__kptn__.requires is None


def test_task_requires_stores_handles() -> None:
    @task(outputs=["duckdb://x"], requires=[a, b])
    def x() -> None: ...

    assert isinstance(x.__kptn__, TaskSpec)
    assert x.__kptn__.requires == [a, b]


def test_sql_and_r_task_requires_field() -> None:
    s = sql_task("q.sql", outputs=["duckdb://s"], requires=[a])
    r = r_task("s.R", outputs=["duckdb://r"], requires=[a])
    assert isinstance(s.__kptn__, SqlTaskSpec)
    assert isinstance(r.__kptn__, RTaskSpec)
    assert s.__kptn__.requires == [a]
    assert r.__kptn__.requires == [a]


def test_any_of_holds_members() -> None:
    grp = any_of(a, b)
    assert isinstance(grp, AnyOf)
    assert grp.members == (a, b)


def test_any_of_empty_raises() -> None:
    with pytest.raises(ValueError):
        any_of()


def test_any_of_rejects_non_handle() -> None:
    with pytest.raises(TypeError):
        any_of(a, 123)


def test_any_of_exported_from_package() -> None:
    assert kptn.any_of is any_of
