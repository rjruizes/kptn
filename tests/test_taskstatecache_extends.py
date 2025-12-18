from types import SimpleNamespace

from kptn.caching.TaskStateCache import TaskStateCache


def _make_cache() -> TaskStateCache:
    cache = object.__new__(TaskStateCache)
    cache.pipeline_name = "basic_other"
    cache.pipeline_config = SimpleNamespace(PIPELINE_NAME="basic_other")
    cache.tasks_config = {
        "graphs": {
            "basic": {"tasks": {"a": None, "b": "a", "c": "b"}},
            "other": {"tasks": {"d": None, "e": "d", "f": "e"}},
            "basic_other": {
                "extends": ["basic", "other"],
                "tasks": {
                    "g": "c",
                    "h": "f",
                },
            },
            "extends_only": {
                "extends": "basic",
            },
        },
        "tasks": {},
    }
    return cache


def test_dep_list_resolves_inherited_graph_tasks():
    cache = _make_cache()

    assert cache.get_dep_list("a") == []
    assert cache.get_dep_list("c") == ["b"]
    assert cache.get_dep_list("f") == ["e"]
    assert cache.get_dep_list("h") == ["f"]


def test_dep_list_allows_extends_only_graph():
    cache = _make_cache()
    cache.pipeline_name = "extends_only"

    assert cache.get_dep_list("a") == []
    assert cache.get_dep_list("b") == ["a"]


def test_graph_task_object_with_args():
    cache = object.__new__(TaskStateCache)
    cache.pipeline_name = "pipe"
    cache.pipeline_config = SimpleNamespace(PIPELINE_NAME="pipe")
    cache.tasks_config = {
        "graphs": {
            "pipe": {
                "tasks": {
                    "a": {"deps": [], "args": {"x": 1}},
                    "b": "a",
                }
            }
        },
        "tasks": {},
    }

    tasks = cache._graph_tasks("pipe")
    assert tasks["a"]["args"] == {"x": 1}
    assert cache.get_dep_list("a") == []
    assert cache.get_dep_list("b") == ["a"]


def test_extends_args_override():
    cache = object.__new__(TaskStateCache)
    cache.pipeline_name = "child"
    cache.pipeline_config = SimpleNamespace(PIPELINE_NAME="child")
    cache.tasks_config = {
        "graphs": {
            "base": {"tasks": {"a": None}},
            "child": {
                "extends": [
                    {
                        "graph": "base",
                        "args": {
                            "a": {"foo": "override"},
                        },
                    }
                ]
            },
        },
        "tasks": {
            "a": {
                "file": "a.py",
                "args": {"foo": "base", "bar": 1},
            }
        },
    }

    args = cache.get_py_func_args("a")
    assert args == {"foo": "override", "bar": 1}
