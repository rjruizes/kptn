import pytest

import kptn
from kptn.graph.nodes import ConfigNode
from kptn.graph.graph import Graph
from kptn.graph.config import config, invoke_config
from kptn.exceptions import TaskError


# ─── AC-3: config() returns a Graph without invoking callables ───────────── #


def test_config_returns_graph():
    def get_engine():
        return "engine"

    result = kptn.config(engine=get_engine)
    assert isinstance(result, Graph)


def test_config_graph_has_single_node():
    def get_engine():
        return "engine"

    result = kptn.config(engine=get_engine)
    assert len(result.nodes) == 1


def test_config_graph_node_is_config_node():
    def get_engine():
        return "engine"

    result = kptn.config(engine=get_engine)
    assert isinstance(result.nodes[0], ConfigNode)


def test_config_callable_not_invoked_during_construction():
    called = []

    def get_engine():
        called.append(True)
        return "engine"

    kptn.config(engine=get_engine)
    assert called == [], "callable should NOT be invoked at graph definition time"


def test_config_node_spec_stores_callable():
    def get_engine():
        return "engine"

    result = kptn.config(engine=get_engine)
    node = result.nodes[0]
    assert isinstance(node, ConfigNode)
    assert node.spec["engine"] is get_engine


def test_config_node_name_is_config_sentinel():
    def get_engine():
        return "engine"

    result = kptn.config(engine=get_engine)
    node = result.nodes[0]
    assert node.name == "config"


def test_config_graph_has_no_edges():
    def get_engine():
        return "engine"

    result = kptn.config(engine=get_engine)
    assert result.edges == []


def test_config_raises_type_error_for_non_callable():
    with pytest.raises(TypeError, match="requires callable values"):
        kptn.config(engine=42)


def test_config_raises_type_error_mentions_key():
    with pytest.raises(TypeError, match="'bad_key'"):
        kptn.config(bad_key="not_callable")


def test_config_raises_type_error_for_zero_arguments():
    with pytest.raises(TypeError, match="at least one callable argument"):
        kptn.config()


# ─── AC-1: invoke_config() resolves callables ─────────────────────────────── #


def test_invoke_config_returns_resolved_value():
    def get_engine():
        return "engine_instance"

    node = ConfigNode(spec={"engine": get_engine})
    result = invoke_config(node)
    assert result == {"engine": "engine_instance"}


def test_invoke_config_multiple_keys():
    def get_engine():
        return "engine_instance"

    def get_conf():
        return {"key": "value"}

    node = ConfigNode(spec={"engine": get_engine, "config": get_conf})
    result = invoke_config(node)
    assert result["engine"] == "engine_instance"
    assert result["config"] == {"key": "value"}


def test_invoke_config_calls_each_callable_once():
    call_counts = {"engine": 0, "config": 0}

    def get_engine():
        call_counts["engine"] += 1
        return "e"

    def get_conf():
        call_counts["config"] += 1
        return "c"

    node = ConfigNode(spec={"engine": get_engine, "config": get_conf})
    invoke_config(node)
    assert call_counts == {"engine": 1, "config": 1}


def test_invoke_config_returns_all_resolved_keys():
    node = ConfigNode(spec={
        "a": lambda: 1,
        "b": lambda: 2,
        "c": lambda: 3,
    })
    result = invoke_config(node)
    assert set(result.keys()) == {"a", "b", "c"}


# ─── AC-2: invoke_config() wraps errors in TaskError ─────────────────────── #


def test_invoke_config_wraps_exception_in_task_error():
    def bad_fn():
        raise ValueError("boom")

    node = ConfigNode(spec={"engine": bad_fn})
    with pytest.raises(TaskError):
        invoke_config(node)


def test_invoke_config_task_error_has_cause():
    original = ValueError("original error")

    def bad_fn():
        raise original

    node = ConfigNode(spec={"engine": bad_fn})
    with pytest.raises(TaskError) as exc_info:
        invoke_config(node)
    assert exc_info.value.__cause__ is original


def test_invoke_config_task_error_cause_is_original_instance():
    class MyError(Exception):
        pass

    original = MyError("specific error")

    def bad_fn():
        raise original

    node = ConfigNode(spec={"engine": bad_fn})
    with pytest.raises(TaskError) as exc_info:
        invoke_config(node)
    assert isinstance(exc_info.value.__cause__, MyError)
    assert exc_info.value.__cause__ is original


# ─── Public API surface ───────────────────────────────────────────────────── #


def test_kptn_config_is_accessible():
    assert hasattr(kptn, "config")
    assert callable(kptn.config)


def test_config_in_kptn_all():
    assert "config" in kptn.__all__


def test_config_node_importable_from_kptn_graph():
    from kptn.graph import ConfigNode as _ConfigNode
    assert _ConfigNode is ConfigNode


def test_config_node_in_kptn_graph_all():
    from kptn import graph
    assert "ConfigNode" in graph.__all__
    assert "config" in graph.__all__
