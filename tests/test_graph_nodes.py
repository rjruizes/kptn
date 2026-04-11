from kptn.graph.nodes import TaskNode
from kptn.graph.decorators import TaskSpec


def _make_spec() -> TaskSpec:
    return TaskSpec(outputs=["duckdb://schema.table"])


def test_task_node_has_fn_spec_name() -> None:
    def my_fn() -> int:
        return 42

    spec = _make_spec()
    node = TaskNode(fn=my_fn, spec=spec, name=my_fn.__name__)

    assert node.fn is my_fn
    assert node.spec is spec
    assert node.name == "my_fn"


def test_task_node_name_from_fn() -> None:
    def extract_data() -> None:
        pass

    spec = _make_spec()
    node = TaskNode(fn=extract_data, spec=spec, name=extract_data.__name__)

    assert node.name == extract_data.__name__
