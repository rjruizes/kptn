from __future__ import annotations

from typing import Any, Callable


def config(**kwargs: Callable[[], Any]) -> "Graph":
    """Declare runtime dependency injection for tasks.

    Usage:
        deps = kptn.config(engine=get_engine, config=get_config)

    Returns a single-node Graph containing a ConfigNode. Compose with >>
    to include in a pipeline. Callables are invoked at task dispatch time
    by the runner (Epic 2), not at graph definition time.

    Raises:
        TypeError: if any value is not callable
    """
    if not kwargs:
        raise TypeError("kptn.config() requires at least one callable argument; got none.")
    for key, val in kwargs.items():
        if not callable(val):
            raise TypeError(
                f"kptn.config() requires callable values; "
                f"got {type(val).__name__!r} for key '{key}'."
            )
    from kptn.graph.graph import Graph
    from kptn.graph.nodes import ConfigNode

    return Graph(nodes=[ConfigNode(spec=dict(kwargs))], edges=[])


def invoke_config(config_node: "ConfigNode") -> dict[str, Any]:
    """Invoke all callables in a ConfigNode and return the resolved values.

    Called at task dispatch time by the runner (Epic 2).

    Raises:
        TaskError: wrapping the original exception if a callable raises
    """
    from kptn.exceptions import TaskError

    result: dict[str, Any] = {}
    for key, fn in config_node.spec.items():
        try:
            result[key] = fn()
        except Exception as exc:
            raise TaskError(f"Config callable '{key}' raised an error") from exc
    return result
