from __future__ import annotations

from typing import Any, Callable


def config(**kwargs) -> "Graph":
    """Declare runtime dependency injection for tasks.

    Usage::

        deps = kptn.config(engine=get_engine, config=get_config)

    The special ``duckdb`` key accepts a ``(factory, alias)`` tuple so kptn
    knows both the connection factory and the kwarg name tasks use::

        deps = kptn.config(duckdb=(get_engine, "engine"), config=get_config)

    Tasks receive ``engine=get_engine()`` as their keyword argument.  kptn uses
    the factory internally for state store operations and output hashing.

    Callables are invoked at task dispatch time by the runner, not at graph
    definition time.

    Raises:
        TypeError: if any value is not callable (or a valid duckdb tuple)
    """
    if not kwargs:
        raise TypeError("kptn.config() requires at least one callable argument; got none.")
    for key, val in kwargs.items():
        if key == "duckdb":
            # Allow (callable, alias_str) tuple for duckdb
            if isinstance(val, tuple):
                if len(val) != 2 or not callable(val[0]) or not isinstance(val[1], str):
                    raise TypeError(
                        "kptn.config() duckdb value must be a callable or a (callable, alias) tuple; "
                        f"got {val!r}."
                    )
            elif not callable(val):
                raise TypeError(
                    f"kptn.config() requires callable values; "
                    f"got {type(val).__name__!r} for key 'duckdb'."
                )
        elif not callable(val):
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
