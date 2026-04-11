from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from kptn.graph.graph import Graph   # avoids circular at runtime


@dataclass
class TaskSpec:
    outputs: list[str]
    optional: str | None = None
    compute: str | None = None

    def __post_init__(self) -> None:
        self.outputs = list(self.outputs)


class _KptnCallable:
    """
    Thin callable wrapper that enables the >> operator on kptn-decorated functions.

    Returned by @kptn.task.  Satisfies all AC constraints:
      - callable:              delegates __call__ to the wrapped function
      - transparent:           returns original return value, no side-effects
      - not a TaskNode:        isinstance(fn, TaskNode) is False
      - carries __kptn__:      holds the TaskSpec as an attribute
    """

    def __init__(self, fn: Callable[..., Any], spec: TaskSpec) -> None:
        self.__wrapped__ = fn
        self.__kptn__: TaskSpec = spec
        self.__name__: str = getattr(fn, "__name__", "")
        self.__doc__: str | None = getattr(fn, "__doc__", None)
        self.__qualname__: str = getattr(fn, "__qualname__", self.__name__)
        self.__module__: str | None = getattr(fn, "__module__", None)
        self.__annotations__: dict[str, Any] = getattr(fn, "__annotations__", {})

    # ------------------------------------------------------------------ #
    # Callable — transparent pass-through                                  #
    # ------------------------------------------------------------------ #

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.__wrapped__(*args, **kwargs)

    # ------------------------------------------------------------------ #
    # Sequential composition operator                                       #
    # ------------------------------------------------------------------ #

    def __rshift__(self, other: Any) -> Any:
        from kptn.graph.graph import Graph

        return Graph._from_node(self) >> other

    def __repr__(self) -> str:
        return f"<kptn task '{self.__name__}'>"


def task(
    outputs: list[str],
    optional: str | None = None,
    compute: str | None = None,
) -> Callable[[Callable[..., Any]], _KptnCallable]:
    """Attach kptn metadata to a function and enable >> chaining."""

    def decorator(fn: Callable[..., Any]) -> _KptnCallable:
        spec = TaskSpec(outputs=outputs, optional=optional, compute=compute)
        return _KptnCallable(fn, spec)

    return decorator  # type: ignore[return-value]


# ──────────────────────────────────────────────────────────────────────────── #
# SQL task factory                                                              #
# ──────────────────────────────────────────────────────────────────────────── #


@dataclass
class SqlTaskSpec:
    path: str
    outputs: list[str]
    optional: str | None = None

    def __post_init__(self) -> None:
        self.outputs = list(self.outputs)          # copy-on-init defensive guard


class _SqlTaskHandle:
    """
    Returned by sql_task().  Enables >> without being a SqlTaskNode.
    Pattern mirrors _KptnCallable: the Graph wraps this into a SqlTaskNode.

    Not callable — SQL tasks are executed by the runner, not Python.
    """

    def __init__(self, spec: SqlTaskSpec) -> None:
        self.__kptn__: SqlTaskSpec = spec
        self.__name__: str = Path(spec.path).stem

    def __rshift__(self, other: Any) -> Any:
        from kptn.graph.graph import Graph

        return Graph._from_node(self) >> other

    def __repr__(self) -> str:
        return f"<kptn sql_task '{self.__name__}'>"


def sql_task(
    path: str,
    outputs: list[str],
    optional: str | None = None,
) -> _SqlTaskHandle:
    """Return a composable handle for a SQL task file."""
    spec = SqlTaskSpec(path=path, outputs=outputs, optional=optional)
    return _SqlTaskHandle(spec)


# ──────────────────────────────────────────────────────────────────────────── #
# R task factory                                                                #
# ──────────────────────────────────────────────────────────────────────────── #


@dataclass
class RTaskSpec:
    path: str
    outputs: list[str]
    compute: str | None = None
    optional: str | None = None

    def __post_init__(self) -> None:
        self.outputs = list(self.outputs)          # copy-on-init defensive guard


class _RTaskHandle:
    """
    Returned by r_task().  Same pattern as _SqlTaskHandle.
    Not callable — R tasks are dispatched as subprocesses by the runner.
    """

    def __init__(self, spec: RTaskSpec) -> None:
        self.__kptn__: RTaskSpec = spec
        self.__name__: str = Path(spec.path).stem

    def __rshift__(self, other: Any) -> Any:
        from kptn.graph.graph import Graph

        return Graph._from_node(self) >> other

    def __repr__(self) -> str:
        return f"<kptn r_task '{self.__name__}'>"


def r_task(
    path: str,
    outputs: list[str],
    compute: str | None = None,
    optional: str | None = None,
) -> _RTaskHandle:
    """Return a composable handle for an R script task."""
    spec = RTaskSpec(path=path, outputs=outputs, compute=compute, optional=optional)
    return _RTaskHandle(spec)


# ──────────────────────────────────────────────────────────────────────────── #
# Noop factory                                                                  #
# ──────────────────────────────────────────────────────────────────────────── #


def noop() -> "Graph":
    """Return a single-node Graph containing a NoopNode sentinel.

    Usage:
        A >> kptn.noop() >> B      # NoopNode between A and B
    """
    from kptn.graph.graph import Graph
    from kptn.graph.nodes import NoopNode

    return Graph(nodes=[NoopNode(name="noop")], edges=[])
