from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


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
