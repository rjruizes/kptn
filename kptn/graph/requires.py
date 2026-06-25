from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AnyOf:
    """A disjunctive requirement group: satisfied if any member is present.

    Created via :func:`any_of`. Holds kptn task handles (objects carrying
    ``__kptn__``). Never pulls tasks into the run — see ``gate_disjunctive``.
    """

    members: tuple[Any, ...]


def any_of(*handles: Any) -> AnyOf:
    """Group task handles into a disjunctive (OR) requirement.

    Usage::

        @kptn.task(outputs=["combined"], requires=[kptn.any_of(A, B)])
        def consumer(): ...

    Raises:
        ValueError: if no handles are given.
        TypeError: if a member is not a kptn task handle.
    """
    if not handles:
        raise ValueError("any_of() requires at least one task")
    for h in handles:
        if not hasattr(h, "__kptn__"):
            raise TypeError(
                f"any_of() expects @kptn.task / sql_task / r_task handles, "
                f"got {type(h).__name__!r}."
            )
    return AnyOf(members=tuple(handles))
