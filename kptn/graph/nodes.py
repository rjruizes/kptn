from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from kptn.graph.decorators import TaskSpec


@dataclass
class TaskNode:
    fn: Callable[..., Any]
    spec: "TaskSpec"
    name: str  # = fn.__name__


# TODO: Story 1.3+ — SqlTaskNode, RTaskNode
# TODO: Story 1.4+ — ParallelNode, StageNode, MapNode
# TODO: Story 1.5+ — NoopNode
# TODO: Story 1.6+ — PipelineNode
