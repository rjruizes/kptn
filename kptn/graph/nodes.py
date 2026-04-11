from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Union

if TYPE_CHECKING:
    from kptn.graph.decorators import TaskSpec, SqlTaskSpec, RTaskSpec


@dataclass
class TaskNode:
    fn: Callable[..., Any]
    spec: "TaskSpec"
    name: str  # = fn.__name__


@dataclass
class SqlTaskNode:
    path: str           # path to .sql file, as declared by the user
    spec: "SqlTaskSpec"
    name: str           # = Path(path).stem  e.g. "query" from "path/to/query.sql"


@dataclass
class RTaskNode:
    path: str           # path to .R script, as declared by the user
    spec: "RTaskSpec"
    name: str           # = Path(path).stem  e.g. "script" from "path/to/script.R"


# Union of all node types present after Epic 1 Stories 1.1–1.3.
# Extend in later stories: add ParallelNode, StageNode, etc. to this union.
AnyNode = Union[TaskNode, SqlTaskNode, RTaskNode]


# TODO: Story 1.4+ — ParallelNode, StageNode, NoopNode
# TODO: Story 1.5+ — MapNode
# TODO: Story 1.6+ — PipelineNode
