from __future__ import annotations

from dataclasses import dataclass, field
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


@dataclass
class ParallelNode:
    name: str = "parallel"   # optional label; passed as first positional string to parallel()
    # Names of every node inside this group's subgraph (excluding the sentinel).
    # Captured at construction so cursor resolution can keep the group atomic even
    # when branches are multi-node subgraphs. compare=False keeps node equality stable.
    members: frozenset[str] = field(default_factory=frozenset, compare=False)


@dataclass
class StageNode:
    name: str                # required — stage name for profile selection (Epic 3)
    members: frozenset[str] = field(default_factory=frozenset, compare=False)


@dataclass
class NoopNode:
    name: str = "noop"       # sentinel label


@dataclass
class MapNode:
    task: Callable[..., Any]   # @kptn.task-decorated callable (has __kptn__ attribute)
    over: str                   # dotted path into runtime context, e.g. "ctx.states"
    name: str                   # = task.__name__, used by topo_sort cycle error reporting


@dataclass
class PipelineNode:
    name: str  # the pipeline name — used by topo_sort cycle error reporting
    members: frozenset[str] = field(default_factory=frozenset, compare=False)


@dataclass
class ConfigNode:
    spec: dict[str, Callable[[], Any]]  # {param_name: factory_callable}
    name: str = "config"                # sentinel label — topo_sort uses n.name


# Union of all node types present after Epic 1 Stories 1.1–1.7.
AnyNode = Union[TaskNode, SqlTaskNode, RTaskNode, ParallelNode, StageNode, NoopNode, MapNode, PipelineNode, ConfigNode]
