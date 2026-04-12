from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from kptn.graph.graph import Graph


@dataclass
class ResolvedGraph:
    graph: Graph
    pipeline: str
    storage_key: str
    bypassed_names: frozenset[str] = field(default_factory=frozenset)
    profile_args: dict[str, dict[str, Any]] = field(default_factory=dict)
