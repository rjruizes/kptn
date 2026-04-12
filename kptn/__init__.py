from __future__ import annotations

# kptn package
__version__ = "0.1.0"

# v0.2.0 public API
from kptn.graph.decorators import task, sql_task, r_task, noop  # noqa: F401
from kptn.graph.composition import parallel, Stage, map  # noqa: F401
from kptn.graph.pipeline import Pipeline  # noqa: F401
from kptn.graph.config import config  # noqa: F401
from kptn.runner.api import run  # noqa: F401

__all__ = ["task", "sql_task", "r_task", "noop", "parallel", "Stage", "map", "Pipeline", "config", "run"]
