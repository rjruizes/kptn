from __future__ import annotations

import logging
from pathlib import Path

from kptn.graph.pipeline import Pipeline
from kptn.profiles.loader import ProfileLoader
from kptn.profiles.resolved import ResolvedGraph
from kptn.profiles.resolver import ProfileResolver
from kptn.runner.executor import execute
from kptn.state_store.factory import init_state_store

logger = logging.getLogger(__name__)


def run(pipeline: Pipeline, *, profile: str | None = None) -> None:
    cwd = Path.cwd()
    config = ProfileLoader.load(cwd / "kptn.yaml")

    if profile is not None:
        resolved = ProfileResolver(config).compile(pipeline, profile)
    else:
        resolved = ResolvedGraph(
            graph=pipeline,
            pipeline=pipeline.name,
            storage_key=config.settings.db_path or ".kptn/kptn.db",
        )

    state_store = init_state_store(config.settings)
    execute(resolved, state_store, cwd=cwd)
