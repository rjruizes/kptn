from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from kptn.graph.nodes import ConfigNode
from kptn.graph.pipeline import Pipeline
from kptn.profiles.loader import ProfileLoader
from kptn.profiles.resolved import ResolvedGraph
from kptn.profiles.resolver import ProfileResolver
from kptn.runner.executor import execute
from kptn.state_store.factory import init_state_store

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


def _find_duckdb_factory(pipeline: Pipeline):
    """Return ``(factory, alias)`` if the pipeline declares ``kptn.config(duckdb=...)``.

    *factory* is the callable; *alias* is the kwarg name tasks use (e.g. ``"engine"``).
    Returns ``(None, None)`` if no duckdb config node exists.
    """
    for node in pipeline.nodes:
        if isinstance(node, ConfigNode) and "duckdb" in node.spec:
            val = node.spec["duckdb"]
            if isinstance(val, tuple):
                factory, alias = val
            else:
                factory, alias = val, "duckdb"
            return factory, alias
    return None, None


def run(
    pipeline: Pipeline,
    *,
    profile: str | None = None,
    keep_db_open: bool = False,
) -> "duckdb.DuckDBPyConnection | None":
    """Run a pipeline.

    Parameters
    ----------
    pipeline:
        The pipeline to execute.
    profile:
        Optional profile name to resolve from ``kptn.yaml``.
    keep_db_open:
        When ``True`` and the pipeline declares ``kptn.config(duckdb=get_engine)``,
        the DuckDB connection is left open after the run and returned to the caller.
        Useful in test fixtures::

            @pytest.fixture(scope="module")
            def engine():
                return main_pipeline.run(keep_db_open=True)

        When ``False`` (default) the connection is closed before returning.

    Returns
    -------
    The live ``duckdb.DuckDBPyConnection`` when ``keep_db_open=True`` and a duckdb
    factory was declared; ``None`` otherwise.
    """
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

    duckdb_factory, duckdb_alias = _find_duckdb_factory(pipeline)
    state_store = init_state_store(config.settings, duckdb_factory=duckdb_factory)

    return execute(
        resolved,
        state_store,
        cwd=cwd,
        duckdb_factory=duckdb_factory,
        duckdb_alias=duckdb_alias,
        keep_db_open=keep_db_open,
    )
