"""The one loaded project.

``kptn.project._prepare_project_imports`` inserts a project root at the front
of ``sys.path`` and purges from ``sys.modules`` every module belonging to any
project root it has ever been given. It is built on the premise that one
process serves one project at a time, and that premise is load-bearing here:
the working directories this UI offers are checkouts of the *same*
repository, so two of them define a module of the same name.

Load the second and the first is evicted. The first project's
:class:`~kptn.graph.pipeline.Pipeline` is left holding task callables bound to
modules that no longer exist in ``sys.modules``, and the next request that
uses it runs code from the wrong checkout. Nothing raises.

Hence this: at most one :class:`~kptn_server.project.ProjectContext` in the
process, behind a lock, reloaded on every switch. The obvious alternative --
a FastAPI sub-application mounted per project, each with its own context --
is exactly the bug above, and it is recorded here so it is not reinvented.

A ``threading.Lock`` and not an asyncio one: the routes that need a pipeline
are ``def``, so FastAPI runs them in a threadpool, and the state being
guarded (``sys.modules``, ``sys.path``, and the working directory that
:mod:`kptn_server.service` chdirs into) is process-global.

What this costs: two plan pages in two tabs, in different projects, reload on
every alternation. Everything that needs only the run store stays outside the
lock and stays concurrent -- which is the run history, every run page, and
every SSE stream.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from kptn_server.project import ProjectContext
from kptn_server.registry import ProjectEntry


class ProjectSlot:
    """Holds at most one loaded project, and serialises access to it."""

    def __init__(self, preloaded: ProjectContext | None = None) -> None:
        # Single-project mode loads the project at app-construction time so a
        # misconfigured directory fails the launcher rather than every page.
        # Handing that context over here keeps the fail-fast load and spares
        # the first request a second one.
        self._lock = threading.Lock()
        self._context = preloaded

    @property
    def loaded_root(self) -> Path | None:
        """The root currently loaded, or ``None``. For tests and diagnostics."""
        return self._context.root if self._context is not None else None

    @contextmanager
    def use(self, entry: ProjectEntry) -> Iterator[ProjectContext]:
        """Hold the slot for *entry*, loading it if another project is in it.

        The lock is held for the whole body, not just the load: the caller is
        about to use a ``Pipeline`` whose modules a concurrent switch would
        pull out from under it, and -- in the lineage routes -- to chdir.

        The body also runs **in the project's directory**, and the load does
        too. Every other way kptn runs pipeline code starts there -- the CLI
        because the reader ``cd``s into the project, run workers because they
        are spawned with ``cwd=project_root`` -- so a pipeline may reasonably
        resolve a path relative to it, at import time or later. nph-curation's
        does: its setup module installs ``soda`` from ``../../packages``, and
        from the Jupyter server's ``/home/jovyan`` that is ``/packages``, so the
        import failed with "No module named 'soda'". Single-project mode never
        hit this because ``kptn ui`` was launched from the project. The working
        directory is process-global state of exactly the kind this lock
        already guards, so it is set and restored here, under the lock.

        Raises :class:`~kptn_server.project.ProjectError` if the project
        cannot be loaded, leaving the slot empty rather than holding a
        half-loaded context that a later request would treat as current. The
        working directory is restored on every exit, including that one.
        """
        with self._lock:
            previous = os.getcwd()
            os.chdir(entry.root)
            try:
                if self._context is None or self._context.root != entry.root:
                    self._context = None
                    self._context = ProjectContext.load(entry.root)
                yield self._context
            finally:
                os.chdir(previous)


__all__ = ["ProjectSlot"]
