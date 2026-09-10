"""Discovery of the kptn project the UI is serving.

The web UI serves exactly one project: the directory the developer launched it
from. :class:`ProjectContext` is the read-only answer to "which project, which
pipeline, which profiles, and where does its UI state live" -- resolved once,
at app-construction time, so a request handler never has to re-derive it.

Two invariants are worth stating plainly, because everything above this module
depends on them:

*Canonical root.* The run store enforces one active run per canonical project
path and locks on ``Path(root).resolve()``. A context holding an unresolved
root would let ``project`` and ``project/../project`` look like two different
projects, defeating that lock, so the root is resolved here once and reused.

*State stays under ``.kptn/``.* UI state is exactly ``.kptn/ui.db`` (run
history) and ``.kptn/runs/`` (captured worker output). Nothing about the UI
writes anywhere else in the project.

Loading goes through the shared CLI loader (:func:`kptn.project.load_pipeline`)
and :class:`kptn.profiles.loader.ProfileLoader` rather than reimplementing
discovery, so the UI can never disagree with ``kptn run`` about what the
project's pipeline and profiles are.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from kptn.exceptions import KptnError, ProfileError, ProjectConfigError
from kptn.graph.pipeline import Pipeline
from kptn.profiles.loader import ProfileLoader
from kptn.profiles.schema import KptnConfig
from kptn.project import load_pipeline

#: Project-relative location of the durable run store.
UI_DATABASE_RELATIVE_PATH = Path(".kptn") / "ui.db"

#: Project-relative directory holding one captured log per run.
RUN_LOG_RELATIVE_DIR = Path(".kptn") / "runs"

#: Project-relative profile configuration, optional.
PROFILE_CONFIG_FILENAME = "kptn.yaml"

_PROJECT_MANIFEST_FILENAME = "pyproject.toml"


class ProjectError(KptnError):
    """Raised when a directory is not a usable kptn project.

    Deliberately distinct from :class:`kptn.exceptions.ProjectConfigError`:
    the UI has to be able to answer "this folder cannot be served, and here is
    why" for a missing manifest, a malformed manifest, no ``[tool.kptn]``
    pipeline entry, an unimportable pipeline module, a pipeline module that
    raises while being imported, and a broken profile file alike. Callers get
    one exception type to handle and the underlying cause on ``__cause__``.
    """


@dataclass(frozen=True)
class ProjectContext:
    """Everything the UI needs to know about the project it is serving."""

    root: Path
    pipeline_name: str
    profiles: tuple[str, ...]
    database_path: Path
    run_log_dir: Path
    # The loaded pipeline and its profile configuration travel with the
    # context so routers (the plan walkthrough in particular) can inspect the
    # graph without reloading the project on every request. Excluded from
    # equality: a Pipeline is a graph object, not a value, and two contexts
    # for the same root describe the same project regardless.
    pipeline: Pipeline = field(compare=False, repr=False)
    config: KptnConfig = field(compare=False, repr=False)

    @classmethod
    def load(cls, root: Path) -> ProjectContext:
        """Resolve *root* into a served project, or raise :class:`ProjectError`."""
        canonical_root = Path(root).resolve()

        manifest = canonical_root / _PROJECT_MANIFEST_FILENAME
        if not manifest.is_file():
            raise ProjectError(
                f"{canonical_root} is not a kptn project: no "
                f"{_PROJECT_MANIFEST_FILENAME} found. Launch the UI from a "
                f"directory containing a {_PROJECT_MANIFEST_FILENAME} with a "
                "[tool.kptn] pipeline entry."
            )

        try:
            pipeline = load_pipeline(canonical_root)
        except ProjectConfigError as exc:
            # The shapes the shared loader names itself: a missing or
            # malformed pyproject.toml, no [tool.kptn] pipeline, an
            # unimportable module, or the wrong attribute type. Its messages
            # already tell the developer what to fix.
            raise ProjectError(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 - see below
            # Anything the project's own pipeline module raises while being
            # *executed* on import: a SyntaxError (which is not an
            # ImportError, so it travels straight through
            # importlib.import_module), a NameError, a bad constant, a step
            # defined against something that does not exist. This is the most
            # common project-authoring mistake, and it must still arrive as a
            # ProjectError -- that is what create_app documents and the only
            # thing `kptn ui` knows how to report as a clean message rather
            # than a traceback.
            raise ProjectError(
                f"Could not load the pipeline for {canonical_root}: "
                f"{type(exc).__name__}: {exc}. Fix the error in the project's "
                "pipeline module and reload."
            ) from exc

        config_path = canonical_root / PROFILE_CONFIG_FILENAME
        try:
            # ProfileLoader returns empty defaults for a missing file, which
            # is a legitimate project shape: profiles are optional.
            config = ProfileLoader.load(config_path)
        except ProfileError as exc:
            raise ProjectError(str(exc)) from exc

        return cls(
            root=canonical_root,
            pipeline_name=pipeline.name,
            # Declaration order, not sorted: the selector should read the way
            # the author wrote kptn.yaml.
            profiles=tuple(config.profiles),
            database_path=canonical_root / UI_DATABASE_RELATIVE_PATH,
            run_log_dir=canonical_root / RUN_LOG_RELATIVE_DIR,
            pipeline=pipeline,
            config=config,
        )


__all__ = [
    "PROFILE_CONFIG_FILENAME",
    "RUN_LOG_RELATIVE_DIR",
    "UI_DATABASE_RELATIVE_PATH",
    "ProjectContext",
    "ProjectError",
]
