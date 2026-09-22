"""Which projects this server may offer, and which one a slug names.

The UI is reached through jupyter-server-proxy, from a Jupyter server whose
working directory is not a kptn project. So the project cannot come from
``Path.cwd()`` any more: it is chosen, by the person, from the working
directories they have on a shared release folder.

    <projects-root>/r4/rruizesparza_main/pyproject.toml
                    ^^  ^^^^^^^^^^^^^^^^
                    release        working directory

Nothing here imports project code. Discovery reads ``pyproject.toml`` and
``kptn.yaml`` as text, which is what makes it safe to run on every page load
-- and what keeps it clear of the one-project-at-a-time rule that governs
:mod:`kptn_server.slot`. See the design note in that module before adding
anything to this one.

The registry is also the security boundary. A slug arriving in a URL is
looked up in the scanned dictionary and never joined onto a path, so no
traversal is expressible and nothing outside the chosen release is reachable.
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

from kptn.exceptions import ProfileError
from kptn.profiles.loader import ProfileLoader
from kptn_server.project import (
    PROFILE_CONFIG_FILENAME,
    RUN_LOG_RELATIVE_DIR,
    UI_DATABASE_RELATIVE_PATH,
)

_MANIFEST_FILENAME = "pyproject.toml"

_DIGITS = re.compile(r"(\d+)")


def natural_key(name: str) -> tuple[object, ...]:
    """Sort key that orders ``r2`` before ``r10``.

    Releases are named ``r<n>``, and the latest is picked by sorting them.
    Lexicographic order puts ``r10`` before ``r2``, which would pin the UI to
    an old release the first time the count reaches double digits -- a bug
    that would not appear for months and would look like anything but a
    sorting problem.
    """
    return tuple(
        int(part) if part.isdigit() else part for part in _DIGITS.split(name)
    )


def default_user() -> str:
    """Whose working directories to offer, absent an explicit ``--user``.

    ``JUPYTERHUB_USER`` is what the single-user server is given; ``USER`` is
    the fallback for a plain shell. An empty answer is not an error here: it
    yields an empty scan, and the project list says what it searched for.
    """
    return os.environ.get("JUPYTERHUB_USER") or os.environ.get("USER") or ""


@dataclass(frozen=True)
class ProjectEntry:
    """A project this server may serve, described without loading it.

    Everything here is readable from the filesystem as text. In particular
    ``display_name`` exists so the app bar can name a project on a page that
    has no business loading a pipeline -- the run history, above all.
    ``profiles`` comes from ``kptn.yaml`` through the same loader the CLI
    uses, so the selector cannot disagree with ``kptn run``.

    ``error`` set means the directory looks like one of this person's
    projects but cannot be served. It is listed with the reason rather than
    hidden: a working directory that silently vanishes from the list is a
    worse bug report than one that says what is wrong with it.
    """

    slug: str
    root: Path
    release: str
    display_name: str
    profiles: tuple[str, ...]
    database_path: Path
    run_log_dir: Path
    error: str | None = None


class ProjectRegistry:
    """The scanned set of projects, and the lookup from slug to project."""

    def __init__(self, projects_root: Path, user: str) -> None:
        self.projects_root = Path(projects_root)
        self.user = user
        self.release: str | None = None
        self._entries: dict[str, ProjectEntry] = {}

    def scan(self) -> tuple[ProjectEntry, ...]:
        """Re-read the filesystem and return this person's projects."""
        self.release = None
        self._entries = {}

        for release in sorted(self._release_names(), key=natural_key, reverse=True):
            entries = self._projects_in(release)
            if entries:
                self.release = release
                self._entries = {entry.slug: entry for entry in entries}
                break

        return self.entries()

    def entries(self) -> tuple[ProjectEntry, ...]:
        """The last scan's projects, in directory-name order."""
        return tuple(self._entries[slug] for slug in sorted(self._entries))

    def resolve(self, slug: str) -> ProjectEntry | None:
        """The servable project named by *slug*, or ``None``.

        A miss rescans once before giving up, so a checkout made after the
        server started is reachable by following a link to it rather than
        only after a restart.

        A slug is a dictionary key, never a path component. That is the whole
        of the traversal defence, and it is why this returns ``None`` instead
        of raising something a caller might be tempted to turn into a path.
        """
        entry = self._entries.get(slug)
        if entry is None:
            self.scan()
            entry = self._entries.get(slug)
        if entry is None or entry.error is not None:
            return None
        return entry

    def _release_names(self) -> list[str]:
        try:
            return [child.name for child in self.projects_root.iterdir() if child.is_dir()]
        except OSError:
            # A root that does not exist, or cannot be read, is an empty
            # scan. The project list reports it; refusing to start would
            # leave nothing to read the report in.
            return []

    def _projects_in(self, release: str) -> list[ProjectEntry]:
        release_dir = self.projects_root / release
        try:
            children = sorted(release_dir.iterdir())
        except OSError:
            return []

        found: list[ProjectEntry] = []
        for child in children:
            if not child.is_dir() or not self._is_mine(child.name):
                continue
            entry = self._describe(child, release)
            if entry is not None:
                found.append(entry)
        return found

    def _is_mine(self, name: str) -> bool:
        """Exactly my name, or my name and an underscore -- not a prefix.

        ``rruizesparza2_main`` belongs to somebody else, and a bare prefix
        test would hand it over.
        """
        return bool(self.user) and (
            name == self.user or name.startswith(f"{self.user}_")
        )

    def _describe(self, workdir: Path, release: str) -> ProjectEntry | None:
        """Describe *workdir*, or ``None`` if it is not a kptn project at all.

        "Not a project" (no manifest, no ``[tool.kptn]``) is invisible: a
        notes directory beside a checkout is not a broken project. "A project
        that cannot be read" is listed with its error.
        """
        manifest = workdir / _MANIFEST_FILENAME
        if not manifest.is_file():
            return None

        root = workdir.resolve()
        error: str | None = None
        profiles: tuple[str, ...] = ()

        try:
            with open(manifest, "rb") as handle:
                config = tomllib.load(handle)
        except tomllib.TOMLDecodeError as exc:
            error = f"Invalid pyproject.toml: {exc}"
        except OSError as exc:
            error = f"Could not read pyproject.toml: {exc}"
        else:
            if not config.get("tool", {}).get("kptn", {}).get("pipeline"):
                return None

        if error is None:
            try:
                profiles = tuple(
                    ProfileLoader.load(root / PROFILE_CONFIG_FILENAME).profiles
                )
            except ProfileError as exc:
                error = str(exc)

        return ProjectEntry(
            slug=workdir.name,
            root=root,
            release=release,
            display_name=workdir.name,
            profiles=profiles,
            database_path=root / UI_DATABASE_RELATIVE_PATH,
            run_log_dir=root / RUN_LOG_RELATIVE_DIR,
            error=error,
        )


__all__ = ["ProjectEntry", "ProjectRegistry", "default_user", "natural_key"]
