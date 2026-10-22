"""Where kptn keeps what it makes in a working directory.

Everything goes under ``.kptn/``, in two directories:

``.kptn/state/``
    Bookkeeping: the task-state database (``kptn.db``, unless ``kptn.yaml``
    names a ``db_path``), the UI's run history (``ui.db``) and its run files
    (``runs/``, unless ``settings.shared_logs`` moves them).
``.kptn/_bronze/``
    The tables sources build, ``<source>/<table>.parquet``.

Earlier versions kept the bookkeeping loose in ``.kptn/`` and the tables in
``_bronze/`` at the top of the working directory. :func:`migrate_layout`
moves them, and every entry point that opens one of these paths calls it
first.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from kptn.exceptions import ProfileError
from kptn.profiles.loader import ProfileLoader

KPTN_DIR = Path(".kptn")

#: kptn's bookkeeping, relative to the working directory.
STATE_DIR = KPTN_DIR / "state"

#: Where built tables live, relative to the working directory.
BRONZE_DIR = KPTN_DIR / "_bronze"

#: The task-state database when ``kptn.yaml`` names no ``db_path``.
DEFAULT_DB_PATH = (STATE_DIR / "kptn.db").as_posix()

#: The key task state is stored under when ``kptn.yaml`` names no
#: ``db_path``. It was the database's path before ``.kptn/state/`` and stays
#: what it was, so moving the file keeps every task's cache -- state kept in
#: a pipeline's own DuckDB database included, which no migration touches.
DEFAULT_STORAGE_KEY = ".kptn/kptn.db"

#: The UI's run history and its run files, relative to the working directory.
UI_DATABASE = STATE_DIR / "ui.db"
RUNS_DIR = STATE_DIR / "runs"

#: Where an earlier kptn kept them.
LEGACY_BRONZE_DIR = Path("_bronze")
LEGACY_RUNS_DIR = KPTN_DIR / "runs"

_LEGACY_DB_NAME = "kptn.db"
_UI_DB_NAME = "ui.db"


def migrate_layout(project_root: Path) -> list[str]:
    """Move an earlier kptn's files in *project_root* to where they go now.

    ``.kptn/ui.db``, ``.kptn/runs/`` and -- when ``kptn.yaml`` names no
    ``db_path`` -- ``.kptn/kptn.db`` move into ``.kptn/state/``, each with
    its sidecar files, and ``_bronze/`` moves to ``.kptn/_bronze/``. Nothing
    is replaced: an old copy whose new place is taken stays where it is, and
    is named in a notice. Once moved, a call checks a few paths and returns.

    Returns one notice per move or copy left behind, for the caller to print.
    """
    root = Path(project_root).resolve()
    kptn_dir = root / KPTN_DIR
    state = root / STATE_DIR
    notices: list[str] = []

    moved: set[str] = set()
    if kptn_dir.is_dir():
        entries = sorted(kptn_dir.iterdir())
        names = [_UI_DB_NAME, LEGACY_RUNS_DIR.name]
        if (kptn_dir / _LEGACY_DB_NAME).exists() and _db_path_is_default(root):
            names.append(_LEGACY_DB_NAME)
        for name in names:
            # A database's sidecars go only where it went: a write-ahead log
            # beside some other database would be replayed into it.
            if not _move(kptn_dir / name, state / name, root, notices):
                continue
            moved.add(name)
            for entry in entries:
                if entry.name != name and _belongs_to(entry.name, name):
                    _move(entry, state / entry.name, root, notices)

    _move(root / LEGACY_BRONZE_DIR, root / BRONZE_DIR, root, notices)

    if LEGACY_RUNS_DIR.name in moved:
        _repoint_run_files(state / _UI_DB_NAME, root)
    return notices


def _db_path_is_default(root: Path) -> bool:
    """Whether ``kptn.yaml`` leaves the task-state database at its default.

    One that does not load says nothing about where the database is, so its
    database stays where it is; the run reports the error itself.
    """
    try:
        return ProfileLoader.load(root / "kptn.yaml").settings.db_path is None
    except (ProfileError, OSError):
        return False


def _belongs_to(entry: str, name: str) -> bool:
    """*entry* is *name* or one of its sidecars (``-wal``, ``-shm``, ``.wal``)."""
    return entry == name or entry.startswith((f"{name}-", f"{name}."))


def _move(source: Path, target: Path, root: Path, notices: list[str]) -> bool:
    """Rename *source* to *target* if *target* is free; whether it moved."""
    if not source.exists() and not source.is_symlink():
        return False
    slash = "/" if source.is_dir() else ""
    old = f"{source.relative_to(root).as_posix()}{slash}"
    new = f"{target.relative_to(root).as_posix()}{slash}"
    if target.exists() or target.is_symlink():
        notices.append(f"[LAYOUT] {old} is no longer used: kptn uses {new}. Delete {old}.")
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(source, target)
    except FileNotFoundError:
        return False  # another process moved it first
    except OSError as exc:
        notices.append(f"[LAYOUT] could not move {old} to {new}: {exc.strerror or exc}")
        return False
    notices.append(f"[LAYOUT] moved {old} to {new}")
    return True


def _repoint_run_files(database: Path, root: Path) -> None:
    """Point the run history's recorded log paths at the moved run files.

    ``ui.db`` records each run's file by absolute path, and only files that
    were in ``.kptn/runs/`` moved; a shared logs directory's stay put.
    """
    if not database.exists():
        return
    old = f"{root / LEGACY_RUNS_DIR}{os.sep}"
    new = f"{root / RUNS_DIR}{os.sep}"
    conn = sqlite3.connect(database)
    try:
        with conn:
            conn.execute(
                "UPDATE runs SET log_path = ? || substr(log_path, ?) "
                "WHERE substr(log_path, 1, ?) = ?",
                (new, len(old) + 1, len(old), old),
            )
    except sqlite3.Error:
        pass  # a database without runs yet, or not a run store at all
    finally:
        conn.close()


__all__ = [
    "BRONZE_DIR",
    "DEFAULT_DB_PATH",
    "DEFAULT_STORAGE_KEY",
    "KPTN_DIR",
    "LEGACY_BRONZE_DIR",
    "LEGACY_RUNS_DIR",
    "RUNS_DIR",
    "STATE_DIR",
    "UI_DATABASE",
    "migrate_layout",
]
