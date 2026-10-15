"""The shared cache: built source tables kept by content key and linked into ``_bronze/``.

Layout, under the root named by ``settings.shared_cache``::

    .tmp/<id>/                       a publish in progress, renamed into place when complete
    <source>/<table>/<key>/
        entry.json                   what built it, from which files; never read by kptn
        <table>.parquet

An entry is immutable and complete whenever it exists. Several Unix accounts
share one cache over NFS, so everything kptn creates here gets explicit
group-writable modes, and a failed rename is trusted only if the entry is then
missing (an NFS client can retry a rename whose reply was lost and report an
error although the first attempt succeeded).
"""

from __future__ import annotations

import getpass
import json
import os
import shutil
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from uuid import uuid4

import duckdb

from kptn.exceptions import ProfileError, TaskError
from kptn.sources.build import SOURCE_FORMAT_VERSION, TablePlan, build_table
from kptn.sources.spec import SourceSpec

#: ``off`` turns sharing off for a whole session; ``kptn run --no-shared-cache`` sets it.
SHARED_CACHE_ENV = "KPTN_SHARED_CACHE"

#: Explicit modes for everything kptn creates in the cache. Accounts in one
#: group share it, and the usual umask of 022 would leave entries unwritable
#: by the group; the setgid bit makes new entries inherit the cache's group.
DIR_MODE = 0o2775
FILE_MODE = 0o664

ENTRY_LABEL = "entry.json"
_TMP = ".tmp"

Outcome = Literal["published", "linked"]


@dataclass(frozen=True)
class SharedCache:
    root: Path

    def entry(self, plan: TablePlan) -> Path:
        """Where the build of *plan*'s table lives, whether or not it exists."""
        return self.root / plan.source / plan.table / plan.key

    def lookup(self, plan: TablePlan) -> Path | None:
        """The entry's parquet, or None when no build of this key is shared."""
        parquet = self.entry(plan) / f"{plan.table}.parquet"
        return parquet if parquet.is_file() else None

    def publish(self, plan: TablePlan, spec: SourceSpec, *, root: Path) -> tuple[Path, Outcome]:
        """Build *plan*'s table into the cache. Returns the entry's parquet and
        whether this call made the entry or found another's.

        A table that fails to build or validate publishes nothing.

        Raises:
            TaskError: the cache root's parent directory does not exist.
        """
        tmp = self._tmp_dir()
        try:
            parquet = tmp / f"{plan.table}.parquet"
            build_table(plan, spec, root=root, dest=parquet)
            label = tmp / ENTRY_LABEL
            label.write_text(json.dumps(_label(plan, parquet, root), indent=2) + "\n")
            for f in (parquet, label):
                os.chmod(f, FILE_MODE)
            entry = self.entry(plan)
            _mkdir(entry.parent.parent)
            _mkdir(entry.parent)
            try:
                os.rename(tmp, entry)
                outcome: Outcome = "published"
            except OSError:
                # Another account published first, or our rename succeeded and
                # its reply was lost. Only the entry's existence is trusted.
                if self.lookup(plan) is None:
                    raise
                outcome = "linked"
            return entry / f"{plan.table}.parquet", outcome
        finally:
            shutil.rmtree(tmp, ignore_errors=True)  # already gone after a rename

    def _tmp_dir(self) -> Path:
        try:
            _mkdir(self.root)
        except FileNotFoundError as exc:
            raise TaskError(
                f"shared cache {str(self.root)!r}: its parent directory does not exist. "
                "Check settings.shared_cache in kptn.yaml."
            ) from exc
        _mkdir(self.root / _TMP)
        tmp = self.root / _TMP / uuid4().hex
        _mkdir(tmp)
        return tmp


def _mkdir(path: Path) -> None:
    """Create one directory with ``DIR_MODE``, whatever the umask. A directory
    that already exists, perhaps another account's, is left as it is."""
    try:
        path.mkdir()
    except FileExistsError:
        return
    os.chmod(path, DIR_MODE)


def link(parquet: Path, target: Path) -> None:
    """Make *parquet* an absolute symlink to *target*, replacing whatever is there."""
    parquet.parent.mkdir(parents=True, exist_ok=True)
    tmp = parquet.with_name(f".{parquet.name}.{uuid4().hex}.link")
    os.symlink(target, tmp)
    try:
        os.replace(tmp, parquet)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _whoami() -> str:
    try:
        return getpass.getuser()
    except (OSError, KeyError):  # no passwd entry, as in some containers
        return str(os.getuid())


def _label(plan: TablePlan, parquet: Path, root: Path) -> dict[str, object]:
    """``entry.json``: which files and code built this entry, and who built it."""
    path = "'" + str(parquet).replace("'", "''") + "'"
    conn = duckdb.connect()
    try:
        row = conn.execute(f"SELECT count(*) FROM read_parquet({path})").fetchone()
        described = conn.execute(f"DESCRIBE SELECT * FROM read_parquet({path})").fetchall()
    finally:
        conn.close()
    from kptn import __version__

    files: list[dict[str, object]] = []
    for f, stamp in zip(plan.files, plan.stamps):
        size, mtime_ns = stamp.split(":")
        files.append({"path": f.rel, "size": int(size), "mtime_ns": int(mtime_ns)})
    check = plan.check_file
    return {
        "key": plan.key,
        "source": plan.source,
        "table": plan.table,
        "input_hash": plan.input_hash,
        "code_hash": plan.code_hash,
        "format_version": SOURCE_FORMAT_VERSION,
        "files": files,
        "check_file": check.relative_to(root).as_posix() if check.exists() else None,
        "rows": row[0] if row else 0,
        "columns": [r[0] for r in described],
        "built_by": _whoami(),
        "host": socket.gethostname(),
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "kptn_version": __version__,
    }


def resolve_shared_cache(
    setting: str | None,
    project_root: Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> SharedCache | None:
    """The cache *setting* names, or None when sharing is off.

    A relative *setting* resolves against *project_root*, the directory
    holding ``kptn.yaml``.

    Raises:
        ProfileError: ``KPTN_SHARED_CACHE`` is set to something other than
            ``on`` or ``off``.
    """
    raw = (os.environ if environ is None else environ).get(SHARED_CACHE_ENV, "")
    value = raw.strip().lower()
    if value not in ("", "on", "off"):
        raise ProfileError(f"{SHARED_CACHE_ENV}={raw!r}: expected 'on' or 'off'")
    if setting is None or value == "off":
        return None
    root = Path(setting).expanduser()
    if not root.is_absolute():
        root = project_root / root
    return SharedCache(root.resolve())


def shared_cache_for(project_root: Path) -> SharedCache | None:
    """The shared cache of the project at *project_root*, from its ``kptn.yaml``."""
    from kptn.profiles.loader import ProfileLoader

    settings = ProfileLoader.load(project_root / "kptn.yaml").settings
    return resolve_shared_cache(settings.shared_cache, project_root)
