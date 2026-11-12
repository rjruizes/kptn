"""The shared bronze: built source tables kept by content key and linked into ``.kptn/_bronze/``.

Layout, under the root named by ``settings.shared_bronze``::

    .tmp/<id>/                       a publish in progress, renamed into place when complete
    <source>/<table>/<key>/
        entry.json                   what built it, from which files; never read by kptn
        <table>.parquet
        _rejected.parquet            the files check= left out, for a source with one
        _warnings.parquet            the checks that warned, for a table with a check file
        _columns.parquet             each file's columns, for a table with a check file

An entry is immutable and complete whenever it exists. Several Unix accounts
share one directory over NFS, so everything kptn creates here gets explicit
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

#: ``off`` turns sharing off for a whole session; ``kptn run --no-shared-bronze`` sets it.
SHARED_BRONZE_ENV = "KPTN_SHARED_BRONZE"

#: Explicit modes for everything kptn creates in the shared bronze. Accounts in one
#: group share it, and the usual umask of 022 would leave entries unwritable
#: by the group; the setgid bit makes new entries inherit the directory's group.
DIR_MODE = 0o2775
FILE_MODE = 0o664

ENTRY_LABEL = "entry.json"
ENTRY_REJECTS = "_rejected.parquet"
ENTRY_WARNINGS = "_warnings.parquet"
ENTRY_COLUMNS = "_columns.parquet"
_TMP = ".tmp"

Outcome = Literal["published", "linked"]


@dataclass(frozen=True)
class SharedBronze:
    root: Path

    def entry(self, plan: TablePlan) -> Path:
        """Where the build of *plan*'s table lives, whether or not it exists."""
        return self.root / plan.source / plan.table / plan.key

    def lookup(self, plan: TablePlan) -> Path | None:
        """The entry's parquet, or None when no build of this key is shared."""
        parquet = self.entry(plan) / f"{plan.table}.parquet"
        if not all(shared.is_file() for _, shared in self.records(plan)):
            return None
        return parquet if parquet.is_file() else None

    def rejects(self, plan: TablePlan) -> Path:
        """The entry's record of rejected files, for a source with ``check=``."""
        return self.entry(plan) / ENTRY_REJECTS

    def warnings(self, plan: TablePlan) -> Path:
        """The entry's record of the checks that warned, for a checked table."""
        return self.entry(plan) / ENTRY_WARNINGS

    def columns(self, plan: TablePlan) -> Path:
        """The entry's record of each file's columns, for a checked table."""
        return self.entry(plan) / ENTRY_COLUMNS

    def records(self, plan: TablePlan) -> list[tuple[Path, Path]]:
        """Each record *plan* keeps, as (its local path, its place in the entry)."""
        pairs = (
            (plan.rejects, self.rejects(plan)),
            (plan.warnings, self.warnings(plan)),
            (plan.columns, self.columns(plan)),
        )
        return [(local, shared) for local, shared in pairs if local is not None]

    def publish(self, plan: TablePlan, spec: SourceSpec, *, root: Path) -> tuple[Path, Outcome]:
        """Build *plan*'s table into the shared bronze. Returns the entry's parquet and
        whether this call made the entry or found another's.

        A table that fails to build or validate publishes nothing.

        Raises:
            TaskError: the shared bronze directory no longer exists.
        """
        tmp = self._tmp_dir()
        try:
            parquet = tmp / f"{plan.table}.parquet"
            rejects = tmp / ENTRY_REJECTS if plan.rejects is not None else None
            records = [tmp / shared.name for _, shared in self.records(plan)]
            build_table(
                plan,
                spec,
                root=root,
                dest=parquet,
                rejects_dest=tmp / ENTRY_REJECTS,
                warnings_dest=tmp / ENTRY_WARNINGS,
                columns_dest=tmp / ENTRY_COLUMNS,
            )
            label = tmp / ENTRY_LABEL
            label.write_text(json.dumps(_label(plan, spec, parquet, rejects, root), indent=2) + "\n")
            for f in (parquet, label, *records):
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
        # The root is never created here: whoever sets it up gives it the
        # pipeline group, and its existence is what turns sharing on.
        try:
            _mkdir(self.root / _TMP)
        except FileNotFoundError as exc:
            raise TaskError(
                f"shared bronze {str(self.root)!r} no longer exists. It was there "
                "when the run started; was it removed or unmounted?"
            ) from exc
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


def _qualname(fn: object) -> str | None:
    if fn is None:
        return None
    module = getattr(fn, "__module__", None)
    name = getattr(fn, "__qualname__", None) or repr(fn)
    return f"{module}.{name}" if module else name


def _label(
    plan: TablePlan, spec: SourceSpec, parquet: Path, rejects: Path | None, root: Path
) -> dict[str, object]:
    """``entry.json``: which files and code built this entry, and who built it."""
    path = "'" + str(parquet).replace("'", "''") + "'"
    conn = duckdb.connect()
    try:
        row = conn.execute(f"SELECT count(*) FROM read_parquet({path})").fetchone()
        described = conn.execute(f"DESCRIBE SELECT * FROM read_parquet({path})").fetchall()
        rejected: list[str] = []
        if rejects is not None:
            rpath = "'" + str(rejects).replace("'", "''") + "'"
            rejected = [
                r[0]
                for r in conn.execute(f"SELECT _source_file FROM read_parquet({rpath})").fetchall()
            ]
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
        "check": _qualname(spec.check),
        "rejected": rejected,
        "rows": row[0] if row else 0,
        "columns": [r[0] for r in described],
        "built_by": _whoami(),
        "host": socket.gethostname(),
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "kptn_version": __version__,
    }


def resolve_shared_bronze(
    setting: str | None,
    project_root: Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> SharedBronze | None:
    """The shared bronze *setting* names, or None when sharing is off.

    A relative *setting* resolves against *project_root*, the directory
    holding ``kptn.yaml``. Sharing is on only where that directory exists:
    one ``kptn.yaml`` serves a server whose shared bronze was set up and a
    laptop where it never is.

    Raises:
        ProfileError: ``KPTN_SHARED_BRONZE`` is set to something other than
            ``on`` or ``off``.
    """
    raw = (os.environ if environ is None else environ).get(SHARED_BRONZE_ENV, "")
    value = raw.strip().lower()
    if value not in ("", "on", "off"):
        raise ProfileError(f"{SHARED_BRONZE_ENV}={raw!r}: expected 'on' or 'off'")
    if setting is None or value == "off":
        return None
    root = _named_root(setting, project_root)
    return SharedBronze(root) if root.is_dir() else None


def missing_shared_bronze(
    setting: str | None,
    project_root: Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """What to tell a run whose ``kptn.yaml`` names a shared bronze that does
    not exist, or None when there is nothing to say: no setting, sharing
    turned off on purpose, or the directory is there."""
    value = (os.environ if environ is None else environ).get(SHARED_BRONZE_ENV, "")
    if setting is None or value.strip().lower() == "off":
        return None
    if _named_root(setting, project_root).is_dir():
        return None
    return f"[SHARE] off — {setting} does not exist, so tables are built here"


def _named_root(setting: str, project_root: Path) -> Path:
    root = Path(setting).expanduser()
    if not root.is_absolute():
        root = project_root / root
    return root.resolve()


def shared_bronze_for(project_root: Path) -> SharedBronze | None:
    """The shared bronze of the project at *project_root*, from its ``kptn.yaml``."""
    from kptn.profiles.loader import ProfileLoader

    settings = ProfileLoader.load(project_root / "kptn.yaml").settings
    return resolve_shared_bronze(settings.shared_bronze, project_root)
