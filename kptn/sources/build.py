"""Per-table work for a source: what to build, whether it is current, building it."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

import duckdb

from kptn.change_detector.hasher import hash_file, hash_task_source
from kptn.exceptions import HashError
from kptn.layout import BRONZE_DIR
from kptn.sources.checks import check_path, run_checks
from kptn.sources.files import SourceFile, group_by_table, resolve_files
from kptn.sources.read import create_table_view
from kptn.sources.spec import SourceSpec

#: Bumped whenever a reading convention changes what a source writes, so every
#: table rebuilds. The pipeline's code hash cannot see kptn's own conventions.
SOURCE_FORMAT_VERSION = "2"

#: Metadata lookups on a network share wait on round trips, not the CPU, so
#: they overlap well. 17,000 files at ~2 ms each is 30+ s one at a time.
_STAT_WORKERS = 32


class _ReadsHashes(Protocol):
    def read_hash(self, storage_key: str, pipeline: str, task: str) -> str | None: ...


def _sha(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def fingerprint(path: Path) -> str:
    """``size:mtime_ns`` of *path*, following symlinks. No content is read.

    Raises:
        HashError: the file is missing or unreadable.
    """
    try:
        st = Path(path).stat()
    except OSError as exc:
        raise HashError(f"Cannot stat {str(path)!r}: {exc}") from exc
    return f"{st.st_size}:{st.st_mtime_ns}"


def fingerprints(paths: Iterable[Path]) -> list[str]:
    """``fingerprint`` for many files, looked up concurrently, in input order."""
    paths = list(paths)
    if len(paths) < 2:
        return [fingerprint(p) for p in paths]
    with ThreadPoolExecutor(max_workers=min(_STAT_WORKERS, len(paths))) as pool:
        return list(pool.map(fingerprint, paths))


@dataclass(frozen=True)
class TablePlan:
    source: str
    table: str
    files: tuple[SourceFile, ...]
    check_file: Path
    input_hash: str
    key: str
    parquet: Path
    #: The ``source_code_hash`` folded into ``key``, kept for ``entry.json``.
    code_hash: str
    #: ``size:mtime_ns`` of each file in ``files``, in the same order.
    stamps: tuple[str, ...]

    @property
    def state_name(self) -> str:
        return f"{self.source}[{self.table}]"


def source_code_hash(spec: SourceSpec) -> str:
    """Hash of the source's own callables and the functions they call."""
    parts = [hash_task_source(fn) for fn in (spec.files, spec.table, spec.read) if callable(fn)]
    return _sha(*parts)


def plan_tables(
    name: str,
    spec: SourceSpec,
    config_values: Mapping[str, Any],
    *,
    root: Path,
) -> list[TablePlan]:
    """One plan per table the source's files feed, in table-name order.

    Raises:
        HashError: a pattern matched nothing, a file is unreadable, or a file
            disappeared while it was being fingerprinted.
    """
    files = resolve_files(name, spec, config_values)
    stamps = dict(zip((f.path for f in files), fingerprints(f.path for f in files)))
    code = source_code_hash(spec)
    plans: list[TablePlan] = []
    for table, table_files in group_by_table(spec, files).items():
        check = check_path(root, name, table)
        parts = [f"{f.rel}:{stamps[f.path]}" for f in table_files]
        parts.append(f"check:{hash_file(str(check)) if check.exists() else 'none'}")
        input_hash = _sha(*parts)
        plans.append(
            TablePlan(
                source=name,
                table=table,
                files=tuple(table_files),
                check_file=check,
                input_hash=input_hash,
                key=_sha(name, table, input_hash, code, SOURCE_FORMAT_VERSION),
                parquet=root / BRONZE_DIR / name / f"{table}.parquet",
                code_hash=code,
                stamps=tuple(stamps[f.path] for f in table_files),
            )
        )
    return plans


def state_value(plan: TablePlan) -> str:
    """What is stored under ``plan.state_name`` once the table is built."""
    return _sha(plan.key, fingerprint(plan.parquet))


def is_current(plan: TablePlan, state_store: _ReadsHashes, storage_key: str, pipeline: str) -> bool:
    """True when the built parquet matches the table's current inputs and code."""
    stored = state_store.read_hash(storage_key, pipeline, plan.state_name)
    if stored is None or not plan.parquet.exists():
        return False
    return stored == state_value(plan)


def read_table(
    conn: duckdb.DuckDBPyConnection,
    plan: TablePlan,
    spec: SourceSpec,
    *,
    root: Path,
    checks: bool = True,
) -> tuple[str, bool]:
    """Create ``raw_<source>."<table>"`` over the plan's files on *conn* and,
    with *checks*, validate it. Returns the view and whether checks ran.
    """
    view = create_table_view(conn, plan.source, plan.table, list(plan.files), spec.read)
    checked = checks and run_checks(
        conn, source=plan.source, table=plan.table, root=root, declared_at=spec.declared_at
    )
    return view, checked


def build_table(plan: TablePlan, spec: SourceSpec, *, root: Path, dest: Path | None = None) -> bool:
    """Read, validate and atomically write one table to *dest*, by default
    ``plan.parquet``. Returns whether checks ran.

    ``os.replace`` onto a symlink replaces the link, never its target, so a
    local build leaves a shared-bronze entry untouched.
    """
    dest = plan.parquet if dest is None else dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{uuid4().hex}.tmp")
    conn = duckdb.connect()
    try:
        view, checked = read_table(conn, plan, spec, root=root)
        target = "'" + str(tmp).replace("'", "''") + "'"
        conn.execute(f"COPY (SELECT * FROM {view}) TO {target} (FORMAT parquet)")
        os.replace(tmp, dest)
        return checked
    finally:
        conn.close()
        tmp.unlink(missing_ok=True)


def remove_stale_tables(source: str, tables: set[str], *, root: Path) -> list[Path]:
    """Delete ``.kptn/_bronze/<source>/*.parquet`` files for tables no longer fed."""
    folder = root / BRONZE_DIR / source
    removed: list[Path] = []
    if folder.is_dir():
        for path in sorted(folder.glob("*.parquet")):
            if path.stem not in tables:
                path.unlink()
                removed.append(path)
    return removed
