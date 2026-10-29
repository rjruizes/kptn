"""Per-table work for a source: what to build, whether it is current, building it."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Collection, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

import duckdb

from kptn.change_detector.hasher import hash_file, hash_task_source
from kptn.exceptions import HashError, TaskError
from kptn.layout import BRONZE_DIR
from kptn.sources.checks import check_path, file_check_reason, run_checks
from kptn.sources.files import SourceFile, group_by_table, resolve_files
from kptn.sources.read import create_table_view, read_file_table, union_view
from kptn.sources.rejects import (
    REJECTED_DIR,
    RejectedFile,
    create_rejects_table,
    rejects_path,
)
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
    #: The record of files the source's ``check=`` left out, for a source
    #: with one; None without.
    rejects: Path | None = None

    @property
    def state_name(self) -> str:
        return f"{self.source}[{self.table}]"


def source_code_hash(spec: SourceSpec) -> str:
    """Hash of the source's own callables and the functions they call."""
    fns = (spec.files, spec.table, spec.read, spec.check)
    parts = [hash_task_source(fn) for fn in fns if callable(fn)]
    return _sha(*parts)


def plan_source(
    name: str,
    spec: SourceSpec,
    config_values: Mapping[str, Any],
    *,
    root: Path,
) -> tuple[list[TablePlan], list[SourceFile]]:
    """One plan per table the source's files feed, in table-name order, and
    the files a ``table=`` callable assigned to no table.

    Raises:
        HashError: a pattern matched nothing, a file is unreadable, or a file
            disappeared while it was being fingerprinted.
    """
    files = resolve_files(name, spec, config_values)
    tables, no_table = group_by_table(spec, files)
    fed = [f for table_files in tables.values() for f in table_files]
    stamps = dict(zip((f.path for f in fed), fingerprints(f.path for f in fed)))
    code = source_code_hash(spec)
    plans: list[TablePlan] = []
    for table, table_files in tables.items():
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
                rejects=rejects_path(root, name, table) if spec.check is not None else None,
            )
        )
    return plans, no_table


def plan_tables(
    name: str,
    spec: SourceSpec,
    config_values: Mapping[str, Any],
    *,
    root: Path,
) -> list[TablePlan]:
    """``plan_source``'s plans alone."""
    return plan_source(name, spec, config_values, root=root)[0]


def state_value(plan: TablePlan) -> str:
    """What is stored under ``plan.state_name`` once the table is built."""
    return _sha(plan.key, fingerprint(plan.parquet))


def is_current(plan: TablePlan, state_store: _ReadsHashes, storage_key: str, pipeline: str) -> bool:
    """True when the built parquet, and its record of rejected files if it
    keeps one, match the table's current inputs and code."""
    stored = state_store.read_hash(storage_key, pipeline, plan.state_name)
    if stored is None or not plan.parquet.exists():
        return False
    if plan.rejects is not None and not plan.rejects.exists():
        return False
    return stored == state_value(plan)


@dataclass(frozen=True)
class TableRead:
    view: str
    #: Whether a check file or ``check=`` ran.
    checked: bool
    #: Files ``check=`` left out, or that could not be read; empty without one.
    rejected: tuple[RejectedFile, ...] = ()


def _read_each_file(
    conn: duckdb.DuckDBPyConnection, plan: TablePlan, spec: SourceSpec, check: Callable[..., Any]
) -> tuple[str, list[RejectedFile]]:
    """Read and check each file on its own, and serve the files that pass."""
    kept: list[str] = []
    rejected: list[RejectedFile] = []
    for i, file in enumerate(plan.files):
        try:
            relation = read_file_table(conn, plan.source, plan.table, file, i, spec.read)
        except Exception as exc:  # anything a file's content makes DuckDB or read= raise
            rejected.append(RejectedFile(plan.table, file.rel, f"cannot be read: {exc}"))
            continue
        failure = file_check_reason(
            check, rows=conn.table(relation), file=file, table=plan.table
        )
        if failure is None:
            kept.append(f"SELECT * FROM {relation}")
        else:
            conn.execute(f"DROP TABLE {relation}")
            rejected.append(RejectedFile(plan.table, file.rel, failure))
    if not kept:
        first = rejected[0]
        raise TaskError(
            f"Every file of {plan.source}.{plan.table} was rejected; the first, "
            f"{first.file}: {first.reason}"
        )
    return union_view(conn, plan.source, plan.table, kept), rejected


def read_table(
    conn: duckdb.DuckDBPyConnection,
    plan: TablePlan,
    spec: SourceSpec,
    *,
    root: Path,
    checks: bool = True,
) -> TableRead:
    """Create ``raw_<source>."<table>"`` over the plan's files on *conn* and,
    with *checks*, validate it with its check file.

    A source's ``check=`` is part of reading it, like ``read=``, so it runs
    whatever *checks* says: each file is read and checked on its own, and
    the files that fail or cannot be read are left out of the view.

    Raises:
        TaskError: a check file failed, or ``check=`` rejected every file.
        TypeError: ``check=`` returned something other than None or a string.
    """
    rejected: list[RejectedFile] = []
    if spec.check is not None:
        view, rejected = _read_each_file(conn, plan, spec, spec.check)
    else:
        view = create_table_view(conn, plan.source, plan.table, list(plan.files), spec.read)
    checked = checks and run_checks(
        conn,
        source=plan.source,
        table=plan.table,
        root=root,
        declared_at=spec.declared_at,
        file_checked=spec.check is not None,
    )
    return TableRead(view, checked or spec.check is not None, tuple(rejected))


def build_table(
    plan: TablePlan,
    spec: SourceSpec,
    *,
    root: Path,
    dest: Path | None = None,
    rejects_dest: Path | None = None,
) -> bool:
    """Read, validate and atomically write one table to *dest*, by default
    ``plan.parquet``, and, for a source with ``check=``, its record of
    rejected files to *rejects_dest*, by default ``plan.rejects``. Returns
    whether checks ran.

    ``os.replace`` onto a symlink replaces the link, never its target, so a
    local build leaves a shared-bronze entry untouched.
    """
    dest = plan.parquet if dest is None else dest
    rejects_dest = plan.rejects if rejects_dest is None else rejects_dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmps = [dest.with_name(f".{dest.name}.{uuid4().hex}.tmp")]
    if rejects_dest is not None:
        rejects_dest.parent.mkdir(parents=True, exist_ok=True)
        tmps.append(rejects_dest.with_name(f".{rejects_dest.name}.{uuid4().hex}.tmp"))
    conn = duckdb.connect()
    try:
        read = read_table(conn, plan, spec, root=root)
        conn.execute(f"COPY (SELECT * FROM {read.view}) TO {_sql_path(tmps[0])} (FORMAT parquet)")
        if rejects_dest is not None:
            create_rejects_table(conn, "_kptn_rejected", list(read.rejected))
            conn.execute(f"COPY _kptn_rejected TO {_sql_path(tmps[1])} (FORMAT parquet)")
            os.replace(tmps[1], rejects_dest)
        os.replace(tmps[0], dest)
        return read.checked
    finally:
        conn.close()
        for tmp in tmps:
            tmp.unlink(missing_ok=True)


def _sql_path(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def remove_stale_tables(
    source: str, tables: Collection[str], *, root: Path, rejects: Collection[str] = ()
) -> list[Path]:
    """Delete ``.kptn/_bronze/<source>/*.parquet`` files for tables no longer
    fed, and records under ``_rejected/`` other than those named in *rejects*.

    Only removed tables are returned: a record holds no table data.
    """
    folder = root / BRONZE_DIR / source
    removed: list[Path] = []
    if folder.is_dir():
        for path in sorted(folder.glob("*.parquet")):
            if path.stem not in tables:
                path.unlink()
                removed.append(path)
        for path in sorted((folder / REJECTED_DIR).glob("*.parquet")):
            if path.stem not in rejects:
                path.unlink()
    return removed
