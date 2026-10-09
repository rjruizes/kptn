"""Which files a source reads, and which table each one feeds."""

from __future__ import annotations

import glob
import inspect
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kptn.exceptions import HashError
from kptn.sources.spec import SourceSpec

#: Extensions kptn reads without a ``read=`` callable. All are built into
#: DuckDB, so nothing is downloaded at run time (production may be offline).
READABLE_EXTENSIONS = frozenset({".csv", ".tsv", ".txt", ".json", ".jsonl", ".ndjson", ".parquet"})

_GLOB_CHARS = re.compile(r"[*?\[]")
_NOT_IDENTIFIER = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class SourceFile:
    """One matched file and the directory its pattern was rooted in."""

    path: Path
    base: Path

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def rel(self) -> str:
        """The ``_source_file`` value: base folder name plus the path below it.

        Independent of where the working directory sits, so every working
        directory builds identical files.
        """
        below = self.path.relative_to(self.base).as_posix()
        folder = self.base.name
        if folder in ("", ".", ".."):
            return below
        return f"{folder}/{below}"


def call_with_config(fn: Any, values: Mapping[str, Any]) -> Any:
    """Call *fn* with the config values its parameters name."""
    sig = inspect.signature(fn)
    params = sig.parameters
    if any(p.kind is p.VAR_KEYWORD for p in params.values()):
        return fn(**values)
    missing = [
        name
        for name, p in params.items()
        if p.default is p.empty
        and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        and name not in values
    ]
    if missing:
        raise HashError(
            f"files= needs {', '.join(missing)}, which kptn.config(...) does not provide."
        )
    return fn(**{name: values[name] for name in params if name in values})


def _patterns(spec: SourceSpec, config_values: Mapping[str, Any]) -> list[str]:
    raw = call_with_config(spec.files, config_values) if callable(spec.files) else spec.files
    if isinstance(raw, (str, Path)):
        return [str(raw)]
    return [str(p) for p in raw]


def _glob_base(pattern: str) -> Path:
    parts: list[str] = []
    for part in Path(pattern).parts:
        if _GLOB_CHARS.search(part):
            break
        parts.append(part)
    return Path(*parts) if parts else Path(".")


def _visible(path: Path) -> bool:
    return not path.name.startswith(".")


def _expand(pattern: str) -> list[SourceFile]:
    path = Path(pattern)
    if _GLOB_CHARS.search(pattern):
        base = _glob_base(pattern)
        matches = (Path(m) for m in glob.glob(pattern, recursive=True))
        return [SourceFile(m, base) for m in matches if m.is_file() and _visible(m)]
    if path.is_dir():
        return [SourceFile(c, path) for c in path.iterdir() if c.is_file() and _visible(c)]
    if path.is_file():
        return [SourceFile(path, path.parent)]
    return []


def resolve_files(name: str, spec: SourceSpec, config_values: Mapping[str, Any]) -> list[SourceFile]:
    """Every file *spec* reads, sorted by ``_source_file`` value.

    Raises:
        HashError: a pattern matched nothing, or a file has an extension kptn
            cannot read and the source has no ``read=``.
    """
    files: list[SourceFile] = []
    seen: set[Path] = set()
    for pattern in _patterns(spec, config_values):
        matched = _expand(pattern)
        if not matched:
            raise HashError(f"Source {name!r}: {pattern!r} matched no files.")
        for f in matched:
            key = f.path.resolve()
            if key not in seen:
                seen.add(key)
                files.append(f)
    if spec.read is None:
        for f in files:
            if f.path.suffix.lower() not in READABLE_EXTENSIONS:
                raise HashError(
                    f"Source {name!r}: kptn cannot read {f.path}. Pass read= for this "
                    "format, or narrow files= with a glob such as 'dir/*.csv'."
                )
    return sorted(files, key=lambda f: f.rel)


def normalize_table_name(raw: str) -> str:
    """Lowercase, with every run of characters other than letters and digits as ``_``."""
    name = _NOT_IDENTIFIER.sub("_", raw.lower()).strip("_")
    if not name:
        raise ValueError(f"{raw!r} does not make a usable table name.")
    return name


def table_name(spec: SourceSpec, file: SourceFile) -> str:
    """The table *file* feeds: ``table=`` if given, otherwise the file's stem."""
    if spec.table is None:
        raw = file.path.stem
    elif isinstance(spec.table, str):
        raw = spec.table
    else:
        raw = spec.table(file)
    if not isinstance(raw, str):
        raise TypeError(f"table= returned {type(raw).__name__} for {file.path}; expected a string.")
    return normalize_table_name(raw)


def group_by_table(spec: SourceSpec, files: list[SourceFile]) -> dict[str, list[SourceFile]]:
    """Files per table, tables in name order, files in ``_source_file`` order."""
    groups: dict[str, list[SourceFile]] = {}
    for f in files:
        groups.setdefault(table_name(spec, f), []).append(f)
    return {t: sorted(fs, key=lambda f: f.rel) for t, fs in sorted(groups.items())}
