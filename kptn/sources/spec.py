"""``kptn.source``: a declaration, not execution.

A source names raw input files. kptn reads them by convention into one
text-only parquet file per table under ``.kptn/_bronze/<source>/`` and serves each
as the view ``raw_<source>.<table>``; see ``kptn.sources.build`` for the work
and the executor for when it runs.
"""

from __future__ import annotations

import inspect
import re
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Union

from kptn.graph.decorators import DocMetadata

if TYPE_CHECKING:
    from kptn.graph.graph import Graph

#: Paths, directories or globs; or a callable taking config values by name.
FilesArg = Union[str, list[str], tuple[str, ...], Callable[..., Any]]
#: A table name for every file, or a callable mapping one file to its table
#: (None for a file that feeds no table).
TableArg = Union[str, Callable[[Any], Union[str, None]]]
#: Reads one file path into a pandas DataFrame, pyarrow Table or DuckDB relation.
ReadFn = Callable[[str], Any]
#: Checks one file, taking any of ``rows``, ``file`` and ``table`` by name;
#: returns None to keep it or the reason to leave it out.
CheckFn = Callable[..., Union[str, None]]

#: The parameters a ``check=`` callable may take, passed by name.
CHECK_PARAMS = ("rows", "file", "table")

_SOURCE_NAME = re.compile(r"[a-z][a-z0-9_]*")
_FROM_CONFIG = "(computed from config)"


def _describe_files(files: FilesArg) -> tuple[str, ...]:
    if callable(files):
        return (_FROM_CONFIG,)
    if isinstance(files, str):
        return (files,)
    return tuple(str(f) for f in files)


def _check_signature(name: str, check: Callable[..., Any]) -> None:
    try:
        params = inspect.signature(check).parameters.values()
    except (TypeError, ValueError):
        return  # a builtin without a signature; it fails when called, if at all
    for p in params:
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD) or p.default is not p.empty:
            continue
        if p.kind is p.POSITIONAL_ONLY or p.name not in CHECK_PARAMS:
            raise TypeError(
                f"kptn.source {name!r}: check= takes its arguments by name, from "
                f"{', '.join(CHECK_PARAMS)}; it cannot take {p.name!r}."
            )


@dataclass
class SourceSpec:
    files: FilesArg
    table: TableArg | None = None
    read: ReadFn | None = None
    check: CheckFn | None = None
    declared_at: tuple[str, int] | None = None
    description: str | None = None
    # Attributes every kptn spec carries. The resolver, ``requires`` handling,
    # checkpointing and inspection read them from any node's spec.
    outputs: list[str] = field(default_factory=list)
    optional: str | None = None
    requires: list[Any] | None = None
    duckdb_checkpoint: bool = False
    docs: str | None = None
    doc: DocMetadata = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.doc = DocMetadata(self.description, _describe_files(self.files), self.docs)


class SourceHandle:
    """What ``kptn.source`` returns: composable with ``>>``."""

    def __init__(self, name: str, spec: SourceSpec) -> None:
        self.__name__ = name
        self.__kptn__ = spec

    def __rshift__(self, other: Any) -> Graph:
        from kptn.graph.graph import Graph

        return Graph._from_node(self) >> other

    def __repr__(self) -> str:
        return f"<kptn source '{self.__name__}'>"


def source(
    name: str,
    files: FilesArg,
    *,
    table: TableArg | None = None,
    read: ReadFn | None = None,
    check: CheckFn | None = None,
) -> SourceHandle:
    """Declare raw input files that kptn reads into ``.kptn/_bronze/<name>/``.

    Only *name* and *files* are required; everything else follows conventions.
    Files are read by extension (CSV, TSV, JSON, Parquet), every value is stored
    as text, column names are lowercased, each row gets a
    ``_source_file`` column, and each table is validated by
    ``checks/<name>/<table>.yml`` when that file exists. With *check*, each
    file is checked on its own first, and files that fail are left out and
    listed in ``raw_<name>._rejected``.

    When ``kptn.config`` names a DuckDB connection, each run also serves every
    table as the view ``raw_<name>.<table>``, whether or not it was rebuilt.
    Place the source right before the tasks that read those views, and after
    any task that rebuilds your database schemas. A source a profile leaves
    out does not run, so after a schema rebuild it has no views; deselect the
    tasks that read them with it.

    Args:
        name: The source's name: its node name, its folder under ``.kptn/_bronze/``
            and its view prefix. Lowercase letters, digits and underscores.
        files: Files, directories, or globs, or a callable receiving config
            values by name (like a task's parameters) and returning them. A
            directory means every visible file directly inside it.
        table: Which table each file feeds: a callable taking the file, or one
            name for every file. Defaults to the file's stem. A callable may
            return None for a file that feeds no table.
        read: For formats kptn does not read itself, such as Excel or R data: a
            callable taking a path and returning a pandas DataFrame, pyarrow
            Table, or DuckDB relation.
        check: Checks each file before it joins its table. Takes, by name, any
            of ``rows`` (a DuckDB relation over the file's rows as the table
            stores them), ``file`` (the file, with ``.path`` and ``.name``) and
            ``table``. Returns None to keep the file, or a string saying why
            to leave it out; ``False`` is not a reason and fails the run.
            A check that raises leaves the file out with the error as its
            reason. Its code is part of the cache key, so it should depend
            on the file alone, not on other files it reads.
    """
    if not isinstance(name, str) or not _SOURCE_NAME.fullmatch(name):
        raise ValueError(
            f"kptn.source name {name!r} must be lowercase letters, digits and "
            "underscores, starting with a letter."
        )
    if not (callable(files) or isinstance(files, (str, list, tuple))):
        raise TypeError("kptn.source files= must be a path, a list of paths, or a callable.")
    if isinstance(files, (list, tuple)) and not files:
        raise ValueError(f"kptn.source {name!r}: files= must not be empty.")
    if table is not None and not (isinstance(table, str) or callable(table)):
        raise TypeError("kptn.source table= must be a string or a callable.")
    if read is not None and not callable(read):
        raise TypeError("kptn.source read= must be a callable.")
    if check is not None:
        if not callable(check):
            raise TypeError("kptn.source check= must be a callable.")
        _check_signature(name, check)
    caller = sys._getframe(1)
    spec = SourceSpec(
        files=files,
        table=table,
        read=read,
        check=check,
        declared_at=(caller.f_code.co_filename, caller.f_lineno),
        description=f"Reads raw files into .kptn/_bronze/{name}/ and serves them as raw_{name}.<table>.",
    )
    return SourceHandle(name, spec)
