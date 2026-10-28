"""``kptn.source``: a declaration, not execution.

A source names raw input files. kptn reads them by convention into one
text-only parquet file per table under ``.kptn/_bronze/<source>/`` and serves each
as the view ``raw_<source>.<table>``; see ``kptn.sources.build`` for the work
and the executor for when it runs.
"""

from __future__ import annotations

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

_SOURCE_NAME = re.compile(r"[a-z][a-z0-9_]*")
_FROM_CONFIG = "(computed from config)"


def _describe_files(files: FilesArg) -> tuple[str, ...]:
    if callable(files):
        return (_FROM_CONFIG,)
    if isinstance(files, str):
        return (files,)
    return tuple(str(f) for f in files)


@dataclass
class SourceSpec:
    files: FilesArg
    table: TableArg | None = None
    read: ReadFn | None = None
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
) -> SourceHandle:
    """Declare raw input files that kptn reads into ``.kptn/_bronze/<name>/``.

    Only *name* and *files* are required; everything else follows conventions.
    Files are read by extension (CSV, TSV, JSON, Parquet), every value is stored
    as text, column names are lowercased, each row gets a
    ``_source_file`` column, and each table is validated by
    ``checks/<name>/<table>.yml`` when that file exists. With
    ``checks/<name>/<table>.file.yml``, each file is validated on its own
    first, and files that fail are left out and listed in
    ``raw_<name>._rejected``.

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
    caller = sys._getframe(1)
    spec = SourceSpec(
        files=files,
        table=table,
        read=read,
        declared_at=(caller.f_code.co_filename, caller.f_lineno),
        description=f"Reads raw files into .kptn/_bronze/{name}/ and serves them as raw_{name}.<table>.",
    )
    return SourceHandle(name, spec)
