from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import ParseError


class SqlLineageError(Exception):
    """Raised when SQL lineage metadata cannot be derived."""


@dataclass
class TableMetadata:
    """Metadata describing a table that originates from a SQL task."""

    task_name: str
    file_path: Path
    table_key: str
    display_name: str
    columns: list[str]
    source_tables: list[str]
    column_sources: dict[str, list[str]]


@dataclass
class SqlAnalysisResult:
    """Result of analyzing a SQL statement."""

    columns: list[str]
    referenced_tables: set[str]
    column_sources: dict[str, list[str]]


class SqlLineageAnalyzer:
    """Builds task/table dependency information using sqlglot."""

    def __init__(self, config: dict, project_root: Path, dialect: Optional[str] = None):
        self._config = config
        self._project_root = Path(project_root)
        self._dialect = dialect or "duckdb"
        self._table_index: dict[str, TableMetadata] = {}
        self._display_name_by_key: dict[str, str] = {}

    def build(self) -> dict[str, TableMetadata]:
        """Parse SQL tasks from kptn.yaml into table metadata."""

        self._table_index = {}
        self._display_name_by_key = {}

        tasks = self._config.get("tasks", {})
        for task_name, task_spec in tasks.items():
            spec_dict = task_spec if isinstance(task_spec, dict) else {}
            file_entry = spec_dict.get("file")
            outputs = [str(output) for output in spec_dict.get("outputs") or []]
            if not file_entry or not outputs:
                continue

            file_path = self._resolve_file(file_entry)
            if not file_path or file_path.suffix.lower() != ".sql":
                continue

            if not file_path.exists():
                raise FileNotFoundError(
                    f"SQL file {file_path} referenced by task '{task_name}' does not exist"
                )

            sql_text = file_path.read_text(encoding="utf-8")
            statements = self._parse_sql_statements(sql_text, file_path)
            statement_index = self._index_statements_by_output(statements)

            for output in outputs:
                table_display = self._output_identifier(output)
                table_key = self._normalize_table(table_display)
                analysis = self._analyze_sql(statements, statement_index, table_key)
                metadata = TableMetadata(
                    task_name=task_name,
                    file_path=file_path,
                    table_key=table_key,
                    display_name=table_display,
                    columns=list(analysis.columns),
                    source_tables=sorted(analysis.referenced_tables),
                    column_sources={
                        column: sorted(sources)
                        for column, sources in analysis.column_sources.items()
                    },
                )
                self._table_index[table_key] = metadata
                self._display_name_by_key[table_key] = table_display

        return self._table_index

    def tables(self) -> dict[str, TableMetadata]:
        if not self._table_index:
            self.build()
        return self._table_index

    def dependency_tree(self) -> dict[str, list[str]]:
        """Return a dependency mapping of table -> upstream tables."""

        tree: dict[str, list[str]] = {}
        for metadata in self.tables().values():
            display_dependencies = [
                self._display_name_by_key.get(dep, dep) for dep in metadata.source_tables
            ]
            tree[metadata.display_name] = display_dependencies
        return tree

    def describe_table(self, table_name: str) -> TableMetadata:
        """Retrieve metadata for a table by name."""

        table_key = self._normalize_table(table_name)
        metadata = self.tables().get(table_key)
        if not metadata:
            available = ", ".join(sorted(self.tables()))
            raise KeyError(
                f"Table '{table_name}' not found in lineage metadata. "
                f"Known tables: {available}"
            )
        return metadata

    def list_columns(self, table_name: str) -> list[str]:
        """Return the ordered list of columns for the supplied table."""

        metadata = self.describe_table(table_name)
        return metadata.columns

    def depends_on(self, table_name: str) -> list[str]:
        """Return the list of upstream tables for the provided table."""

        metadata = self.describe_table(table_name)
        return [
            self._display_name_by_key.get(dep, dep) for dep in metadata.source_tables
        ]

    def _resolve_file(self, file_entry: str) -> Optional[Path]:
        file_part = file_entry.split(":", 1)[0]
        path = Path(file_part)
        if not path.is_absolute():
            path = (self._project_root / path).resolve()
        return path

    def _parse_sql_statements(self, sql_text: str, file_path: Path) -> list[exp.Expression]:
        try:
            expressions = sqlglot.parse(sql_text, read=self._dialect)
        except ParseError as exc:  # pragma: no cover - parse errors should surface in tests
            raise SqlLineageError(
                f"Failed to parse SQL in {file_path} using dialect '{self._dialect}'"
            ) from exc
        return expressions

    def _analyze_sql(
        self,
        statements: list[exp.Expression],
        statement_index: dict[str, list[exp.Expression]],
        target_table_key: str,
    ) -> SqlAnalysisResult:
        statement = self._select_statement(statements, statement_index, target_table_key)
        if not statement:
            return SqlAnalysisResult(columns=[], referenced_tables=set())

        select_expr = self._find_primary_select(statement)
        if not select_expr:
            return SqlAnalysisResult(columns=[], referenced_tables=set())

        columns = self._extract_select_columns(select_expr)
        upstream_tables = self._collect_referenced_tables(select_expr)
        column_sources = self._column_lineage(select_expr)

        return SqlAnalysisResult(
            columns=columns,
            referenced_tables=upstream_tables,
            column_sources=column_sources,
        )

    def _select_statement(
        self,
        statements: list[exp.Expression],
        statement_index: dict[str, list[exp.Expression]],
        target_table_key: str,
    ) -> Optional[exp.Expression]:
        if target_table_key and target_table_key in statement_index:
            return statement_index[target_table_key][-1]

        for expression in reversed(statements):
            if self._find_primary_select(expression):
                return expression
        return None

    def _find_primary_select(self, expression: exp.Expression) -> Optional[exp.Select]:
        if isinstance(expression, exp.Create):
            inner = expression.expression
            if isinstance(inner, exp.Select):
                return inner
        if isinstance(expression, exp.Insert):
            inner = expression.expression
            if isinstance(inner, exp.Select):
                return inner
        if isinstance(expression, exp.Select):
            return expression
        return expression.find(exp.Select)

    def _extract_select_columns(self, select_expr: exp.Select) -> list[str]:
        columns: list[str] = []
        for projection in select_expr.expressions or []:
            if isinstance(projection, exp.Star):
                columns.extend(self._expand_star(select_expr, projection))
                continue

            alias = projection.alias_or_name
            if alias:
                columns.append(alias)
                continue

            columns.append(projection.sql(dialect=self._dialect))

        return columns

    def _expand_star(self, select_expr: exp.Select, star_expr: exp.Star) -> list[str]:
        alias_identifier = star_expr.this
        from_clause = select_expr.args.get("from") or select_expr.args.get("from_")
        source_expr = getattr(from_clause, "this", None)

        # If the FROM clause defines explicit column names (e.g., VALUES ... AS t(col1, col2))
        alias_expr = getattr(source_expr, "args", {}).get("alias") if source_expr else None
        if alias_expr and alias_expr.args.get("columns"):
            return [identifier.name for identifier in alias_expr.args["columns"]]

        # Handle qualified stars such as table.*
        if alias_identifier:
            qualifier = alias_identifier.sql(dialect=self._dialect)
            return [f"{qualifier}.*"]

        return ["*"]

    def _table_sql_name(self, table_expr: exp.Table) -> str:
        name_expr = table_expr.this
        name = ""
        if hasattr(name_expr, "sql"):
            name = name_expr.sql(dialect=self._dialect)
        elif name_expr is not None:
            name = str(name_expr)

        db_expr = table_expr.args.get("db")
        if db_expr:
            db_name = db_expr.sql(dialect=self._dialect)
            return f"{db_name}.{name}"
        return name

    def _collect_referenced_tables(self, select_expr: exp.Select) -> set[str]:
        table_names: set[str] = set()
        cte_names = self._cte_names(select_expr)
        for table in select_expr.find_all(exp.Table):
            normalized = self._normalize_table(self._table_sql_name(table))
            if not normalized or normalized in cte_names:
                continue
            table_names.add(normalized)
        return table_names

    def _column_lineage(self, select_expr: exp.Select) -> dict[str, list[str]]:
        cte_map = self._cte_definitions(select_expr)
        cache: dict[str, dict[str, list[str]]] = {}
        return self._column_lineage_internal(
            select_expr=select_expr,
            cte_map=cte_map,
            lineage_cache=cache,
            visited=set(),
        )

    def _column_lineage_internal(
        self,
        select_expr: exp.Select,
        cte_map: dict[str, exp.Expression],
        lineage_cache: dict[str, dict[str, list[str]]],
        visited: set[str],
    ) -> dict[str, list[str]]:
        alias_map = self._select_source_aliases(select_expr)
        column_lineage: dict[str, list[str]] = {}

        for projection in select_expr.expressions or []:
            if isinstance(projection, exp.Star):
                star_lineage = self._star_lineage(
                    select_expr,
                    projection,
                    alias_map,
                    cte_map,
                    lineage_cache,
                    visited,
                )
                column_lineage.update(star_lineage)
                continue

            column_name = projection.alias_or_name or projection.sql(dialect=self._dialect)
            sources = self._expression_sources(
                projection,
                select_expr,
                alias_map,
                cte_map,
                lineage_cache,
                visited,
            )
            column_lineage[column_name] = sorted(sources)

        return column_lineage

    def _expression_sources(
        self,
        expression: exp.Expression,
        select_expr: exp.Select,
        alias_map: dict[str, str],
        cte_map: dict[str, exp.Expression],
        lineage_cache: dict[str, dict[str, list[str]]],
        visited: set[str],
    ) -> set[str]:
        sources: set[str] = set()
        for column in expression.find_all(exp.Column):
            sources.update(
                self._resolve_column_sources(
                    column,
                    select_expr,
                    alias_map,
                    cte_map,
                    lineage_cache,
                    visited,
                )
            )
        return sources

    def _resolve_column_sources(
        self,
        column: exp.Column,
        select_expr: exp.Select,
        alias_map: dict[str, str],
        cte_map: dict[str, exp.Expression],
        lineage_cache: dict[str, dict[str, list[str]]],
        visited: set[str],
    ) -> set[str]:
        qualifier_raw = column.table or ""
        qualifier_key = self._normalize_identifier(qualifier_raw) if qualifier_raw else ""

        if not qualifier_key:
            if len(alias_map) == 1:
                qualifier_key = next(iter(alias_map))
            else:
                qualifier_key = ""

        alias_target = alias_map.get(qualifier_key)
        alias_target_norm = self._normalize_identifier(alias_target) if alias_target else ""

        if qualifier_key and qualifier_key in cte_map:
            if qualifier_key in visited:
                return {f"{alias_target or qualifier_raw}.{column.name}"}
            cte_lineage = lineage_cache.get(qualifier_key)
            if cte_lineage is None:
                visited.add(qualifier_key)
                cte_lineage = self._column_lineage_internal(
                    cte_map[qualifier_key],
                    cte_map,
                    lineage_cache,
                    visited,
                )
                visited.remove(qualifier_key)
                lineage_cache[qualifier_key] = cte_lineage
            return set(cte_lineage.get(column.name, [f"{qualifier_raw}.{column.name}"]))

        if alias_target_norm and alias_target_norm in cte_map:
            if alias_target_norm in visited:
                return {f"{alias_target}.{column.name}"}
            cte_lineage = lineage_cache.get(alias_target_norm)
            if cte_lineage is None:
                visited.add(alias_target_norm)
                cte_lineage = self._column_lineage_internal(
                    cte_map[alias_target_norm],
                    cte_map,
                    lineage_cache,
                    visited,
                )
                visited.remove(alias_target_norm)
                lineage_cache[alias_target_norm] = cte_lineage
            return set(cte_lineage.get(column.name, [f"{alias_target}.{column.name}"]))

        source_name = alias_target or qualifier_raw
        if source_name and column.name:
            return {f"{source_name}.{column.name}"}
        if column.name:
            return {column.name}
        return {column.sql(dialect=self._dialect)}

    def _star_lineage(
        self,
        select_expr: exp.Select,
        star_expr: exp.Star,
        alias_map: dict[str, str],
        cte_map: dict[str, exp.Expression],
        lineage_cache: dict[str, dict[str, list[str]]],
        visited: set[str],
    ) -> dict[str, list[str]]:
        alias_identifier = star_expr.this
        from_clause = select_expr.args.get("from") or select_expr.args.get("from_")
        source_expr = getattr(from_clause, "this", None)

        column_lineage: dict[str, list[str]] = {}
        if alias_identifier:
            alias_key = self._normalize_identifier(alias_identifier.sql(dialect=self._dialect))
            source_name = alias_map.get(alias_key, alias_identifier.sql(dialect=self._dialect))
            column_lineage[f"{source_name}.*"] = [source_name]
            return column_lineage

        alias_expr = getattr(source_expr, "args", {}).get("alias") if source_expr else None
        alias_columns = (alias_expr.args.get("columns") if alias_expr else None) or []
        source_name = alias_expr.alias_or_name if alias_expr else ""
        lineage_source = source_name or alias_map.get(self._normalize_identifier(source_name), source_name)

        for identifier in alias_columns:
            column_lineage[identifier.name] = [f"{lineage_source}.{identifier.name}"]

        if not column_lineage:
            column_lineage["*"] = ["*"]

        return column_lineage

    def _select_source_aliases(self, select_expr: exp.Select) -> dict[str, str]:
        aliases: dict[str, str] = {}
        from_clause = select_expr.args.get("from") or select_expr.args.get("from_")
        if not from_clause:
            return aliases

        def add_source(table_expr: exp.Expression) -> None:
            display = ""
            if isinstance(table_expr, exp.Table):
                display = self._table_sql_name(table_expr)
            elif hasattr(table_expr, "sql"):
                display = table_expr.sql(dialect=self._dialect)

            alias = table_expr.alias_or_name if hasattr(table_expr, "alias_or_name") else None
            if alias:
                key = self._normalize_identifier(alias)
                aliases[key] = display or alias
            elif display:
                aliases[self._normalize_identifier(display)] = display

        source = from_clause.this
        if source is not None:
            add_source(source)

        for join in from_clause.args.get("joins") or []:
            add_source(join.this)

        for join in select_expr.args.get("joins") or []:
            add_source(join.this)

        return aliases

    def _cte_names(self, select_expr: exp.Select) -> set[str]:
        names: set[str] = set()
        with_clause = select_expr.args.get("with") or select_expr.args.get("with_")
        if not with_clause:
            return names

        for cte in with_clause.expressions or []:
            alias = cte.alias_or_name
            if not alias:
                continue
            names.add(self._normalize_identifier(alias))
        return names

    def _cte_definitions(self, select_expr: exp.Select) -> dict[str, exp.Expression]:
        definitions: dict[str, exp.Expression] = {}
        with_clause = select_expr.args.get("with") or select_expr.args.get("with_")
        if not with_clause:
            return definitions

        for cte in with_clause.expressions or []:
            alias = cte.alias_or_name
            if not alias:
                continue
            definitions[self._normalize_identifier(alias)] = cte.this

        return definitions

    def _index_statements_by_output(
        self, statements: list[exp.Expression]
    ) -> dict[str, list[exp.Expression]]:
        index: dict[str, list[exp.Expression]] = {}
        for expression in statements:
            target_key = self._statement_output_key(expression)
            if not target_key:
                continue
            index.setdefault(target_key, []).append(expression)
        return index

    def _statement_output_key(self, expression: exp.Expression) -> Optional[str]:
        target_table: Optional[exp.Table] = None
        if isinstance(expression, exp.Create):
            if isinstance(expression.this, exp.Table):
                target_table = expression.this
        elif isinstance(expression, exp.Insert):
            if isinstance(expression.this, exp.Table):
                target_table = expression.this

        if not target_table:
            return None
        return self._normalize_table(self._table_sql_name(target_table))

    @staticmethod
    def _output_identifier(output: str) -> str:
        if "://" in output:
            output = output.split("://", 1)[1]
        return output.lstrip("/")

    @staticmethod
    def _normalize_table(name: str) -> str:
        value = name.strip()
        if not value:
            return ""
        if "." in value:
            value = value.split(".")[-1]
        return value.strip('"').lower()

    @staticmethod
    def _normalize_identifier(name: str) -> str:
        return name.strip().strip('"').lower() if name else ""
