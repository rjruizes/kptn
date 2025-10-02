import ast
import glob
import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Iterable

from kapten.caching.r_imports import get_file_list, hash_r_files
from kapten.util.hash import hash_file, hash_obj
from kapten.util.logger import get_logger
from kapten.util.read_tasks_config import read_tasks_configs

if TYPE_CHECKING:  # pragma: no cover - only for static typing
    import duckdb

logger = get_logger()
var_pattern = re.compile(r"\$\{([a-zA-Z0-9\-_\.]+)\}")

DUCKDB_OUTPUT_PREFIX = "duckdb://"
DUCKDB_EMPTY_SENTINEL = "duckdb-empty-table"
DUCKDB_EMPTY_HASH = hashlib.md5(DUCKDB_EMPTY_SENTINEL.encode()).hexdigest()


@dataclass(frozen=True)
class FunctionRef:
    module: str
    name: str
    file_path: Path

    @property
    def qualname(self) -> str:
        if self.module:
            return f"{self.module}.{self.name}"
        return self.name


class ModuleSummary:
    def __init__(self, file_path: Path, module_name: str, source: str, tree: ast.AST):
        self.file_path = file_path
        self.module_name = module_name
        self._package_parts = module_name.split(".")[:-1] if module_name else []
        self.source = source
        self.tree = tree
        self.functions: dict[str, ast.AST] = {}
        self.module_aliases: dict[str, str] = {}
        self.symbol_aliases: dict[str, tuple[str, str]] = {}
        self._index()

    def _index(self) -> None:
        for node in self.tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.functions[node.name] = node
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "*":  # pragma: no cover - invalid syntax, defensive
                        continue
                    bound_name = alias.asname or alias.name.split(".")[0]
                    self.module_aliases[bound_name] = alias.name
            elif isinstance(node, ast.ImportFrom):
                if node.names and any(alias.name == "*" for alias in node.names):
                    continue
                module_path = self._resolve_absolute_module(node.module, node.level)
                if module_path is None:
                    continue
                for alias in node.names:
                    bound_name = alias.asname or alias.name
                    self.symbol_aliases[bound_name] = (module_path, alias.name)

    def has_function(self, name: str) -> bool:
        return name in self.functions

    def get_function(self, name: str) -> ast.AST | None:
        return self.functions.get(name)

    def iter_call_targets(self, node: ast.AST) -> Iterable[tuple[str, object]]:
        stack = [node]
        root_ids = {id(node)}
        while stack:
            current = stack.pop()
            if id(current) not in root_ids and isinstance(
                current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            ):
                continue
            for child in ast.iter_child_nodes(current):
                stack.append(child)
            if isinstance(current, ast.Call):
                func = current.func
                if isinstance(func, ast.Name):
                    yield ("name", func.id)
                elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                    yield ("attr", (func.value.id, func.attr))

    def _resolve_absolute_module(self, module: str | None, level: int) -> str | None:
        if level == 0:
            return module
        if level - 1 > len(self._package_parts):
            return None
        base_parts = self._package_parts[: len(self._package_parts) - (level - 1)]
        extra_parts: list[str] = []
        if module:
            extra_parts = [part for part in module.split(".") if part]
        parts = base_parts + extra_parts
        if not parts:
            return None
        return ".".join(parts)


class PythonFunctionAnalyzer:
    def __init__(self, py_dirs: list[str] | None = None):
        self.root_dirs = [Path(d) for d in (py_dirs or [])]
        self._module_cache: dict[Path, ModuleSummary] = {}
        self._module_cache_by_name: dict[str, ModuleSummary] = {}

    def build_function_hashes(self, file_path: Path, function_name: str) -> list[dict[str, str]]:
        summary = self._load_module_from_path(file_path)
        if summary is None:
            raise FileNotFoundError(f"Unable to parse module at {file_path}")
        if not summary.has_function(function_name):
            raise KeyError(f"Function '{function_name}' not found in {file_path}")
        start = FunctionRef(summary.module_name, function_name, summary.file_path)
        closure = self._collect_closure(start)
        digests: list[dict[str, str]] = []
        for ref in sorted(closure, key=lambda r: (r.qualname, str(r.file_path))):
            source = self._get_function_source(ref)
            if source is None:
                raise ValueError(f"Unable to extract source for {ref.qualname}")
            digest = hashlib.sha1(source.encode()).hexdigest()
            digests.append({"function": ref.qualname, "hash": digest})
        return digests

    def _collect_closure(self, seed: FunctionRef) -> set[FunctionRef]:
        visited: set[FunctionRef] = set()
        stack: list[FunctionRef] = [seed]
        while stack:
            ref = stack.pop()
            if ref in visited:
                continue
            visited.add(ref)
            summary = self._load_module_from_path(ref.file_path)
            if summary is None:
                continue
            node = summary.get_function(ref.name)
            if node is None:
                continue
            for kind, payload in summary.iter_call_targets(node):
                dep = self._resolve_call_target(summary, kind, payload)
                if dep and dep not in visited:
                    stack.append(dep)
        return visited

    def _resolve_call_target(self, summary: ModuleSummary, kind: str, payload: object) -> FunctionRef | None:
        if kind == "name":
            name = payload  # type: ignore[assignment]
            if not isinstance(name, str):
                return None
            if summary.has_function(name):
                return FunctionRef(summary.module_name, name, summary.file_path)
            symbol = summary.symbol_aliases.get(name)
            if symbol:
                module_name, original = symbol
                module_summary = self._load_module_by_name(module_name)
                if module_summary and module_summary.has_function(original):
                    return FunctionRef(module_summary.module_name, original, module_summary.file_path)
            return None
        if kind == "attr":
            if not isinstance(payload, tuple) or len(payload) != 2:
                return None
            base, attr = payload
            if not isinstance(base, str) or not isinstance(attr, str):
                return None
            module_name = summary.module_aliases.get(base)
            if not module_name:
                symbol = summary.symbol_aliases.get(base)
                if symbol:
                    module_name = symbol[0]
            if not module_name:
                return None
            module_summary = self._load_module_by_name(module_name)
            if module_summary and module_summary.has_function(attr):
                return FunctionRef(module_summary.module_name, attr, module_summary.file_path)
        return None

    def _get_function_source(self, ref: FunctionRef) -> str | None:
        summary = self._load_module_from_path(ref.file_path)
        if summary is None:
            return None
        node = summary.get_function(ref.name)
        if node is None:
            return None
        segment = ast.get_source_segment(summary.source, node)
        if segment is not None:
            return segment
        if not hasattr(node, "lineno") or not hasattr(node, "end_lineno"):
            return None
        lines = summary.source.splitlines(keepends=True)
        start = getattr(node, "lineno", None)
        end = getattr(node, "end_lineno", None)
        if start is None or end is None:
            return None
        return "".join(lines[start - 1 : end])

    def _load_module_by_name(self, module_name: str) -> ModuleSummary | None:
        cached = self._module_cache_by_name.get(module_name)
        if cached:
            return cached
        file_path = self._find_module_path(module_name)
        if not file_path:
            return None
        summary = self._parse_module(file_path, module_name)
        if summary:
            self._module_cache[file_path] = summary
            if summary.module_name:
                self._module_cache_by_name[summary.module_name] = summary
        return summary

    def _load_module_from_path(self, file_path: Path) -> ModuleSummary | None:
        resolved = file_path.resolve()
        cached = self._module_cache.get(resolved)
        if cached:
            return cached
        module_name = self._infer_module_name(resolved)
        summary = self._parse_module(resolved, module_name)
        if summary:
            self._module_cache[resolved] = summary
            if summary.module_name:
                self._module_cache_by_name[summary.module_name] = summary
        return summary

    def _parse_module(self, file_path: Path, module_name: str | None) -> ModuleSummary | None:
        try:
            source = file_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):  # pragma: no cover - IO errors
            return None
        try:
            tree = ast.parse(source, filename=str(file_path), type_comments=True)
        except SyntaxError:
            return None
        return ModuleSummary(file_path, module_name or "", source, tree)

    def _infer_module_name(self, file_path: Path) -> str | None:
        for root in self.root_dirs:
            try:
                relative = file_path.relative_to(root)
            except ValueError:
                continue
            if relative.stem == "__init__":
                parts = list(relative.parent.parts)
            else:
                parts = list(relative.with_suffix("" ).parts)
            return ".".join(part for part in parts if part)
        return None

    def _find_module_path(self, module_name: str) -> Path | None:
        module_rel = Path(*module_name.split("."))
        for root in self.root_dirs:
            candidate = (root / module_rel).with_suffix(".py")
            if candidate.exists():
                return candidate.resolve()
            init_candidate = root / module_rel / "__init__.py"
            if init_candidate.exists():
                return init_candidate.resolve()
        return None

class Hasher:
    def __init__(
        self,
        py_dirs: list[str] | None = None,
        r_dirs: list[str] | None = None,
        output_dir=None,
        tasks_config=None,
        tasks_config_paths=None,
        runtime_config=None,
    ):
        self.r_dirs = r_dirs or []
        self.py_dirs = py_dirs or []
        self.output_dir = output_dir
        self.tasks_config = tasks_config or read_tasks_configs(tasks_config_paths)
        self.runtime_config = runtime_config
        self._duckdb_connection = None
        self._py_function_analyzer: PythonFunctionAnalyzer | None = None

    def get_task(self, name: str):
        """Return the task configuration."""
        try:
            task = self.tasks_config["tasks"][name]
        except KeyError:
            taskname_keys = self.tasks_config["tasks"].keys()
            raise KeyError(f"Task '{name}' not found in list of tasks, {taskname_keys}")
        return task

    def get_full_r_script_paths(self, filename: str) -> tuple[list[Path], list[str]]:
        """Search r_dirs for matching R script(s)."""
        matching_r_scripts = []
        matching_r_dir = None
        for r_dir in self.r_dirs:
            if "$" in filename:
                # Regex replace any variables ${.*} -> '*'
                r_script_pattern = Path(r_dir) / re.sub(var_pattern, "*", filename)
                matching_r_scripts.extend(glob.glob(str(r_script_pattern)))
                matching_r_dir = r_dir
            else:
                r_script_path = Path(r_dir) / filename
                if r_script_path.exists():
                    matching_r_scripts.append(r_script_path)
                    matching_r_dir = r_dir
        if len(matching_r_scripts) == 0:
            raise FileNotFoundError(f"No R script {filename} not found in {self.r_dirs}; cwd={Path.cwd()}")
        return matching_r_scripts, matching_r_dir

    def get_task_filelist(self, task: dict) -> list[str]:
        """Return a list of all files associated with a task."""
        filename = task["r_script"]
        full_paths, r_tasks_dir = self.get_full_r_script_paths(filename)
        abs_file_list = get_file_list(full_paths)
        return abs_file_list

    def build_r_code_hashes(self, name: str, task: dict = None) -> list[dict[str, str]]:
        """Hash the code of a task to determine if it has changed."""
        if task is None:
            task = self.get_task(name)
        filename = task["r_script"]
        full_paths, r_tasks_dir = self.get_full_r_script_paths(filename)
        logger.info(f"Building R code hashes for {name}, paths: {full_paths}")
        return hash_r_files(full_paths, r_tasks_dir)

    def get_full_py_script_path(self, filename: str) -> Path:
        """Search py_dirs for the Python script."""
        for py_dir in self.py_dirs:
            py_script_path = Path(py_dir) / filename
            if py_script_path.exists():
                return py_script_path
            else:
                print(f"Debug: {py_script_path} does not exist; cwd={Path.cwd()}")
        raise FileNotFoundError(f"Python script {filename} not found in {self.py_dirs}")

    def build_py_code_hashes(self, name: str, task: dict = None) -> str:
        """Hash the code of a task to determine if it has changed."""
        if task is None:
            task = self.get_task(name)
        # If task["py_script"] is a string, use it as the filename
        filename = task["py_script"] if isinstance(task.get("py_script"), str) else name + ".py"
        full_path = self.get_full_py_script_path(filename)
        logger.info(f"Building Python code hashes for {name}, path: {full_path}")
        analyzer = self._get_py_function_analyzer()
        function_name = task.get("py_function") or name
        try:
            return analyzer.build_function_hashes(full_path, function_name)
        except Exception as exc:
            logger.warning(
                "Falling back to file hash for %s due to %s", name, exc, exc_info=False
            )
            return [{"function": "__file__", "hash": hash_file(full_path)}]

    def hash_code_for_task(self, name: str):
        task = self.get_task(name)
        if "r_script" in task:
            code_hashes = self.build_r_code_hashes(name, task)
        elif "py_script" in task:
            code_hashes = self.build_py_code_hashes(name, task)
        else:
            raise KeyError(f"Task '{name}' has no R or Python script")
        return code_hashes

    def _get_py_function_analyzer(self) -> PythonFunctionAnalyzer:
        if self._py_function_analyzer is None:
            self._py_function_analyzer = PythonFunctionAnalyzer(self.py_dirs or [])
        return self._py_function_analyzer

    @staticmethod
    def _infer_python_function_name(task_name: str, task: dict, filename: str) -> str:
        script_spec = task.get("py_script")
        if isinstance(script_spec, str):
            return Path(filename).stem
        return task_name

    def _ensure_duckdb_connection(self):
        if self.runtime_config is None:
            return None
        if self._duckdb_connection is not None:
            return self._duckdb_connection
        try:
            import duckdb  # type: ignore import-not-found
        except ImportError:
            logger.warning("DuckDB outputs requested but duckdb is not installed")
            return None

        engine = getattr(self.runtime_config, "duckdb", None)
        if engine is None:
            logger.warning("DuckDB outputs requested but runtime config has no 'duckdb' connection")
            return None
        if not isinstance(engine, duckdb.DuckDBPyConnection):
            logger.warning(
                "DuckDB outputs requested but runtime config engine is %s",
                type(engine),
            )
            return None
        self._duckdb_connection = engine
        return self._duckdb_connection

    @staticmethod
    def _quote_duckdb_identifier(identifier: str) -> str:
        return f'"{identifier.replace("\"", "\"\"")}"'

    def _parse_duckdb_target(self, target: str) -> tuple[str | None, str]:
        if not target.startswith(DUCKDB_OUTPUT_PREFIX):
            raise ValueError(f"Not a DuckDB target: {target}")
        body = target[len(DUCKDB_OUTPUT_PREFIX) :]
        if not body:
            raise ValueError("DuckDB output spec missing table name")
        if "." in body:
            schema, table = body.split(".", 1)
            schema = schema or None
        else:
            schema, table = None, body
        return schema, table

    def _hash_duckdb_target(self, target: str) -> str | None:
        conn = self._ensure_duckdb_connection()
        if conn is None:
            return None
        schema, table = self._parse_duckdb_target(target)
        qualified = self._quote_duckdb_identifier(table)
        if schema:
            qualified = f"{self._quote_duckdb_identifier(schema)}.{qualified}"
        alias = "tscache_tbl"
        row_hash_expr = f"md5({alias}::TEXT)"
        aggregate_expr = f"string_agg({row_hash_expr}, '' ORDER BY {row_hash_expr})"
        query = (
            f"SELECT {aggregate_expr} AS concatenated_hashes "
            f"FROM {qualified} AS {alias}"
        )
        try:
            result = conn.execute(query).fetchone()
        except Exception as exc:  # pragma: no cover - depends on duckdb exceptions
            logger.warning("Failed to hash DuckDB output '%s': %s", target, exc)
            return None
        if not result:
            return DUCKDB_EMPTY_HASH
        concatenated_hashes = result[0]
        if concatenated_hashes is None:
            return DUCKDB_EMPTY_HASH
        if not isinstance(concatenated_hashes, str):
            concatenated_hashes = str(concatenated_hashes)
        return hashlib.md5(concatenated_hashes.encode()).hexdigest()

    def _hash_duckdb_outputs(self, targets: list[str]) -> list[dict[str, str]]:
        digests: list[dict[str, str]] = []
        for target in sorted(set(targets)):
            digest = self._hash_duckdb_target(target)
            if digest:
                digests.append({target: digest})
        return digests

    def hash_task_outputs(self, name: str) -> str:
        """Hash output files of a task to determine if they have changed."""
        task = self.get_task(name)
        if "outputs" not in task:
            return ""
        output_filepaths: list[str] = task["outputs"]
        duckdb_targets = [
            output for output in output_filepaths if isinstance(output, str) and output.startswith(DUCKDB_OUTPUT_PREFIX)
        ]
        file_patterns = [
            output for output in output_filepaths if output not in duckdb_targets
        ]
        if self.output_dir is None and file_patterns:
            raise ValueError("Output directory not set")
        # Search for all output files in the scratch directory
        file_list = set()
        for output_filepath in file_patterns:
            if "$" in output_filepath:
                # Check if all variables in the pattern are in the environment
                # Replace any unknown variables with '*'
                all_vars = var_pattern.findall(output_filepath)
                for var in all_vars:
                    output_filepath = output_filepath.replace(f"${{{var}}}", "*")
                glob_pattern = str(Path(self.output_dir) / output_filepath)
                matching_files = glob.glob(glob_pattern)
                if len(matching_files) > 0:
                    file_list.update(matching_files)
                else:
                    print(f"Warning: File {glob_pattern} not found")
            else:
                if (Path(self.output_dir) / output_filepath).exists():
                    file_list.add(output_filepath)
                else:
                    print(f"Warning: File {output_filepath} not found")
        # Sort the files by name
        sorted_file_list = sorted(file_list)

        hashed_outputs: list[dict[str, str]] = []
        if len(sorted_file_list) > 0:
            # Hash the contents of the files
            hashed_outputs.extend(
                {file: hash_file(Path(self.output_dir) / file)} for file in sorted_file_list
            )

        if duckdb_targets:
            hashed_outputs.extend(self._hash_duckdb_outputs(duckdb_targets))

        if not hashed_outputs:
            return
        return hash_obj(hashed_outputs)

    def hash_subtask_outputs(self, name:str, env: dict) -> str:
        """Hash the outputs of a subtask to determine if they have changed."""
        task = self.get_task(name)
        if self.output_dir is None:
            raise ValueError("Output directory not set")
        if "outputs" not in task:
            return ""
        filename_patterns: list[str] = task["outputs"]
        file_list = set()
        for pattern in filename_patterns:
            # If the pattern contains a variable, replace it with the value from the environment
            if "$" in pattern:
                # Check if all variables in the pattern are in the environment
                # Replace any unknown variables with '*'
                all_vars = var_pattern.findall(pattern)
                for var in all_vars:
                    if var in env:
                        pattern = pattern.replace(f"${{{var}}}", str(env[var]))
                    else:
                        # replace with '*' to match any file
                        pattern = pattern.replace(f"${{{var}}}", "*")
                
                glob_pattern = str(Path(self.output_dir) / pattern)
                matching_files = glob.glob(glob_pattern)
                if len(matching_files) == 0:
                    print(f"Warning: File {glob_pattern} not found")
                else:
                    file_list.update(matching_files)
        # Sort the files by name
        sorted_file_list = sorted(file_list)
        if len(file_list) == 0:
            return
        # Hash the contents of the files
        hashed_output_files = [
            {file: hash_file(str(Path(self.output_dir) / file))} for file in sorted_file_list
        ]
        return hash_obj(hashed_output_files)
