from dataclasses import dataclass
from datetime import datetime
from contextlib import suppress
import functools
import importlib
import importlib.util
import logging
import os
import json
import sys
from typing import Callable, Optional, Union, Mapping, Iterable
from types import ModuleType
import requests
import time
from pathlib import Path
import inspect

from kptn.caching.Hasher import Hasher
from kptn.caching.models import TaskState
from kptn.caching.client.DbClientBase import DbClientBase, init_db_client
from kptn.util.flow_type import is_flow_prefect
from kptn.util.logger import get_logger
from kptn.util.pipeline_config import PipelineConfig, get_storage_key
from kptn.util.runtime_config import RuntimeConfig
from kptn.util.rscript import r_script
from kptn.util.read_tasks_config import read_tasks_config
from kptn.util.hash import hash_file, hash_obj
from kptn.util.task_args import plan_python_call
from kptn.util.task_dirs import resolve_python_task_dirs


@dataclass
class TaskSubmissionDecision:
    """Outcome of evaluating whether a task should be submitted for execution."""
    task_name: str
    task: dict
    cached_state: TaskState | None
    should_run: bool
    reason: str | None = None

class TaskStateCache():
    """
    Given a branch (used in the cache key), a dictionary of task configurations, a directory of R tasks,
    and a scratch directory (where output files are stored) this class serves as a proxy that submits
    Prefect tasks only if:
    1. The python function or R code of the task has changed
    2. The input files of the task have changed
    4. The input (Python-generated) data of the task has changed
    4. The override cache flag is set
    """

    pipeline_config: PipelineConfig
    db_client: DbClientBase
    hasher: Hasher
    logger: Union[logging.Logger, logging.LoggerAdapter]

    PYTHON_SUFFIXES = {".py", ".pyw"}
    R_SUFFIXES = {".r"}
    DUCKDB_SQL_SUFFIXES = {".sql"}

    _instance = None

    # Singleton pattern to avoid multiple cache clients and reads of the tasks config
    def __new__(
        cls,
        pipeline_config: PipelineConfig,
        db_client: Optional[DbClientBase] = None,
        tasks_config = None,
        tasks_config_paths: list[str] | None = None,
    ):
        self = cls._instance
        if self is None:
            self = super(TaskStateCache, cls).__new__(cls)
            self.pipeline_config = pipeline_config
            storage_key = get_storage_key(pipeline_config)
            self.pipeline_name = pipeline_config.PIPELINE_NAME
            config_paths = [str(path) for path in (tasks_config_paths or []) if str(path)]
            primary_config_path = config_paths[0] if config_paths else getattr(pipeline_config, "TASKS_CONFIG_PATH", "")
            if not primary_config_path:
                raise ValueError("TaskStateCache requires at least one tasks_config_path")
            self.tasks_config_paths = [str(path) for path in (config_paths or [primary_config_path])]
            self.tasks_config = tasks_config or read_tasks_config(primary_config_path)
            self.db_client = db_client or init_db_client(
                table_name=os.getenv("DYNAMODB_TABLE_NAME", "tasks"),
                storage_key=storage_key,
                pipeline=pipeline_config.PIPELINE_NAME,
                tasks_config=self.tasks_config,
                tasks_config_path=primary_config_path,
            )
            tasks_config_path = Path(primary_config_path)
            self.tasks_root_dir = tasks_config_path.parent
            duckdb_dir_setting = self.tasks_config.get("settings", {}).get("duckdb_tasks_dir")
            if duckdb_dir_setting:
                self.duckdb_tasks_dir = (self.tasks_root_dir / duckdb_dir_setting).resolve()
            else:
                self.duckdb_tasks_dir = self.tasks_root_dir
            self.py_task_dirs = resolve_python_task_dirs(
                self.tasks_root_dir,
                tasks_config=self.tasks_config,
                module_path=getattr(pipeline_config, "PY_MODULE_PATH", None),
            )

            r_task_dirs: list[Path] = []
            for entry in getattr(pipeline_config, "R_TASKS_DIRS", ()):
                entry_path = Path(entry)
                if entry_path.is_absolute():
                    r_task_dirs.append(entry_path.resolve())
                else:
                    r_task_dirs.append((self.tasks_root_dir / entry_path).resolve())
            if not r_task_dirs:
                r_task_dirs.append(self.tasks_root_dir)
            self.r_task_dirs = r_task_dirs

            self.runtime_config = self.build_runtime_config()
            self.hasher = Hasher(
                r_dirs=[str(path) for path in self.r_task_dirs],
                output_dir=pipeline_config.scratch_dir,
                tasks_config=self.tasks_config,
                tasks_config_paths=self.tasks_config_paths,
                runtime_config=self.runtime_config,
                pipeline_config=pipeline_config,
            )
            self.logger = get_logger()
            self._duckdb_sql_functions: dict[str, Callable[..., object]] = {}
            self._python_module_cache: dict[str, ModuleType] = {}
            self._task_has_prior_runs: dict[str, bool] = {}
        return self

    def __str__(self):
        storage_key = get_storage_key(self.pipeline_config)
        return f"TaskStateCache(storage_key={storage_key}, client={self.db_client}, tasks_config={self.tasks_config})"

    def _flow_type_override(self) -> str | None:
        """Return flow type override from environment, if provided."""
        override = os.getenv("KPTN_FLOW_TYPE")
        if override:
            return override.strip().lower()
        return None

    def _configured_flow_type(self) -> str | None:
        """Return the flow type configured in kptn.yaml."""
        settings = self.tasks_config.get("settings")
        if not isinstance(settings, Mapping):
            return None
        configured = settings.get("flow_type")
        if isinstance(configured, str) and configured.strip():
            return configured.strip().lower()
        return None

    def _effective_flow_type(self) -> str:
        """Compute the effective flow type, honoring environment overrides."""
        return self._flow_type_override() or self._configured_flow_type() or "vanilla"

    def is_flow_prefect(self) -> bool:
        """Check if the workflow type should use Prefect execution."""
        return self._effective_flow_type() == "prefect"

    def is_flow_stepfunctions(self) -> bool:
        """Check if the workflow type uses Step Functions artifacts."""
        return self._effective_flow_type() == "stepfunctions"

    def get_dep_list(self, task_name: str) -> list[str]:
        """Return the names of the dependencies of a task."""
        if task_name not in self.tasks_config["graphs"][self.pipeline_name]["tasks"]:
            pipeline_keys_str = json.dumps(list(self.tasks_config["graphs"][self.pipeline_name]["tasks"].keys()))
            raise KeyError(f"Task ({task_name}) not found in list of tasks; pipeline: {self.pipeline_name}; pipeline_keys: {pipeline_keys_str}")
        deps = self.tasks_config["graphs"][self.pipeline_name]["tasks"][task_name]
        if type(deps) == list:
            return deps
        elif type(deps) == str:
            return [deps]
        else:
            return []
    
    def get_dep_states(self, task_name: str) -> list[tuple[str, TaskState]]:
        """Return the states of the dependencies of a task."""
        deps = self.get_dep_list(task_name)
        if not deps:
            return []
        return [(dep, self.fetch_state(dep)) for dep in deps]

    def get_task(self, name: str):
        """Return the task configuration."""
        try:
            task = self.tasks_config["tasks"][name]
        except KeyError:
            taskname_keys = self.tasks_config["tasks"].keys()
            raise KeyError(f"Task '{name}' not found in list of tasks, {taskname_keys}")
        return task

    def get_task_dask_worker_vars(self, name: str) -> dict:
        """Return task-specific worker_cpu, worker_mem kwargs to the Dask"""
        task = self.get_task(name)
        if "compute" not in task:
            return {}
        cpu = task["compute"]["cpu"]
        mem = task["compute"]["memory"]
        return {"worker_cpu": cpu, "worker_mem": mem}

    def get_task_rscript_path(self, name: str):
        task = self.get_task(name)
        if not self.is_rscript(name, task):
            raise ValueError(f"Task '{name}' is not an R task")
        file_value = self._get_task_file(name, task)
        for root in self.r_task_dirs:
            candidate = (root / Path(file_value)).resolve()
            if candidate.exists():
                return str(candidate)
        fallback_root = self.r_task_dirs[0] if self.r_task_dirs else self.tasks_root_dir
        return str((fallback_root / Path(file_value)).resolve())

    def should_cache_result(self, task_name: str) -> bool:
        """Check if the task should cache its results."""
        task = self.get_task(task_name)
        return task.get("cache_result") == True

    def should_call_on_main_flow(self, task_name: str) -> bool:
        """Check if the task should be called on the main flow."""
        task = self.get_task(task_name)
        return task.get("main_flow") == True

    def _parse_file_spec(self, task_name: str, task: dict | None = None) -> tuple[str, str | None]:
        if task is None:
            task = self.get_task(task_name)
        file_value = task.get("file")
        if not file_value:
            raise KeyError(f"Task '{task_name}' is missing required 'file' field")
        if ":" in file_value:
            file_path, func_name = file_value.rsplit(":", 1)
        else:
            file_path, func_name = file_value, None
        file_path = file_path.strip()
        func_name = func_name.strip() if func_name and func_name.strip() else None
        return file_path, func_name

    def _get_task_file(self, task_name: str, task: dict | None = None) -> str:
        file_path, _ = self._parse_file_spec(task_name, task)
        return file_path

    def _get_task_function(self, task_name: str, task: dict | None = None) -> str | None:
        _, func_name = self._parse_file_spec(task_name, task)
        return func_name

    def _get_task_language(self, task_name: str, task: dict | None = None) -> str:
        file_path = self._get_task_file(task_name, task)
        suffix = Path(file_path).suffix.lower()
        if suffix in self.PYTHON_SUFFIXES:
            return "python"
        if suffix in self.R_SUFFIXES:
            return "r"
        if suffix in self.DUCKDB_SQL_SUFFIXES:
            return "duckdb_sql"
        raise ValueError(
            f"Task '{task_name}' has unsupported file suffix '{suffix}' for file '{file_path}'"
        )

    def is_python_task(self, task_name: str, task: dict | None = None) -> bool:
        return self._get_task_language(task_name, task) == "python"

    def is_rscript(self, task_name: str, task: dict | None = None) -> bool:
        """Check if the task is an R script."""
        return self._get_task_language(task_name, task) == "r"

    def is_duckdb_sql_task(self, task_name: str, task: dict | None = None) -> bool:
        """Check if the task is backed by a DuckDB SQL script."""
        try:
            return self._get_task_language(task_name, task) == "duckdb_sql"
        except ValueError:
            return False

    def _as_python_task_spec(self, task_name: str, task: dict) -> dict:
        python_file, python_function = self._parse_file_spec(task_name, task)
        spec = {**task, "py_script": python_file}
        if python_function:
            spec["py_function"] = python_function
        return spec

    def _as_r_task_spec(self, task_name: str, task: dict) -> dict:
        return {**task, "r_script": self._get_task_file(task_name, task)}

    def _resolve_duckdb_sql_path(self, task_name: str, task: dict | None = None) -> Path:
        if task is None:
            task = self.get_task(task_name)
        script_location = self._get_task_file(task_name, task)
        candidate_path = Path(script_location)
        if candidate_path.is_absolute():
            resolved = candidate_path
        else:
            search_dirs: list[Path] = []
            if self.duckdb_tasks_dir:
                search_dirs.append(Path(self.duckdb_tasks_dir))
            if self.tasks_root_dir not in search_dirs:
                search_dirs.append(self.tasks_root_dir)
            resolved = None
            for base_dir in search_dirs:
                potential = (base_dir / candidate_path).resolve()
                if potential.exists():
                    resolved = potential
                    break
            if resolved is None:
                searched = ", ".join(str(directory) for directory in search_dirs if directory)
                raise FileNotFoundError(
                    f"DuckDB SQL file '{script_location}' for task '{task_name}' not found (searched: {searched or 'n/a'})"
                )
        if not resolved.exists():
            raise FileNotFoundError(
                f"DuckDB SQL file '{script_location}' for task '{task_name}' not found at {resolved}"
            )
        return resolved

    def _build_duckdb_sql_hashes(self, task_name: str, task: dict | None = None) -> list[dict[str, str]]:
        script_path = self._resolve_duckdb_sql_path(task_name, task)
        digest = hash_file(str(script_path))
        relative_path = str(script_path)
        with suppress(ValueError):
            relative_path = str(script_path.relative_to(self.tasks_root_dir))
        return [{"file": relative_path, "hash": digest}]

    def build_task_code_hashes(
        self,
        task_name: str,
        task: dict,
        *,
        is_r_task: bool | None = None,
        is_duckdb_sql_task: bool | None = None,
        is_python_task: bool | None = None,
    ) -> tuple[list[dict[str, str]] | None, str | None]:
        if is_r_task is None:
            is_r_task = self.is_rscript(task_name, task)
        if is_duckdb_sql_task is None:
            is_duckdb_sql_task = self.is_duckdb_sql_task(task_name, task)
        if is_python_task is None:
            is_python_task = self.is_python_task(task_name, task)

        if is_r_task:
            r_task_spec = self._as_r_task_spec(task_name, task)
            return self.hasher.build_r_code_hashes(task_name, r_task_spec), "R"
        if is_duckdb_sql_task:
            return self._build_duckdb_sql_hashes(task_name, task), "DuckDB SQL"
        if is_python_task:
            py_task_spec = self._as_python_task_spec(task_name, task)
            return self.hasher.build_py_code_hashes(task_name, py_task_spec), "Python"

        return None, None

    def _ensure_duckdb_sql_callable(self, task_name: str) -> Callable[[RuntimeConfig], object]:
        cached = self._duckdb_sql_functions.get(task_name)
        if cached is not None:
            return cached

        task = self.get_task(task_name)
        script_path = self._resolve_duckdb_sql_path(task_name, task)
        script_dir = script_path.parent
        logger = self.logger

        def duckdb_sql_runner(runtime_config: RuntimeConfig, **kwargs):
            conn = getattr(runtime_config, "duckdb", None)
            if conn is None:
                raise RuntimeError(
                    f"Task '{task_name}' requires a DuckDB connection named 'duckdb' in the runtime configuration"
                )

            if kwargs:
                logger.debug(
                    "DuckDB SQL task %s received keyword args %s; they are ignored by the runner",
                    task_name,
                    list(kwargs.keys()),
                )

            previous_search_path: str | None = None
            with suppress(Exception):
                row = conn.execute("SELECT current_setting('file_search_path')").fetchone()
                if row:
                    previous_search_path = row[0]

            try:
                conn.execute("SET file_search_path = ?", [str(script_dir)])
                sql = script_path.read_text(encoding="utf-8")
                logger.info("Executing DuckDB SQL script %s for task %s", script_path, task_name)
                sql_parameters = self._build_duckdb_sql_parameters(runtime_config)
                for statement in self._split_duckdb_sql(sql):
                    statement_params = self._extract_statement_parameters(statement, sql_parameters)
                    if statement_params:
                        conn.execute(statement, statement_params)
                    else:
                        conn.execute(statement)
            finally:
                if previous_search_path is not None:
                    with suppress(Exception):
                        conn.execute("SET file_search_path = ?", [previous_search_path])
                else:
                    with suppress(Exception):
                        conn.execute("RESET file_search_path")

        duckdb_sql_runner.__name__ = task_name
        self._duckdb_sql_functions[task_name] = duckdb_sql_runner
        return duckdb_sql_runner

    def _build_duckdb_sql_parameters(self, runtime_config: RuntimeConfig) -> dict[str, object]:
        """Provide runtime config entries as named SQL parameters."""
        runtime_values = runtime_config.as_dict()
        config_block = self.tasks_config.get("config", {})
        duckdb_conn = runtime_values.get("duckdb")
        parameters: dict[str, object] = {}
        for key, value in runtime_values.items():
            if key == "duckdb" or value is duckdb_conn:
                continue
            entry_spec = config_block.get(key)
            include_override = self._extract_include_override(entry_spec)
            if include_override is not None:
                parameters[key] = include_override
                continue
            parameters[key] = value
        return parameters

    def _extract_include_override(self, entry_spec: Mapping[str, object] | object) -> object | None:
        """Return resolved include path(s) for config entries defined solely via include."""
        if not isinstance(entry_spec, Mapping):
            return None
        include_value = entry_spec.get("include")
        other_keys = [name for name in entry_spec.keys() if name != "include"]
        if include_value is None or other_keys:
            return None
        return self._normalise_include_value(include_value)

    def _normalise_include_value(self, include_value: object) -> object | None:
        if isinstance(include_value, str):
            return self._resolve_include_path(include_value)
        if isinstance(include_value, Iterable):
            resolved_entries: list[str] = []
            for entry in include_value:
                if not isinstance(entry, str):
                    continue
                resolved_entries.append(self._resolve_include_path(entry))
            if not resolved_entries:
                return None
            return resolved_entries
        return None

    def _resolve_include_path(self, include_entry: str) -> str:
        entry_path = Path(include_entry)
        if entry_path.is_absolute():
            return str(entry_path.resolve())
        return str((self.tasks_root_dir / entry_path).resolve())

    def _split_duckdb_sql(self, sql: str) -> list[str]:
        statements: list[str] = []
        current: list[str] = []
        in_single = False
        in_double = False
        in_line_comment = False
        in_block_comment = False
        preserve_line_comment = False
        preserve_block_comment = False
        has_content = False
        i = 0
        length = len(sql)
        while i < length:
            ch = sql[i]
            nxt = sql[i + 1] if i + 1 < length else ""

            if in_single:
                current.append(ch)
                if ch == "'" and nxt == "'":
                    current.append(nxt)
                    i += 2
                    continue
                if ch == "'":
                    in_single = False
                else:
                    if not ch.isspace():
                        has_content = True
                i += 1
                continue

            if in_double:
                current.append(ch)
                if ch == '"' and nxt == '"':
                    current.append(nxt)
                    i += 2
                    continue
                if ch == '"':
                    in_double = False
                else:
                    if not ch.isspace():
                        has_content = True
                i += 1
                continue

            if in_line_comment:
                if preserve_line_comment:
                    current.append(ch)
                if ch == "\n":
                    in_line_comment = False
                    preserve_line_comment = False
                i += 1
                continue

            if in_block_comment:
                if preserve_block_comment:
                    current.append(ch)
                if ch == "*" and nxt == "/":
                    if preserve_block_comment:
                        current.append(nxt)
                    in_block_comment = False
                    preserve_block_comment = False
                    i += 2
                else:
                    i += 1
                continue

            if ch == "-" and nxt == "-":
                preserve_line_comment = has_content
                if preserve_line_comment:
                    current.append(ch)
                    current.append(nxt)
                in_line_comment = True
                i += 2
                continue

            if ch == "/" and nxt == "*":
                preserve_block_comment = has_content
                if preserve_block_comment:
                    current.append(ch)
                    current.append(nxt)
                in_block_comment = True
                i += 2
                continue

            if ch == "'":
                in_single = True
                current.append(ch)
                has_content = True
                i += 1
                continue

            if ch == '"':
                in_double = True
                current.append(ch)
                has_content = True
                i += 1
                continue

            if ch == ";":
                statement = "".join(current).strip()
                if statement and not statement.startswith(("--", "/*")):
                    statements.append(statement)
                current = []
                has_content = False
                i += 1
                continue

            current.append(ch)
            if not ch.isspace():
                has_content = True
            i += 1

        tail = "".join(current).strip()
        if tail and not tail.startswith(("--", "/*")):
            statements.append(tail)
        return statements

    def _extract_statement_parameters(
        self,
        statement: str,
        available: Mapping[str, object],
    ) -> dict[str, object]:
        used: set[str] = set()
        in_single = False
        in_double = False
        in_line_comment = False
        in_block_comment = False
        i = 0
        length = len(statement)

        while i < length:
            ch = statement[i]
            nxt = statement[i + 1] if i + 1 < length else ""

            if in_single:
                if ch == "'" and nxt == "'":
                    i += 2
                    continue
                if ch == "'":
                    in_single = False
                i += 1
                continue

            if in_double:
                if ch == '"' and nxt == '"':
                    i += 2
                    continue
                if ch == '"':
                    in_double = False
                i += 1
                continue

            if in_line_comment:
                if ch == "\n":
                    in_line_comment = False
                i += 1
                continue

            if in_block_comment:
                if ch == "*" and nxt == "/":
                    in_block_comment = False
                    i += 2
                else:
                    i += 1
                continue

            if ch == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue

            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue

            if ch == "'":
                in_single = True
                i += 1
                continue

            if ch == '"':
                in_double = True
                i += 1
                continue

            if ch in (":", "$") and nxt and (nxt.isalpha() or nxt == "_"):
                if ch == ":" and i > 0 and statement[i - 1] == ":":
                    i += 1
                    continue
                start = i + 1
                while start < length and (statement[start].isalnum() or statement[start] == "_"):
                    start += 1
                name = statement[i + 1 : start]
                if name in available:
                    used.add(name)
                i = start
                continue

            i += 1

        return {name: available[name] for name in used}

    def _resolve_task_file_path(self, file_path: str) -> Path:
        candidate = Path(file_path)
        if candidate.is_absolute():
            return candidate.resolve()
        if self.tasks_root_dir:
            return (self.tasks_root_dir / candidate).resolve()
        return candidate.resolve()

    def _python_module_name_options(
        self,
        abs_file_path: Path,
        *,
        relative_spec: str | None = None,
    ) -> list[str]:
        """Return potential import paths for a Python task module."""
        candidates: list[str] = []
        seen: set[str] = set()

        def add_from_path(path: Path):
            parts = [part for part in path.with_suffix("").parts if part and part != "."]
            if not parts:
                return
            dotted = ".".join(parts)
            if dotted in seen:
                return
            seen.add(dotted)
            candidates.append(dotted)

        if relative_spec:
            add_from_path(Path(relative_spec))

        if self.tasks_root_dir:
            try:
                relative = abs_file_path.resolve().relative_to(self.tasks_root_dir.resolve())
            except ValueError:
                relative = None
            if relative:
                add_from_path(relative)
        return candidates

    def _load_module_from_file(self, module_name: str, file_path: Path) -> ModuleType:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load python module for task from {file_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    def _load_python_module_for_task(self, task_name: str, task: dict) -> ModuleType:
        file_value = self._get_task_file(task_name, task)
        abs_path = self._resolve_task_file_path(file_value)
        cache_key = str(abs_path)
        cached = self._python_module_cache.get(cache_key)
        if cached:
            return cached

        module: ModuleType | None = None
        module_candidates = self._python_module_name_options(
            abs_path, relative_spec=file_value
        )
        for candidate in module_candidates:
            try:
                module = importlib.import_module(candidate)
            except ModuleNotFoundError:
                continue
            else:
                break

        if module is None:
            module_name = module_candidates[0] if module_candidates else f"kptn_task_{abs(hash(cache_key))}"
            module = self._load_module_from_file(module_name, abs_path)

        self._python_module_cache[cache_key] = module
        return module

    def get_python_callable(self, task_name: str):
        """Return the Python callable for a task, creating one for DuckDB SQL tasks if needed."""
        task = self.get_task(task_name)
        if self.is_duckdb_sql_task(task_name, task):
            duckdb_callable = self._ensure_duckdb_sql_callable(task_name)
            return duckdb_callable
        if not self.is_python_task(task_name, task):
            raise AttributeError(
                f"Task '{task_name}' is not configured as a Python task and cannot be executed as such"
            )
        module = self._load_python_module_for_task(task_name, task)
        func_name = self.get_py_func_name(task_name)
        if hasattr(module, func_name):
            return getattr(module, func_name)
        file_value = self._get_task_file(task_name, task)
        raise AttributeError(
            f"Task '{task_name}' callable '{func_name}' not found in module loaded from '{file_value}'"
        )

    def task_returns_list(self, task_name: str) -> bool:
        """Check if the task is a mapped task."""
        return "iterable_item" in self.get_task(task_name)

    def has_mapped_task_deps(self, task_name: str) -> bool:
        """Check if the task has mapped task dependencies."""
        deps = self.get_dep_list(task_name)
        for dep in deps:
            if self.task_returns_list(dep):
                return True

    def is_mapped_task(self, task_name: str) -> bool:
        """Check if the task is a mapped task."""
        return "map_over" in self.get_task(task_name)

    def get_map_over_key(self, task_name: str) -> str|None:
        """Return the key name of a task."""
        task = self.get_task(task_name)
        return task.get("map_over")

    def get_map_over_count(self, task_name: str) -> int | None:
        """Return the number of items that a mapped task will iterate over."""
        if not self.is_mapped_task(task_name):
            return None
        from kptn.caching.TSCacheUtils import fetch_cached_dep_data

        _, _, count = fetch_cached_dep_data(self, task_name)
        return count

    def get_key_value(self, task_name: str, kwargs) -> str:
        """Return the key value of a task."""
        key_name = self.get_map_over_key(task_name)
        if key_name:
            if "," in key_name:
                keys = key_name.split(",")
                if all(key in kwargs for key in keys):
                    return ",".join([str(kwargs[key]) for key in keys])
            elif key_name in kwargs:
                return kwargs[key_name]
        return None

    def get_custom_log_path(self, task_name: str) -> str|None:
        task = self.get_task(task_name)
        return task.get("logs")

    def get_cli_args(self, task_name: str) -> str:
        """Return the args of a task."""
        task = self.get_task(task_name)
        if "cli_args" in task:
            if "prefix_args" in task:
                return task["prefix_args"], task["cli_args"]
            else:
                return "", task["cli_args"]
        elif "prefix_args" in task:
            return task["prefix_args"], ""
        else:
            return "", ""

    def build_runtime_config(
        self,
        task_name: str | None = None,
        task_lang: str | None = None,
    ) -> RuntimeConfig:
        """Construct a runtime configuration for task execution."""
        tasks_config_path = Path(self.pipeline_config.TASKS_CONFIG_PATH)
        task_info: dict[str, str | None] | None = None
        if task_name:
            resolved_lang = task_lang or self._get_task_language(task_name)
            task_info = {
                "task_name": task_name,
                "task_lang": resolved_lang,
            }
        return RuntimeConfig.from_tasks_config(
            self.tasks_config,
            base_dir=tasks_config_path.parent,
            fallback=self.pipeline_config,
            task_info=task_info,
        )

    def get_py_func_name(self, task_name: str) -> str:
        """Return the Python function name of a task."""
        task = self.get_task(task_name)
        if not self.is_python_task(task_name, task):
            return task_name
        func_name = self._get_task_function(task_name, task)
        if func_name:
            return func_name
        return task_name

    def get_py_func_args(self, task_name: str) -> dict|None:
        """Return the args of a task."""
        task = self.get_task(task_name)
        return task.get("args")

    def code_changed(
        self,
        code_hashes,
        cached_state: TaskState | None = None,
        *,
        code_kind: str | None = None,
    ) -> bool:
        """Check if the task code has changed."""
        if code_hashes is None:
            return bool(cached_state and cached_state.code_hashes)

        latest_version = hash_obj(code_hashes)
        if not cached_state:
            return True

        cached_version = cached_state.code_version
        cached_hashes = cached_state.code_hashes
        if latest_version != cached_version:
            descriptor = f"{code_kind} code" if code_kind else "Task code"
            self.logger.info(
                "%s changed: %s (local=%s) != %s (cached=%s)",
                descriptor,
                code_hashes,
                latest_version,
                cached_hashes,
                cached_version,
            )
            return True

        return False

    def inputs_changed(
        self, input_hashes: dict[str, str], cached_state: TaskState = None
    ) -> bool:
        """Check if the inputs of a task have changed."""
        if cached_state:
            return cached_state.inputs_version != hash_obj(input_hashes)
        else:
            return True

    def data_changed(
        self, data_hashes: dict[str, str], cached_state: TaskState = None
    ) -> bool:
        """Check if the data of a task has changed."""
        if cached_state:
            return cached_state.input_data_version != hash_obj(data_hashes)
        else:
            return True

    def get_input_hashes(self, name: str, dep_states: list[tuple[str, TaskState]]) -> dict[str, str]:
        """Return the output file hashes of the inputs of a task."""
        inputs_version_tree = {}
        for dep, dep_state in dep_states:
            dep_outputs_version = dep_state.outputs_version if dep_state else None
            if dep_outputs_version:
                inputs_version_tree[dep] = dep_outputs_version
        self.logger.info(f"{name} inputs_version_tree: {inputs_version_tree}")
        if not inputs_version_tree:
            return None
        return inputs_version_tree

    def get_data_hashes(self, name: str, dep_states: list[tuple[str, TaskState]] = None) -> dict[str, str]:
        """Return the output data hashes of the inputs of a task."""
        if not dep_states:
            dep_states = self.get_dep_states(name)
        data_version_tree = {}
        for dep, dep_state in dep_states:
            if dep_state and dep_state.output_data_version:
                data_version_tree[dep] = dep_state.output_data_version
        self.logger.info(f"task={name} data_version_tree={data_version_tree}")
        if not data_version_tree:
            return None
        return data_version_tree

    def fetch_state(self, task_name) -> Optional[TaskState]:
        """Get cache for a flow or task; return None if not found or outdated."""
        cached_state = self.db_client.get_task(task_name, include_data=True, subset_mode=self.pipeline_config.SUBSET_MODE)
        if not cached_state:
            return None
        return TaskState.model_validate(cached_state)

    def delete_state(self, task_name: str):
        """Delete cache for a task"""
        self.db_client.delete_task(task_name)

    def evaluate_submission(
        self,
        task_name: str,
        parameters: dict | None = None,
        ignore_cache: bool = False,
    ) -> TaskSubmissionDecision:
        """
        Determine whether a task should be submitted for execution.

        Returns a TaskSubmissionDecision containing the task configuration, cached state,
        and the reason (if any) that the task should run.
        """
        if parameters is None:
            parameters = {}
        task = self.get_task(task_name)
        cached_state = self.fetch_state(task_name)
        is_r_task = self.is_rscript(task_name, task)
        is_python_task = self.is_python_task(task_name, task)
        is_duckdb_sql_task = self.is_duckdb_sql_task(task_name, task)
        code_hashes, code_kind = self.build_task_code_hashes(
            task_name,
            task,
            is_r_task=is_r_task,
            is_duckdb_sql_task=is_duckdb_sql_task,
            is_python_task=is_python_task,
        )

        reason = None
        if not cached_state:
            reason = "No cached state"
        elif ignore_cache:
            reason = "ignore_cache is set"
        elif self.pipeline_config.SUBSET_MODE:
            reason = "Subset mode"
        elif cached_state.status == "FAILURE":
            reason = "Task previously failed all subtasks"
        elif self.code_changed(
            code_hashes,
            cached_state,
            code_kind=code_kind,
        ):
            descriptor = f"{code_kind} code" if code_kind else "Task code"
            reason = f"{descriptor} changed"
        else:
            dep_states = self.get_dep_states(task_name)
            if self.inputs_changed(self.get_input_hashes(task_name, dep_states), cached_state):
                reason = "Inputs changed"
            elif self.data_changed(self.get_data_hashes(task_name, dep_states), cached_state):
                reason = "Data changed"
            elif cached_state.status == "INCOMPLETE":
                reason = "INCOMPLETE"
            elif not cached_state.end_time:
                reason = "Not finished"

        return TaskSubmissionDecision(
            task_name=task_name,
            task=task,
            cached_state=cached_state,
            should_run=bool(reason),
            reason=reason,
        )

    def submit(self, task_name: str, parameters, ignore_cache: bool):
        """Submit Prefect task if task state is out-of-date (code or inputs changed)."""
        self.logger.debug(f"tscache.submit({task_name}, {parameters}, ignore_cache={ignore_cache}) called")
        decision = self.evaluate_submission(task_name, parameters, ignore_cache)
        if not decision.should_run:
            self.logger.info(f"Skipping task {task_name}")
            return None

        reason = decision.reason or "No cached state"
        self.logger.info(f"Submitting task {task_name} because {reason}")
        storage_key = get_storage_key(self.pipeline_config)
        task = decision.task
        deployment_name = (
            f"{run_task.__name__.replace('_', '-')}/{self.pipeline_config.PIPELINE_NAME}-RunTask-{storage_key}"
        )
        # Run as separate flow container in prod
        if self.is_flow_prefect():
            if not os.getenv("DEPLOY_AS_INLINE_SUBFLOWS") == "1":
                from kptn.caching.prefect import run_deployment_task
                run_deployment_task(deployment_name, task_name, self.pipeline_config, task, reason, self.logger)
            else:  # Run as subflow locally
                import prefect
                parameters = parameters or {}
                parameters["task_name"] = task_name
                parameters["reason"] = reason
                flow_run_name = f"{task_name}-{prefect.runtime.flow_run.name}-{datetime.now().strftime('%H:%M:%S')}"
                run_task.with_options(flow_run_name=flow_run_name)(
                    self.pipeline_config, **parameters
                )
        else:
            kwargs = {}
            run_task(self.pipeline_config, task_name, **kwargs)

    def log_ecs_task_id(self) -> str:
        """Log the ECS Task ID and memory graph URL"""
        ecs_task_id = fetch_ecs_task_id()
        metrics_url = build_metrics_url()
        if os.getenv("IS_PROD") == "1":
            self.logger.info(f"Task running as ECS Task {ecs_task_id}; memory graph: {metrics_url}")
        return ecs_task_id

    def set_initial_state(self, task_name: str) -> TaskState:
        """Set initial state for a task before execution."""
        self.log_ecs_task_id()
        initial_state = TaskState(
            start_time=datetime.now().isoformat(),
        )
        task = self.get_task(task_name)
        existing_state = self.db_client.get_task(
            task_name,
            include_data=False,
            subset_mode=self.pipeline_config.SUBSET_MODE,
        )
        self._task_has_prior_runs[task_name] = bool(existing_state)
        is_python_task = self.is_python_task(task_name, task)
        is_duckdb_task = self.is_duckdb_sql_task(task_name, task)
        if (is_python_task or is_duckdb_task) and self.pipeline_config.SUBSET_MODE:
            # When in subset mode, only create the task if it doesn't exist
            if not existing_state:
                self.db_client.create_task(task_name, initial_state)
        else:
            self.db_client.create_task(task_name, initial_state)
        return initial_state

    def set_final_state(self, task_name: str, status: str = None):
        """Set final state for a task"""
        
        task = self.get_task(task_name)
        dep_states = self.get_dep_states(task_name)
        input_file_hashes = self.get_input_hashes(task_name, dep_states)
        input_data_hashes = self.get_data_hashes(task_name, dep_states)
        should_hash_outputs = self._task_has_prior_runs.pop(task_name, False)
        output_hashes = None
        if should_hash_outputs:
            output_hashes = self.hasher.hash_task_outputs(task_name)

        # Since this function is called by RunTask, a separate flow from the main flow,
        # recompute the hashes to ensure they are up-to-date
        code_hashes, _ = self.build_task_code_hashes(task_name, task)

        final_state = TaskState(
            code_hashes=code_hashes if code_hashes else None,
            outputs_version=str(output_hashes) if output_hashes else None,
            input_hashes=str(input_file_hashes) if input_file_hashes else None,
            input_data_hashes=str(input_data_hashes) if input_data_hashes else None,
            updated_at=datetime.now().isoformat(),
        )
        if status:
            final_state.status = status
        # FYI output_data_version has already been set in the set_task_ended function
        self.db_client.update_task(task_name, final_state)


def fetch_ecs_task_id():
    """Fetch the ECS Task ID from the ECS metadata endpoint"""
    if os.getenv("IS_PROD") == "1":
        resp = requests.get(f"{os.getenv('ECS_CONTAINER_METADATA_URI_V4')}/task")
        ecs_task_id = resp.json()["TaskARN"].split("/")[-1]
        return ecs_task_id
    else:
        return "local"

def build_metrics_url():
    """Build the URL to the CloudWatch metrics for the ECS Task"""
    ecs_task_id = fetch_ecs_task_id()
    REGION = os.getenv("AWS_REGION")
    METRIC_NS = "bravo"
    METRIC_NAME = "task-memory"
    return f"https://{REGION}.console.aws.amazon.com/cloudwatch/home?region={REGION}#metricsV2?graph=~(view~'timeSeries~stacked~false~metrics~(~(~'{METRIC_NS}~'{METRIC_NAME}~'TaskId~'{ecs_task_id}))~region~'{REGION}~stat~'Maximum~period~60)"

def rscript_task(pipeline_config: PipelineConfig, task_name: str, **kwargs):
    """Call a task's R script (this function is called by single and mapped tasks)"""
    tscache = TaskStateCache(pipeline_config)
    key = tscache.get_key_value(task_name, kwargs)
    idx = kwargs.pop("idx", None)
    if key:
        tscache.db_client.set_subtask_started(task_name, idx)
    else:
        tscache.set_initial_state(task_name)
    rscript_path = tscache.get_task_rscript_path(task_name)
    env = { **kwargs }
    prefix_args, cli_args = tscache.get_cli_args(task_name)
    tscache.logger.info(f"Calling R script {rscript_path} with env {env}")
    custom_log_path = tscache.get_custom_log_path(task_name)
    r_script(task_name, key, pipeline_config, rscript_path, env, prefix_args, cli_args, custom_log_path)
    if key:
        start = time.time()
        hash = tscache.hasher.hash_subtask_outputs(task_name, env)
        tscache.logger.info(f"Hashing output files took {time.time() - start} seconds")
        tscache.db_client.set_subtask_ended(task_name, idx, hash)
    else:
        tscache.db_client.set_task_ended(task_name)

def py_task(pipeline_config: PipelineConfig, task_name: str, **kwargs):
    """Call a Python function (this function is called by single and mapped tasks)"""
    if is_flow_prefect():
        import prefect
        if isinstance(pipeline_config, prefect.unmapped):
            pipeline_config = pipeline_config.value
    tscache = TaskStateCache(pipeline_config)
    key = tscache.get_key_value(task_name, kwargs)
    idx = kwargs.pop("idx", None)
    if key:
        tscache.db_client.set_subtask_started(task_name, idx)
    else:
        tscache.set_initial_state(task_name)
    # Add any constant arguments to the kwargs (data_args are already present in kwargs)
    func_args = tscache.get_py_func_args(task_name)
    if func_args:
        for arg_name, arg_value in func_args.items():
            if arg_name not in kwargs:
                kwargs[arg_name] = arg_value
    runtime_config = tscache.build_runtime_config(task_name=task_name)
    task_callable = tscache.get_python_callable(task_name)
    signature = inspect.signature(task_callable)
    call_args, call_kwargs, missing = plan_python_call(
        signature,
        kwargs,
        runtime_config,
    )
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise TypeError(
            f"Task '{task_name}' callable '{task_callable.__name__}' is missing required arguments: {missing_list}"
        )

    result = task_callable(*call_args, **call_kwargs)
    if key:
        tscache.db_client.set_subtask_ended(task_name, idx)
    else:
        tscache.db_client.set_task_ended(task_name, result=result, result_hash=hash_obj(result), subset_mode=pipeline_config.SUBSET_MODE)
    

def run_task(
    pipeline_config: PipelineConfig, task_name: str, reason: str = ""
):
    """Wrapper to call run_task flow"""
    if is_flow_prefect():
        import kptn.caching.prefect
        return kptn.caching.prefect.run_task_prefect(pipeline_config, task_name, reason)
    else:
        import kptn.caching.vanilla
        return kptn.caching.vanilla.run_task_vanilla(pipeline_config, task_name, reason)
