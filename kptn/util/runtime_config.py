from __future__ import annotations

import importlib
import inspect
import json
import re
import sys
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml


# Example config (YAML):
# config:
#   my_global: 42
#   engine: src.utils:get_engine
#   include:
#   - config.json


class RuntimeConfigError(RuntimeError):
    """Raised when runtime configuration cannot be interpreted."""


def ensure_pythonpath(
    base_dir: str | Path | None,
    module_path: str | None = None,
    extra_paths: Iterable[str | Path] | None = None,
) -> None:
    """Ensure the Python import system can locate project and task modules."""
    if base_dir is None:
        base_path: Path | None = None
    else:
        base_path = Path(base_dir).resolve()
    candidates: list[Path] = []
    if base_path and base_path.is_dir():
        candidates.append(base_path)
        if module_path:
            module_parts = [part for part in module_path.split(".") if part]
            if module_parts:
                module_dir = base_path.joinpath(*module_parts).resolve()
                module_parent = module_dir.parent
                if module_parent.is_dir():
                    candidates.append(module_parent)
                if module_dir.is_dir():
                    candidates.append(module_dir)
        if extra_paths:
            for entry in extra_paths:
                entry_path = Path(entry)
                if entry_path.is_absolute():
                    candidates.append(entry_path.resolve())
                elif base_path:
                    candidates.append(base_path.joinpath(entry_path).resolve())

    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or not candidate.is_dir():
            continue
        candidate_str = str(candidate)
        if candidate_str in seen:
            continue
        seen.add(candidate_str)
        if candidate_str not in sys.path:
            sys.path.insert(0, candidate_str)


@dataclass(frozen=True)
class RuntimeConfig:
    """Runtime configuration resolved from the ``config`` block in ``kptn.yaml``.

    The configuration is resolved as follows:

    * Plain values are copied as-is (after deep-merging any ``include`` files).
    * Strings of the form ``module.path:function`` are imported and executed. If the
      callable defines a ``task_info`` parameter, a dictionary containing the current
      task metadata will be provided when available (with ``None`` defaults otherwise).
    * ``include`` entries are treated as JSON/YAML files whose contents are
      merged into the runtime configuration prior to resolving remaining keys.
    """

    _data: Mapping[str, Any]
    _fallback: Any | None = None

    _CALLABLE_PATTERN = re.compile(
        r"^(?P<module>[A-Za-z_][\w.]*):(?P<attr>[A-Za-z_][\w.]*)$"
    )

    @classmethod
    def from_tasks_config(
        cls,
        tasks_config: Mapping[str, Any] | None,
        *,
        base_dir: str | Path | None = None,
        fallback: Any | None = None,
        task_info: Mapping[str, Any] | None = None,
    ) -> "RuntimeConfig":
        """Create a runtime config from a full tasks configuration mapping."""

        config_block = deepcopy(tasks_config.get("config", {}) if tasks_config else {})
        module_path = getattr(fallback, "PY_MODULE_PATH", None) if fallback else None
        extra_paths = getattr(fallback, "PY_TASKS_DIRS", None) if fallback else None
        ensure_pythonpath(base_dir, module_path, extra_paths)
        resolved = cls._resolve_config_block(config_block, base_dir, task_info)
        return cls(resolved, fallback)

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any] | None,
        *,
        base_dir: str | Path | None = None,
        fallback: Any | None = None,
        task_info: Mapping[str, Any] | None = None,
    ) -> "RuntimeConfig":
        """Alternate constructor when only the ``config`` block is available."""

        config_block = deepcopy(config or {})
        module_path = getattr(fallback, "PY_MODULE_PATH", None) if fallback else None
        extra_paths = getattr(fallback, "PY_TASKS_DIRS", None) if fallback else None
        ensure_pythonpath(base_dir, module_path, extra_paths)
        resolved = cls._resolve_config_block(config_block, base_dir, task_info)
        return cls(resolved, fallback)

    @classmethod
    def _resolve_config_block(
        cls,
        config_block: Mapping[str, Any],
        base_dir: str | Path | None,
        task_info: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        base_path = Path(base_dir) if base_dir else Path.cwd()
        resolved_entry = cls._resolve_entry(dict(config_block), base_path, task_info)
        resolved_value = resolved_entry.value
        if not isinstance(resolved_value, Mapping):
            raise RuntimeConfigError("Config block must decode to a mapping")

        resolved = dict(resolved_value)
        if resolved_entry.aliases:
            cls._apply_aliases(resolved, resolved_entry.aliases)

        cls._apply_duckdb_overrides(resolved)
        return resolved

    @classmethod
    def _apply_duckdb_overrides(cls, resolved: dict[str, Any]) -> None:
        """Normalise duckdb config entries to maintain backwards compatibility."""
        duckdb_entry = resolved.get("duckdb")
        if not isinstance(duckdb_entry, Mapping):
            return

        if "function" not in duckdb_entry:
            if any(key in duckdb_entry for key in ("alias", "parameter_name")):
                raise RuntimeConfigError(
                    "DuckDB config mapping must define a 'function' entry"
                )
            # Entry already resolved (new-style config); nothing further required.
            return

        connection = duckdb_entry.get("function")
        if connection is None:
            raise RuntimeConfigError(
                "DuckDB config mapping must define a 'function' entry"
            )

        alias = duckdb_entry.get("alias")
        if alias is None and "parameter_name" in duckdb_entry:
            alias = duckdb_entry.get("parameter_name")

        alias = cls._normalise_alias(alias)

        resolved["duckdb"] = connection

        if alias:
            resolved[alias] = connection

    @classmethod
    def _resolve_value(
        cls,
        value: Any,
        base_path: Path,
        task_info: Mapping[str, Any] | None,
    ) -> Any:
        return cls._resolve_entry(value, base_path, task_info).value

    @classmethod
    def _resolve_entry(
        cls,
        value: Any,
        base_path: Path,
        task_info: Mapping[str, Any] | None,
    ) -> "_ResolvedEntry":
        if isinstance(value, Mapping):
            return cls._resolve_mapping_entry(dict(value), base_path, task_info)
        if isinstance(value, list):
            resolved_items: list[Any] = []
            for item in value:
                item_entry = cls._resolve_entry(item, base_path, task_info)
                if item_entry.aliases:
                    raise RuntimeConfigError(
                        "Alias definitions are not supported inside lists"
                    )
                resolved_items.append(item_entry.value)
            return _ResolvedEntry(resolved_items, [])
        if isinstance(value, str):
            callable_result = cls._maybe_call_callable(value.strip(), task_info)
            if callable_result is not _Sentinel.NO_RESULT:
                return _ResolvedEntry(callable_result, [])
            return _ResolvedEntry(value, [])
        return _ResolvedEntry(value, [])

    @classmethod
    def _resolve_mapping_entry(
        cls,
        mapping: dict[str, Any],
        base_path: Path,
        task_info: Mapping[str, Any] | None,
    ) -> "_ResolvedEntry":
        if cls._is_config_entry_mapping(mapping):
            return cls._resolve_config_entry_mapping(mapping, base_path, task_info)

        include_value = mapping.pop("include", None)

        merged: dict[str, Any] = {}
        if include_value is not None:
            for include_path in cls._normalise_includes(include_value):
                include_data_raw = cls._load_include(base_path, include_path)
                include_entry = cls._resolve_entry(include_data_raw, base_path, task_info)
                include_value_resolved = include_entry.value
                if not isinstance(include_value_resolved, Mapping):
                    raise RuntimeConfigError(
                        f"Included file '{include_path}' did not decode to a mapping"
                    )
                merged = cls._deep_merge(merged, dict(include_value_resolved))
                if include_entry.aliases:
                    cls._apply_aliases(merged, include_entry.aliases)

        current: dict[str, Any] = {}
        alias_entries: list[tuple[str, Any]] = []
        for key, raw_value in mapping.items():
            if key == "include":
                continue
            resolved_entry = cls._resolve_entry(raw_value, base_path, task_info)
            current[key] = resolved_entry.value
            if resolved_entry.aliases:
                alias_entries.extend(resolved_entry.aliases)

        resolved = cls._deep_merge(merged, current)
        if alias_entries:
            cls._apply_aliases(resolved, alias_entries)

        return _ResolvedEntry(resolved, [])

    @classmethod
    def _resolve_config_entry_mapping(
        cls,
        mapping: dict[str, Any],
        base_path: Path,
        task_info: Mapping[str, Any] | None,
    ) -> "_ResolvedEntry":
        include_value = mapping.pop("include", None)
        if include_value is not None:
            raise RuntimeConfigError("Config entry mappings do not support 'include'")

        alias_raw = mapping.pop("alias", None)
        parameter_raw = mapping.pop("parameter_name", None)
        alias_name = cls._coalesce_alias(alias_raw, parameter_raw)

        has_value = "value" in mapping
        has_function = "function" in mapping
        if has_value and has_function:
            raise RuntimeConfigError(
                "Config entry cannot define both 'value' and 'function'"
            )
        if not has_value and not has_function:
            raise RuntimeConfigError(
                "Config entry must define either 'value' or 'function'"
            )

        if has_function:
            function_spec = mapping.pop("function")
            if not isinstance(function_spec, str):
                raise RuntimeConfigError(
                    "Config entry 'function' must be provided as a string"
                )
            resolved_function = cls._resolve_entry(function_spec, base_path, task_info)
            if resolved_function.aliases:
                raise RuntimeConfigError(
                    "Function specifications cannot define alias entries"
                )
            resolved_value = resolved_function.value
        else:
            value_spec = mapping.pop("value")
            resolved_value_entry = cls._resolve_entry(value_spec, base_path, task_info)
            if resolved_value_entry.aliases:
                raise RuntimeConfigError(
                    "Alias definitions are not supported within config 'value' fields"
                )
            resolved_value = resolved_value_entry.value

        if mapping:
            extra_keys = ", ".join(sorted(mapping.keys()))
            raise RuntimeConfigError(
                f"Config entry mapping contains unsupported keys: {extra_keys}"
            )

        alias_entries: list[tuple[str, Any]] = []
        if alias_name:
            alias_entries.append((alias_name, resolved_value))

        return _ResolvedEntry(resolved_value, alias_entries)

    @staticmethod
    def _is_config_entry_mapping(mapping: Mapping[str, Any]) -> bool:
        return "value" in mapping or "function" in mapping

    @classmethod
    def _coalesce_alias(cls, alias: Any, parameter_name: Any) -> str | None:
        alias_set = alias is not None
        parameter_set = parameter_name is not None

        if alias_set and parameter_set:
            alias_name = cls._normalise_alias(alias)
            parameter_alias = cls._normalise_alias(parameter_name)
            if alias_name != parameter_alias:
                raise RuntimeConfigError(
                    "Config entry defines conflicting 'alias' and 'parameter_name' values"
                )
            return alias_name

        if alias_set:
            return cls._normalise_alias(alias)
        if parameter_set:
            return cls._normalise_alias(parameter_name)
        return None

    @staticmethod
    def _apply_aliases(target: dict[str, Any], aliases: Iterable[tuple[str, Any]]) -> None:
        for alias_name, alias_value in aliases:
            target[alias_name] = alias_value

    @staticmethod
    def _normalise_alias(alias: Any) -> str | None:
        if alias is None:
            return None
        if not isinstance(alias, str):
            raise RuntimeConfigError("Alias must be provided as a string")
        alias_str = alias.strip()
        if not alias_str:
            raise RuntimeConfigError("Alias strings must not be empty")
        if not alias_str.isidentifier():
            raise RuntimeConfigError(
                f"Alias '{alias_str}' is not a valid identifier"
            )
        return alias_str

    @classmethod
    def _maybe_call_callable(
        cls,
        candidate: str,
        task_info: Mapping[str, Any] | None,
    ) -> Any:
        match = cls._CALLABLE_PATTERN.match(candidate)
        if not match:
            return _Sentinel.NO_RESULT

        module = importlib.import_module(match.group("module"))
        attr_path = match.group("attr")
        attr = module
        for component in attr_path.split('.'):
            try:
                attr = getattr(attr, component)
            except AttributeError as exc:
                raise RuntimeConfigError(
                    f"Attribute '{attr_path}' not found in module '{module.__name__}'"
                ) from exc

        if not callable(attr):
            raise RuntimeConfigError(
                f"Resolved attribute '{attr_path}' from module '{module.__name__}' is not callable"
            )

        signature = inspect.signature(attr)
        if "task_info" in signature.parameters:
            payload = cls._prepare_task_info(task_info)
            return attr(task_info=payload)

        return attr()

    @staticmethod
    def _prepare_task_info(task_info: Mapping[str, Any] | None) -> dict[str, Any]:
        payload = dict(task_info or {})
        payload.setdefault("task_name", None)
        lang = payload.get("task_lang")
        if lang is None:
            lang = payload.get("task_language")
        payload["task_lang"] = lang
        payload["task_language"] = lang
        return payload

    @classmethod
    def _load_include(cls, base_path: Path, include_entry: str) -> Any:
        resolved_path = (base_path / include_entry).resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"Config include '{include_entry}' not found at {resolved_path}")

        suffix = resolved_path.suffix.lower()
        if suffix == ".json":
            with resolved_path.open("r", encoding="utf-8") as file:
                return json.load(file)
        if suffix in (".yml", ".yaml"):
            with resolved_path.open("r", encoding="utf-8") as file:
                return yaml.safe_load(file)

        with resolved_path.open("r", encoding="utf-8") as file:
            return file.read()

    @classmethod
    def _normalise_includes(cls, include_value: Any) -> Iterable[str]:
        if isinstance(include_value, str):
            return [include_value]
        if isinstance(include_value, Iterable):
            includes: list[str] = []
            for entry in include_value:
                if not isinstance(entry, str):
                    raise RuntimeConfigError("Include entries must be strings")
                includes.append(entry)
            return includes
        raise RuntimeConfigError("Include must be a string or list of strings")

    @classmethod
    def _deep_merge(cls, first: Mapping[str, Any], second: Mapping[str, Any]) -> dict[str, Any]:
        merged = dict(first)
        for key, value in second.items():
            if key in merged and isinstance(merged[key], Mapping) and isinstance(value, Mapping):
                merged[key] = cls._deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged

    def as_dict(self) -> dict[str, Any]:
        """Return a copy of the resolved configuration."""

        return dict(self._data)

    def get(self, key: str, default: Any | None = None) -> Any:
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def __getattr__(self, item: str) -> Any:
        if item in self._data:
            return self._data[item]
        fallback = object.__getattribute__(self, "_fallback")
        if fallback is not None and hasattr(fallback, item):
            return getattr(fallback, item)
        raise AttributeError(item)

    def __repr__(self) -> str:
        return f"RuntimeConfig({self._data!r})"


class _Sentinel:
    NO_RESULT = object()


@dataclass
class _ResolvedEntry:
    value: Any
    aliases: list[tuple[str, Any]]
