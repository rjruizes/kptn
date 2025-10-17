from __future__ import annotations

import importlib
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
#   engine: src.utils:get_engine()
#   include:
#   - config.json


class RuntimeConfigError(RuntimeError):
    """Raised when runtime configuration cannot be interpreted."""


def ensure_pythonpath(
    base_dir: str | Path | None,
    module_path: str | None = None,
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
    """Runtime configuration resolved from the ``config`` block in ``kapten.yaml``.

    The configuration is resolved as follows:

    * Plain values are copied as-is (after deep-merging any ``include`` files).
    * Strings of the form ``module.path:function()`` are imported and executed.
    * ``include`` entries are treated as JSON/YAML files whose contents are
      merged into the runtime configuration prior to resolving remaining keys.
    """

    _data: Mapping[str, Any]
    _fallback: Any | None = None

    _CALLABLE_PATTERN = re.compile(
        r"^(?P<module>[A-Za-z_][\w.]*):(?P<attr>[A-Za-z_][\w.]*)\(\)$"
    )

    @classmethod
    def from_tasks_config(
        cls,
        tasks_config: Mapping[str, Any] | None,
        *,
        base_dir: str | Path | None = None,
        fallback: Any | None = None,
    ) -> "RuntimeConfig":
        """Create a runtime config from a full tasks configuration mapping."""

        config_block = deepcopy(tasks_config.get("config", {}) if tasks_config else {})
        module_path = getattr(fallback, "PY_MODULE_PATH", None) if fallback else None
        ensure_pythonpath(base_dir, module_path)
        resolved = cls._resolve_config_block(config_block, base_dir)
        return cls(resolved, fallback)

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any] | None,
        *,
        base_dir: str | Path | None = None,
        fallback: Any | None = None,
    ) -> "RuntimeConfig":
        """Alternate constructor when only the ``config`` block is available."""

        config_block = deepcopy(config or {})
        module_path = getattr(fallback, "PY_MODULE_PATH", None) if fallback else None
        ensure_pythonpath(base_dir, module_path)
        resolved = cls._resolve_config_block(config_block, base_dir)
        return cls(resolved, fallback)

    @classmethod
    def _resolve_config_block(
        cls,
        config_block: Mapping[str, Any],
        base_dir: str | Path | None,
    ) -> dict[str, Any]:
        base_path = Path(base_dir) if base_dir else Path.cwd()

        include_value = config_block.pop("include", None)

        merged: dict[str, Any] = {}
        if include_value is not None:
            for include_path in cls._normalise_includes(include_value):
                include_data_raw = cls._load_include(base_path, include_path)
                include_data = cls._resolve_value(include_data_raw, base_path)
                if not isinstance(include_data, Mapping):
                    raise RuntimeConfigError(
                        f"Included file '{include_path}' did not decode to a mapping"
                    )
                merged = cls._deep_merge(merged, dict(include_data))

        current = {
            key: cls._resolve_value(value, base_path)
            for key, value in config_block.items()
            if key != "include"
        }

        resolved = cls._deep_merge(merged, current)
        cls._apply_duckdb_overrides(resolved)
        return resolved

    @classmethod
    def _apply_duckdb_overrides(cls, resolved: dict[str, Any]) -> None:
        """Normalise duckdb config entries to maintain backwards compatibility."""
        duckdb_entry = resolved.get("duckdb")
        if not isinstance(duckdb_entry, Mapping):
            return

        connection = duckdb_entry.get("function")
        if connection is None:
            raise RuntimeConfigError(
                "DuckDB config mapping must define a 'function' entry"
            )

        alias = duckdb_entry.get("parameter_name")
        if isinstance(alias, str):
            alias = alias.strip()
            if alias and not alias.isidentifier():
                raise RuntimeConfigError(
                    f"DuckDB config alias '{alias}' is not a valid identifier"
                )
        else:
            alias = None

        resolved["duckdb"] = connection

        if alias:
            resolved[alias] = connection

    @classmethod
    def _resolve_value(cls, value: Any, base_path: Path) -> Any:
        if isinstance(value, Mapping):
            return cls._resolve_mapping(dict(value), base_path)
        if isinstance(value, list):
            return [cls._resolve_value(item, base_path) for item in value]
        if isinstance(value, str):
            callable_result = cls._maybe_call_callable(value.strip())
            if callable_result is not _Sentinel.NO_RESULT:
                return callable_result
            return value
        return value

    @classmethod
    def _resolve_mapping(cls, mapping: dict[str, Any], base_path: Path) -> dict[str, Any]:
        include_value = mapping.pop("include", None)

        merged: dict[str, Any] = {}
        if include_value is not None:
            for include_path in cls._normalise_includes(include_value):
                include_data_raw = cls._load_include(base_path, include_path)
                include_data = cls._resolve_value(include_data_raw, base_path)
                if not isinstance(include_data, Mapping):
                    raise RuntimeConfigError(
                        f"Included file '{include_path}' did not decode to a mapping"
                    )
                merged = cls._deep_merge(merged, dict(include_data))

        current = {
            key: cls._resolve_value(value, base_path)
            for key, value in mapping.items()
            if key != "include"
        }

        return cls._deep_merge(merged, current)

    @classmethod
    def _maybe_call_callable(cls, candidate: str) -> Any:
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

        return attr()

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
