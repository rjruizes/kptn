from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

PYTHON_SUFFIXES = {".py", ".pyw"}


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    """Return unique resolved paths while preserving original order."""
    seen: set[str] = set()
    unique: list[Path] = []
    for entry in paths:
        resolved = entry.resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    return unique


def _resolve_entry(tasks_root_dir: Path | None, entry: str) -> Path:
    entry_path = Path(entry)
    if entry_path.is_absolute() or tasks_root_dir is None:
        return entry_path.resolve()
    return (tasks_root_dir / entry_path).resolve()


def _normalise_entries(setting: Any) -> list[str]:
    if setting is None:
        return []
    if isinstance(setting, str):
        candidates: Sequence[str] = [setting]
    elif isinstance(setting, Sequence):
        candidates = setting  # type: ignore[assignment]
    else:
        raise TypeError(
            f"'py_tasks_dir' must be a string or sequence of strings, received {type(setting)}"
        )
    entries: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            raise TypeError(
                f"'py_tasks_dir' entries must be strings, received {type(candidate)}"
            )
        cleaned = candidate.strip()
        if not cleaned:
            continue
        entries.append(cleaned)
    return entries


def _collect_py_dir_entries(
    *,
    tasks_config: Mapping[str, Any] | None = None,
    explicit_dirs: Iterable[str] | None = None,
) -> list[str]:
    entries: list[str] = []
    if explicit_dirs:
        entries.extend(entry.strip() for entry in explicit_dirs if entry and entry.strip())
    if not entries and tasks_config:
        setting = tasks_config.get("settings", {}).get("py_tasks_dir")
        try:
            entries.extend(_normalise_entries(setting))
        except (TypeError, ValueError):
            pass
    return entries


def _iter_python_task_files(tasks_config: Mapping[str, Any] | None) -> Iterable[str]:
    if not tasks_config:
        return []
    tasks = tasks_config.get("tasks", {})
    if not isinstance(tasks, Mapping):
        return []
    for task in tasks.values():
        if not isinstance(task, Mapping):
            continue
        file_value = task.get("file")
        if not isinstance(file_value, str):
            continue
        path_part = file_value.split(":", 1)[0].strip()
        if not path_part:
            continue
        if Path(path_part).suffix.lower() in PYTHON_SUFFIXES:
            yield path_part


def _dirs_from_task_files(tasks_root_dir: Path | None, tasks_config: Mapping[str, Any] | None) -> list[Path]:
    dirs: list[Path] = []
    for file_spec in _iter_python_task_files(tasks_config):
        file_path = Path(file_spec)
        if file_path.is_absolute():
            dirs.append(file_path.parent.resolve())
        elif tasks_root_dir is not None:
            dirs.append((tasks_root_dir / file_path).resolve().parent)
    return dirs


def resolve_python_task_dirs(
    tasks_root_dir: Path | None,
    *,
    tasks_config: Mapping[str, Any] | None = None,
    module_path: str | None = None,
    explicit_dirs: Iterable[str] | None = None,
) -> list[Path]:
    """Resolve Python task directories based on kptn task configuration or overrides."""

    entries = _collect_py_dir_entries(tasks_config=tasks_config, explicit_dirs=explicit_dirs)
    resolved_dirs = [_resolve_entry(tasks_root_dir, entry) for entry in entries]

    if not resolved_dirs:
        resolved_dirs.extend(_dirs_from_task_files(tasks_root_dir, tasks_config))

    if not resolved_dirs and module_path and tasks_root_dir is not None:
        module_path_parts = [part for part in module_path.split(".") if part]
        if module_path_parts:
            candidate = tasks_root_dir.joinpath(*module_path_parts).resolve()
            resolved_dirs.append(candidate)

    return _dedupe_paths(resolved_dirs)


def python_module_name_candidates(
    *,
    tasks_config: Mapping[str, Any] | None = None,
    module_path: str | None = None,
    explicit_dirs: Iterable[str] | None = None,
    tasks_root_dir: Path | None = None,
) -> list[str]:
    """Return potential Python module import paths for task packages."""
    candidates: list[str] = []
    if module_path:
        candidates.append(module_path)

    entries = _collect_py_dir_entries(tasks_config=tasks_config, explicit_dirs=explicit_dirs)
    for entry in entries:
        path = Path(entry)
        parts = [part for part in path.parts if part and part != "."]
        if parts:
            dotted = ".".join(parts)
            if dotted not in candidates:
                candidates.append(dotted)
            tail = parts[-1]
            if tail not in candidates:
                candidates.append(tail)

    if tasks_config:
        for file_spec in _iter_python_task_files(tasks_config):
            file_path = Path(file_spec)
            if file_path.is_absolute():
                continue
            parts = [part for part in file_path.with_suffix("").parts if part and part != "."]
            if not parts:
                continue
            parent_parts = parts[:-1]
            if parent_parts:
                dotted_parent = ".".join(parent_parts)
                if dotted_parent not in candidates:
                    candidates.append(dotted_parent)
            if tasks_root_dir:
                try:
                    rel_parent = (tasks_root_dir / file_path).resolve().parent.relative_to(tasks_root_dir.resolve())
                except ValueError:
                    continue
                rel_parts = [part for part in rel_parent.parts if part and part != "."]
                if rel_parts:
                    dotted = ".".join(rel_parts)
                    if dotted not in candidates:
                        candidates.append(dotted)

    return candidates
