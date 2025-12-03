from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from kptn.util.pipeline_config import _module_path_from_dir, normalise_dir_setting
from kptn.util.read_tasks_config import read_tasks_config
from kptn.util.task_dirs import resolve_python_task_dirs


class BundleDeciderError(RuntimeError):
    """Raised when the decider bundle cannot be created."""


@dataclass(frozen=True)
class BundleResult:
    bundle_dir: Path
    pipeline_name: str


def _choose_pipeline(kap_conf: Mapping[str, object], requested: str | None) -> str | None:
    graphs = kap_conf.get("graphs") or {}
    if not isinstance(graphs, Mapping) or not graphs:
        return None

    if requested:
        if requested not in graphs:
            available = ", ".join(sorted(graphs))
            raise BundleDeciderError(
                f"Pipeline '{requested}' not found; available pipelines: {available}"
            )
        return requested

    if len(graphs) == 1:
        return next(iter(graphs))

    return None


def _resolve_python_dirs(project_root: Path, kap_conf: Mapping[str, object]) -> list[Path]:
    settings = kap_conf.get("settings") or {}
    if not isinstance(settings, Mapping):
        raise BundleDeciderError("kptn.yaml settings block must be a mapping")

    py_tasks_setting = settings.get("py_tasks_dir")
    explicit_dirs: list[str] = []
    module_path: str | None = None

    if py_tasks_setting is not None:
        try:
            explicit_dirs = normalise_dir_setting(py_tasks_setting, setting_name="py_tasks_dir")
        except (TypeError, ValueError) as exc:
            raise BundleDeciderError(f"Invalid py_tasks_dir setting: {exc}") from exc
        if explicit_dirs:
            try:
                module_path = _module_path_from_dir(explicit_dirs[0])
            except ValueError as exc:
                raise BundleDeciderError(f"Unable to derive module path: {exc}") from exc

    dirs = resolve_python_task_dirs(
        project_root,
        tasks_config=kap_conf,
        module_path=module_path,
        explicit_dirs=explicit_dirs,
    )
    return [path.resolve() for path in dirs]


def _resolve_r_dirs(project_root: Path, kap_conf: Mapping[str, object]) -> list[Path]:
    settings = kap_conf.get("settings") or {}
    if not isinstance(settings, Mapping):
        raise BundleDeciderError("kptn.yaml settings block must be a mapping")

    raw_setting = settings.get("r_tasks_dir")
    entries: list[str]
    if raw_setting is None:
        return []
    try:
        entries = normalise_dir_setting(raw_setting, setting_name="r_tasks_dir")
    except (TypeError, ValueError) as exc:
        raise BundleDeciderError(f"Invalid r_tasks_dir setting: {exc}") from exc
    if not entries:
        return []

    resolved: list[Path] = []
    for entry in entries:
        entry_path = Path(entry)
        if entry_path.is_absolute():
            resolved.append(entry_path.resolve())
        else:
            resolved.append((project_root / entry_path).resolve())
    return resolved


def _collect_task_references(project_root: Path, kap_conf: Mapping[str, object]) -> tuple[set[Path], set[Path]]:
    tasks_section = kap_conf.get("tasks") or {}
    if not isinstance(tasks_section, Mapping):
        return set(), set()

    files: set[Path] = set()
    directories: set[Path] = set()
    for task_spec in tasks_section.values():
        if not isinstance(task_spec, Mapping):
            continue

        file_entry = task_spec.get("file")
        if isinstance(file_entry, str) and file_entry.strip():
            file_part = file_entry.split(":", 1)[0].strip()
            if file_part:
                resolved = _resolve_project_path(project_root, Path(file_part), allow_missing=True)
                _register_reference(project_root, resolved, files, directories)

        py_script = task_spec.get("py_script")
        if isinstance(py_script, str) and py_script.strip():
            resolved = _resolve_project_path(project_root, Path(py_script.strip()), allow_missing=True)
            _register_reference(project_root, resolved, files, directories)

        r_script = task_spec.get("r_script")
        if isinstance(r_script, str) and r_script.strip():
            resolved = _resolve_project_path(project_root, Path(r_script.strip()), allow_missing=True)
            _register_reference(project_root, resolved, files, directories)

    return files, directories


def _resolve_project_path(project_root: Path, path: Path, *, allow_missing: bool = False) -> Path:
    if path.is_absolute():
        resolved = path.resolve()
        if not _is_relative_to(resolved, project_root):
            raise BundleDeciderError(
                f"Task references file outside the project root: {resolved}"
            )
        if not resolved.exists() and not allow_missing:
            raise BundleDeciderError(f"Referenced file does not exist: {resolved}")
        return resolved
    resolved = (project_root / path).resolve()
    if not resolved.exists() and not allow_missing:
        raise BundleDeciderError(f"Referenced file does not exist: {resolved}")
    return resolved


def _find_existing_parent(path: Path, project_root: Path) -> Path | None:
    current = path
    while True:
        if current.exists() and current.is_dir() and _is_relative_to(current, project_root):
            return current
        if current == project_root or current.parent == current:
            return None
        current = current.parent


def _register_reference(
    project_root: Path,
    resolved_path: Path,
    files: set[Path],
    directories: set[Path],
) -> None:
    if resolved_path.exists():
        if resolved_path.is_file():
            files.add(resolved_path)
            parent = resolved_path.parent
            if (
                parent.exists()
                and parent.is_dir()
                and parent != project_root
            ):
                directories.add(parent)
        elif resolved_path.is_dir():
            if resolved_path != project_root:
                directories.add(resolved_path)
        return

    ancestor = _find_existing_parent(resolved_path, project_root)
    if ancestor and ancestor != project_root:
        directories.add(ancestor)


def _ensure_exists(paths: Iterable[Path]) -> None:
    for entry in paths:
        if not entry.exists():
            raise BundleDeciderError(f"Required path does not exist: {entry}")


def _prune_nested_directories(paths: Iterable[Path]) -> list[Path]:
    resolved = sorted({path.resolve() for path in paths}, key=lambda p: (len(p.parts), str(p)))
    pruned: list[Path] = []
    for candidate in resolved:
        if any(_is_relative_to(candidate, existing) for existing in pruned):
            continue
        pruned.append(candidate)
    return pruned


def _run_uv_install(*, installer_args: Sequence[str], runner: callable | None, cwd: Path) -> None:
    if runner is not None:
        runner(installer_args)
        return

    subprocess.run(
        installer_args,
        check=True,
        cwd=cwd,
    )


def _resolve_local_kptn_root() -> Path | None:
    candidate = Path(__file__).resolve().parents[2]
    if not (candidate / "kptn" / "aws" / "decider.py").exists():
        return None

    # Ignore site-packages installs; only treat a real checkout as "local".
    if "site-packages" in candidate.parts:
        return None

    if (candidate / "pyproject.toml").exists():
        return candidate
    return None


def bundle_decider_lambda(
    *,
    project_root: Path,
    output_dir: Path,
    pipeline: str | None = None,
    kptn_source: str | Path | None = None,
    project_source: str | Path | None = None,
    python_version: str = "3.11",
    python_platform: str = "x86_64-manylinux2014",
    installer: callable | None = None,
    install_project: bool = False,
    prefer_local_kptn: bool = True,
) -> BundleResult:
    """Build the kptn decider Lambda bundle for the given project."""
    project_root = project_root.resolve()
    kptn_config_path = project_root / "kptn.yaml"
    if not kptn_config_path.exists():
        raise BundleDeciderError(f"kptn.yaml not found in {project_root}")

    kap_conf = read_tasks_config(str(kptn_config_path))
    pipeline_name = _choose_pipeline(kap_conf, pipeline)

    bundle_dir = output_dir.resolve()
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    install_targets: list[str] = []
    chosen_kptn_source = kptn_source

    # Prefer a locally checked-out kptn source when available unless explicitly disabled.
    if prefer_local_kptn and not chosen_kptn_source:
        candidate = _resolve_local_kptn_root()
        if candidate is not None:
            chosen_kptn_source = candidate

    if chosen_kptn_source:
        install_targets.append(str(chosen_kptn_source))
    else:
        install_targets.append("kptn")

    if install_project:
        if project_source is None:
            project_source = project_root
        install_targets.append(str(project_source))

    base_args = [
        "uv",
        "pip",
        "install",
        "--target",
        str(bundle_dir),
        "--python-version",
        python_version,
        "--python-platform",
        python_platform,
    ]

    for target in install_targets:
        args = [*base_args, target]
        try:
            _run_uv_install(
                installer_args=args,
                runner=installer,
                cwd=project_root,
            )
        except FileNotFoundError as exc:  # pragma: no cover - depends on environment
            raise BundleDeciderError("uv is required to build the decider bundle") from exc
        except subprocess.CalledProcessError as exc:
            raise BundleDeciderError(f"uv failed while installing {target}") from exc

    python_dirs = _resolve_python_dirs(project_root, kap_conf)
    r_dirs = _resolve_r_dirs(project_root, kap_conf)
    task_files, referenced_dirs = _collect_task_references(project_root, kap_conf)

    _ensure_exists(python_dirs)
    _ensure_exists(r_dirs)

    directories: list[Path] = []
    directories.extend(python_dirs)
    directories.extend(r_dirs)
    directories.extend(referenced_dirs)
    directories = [path for path in directories if path != project_root]

    pruned_dirs = _prune_nested_directories(directories)

    for directory in pruned_dirs:
        try:
            rel = directory.relative_to(project_root)
        except ValueError as exc:
            raise BundleDeciderError(
                f"Directory {directory} is outside the project root; adjust kptn.yaml to use project-relative paths"
            ) from exc
        dest = bundle_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(
            directory,
            dest,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )

    for file_path in sorted(task_files):
        if any(_is_relative_to(file_path, directory) for directory in pruned_dirs):
            continue
        try:
            rel = file_path.relative_to(project_root)
        except ValueError as exc:
            raise BundleDeciderError(
                f"File {file_path} is outside the project root; adjust kptn.yaml to use project-relative paths"
            ) from exc
        dest = bundle_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, dest)

    shutil.copy2(kptn_config_path, bundle_dir / "kptn.yaml")

    handler_path = bundle_dir / "lambda_function.py"
    handler_path.write_text(
        "from kptn.aws.decider import handler as _handler\n\n\n"
        "def lambda_handler(event, context):\n"
        "    return _handler(event, context)\n",
        encoding="utf-8",
    )

    return BundleResult(bundle_dir=bundle_dir, pipeline_name=pipeline_name)
def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False
