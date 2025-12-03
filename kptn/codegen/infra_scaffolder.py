from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

# Templates directory location
TEMPLATES_DIR = Path(__file__).parent / "templates" / "stepfunctions"


def _load_template(filename: str) -> str:
    """Load a template file from the templates directory."""
    template_path = TEMPLATES_DIR / filename
    return template_path.read_text(encoding="utf-8")


@dataclass
class ScaffoldReport:
    created: list[Path]
    skipped: list[Path]
    output_dir: Path
    state_machine_files: dict[str, Path]  # graph_name -> file path
    state_machine_files_missing: list[str]  # graph names with missing files
    terraform_tfvars_path: Path | None
    warnings: list[str]

    @property
    def state_machine_file(self) -> Path | None:
        """Backward-compatible accessor for the first state machine definition."""
        if not self.state_machine_files:
            return None
        first_graph = sorted(self.state_machine_files)[0]
        return self.state_machine_files[first_graph]

    @property
    def state_machine_file_exists(self) -> bool:
        """Return True when the first state machine definition file exists."""
        file_path = self.state_machine_file
        return file_path.exists() if file_path else False


def _ensure_trailing_newline(content: str) -> str:
    return content if content.endswith("\n") else f"{content}\n"


def _relative_definition_path(flows_dir: Path, graph_name: str, output_dir: Path) -> tuple[str, Path]:
    # Check for .json.tpl first (template file), then .json
    tpl_path = flows_dir / f"{graph_name}.json.tpl"
    json_path = flows_dir / f"{graph_name}.json"
    definition_path = tpl_path if tpl_path.exists() else json_path
    rel_path = os.path.relpath(definition_path, output_dir)
    return Path(rel_path).as_posix(), definition_path


def _to_hcl(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Path):
        return _to_hcl(str(value))
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(value, set):
        value = sorted(value)
    if isinstance(value, (list, tuple)):
        items = ", ".join(_to_hcl(item) for item in value)
        return f"[{items}]"
    if isinstance(value, dict):
        parts = [f"{key} = {_to_hcl(val)}" for key, val in value.items()]
        inner = ",\n  ".join(parts)
        return "{\n  " + inner + "\n}"
    raise TypeError(f"Unsupported value type for tfvars serialization: {type(value)!r}")


def _format_tfvars(values: dict[str, Any]) -> str:
    lines: list[str] = []
    for key in sorted(values):
        lines.append(f"{key} = {_to_hcl(values[key])}")
    return "\n".join(lines)


def _format_state_machines_default(state_machines: dict[str, dict[str, str]]) -> str:
    """Render a Terraform map for the state_machines variable default."""
    lines = ["{", ""]
    for name, config in sorted(state_machines.items()):
        definition_file = config["definition_file"]
        lines.extend(
            [
                f"  {name} = {{",
                f'    definition_file = "{definition_file}"',
                "  }",
                "",
            ]
        )
    lines.append("}")
    return "\n".join(lines)


def _discover_graph_names(flows_dir: Path) -> list[str]:
    """Return all graph names under flows_dir by inspecting .json/.json.tpl files."""

    def _graph_name(path: Path) -> str:
        # Handle files like foo.json.tpl by stripping both suffixes
        base = path.stem  # remove last suffix
        if base.endswith(".json"):
            base = Path(base).stem
        return base

    graph_names = {
        _graph_name(path)
        for pattern in ("*.json.tpl", "*.json")
        for path in flows_dir.glob(pattern)
    }
    if not graph_names:
        raise ValueError(f"No state machine definitions found in {flows_dir}")
    return sorted(graph_names)


def scaffold_stepfunctions_infra(
    *,
    output_dir: Path,
    pipeline_name: str,
    graph_names: list[str] | None = None,
    flows_dir: Path,
    force: bool = False,
    tfvars_values: dict[str, Any] | None = None,
    warnings: Iterable[str] | None = None,
) -> ScaffoldReport:
    output_dir = output_dir.resolve()
    flows_dir = flows_dir.resolve()

    if graph_names is None:
        graph_names = _discover_graph_names(flows_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Build state_machines map for terraform
    state_machines_map: dict[str, dict[str, str]] = {}
    state_machine_files: dict[str, Path] = {}
    state_machine_files_missing: list[str] = []

    for graph_name in graph_names:
        rel_definition_path, definition_abs_path = _relative_definition_path(
            flows_dir, graph_name, output_dir
        )
        state_machines_map[graph_name] = {"definition_file": rel_definition_path}
        state_machine_files[graph_name] = definition_abs_path
        if not definition_abs_path.exists():
            state_machine_files_missing.append(graph_name)

    files: dict[str, str] = {
        "main.tf": _load_template("main.tf"),
        "networking.tf": _load_template("networking.tf"),
        "ecs.tf": _load_template("ecs.tf"),
        "task_definition.tf": _load_template("task_definition.tf"),
        "lambda.tf": _load_template("lambda.tf"),
        "ecr.tf": _load_template("ecr.tf"),
        "task_execution_role.tf": _load_template("task_execution_role.tf"),
        "task_role.tf": _load_template("task_role.tf"),
        "docker_image.tf": _load_template("docker_image.tf"),
        "dynamodb.tf": _load_template("dynamodb.tf"),
        "efs.tf": _load_template("efs.tf"),
        "batch.tf": _load_template("batch.tf"),
        "locals.tf": _load_template("locals.tf"),
        "stack_info.tf": _load_template("stack_info.tf"),
        "variables.tf": _load_template("variables.tf.tpl").replace(
            "STATE_MACHINES_PLACEHOLDER",
            _format_state_machines_default(state_machines_map),
        ),
        "outputs.tf": _load_template("outputs.tf"),
        "README.md": _load_template("README.md.tpl").format(pipeline_name=pipeline_name),
        "DOCKER_BUILD.md": _load_template("DOCKER_BUILD.md"),
        ".gitignore": _load_template(".gitignore"),
    }

    created: list[Path] = []
    skipped: list[Path] = []

    for filename, template in files.items():
        destination = output_dir / filename
        if destination.exists() and not force:
            skipped.append(destination)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            _ensure_trailing_newline(template.strip()),
            encoding="utf-8",
        )
        created.append(destination)

    tfvars_path: Path | None = None
    if tfvars_values:
        # Add state_machines to tfvars
        tfvars_with_machines = {**tfvars_values, "state_machines": state_machines_map}
        tfvars_path = output_dir / "terraform.tfvars"
        tfvars_content = _ensure_trailing_newline(_format_tfvars(tfvars_with_machines))
        if tfvars_path.exists() and not force:
            skipped.append(tfvars_path)
        else:
            tfvars_path.write_text(tfvars_content, encoding="utf-8")
            created.append(tfvars_path)

    return ScaffoldReport(
        created=created,
        skipped=skipped,
        output_dir=output_dir,
        state_machine_files=state_machine_files,
        state_machine_files_missing=state_machine_files_missing,
        terraform_tfvars_path=tfvars_path,
        warnings=list(warnings or []),
    )
