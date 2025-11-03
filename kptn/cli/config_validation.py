from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import yaml
from jsonschema import Draft7Validator, exceptions as jsonschema_exceptions


class SchemaValidationError(RuntimeError):
    """Raised when the schema definition itself is invalid."""


@dataclass(frozen=True)
class ValidationIssue:
    path: str
    message: str


def _format_error_path(error_path: Iterable[object]) -> str:
    formatted = ""
    for segment in error_path:
        if isinstance(segment, int):
            formatted += f"[{segment}]"
        else:
            formatted += f".{segment}" if formatted else str(segment)
    return formatted or "<root>"


def validate_kptn_config(config_path: Path, schema_path: Path) -> List[ValidationIssue]:
    """
    Validate a kptn configuration file against the schema.

    Returns a list of issues; the list is empty when validation succeeds.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"kptn.yaml not found at {config_path.resolve()}")

    if not schema_path.exists():
        raise FileNotFoundError(f"kptn-schema.json not found at {schema_path.resolve()}")

    try:
        config_data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise exc

    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise exc

    try:
        Draft7Validator.check_schema(schema)
    except jsonschema_exceptions.SchemaError as exc:  # pragma: no cover - schema should be valid
        raise SchemaValidationError(f"Schema error in kptn-schema.json: {exc}") from exc

    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(config_data), key=lambda err: err.path)

    issues = [
        ValidationIssue(path=_format_error_path(error.path), message=error.message)
        for error in errors
    ]

    return issues
