from __future__ import annotations

from typing import Any, Mapping


def _parse_number(raw: Any) -> float | None:
    """Best-effort parse of numeric CPU/memory values."""
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = raw.strip()
        if raw == "":
            return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _format_number(value: float) -> str:
    """Render floats without trailing zeros (e.g., 2.0 -> '2')."""
    if value.is_integer():
        return str(int(value))
    text = str(value)
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _normalise_vcpu(raw: Any) -> str | None:
    """
    Convert Fargate-style CPU units to the Batch vCPU string expected by resource requirements.

    Values >=64 are treated as CPU units (e.g., 256 -> 0.25 vCPU); smaller values are assumed
    to already be expressed in vCPU to avoid double conversion when users provide fractional CPUs.
    """
    parsed = _parse_number(raw)
    if parsed is None or parsed <= 0:
        return None
    vcpu = parsed if parsed < 64 else parsed / 1024
    return _format_number(vcpu)


def _normalise_memory(raw: Any) -> str | None:
    """Return memory as a string when positive, otherwise None."""
    parsed = _parse_number(raw)
    if parsed is None or parsed <= 0:
        return None
    return _format_number(parsed)


def compute_resource_requirements(compute: Mapping[str, Any] | None) -> list[dict[str, str]]:
    """
    Translate a task's compute block into AWS Batch resource requirement entries.

    Returns dictionaries using lower-case keys matching boto3's submit_job signature:
    [{"type": "VCPU", "value": "0.25"}, {"type": "MEMORY", "value": "512"}]
    """
    if not isinstance(compute, Mapping):
        return []

    requirements: list[dict[str, str]] = []

    vcpu = _normalise_vcpu(compute.get("cpu"))
    if vcpu:
        requirements.append({"type": "VCPU", "value": vcpu})

    memory = _normalise_memory(compute.get("memory"))
    if memory:
        requirements.append({"type": "MEMORY", "value": memory})

    return requirements
