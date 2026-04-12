from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from kptn.exceptions import ProfileError
from kptn.profiles.schema import KptnConfig, KptnSettings, ProfileSpec

_KNOWN_PROFILE_KEYS = frozenset({"extends", "args", "start_from", "stop_after"})


def _parse_profile_spec(raw: dict[str, Any]) -> ProfileSpec:
    known = {k: v for k, v in raw.items() if k in _KNOWN_PROFILE_KEYS}
    stage_selections: dict[str, list[str]] = {}
    optional_groups: dict[str, bool] = {}
    for k, v in raw.items():
        if k in _KNOWN_PROFILE_KEYS:
            continue
        if isinstance(v, bool):
            optional_groups[k] = v
        elif isinstance(v, list):
            stage_selections[k] = v
        # Any other types are ignored (forward-compat)
    return ProfileSpec(**known, stage_selections=stage_selections, optional_groups=optional_groups)


class ProfileLoader:
    @classmethod
    def load(cls, path: str | Path) -> KptnConfig:
        if not Path(path).exists():
            return KptnConfig()

        try:
            with open(path) as f:
                raw = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ProfileError(f"{path}: YAML parse error: {e}") from e

        if raw is None:
            return KptnConfig()
        if not isinstance(raw, dict):
            raise ProfileError(f"{path}: expected a YAML mapping at top level")

        settings_raw = raw.get("settings", {})
        if not isinstance(settings_raw, dict):
            raise ProfileError(f"{path}: settings: expected a mapping")
        try:
            settings = KptnSettings(**settings_raw)
        except ValidationError as e:
            raise ProfileError(f"{path}: settings: {e}") from e

        profiles_raw = raw.get("profiles", {})
        if not isinstance(profiles_raw, dict):
            raise ProfileError(f"{path}: profiles: expected a mapping")

        profiles_dict: dict[str, ProfileSpec] = {}
        for name, profile_raw in profiles_raw.items():
            if not isinstance(profile_raw, dict):
                raise ProfileError(
                    f"{path}: profiles.{name}: expected a mapping, "
                    f"got {type(profile_raw).__name__}"
                )
            try:
                profiles_dict[name] = _parse_profile_spec(profile_raw)
            except ValidationError as e:
                raise ProfileError(f"{path}: profiles.{name}: {e}") from e

        return KptnConfig(settings=settings, profiles=profiles_dict)
