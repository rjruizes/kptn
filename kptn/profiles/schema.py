from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

StageSelection = list[str]
ArgsOverride = dict[str, Any]


class KptnSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    db: str = "sqlite"
    db_path: str | None = None
    cache_namespace: str | None = None
    #: Where built source tables are shared between working directories,
    #: relative to the project root. Unset, or not an existing directory:
    #: no sharing.
    shared_bronze: str | None = None
    #: Where recorded runs keep their logs, one subdirectory per working
    #: directory, relative to the project root. Unset, or not an existing
    #: directory: ``.kptn/runs``.
    shared_logs: str | None = None


class ProfileSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    extends: str | list[str] | None = None
    args: dict[str, ArgsOverride] = {}
    start_from: str | None = None
    stop_after: str | None = None
    stage_selections: dict[str, StageSelection] = {}
    optional_groups: dict[str, bool] = {}


class KptnConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    settings: KptnSettings = KptnSettings()
    profiles: dict[str, ProfileSpec] = {}
