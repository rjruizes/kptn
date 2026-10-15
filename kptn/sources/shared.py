"""The shared cache: built source tables kept by content key and linked into ``_bronze/``.

Layout, under the root named by ``settings.shared_cache``::

    .tmp/<id>/                       a publish in progress, renamed into place when complete
    <source>/<table>/<key>/
        entry.json                   what built it, from which files; never read by kptn
        <table>.parquet

An entry is immutable and complete whenever it exists. Several Unix accounts
share one cache over NFS, so everything kptn creates here gets explicit
group-writable modes, and a failed rename is trusted only if the entry is then
missing (an NFS client can retry a rename whose reply was lost and report an
error although the first attempt succeeded).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from kptn.exceptions import ProfileError

#: ``off`` turns sharing off for a whole session; ``kptn run --no-shared-cache`` sets it.
SHARED_CACHE_ENV = "KPTN_SHARED_CACHE"


@dataclass(frozen=True)
class SharedCache:
    root: Path


def resolve_shared_cache(
    setting: str | None,
    project_root: Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> SharedCache | None:
    """The cache *setting* names, or None when sharing is off.

    A relative *setting* resolves against *project_root*, the directory
    holding ``kptn.yaml``.

    Raises:
        ProfileError: ``KPTN_SHARED_CACHE`` is set to something other than
            ``on`` or ``off``.
    """
    raw = (os.environ if environ is None else environ).get(SHARED_CACHE_ENV, "")
    value = raw.strip().lower()
    if value not in ("", "on", "off"):
        raise ProfileError(f"{SHARED_CACHE_ENV}={raw!r}: expected 'on' or 'off'")
    if setting is None or value == "off":
        return None
    root = Path(setting).expanduser()
    if not root.is_absolute():
        root = project_root / root
    return SharedCache(root.resolve())


def shared_cache_for(project_root: Path) -> SharedCache | None:
    """The shared cache of the project at *project_root*, from its ``kptn.yaml``."""
    from kptn.profiles.loader import ProfileLoader

    settings = ProfileLoader.load(project_root / "kptn.yaml").settings
    return resolve_shared_cache(settings.shared_cache, project_root)
