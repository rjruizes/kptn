from __future__ import annotations

from pathlib import Path

import pytest

from kptn.exceptions import ProfileError
from kptn.profiles.loader import ProfileLoader
from kptn.sources.shared import SHARED_CACHE_ENV, SharedCache, resolve_shared_cache, shared_cache_for


@pytest.fixture(autouse=True)
def _no_ambient_setting(monkeypatch) -> None:
    monkeypatch.delenv(SHARED_CACHE_ENV, raising=False)


def test_no_setting_means_no_sharing(tmp_path: Path) -> None:
    assert resolve_shared_cache(None, tmp_path) is None


def test_relative_setting_resolves_against_project_root(tmp_path: Path) -> None:
    project = tmp_path / "r6" / "alice_main"
    project.mkdir(parents=True)
    cache = resolve_shared_cache("../_kptn_cache", project)
    assert cache == SharedCache((tmp_path / "r6" / "_kptn_cache").resolve())


def test_absolute_setting_is_kept(tmp_path: Path) -> None:
    assert resolve_shared_cache(str(tmp_path / "c"), Path("/elsewhere")) == SharedCache(
        (tmp_path / "c").resolve()
    )


@pytest.mark.parametrize("value", ["off", "OFF", " off "])
def test_env_off_turns_sharing_off(tmp_path: Path, value: str) -> None:
    assert resolve_shared_cache("c", tmp_path, environ={SHARED_CACHE_ENV: value}) is None


@pytest.mark.parametrize("value", ["", "on"])
def test_env_on_or_empty_defers_to_setting(tmp_path: Path, value: str) -> None:
    assert resolve_shared_cache("c", tmp_path, environ={SHARED_CACHE_ENV: value}) is not None


def test_env_typo_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ProfileError, match="KPTN_SHARED_CACHE"):
        resolve_shared_cache("c", tmp_path, environ={SHARED_CACHE_ENV: "of"})


def test_env_is_read_from_the_process(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv(SHARED_CACHE_ENV, "off")
    assert resolve_shared_cache("c", tmp_path) is None


def test_setting_loads_from_kptn_yaml(tmp_path: Path) -> None:
    (tmp_path / "kptn.yaml").write_text("settings:\n  shared_cache: ../_kptn_cache\n")
    assert ProfileLoader.load(tmp_path / "kptn.yaml").settings.shared_cache == "../_kptn_cache"
    assert shared_cache_for(tmp_path) == SharedCache((tmp_path.parent / "_kptn_cache").resolve())


def test_shared_cache_for_without_kptn_yaml(tmp_path: Path) -> None:
    assert shared_cache_for(tmp_path) is None
