from __future__ import annotations

import json
import os
import shutil
import stat
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

import kptn
from kptn.exceptions import ProfileError, TaskError
from kptn.profiles.loader import ProfileLoader
from kptn.sources.build import plan_tables
from kptn.sources.shared import (
    DIR_MODE,
    ENTRY_LABEL,
    FILE_MODE,
    SHARED_BRONZE_ENV,
    SharedBronze,
    link,
    missing_shared_bronze,
    resolve_shared_bronze,
    shared_bronze_for,
)


@pytest.fixture(autouse=True)
def _no_ambient_setting(monkeypatch) -> None:
    monkeypatch.delenv(SHARED_BRONZE_ENV, raising=False)


def test_no_setting_means_no_sharing(tmp_path: Path) -> None:
    assert resolve_shared_bronze(None, tmp_path) is None


def test_relative_setting_resolves_against_project_root(tmp_path: Path) -> None:
    project = tmp_path / "r6" / "alice_main"
    project.mkdir(parents=True)
    (tmp_path / "r6" / "_kptn_bronze").mkdir()
    cache = resolve_shared_bronze("../_kptn_bronze", project)
    assert cache == SharedBronze((tmp_path / "r6" / "_kptn_bronze").resolve())


def test_absolute_setting_is_kept(tmp_path: Path) -> None:
    (tmp_path / "c").mkdir()
    assert resolve_shared_bronze(str(tmp_path / "c"), Path("/elsewhere")) == SharedBronze(
        (tmp_path / "c").resolve()
    )


def test_missing_directory_means_no_sharing(tmp_path: Path) -> None:
    assert resolve_shared_bronze("../_kptn_bronze", tmp_path) is None
    assert not (tmp_path.parent / "_kptn_bronze").exists()


def test_missing_directory_is_reported(tmp_path: Path) -> None:
    assert missing_shared_bronze("../_kptn_bronze", tmp_path) == (
        "[SHARE] off — ../_kptn_bronze does not exist, so tables are built here"
    )


@pytest.mark.parametrize(
    ("setting", "environ"),
    [(None, {}), ("c", {}), ("missing", {SHARED_BRONZE_ENV: "off"})],
    ids=["no setting", "directory exists", "turned off"],
)
def test_nothing_is_reported_otherwise(
    tmp_path: Path, setting: str | None, environ: dict[str, str]
) -> None:
    (tmp_path / "c").mkdir()
    assert missing_shared_bronze(setting, tmp_path, environ=environ) is None


@pytest.mark.parametrize("value", ["off", "OFF", " off "])
def test_env_off_turns_sharing_off(tmp_path: Path, value: str) -> None:
    (tmp_path / "c").mkdir()
    assert resolve_shared_bronze("c", tmp_path, environ={SHARED_BRONZE_ENV: value}) is None


@pytest.mark.parametrize("value", ["", "on"])
def test_env_on_or_empty_defers_to_setting(tmp_path: Path, value: str) -> None:
    (tmp_path / "c").mkdir()
    assert resolve_shared_bronze("c", tmp_path, environ={SHARED_BRONZE_ENV: value}) is not None


def test_env_typo_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ProfileError, match="KPTN_SHARED_BRONZE"):
        resolve_shared_bronze("c", tmp_path, environ={SHARED_BRONZE_ENV: "of"})


def test_env_is_read_from_the_process(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv(SHARED_BRONZE_ENV, "off")
    assert resolve_shared_bronze("c", tmp_path) is None


def test_setting_loads_from_kptn_yaml(tmp_path: Path) -> None:
    (tmp_path / "kptn.yaml").write_text("settings:\n  shared_bronze: ../_kptn_bronze\n")
    (tmp_path.parent / "_kptn_bronze").mkdir()
    assert ProfileLoader.load(tmp_path / "kptn.yaml").settings.shared_bronze == "../_kptn_bronze"
    assert shared_bronze_for(tmp_path) == SharedBronze((tmp_path.parent / "_kptn_bronze").resolve())


def test_shared_bronze_for_without_kptn_yaml(tmp_path: Path) -> None:
    assert shared_bronze_for(tmp_path) is None


# -- lookup, publish and link ------------------------------------------------


def _project(root: Path) -> Path:
    p = root / "in" / "a.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("x,y\n1,2\n3,4\n")
    return root


def _spec(root: Path):
    return kptn.source("s", files=str(root / "in")).__kptn__


def _plan(root: Path):
    [plan] = plan_tables("s", _spec(root), {}, root=root)
    return plan


@pytest.fixture
def strict_umask():
    old = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(old)


def _mode(p: Path) -> int:
    return stat.S_IMODE(p.stat().st_mode)


def _shared(root: Path) -> SharedBronze:
    """A shared bronze whose directory was set up, as kptn requires."""
    root.mkdir()
    return SharedBronze(root)


def _tmp_entries(cache: SharedBronze) -> list[Path]:
    return list((cache.root / ".tmp").iterdir())


def test_lookup_misses_an_empty_cache(tmp_path: Path) -> None:
    root = _project(tmp_path / "w")
    assert SharedBronze(tmp_path / "cache").lookup(_plan(root)) is None


def test_publish_creates_entry_and_label(tmp_path: Path) -> None:
    root = _project(tmp_path / "w")
    cache = _shared(tmp_path / "cache")
    plan = _plan(root)
    with pytest.warns(UserWarning):  # no check file
        target, outcome = cache.publish(plan, _spec(root), root=root)
    assert outcome == "published"
    assert target == cache.entry(plan) / "a.parquet" == cache.lookup(plan)
    assert duckdb.sql(f"SELECT count(*) FROM '{target}'").fetchone() == (2,)
    assert not plan.parquet.exists()  # linking is the caller's step

    label = json.loads((cache.entry(plan) / ENTRY_LABEL).read_text())
    assert (label["key"], label["source"], label["table"]) == (plan.key, "s", "a")
    assert label["code_hash"] == plan.code_hash and label["input_hash"] == plan.input_hash
    [f] = label["files"]
    st = (root / "in/a.csv").stat()
    assert f == {"path": plan.files[0].rel, "size": st.st_size, "mtime_ns": st.st_mtime_ns}
    assert label["check_file"] is None
    assert label["rows"] == 2 and label["columns"] == ["x", "y", "_source_file"]
    assert label["kptn_version"] == kptn.__version__
    assert label["built_by"] and label["host"] and label["built_at"].endswith("+00:00")
    assert _tmp_entries(cache) == []


def test_label_names_the_check_file(tmp_path: Path) -> None:
    root = _project(tmp_path / "w")
    check = root / "checks" / "s" / "a.yml"
    check.parent.mkdir(parents=True)
    check.write_text("checks for a:\n  - row_count > 0\n")
    cache = _shared(tmp_path / "cache")
    plan = _plan(root)
    with patch("kptn.sources.build.run_checks", return_value=[]):
        cache.publish(plan, _spec(root), root=root)
    label = json.loads((cache.entry(plan) / ENTRY_LABEL).read_text())
    assert label["check_file"] == "checks/s/a.yml"


def test_publish_sets_group_modes_whatever_the_umask(tmp_path: Path, strict_umask) -> None:
    root = _project(tmp_path / "w")
    cache = _shared(tmp_path / "cache")
    plan = _plan(root)
    with pytest.warns(UserWarning):
        cache.publish(plan, _spec(root), root=root)
    entry = cache.entry(plan)
    for d in (cache.root / ".tmp", entry.parent.parent, entry.parent, entry):
        # Some systems drop the setgid bit when the owner is not in the
        # directory's group; the rest of the mode must hold everywhere.
        assert _mode(d) & 0o775 == 0o775, d
        if d.stat().st_gid in os.getgroups():
            assert _mode(d) == DIR_MODE, d
    for f in (entry / "a.parquet", entry / ENTRY_LABEL):
        assert _mode(f) == FILE_MODE, f


def test_existing_root_keeps_its_mode(tmp_path: Path) -> None:
    root = _project(tmp_path / "w")
    cache = SharedBronze(tmp_path / "cache")
    cache.root.mkdir(mode=0o755)
    os.chmod(cache.root, 0o755)
    with pytest.warns(UserWarning):
        cache.publish(_plan(root), _spec(root), root=root)
    assert _mode(cache.root) == 0o755


def test_root_removed_after_the_run_started_is_an_error(tmp_path: Path) -> None:
    root = _project(tmp_path / "w")
    cache = SharedBronze(tmp_path / "cache")
    with pytest.raises(TaskError, match="no longer exists"):
        cache.publish(_plan(root), _spec(root), root=root)
    assert not cache.root.exists()


def test_failing_build_publishes_nothing(tmp_path: Path) -> None:
    root = _project(tmp_path / "w")
    cache = _shared(tmp_path / "cache")
    plan = _plan(root)
    with patch("kptn.sources.shared.build_table", side_effect=RuntimeError("check failed")):
        with pytest.raises(RuntimeError):
            cache.publish(plan, _spec(root), root=root)
    assert cache.lookup(plan) is None
    assert _tmp_entries(cache) == []


def test_second_publish_of_a_key_links_to_the_first(tmp_path: Path) -> None:
    a = _project(tmp_path / "alice" / "main")
    b = tmp_path / "bob" / "feature"
    shutil.copytree(a, b)  # same sizes and modification times, so the same key
    assert _plan(a).key == _plan(b).key
    cache = _shared(tmp_path / "cache")
    with pytest.warns(UserWarning):
        first, o1 = cache.publish(_plan(a), _spec(a), root=a)
        second, o2 = cache.publish(_plan(b), _spec(b), root=b)
    assert (o1, o2) == ("published", "linked")
    assert first == second
    assert [p.name for p in cache.entry(_plan(a)).parent.iterdir()] == [_plan(a).key]
    assert _tmp_entries(cache) == []


def test_rename_error_after_success_still_links(tmp_path: Path) -> None:
    """An NFS client may retry a rename whose reply was lost, then report an error."""
    root = _project(tmp_path / "w")
    cache = _shared(tmp_path / "cache")
    real = os.rename

    def lost_reply(src, dst):
        real(src, dst)
        raise FileExistsError(dst)

    with patch("kptn.sources.shared.os.rename", lost_reply), pytest.warns(UserWarning):
        target, outcome = cache.publish(_plan(root), _spec(root), root=root)
    assert outcome == "linked" and target.is_file()


def test_rename_error_without_entry_is_raised(tmp_path: Path) -> None:
    root = _project(tmp_path / "w")
    cache = _shared(tmp_path / "cache")
    with patch("kptn.sources.shared.os.rename", side_effect=PermissionError("denied")):
        with pytest.raises(PermissionError), pytest.warns(UserWarning):
            cache.publish(_plan(root), _spec(root), root=root)
    assert _tmp_entries(cache) == []


def test_link_replaces_a_file_or_link_atomically(tmp_path: Path) -> None:
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    a.write_text("a")
    b.write_text("b")
    parquet = tmp_path / ".kptn" / "_bronze" / "s" / "t.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_text("local")
    link(parquet, a)
    assert parquet.is_symlink() and os.readlink(parquet) == str(a)
    link(parquet, b)
    assert parquet.read_text() == "b" and a.read_text() == "a"
    assert [p.name for p in parquet.parent.iterdir()] == ["t.parquet"]
