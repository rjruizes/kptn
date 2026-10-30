"""Discovery of the working directories a person may serve.

The deployment is a shared folder of release directories, each holding one
working directory per person per branch:

    <root>/r4/rruizesparza_main/pyproject.toml

Only the person's own directories are offered, and only from the latest
release that has one of theirs. Nothing here imports project code: discovery
reads pyproject.toml and kptn.yaml as text, so listing is cheap and cannot
disturb sys.modules.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from kptn_server.registry import ProjectRegistry, default_user, natural_key

USER = "rruizesparza"

PYPROJECT = """\
[project]
name = "fixture"
version = "0.0.0"

[tool.kptn]
pipeline = "ui_pipeline"
"""


def make_workdir(root: Path, release: str, name: str, *, manifest: str = PYPROJECT) -> Path:
    workdir = root / release / name
    workdir.mkdir(parents=True)
    (workdir / "pyproject.toml").write_text(manifest)
    return workdir


def test_offers_the_users_directories_in_the_latest_release(tmp_path: Path) -> None:
    make_workdir(tmp_path, "r1", f"{USER}_main")
    make_workdir(tmp_path, "r2", f"{USER}_main")
    make_workdir(tmp_path, "r2", f"{USER}_featureA")

    registry = ProjectRegistry(tmp_path, USER)
    entries = registry.scan()

    assert registry.release == "r2"
    assert {entry.slug for entry in entries} == {f"{USER}_main", f"{USER}_featureA"}


def test_release_order_is_natural_not_lexicographic(tmp_path: Path) -> None:
    """r10 is later than r2. Sorted as strings it is not."""
    make_workdir(tmp_path, "r2", f"{USER}_main")
    make_workdir(tmp_path, "r10", f"{USER}_main")

    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    assert registry.release == "r10"


def test_latest_release_is_the_latest_with_one_of_mine(tmp_path: Path) -> None:
    """A newer release nobody has cut a directory for yet is not the answer."""
    make_workdir(tmp_path, "r3", f"{USER}_main")
    make_workdir(tmp_path, "r4", "someoneelse_main")

    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    assert registry.release == "r3"


def test_other_peoples_directories_are_listed_as_theirs(tmp_path: Path) -> None:
    make_workdir(tmp_path, "r1", f"{USER}_main")
    make_workdir(tmp_path, "r1", "someoneelse_main")

    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    assert [entry.slug for entry in registry.mine()] == [f"{USER}_main"]
    assert [entry.slug for entry in registry.others()] == ["someoneelse_main"]
    assert registry.resolve("someoneelse_main").owned is False


def test_with_none_of_mine_the_latest_release_with_anyones_is_listed(
    tmp_path: Path,
) -> None:
    make_workdir(tmp_path, "r1", "someoneelse_old")
    make_workdir(tmp_path, "r2", "someoneelse_main")

    registry = ProjectRegistry(tmp_path, USER)

    assert [entry.slug for entry in registry.scan()] == ["someoneelse_main"]
    assert registry.release == "r2"
    assert registry.mine() == ()


def test_someone_elses_broken_project_resolves_only_for_viewing(tmp_path: Path) -> None:
    """Their history needs none of what failed; their pipeline is never served."""
    make_workdir(tmp_path, "r1", f"{USER}_main")
    workdir = make_workdir(tmp_path, "r1", "someoneelse_broken")
    (workdir / "pyproject.toml").write_text("[tool.kptn\n")

    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    assert registry.resolve("someoneelse_broken") is None
    assert registry.resolve("someoneelse_broken", servable_only=False) is not None


def test_a_bare_username_directory_is_mine(tmp_path: Path) -> None:
    make_workdir(tmp_path, "r1", USER)

    registry = ProjectRegistry(tmp_path, USER)

    assert [entry.slug for entry in registry.scan()] == [USER]


def test_a_longer_username_with_the_same_prefix_is_not_mine(tmp_path: Path) -> None:
    """`rruizesparza2_main` is somebody else. Prefix matching stops at the _."""
    make_workdir(tmp_path, "r1", f"{USER}_main")
    make_workdir(tmp_path, "r1", f"{USER}2_main")

    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    assert [entry.slug for entry in registry.mine()] == [f"{USER}_main"]
    assert [entry.slug for entry in registry.others()] == [f"{USER}2_main"]


def test_a_directory_without_tool_kptn_is_not_a_project(tmp_path: Path) -> None:
    make_workdir(tmp_path, "r1", f"{USER}_main")
    make_workdir(
        tmp_path, "r1", f"{USER}_notes", manifest='[project]\nname = "x"\nversion = "0"\n'
    )

    registry = ProjectRegistry(tmp_path, USER)

    assert [entry.slug for entry in registry.scan()] == [f"{USER}_main"]


def test_a_malformed_manifest_is_listed_with_its_error(tmp_path: Path) -> None:
    """Silently missing is worse than visibly broken."""
    make_workdir(tmp_path, "r1", f"{USER}_broken", manifest="[project\n")

    registry = ProjectRegistry(tmp_path, USER)
    (entry,) = registry.scan()

    assert entry.slug == f"{USER}_broken"
    assert entry.error is not None
    assert "pyproject.toml" in entry.error


def test_a_broken_project_does_not_resolve(tmp_path: Path) -> None:
    make_workdir(tmp_path, "r1", f"{USER}_broken", manifest="[project\n")

    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    assert registry.resolve(f"{USER}_broken") is None


def test_profiles_are_read_from_kptn_yaml(tmp_path: Path) -> None:
    """The app bar's selector needs them, and must not load a pipeline to get them."""
    workdir = make_workdir(tmp_path, "r1", f"{USER}_main")
    (workdir / "kptn.yaml").write_text("profiles:\n  dev: {}\n  prod: {}\n")

    registry = ProjectRegistry(tmp_path, USER)
    (entry,) = registry.scan()

    assert entry.profiles == ("dev", "prod")


def test_state_paths_are_under_dot_kptn(tmp_path: Path) -> None:
    workdir = make_workdir(tmp_path, "r1", f"{USER}_main")

    registry = ProjectRegistry(tmp_path, USER)
    (entry,) = registry.scan()

    assert entry.database_path == workdir.resolve() / ".kptn" / "state" / "ui.db"
    assert entry.run_log_dir == workdir.resolve() / ".kptn" / "state" / "runs"


@pytest.mark.parametrize("slug", ["nope", "..", "../..", "r1/rruizesparza_main", "/etc"])
def test_an_unknown_or_crafted_slug_does_not_resolve(tmp_path: Path, slug: str) -> None:
    """The whole security boundary: a slug is looked up, never joined onto a path."""
    make_workdir(tmp_path, "r1", f"{USER}_main")

    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    assert registry.resolve(slug) is None


def test_resolve_rescans_so_a_new_checkout_is_found(tmp_path: Path) -> None:
    make_workdir(tmp_path, "r1", f"{USER}_main")
    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    make_workdir(tmp_path, "r1", f"{USER}_featureB")

    assert registry.resolve(f"{USER}_featureB") is not None


def test_recent_reuses_a_fresh_scan_and_redoes_a_stale_one(tmp_path: Path) -> None:
    """The picker is on every page; it must not rescan NFS on every render."""
    make_workdir(tmp_path, "r1", f"{USER}_main")
    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()
    make_workdir(tmp_path, "r1", f"{USER}_featureB")

    assert [e.slug for e in registry.recent(max_age=60)] == [f"{USER}_main"]
    assert [e.slug for e in registry.recent(max_age=0)] == [
        f"{USER}_featureB",
        f"{USER}_main",
    ]


def test_no_projects_is_an_empty_scan_not_an_exception(tmp_path: Path) -> None:
    """A server that refuses to boot inside a proxy is very hard to diagnose."""
    registry = ProjectRegistry(tmp_path, USER)

    assert registry.scan() == ()
    assert registry.release is None


def test_a_missing_projects_root_is_an_empty_scan(tmp_path: Path) -> None:
    registry = ProjectRegistry(tmp_path / "absent", USER)

    assert registry.scan() == ()


def test_natural_key_orders_numbers_numerically() -> None:
    assert sorted(["r10", "r2", "r1"], key=natural_key) == ["r1", "r2", "r10"]


def test_default_user_prefers_jupyterhub_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JUPYTERHUB_USER", "hub-person")
    monkeypatch.setenv("USER", "shell-person")

    assert default_user() == "hub-person"


def test_default_user_falls_back_to_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JUPYTERHUB_USER", raising=False)
    monkeypatch.setenv("USER", "shell-person")

    assert default_user() == "shell-person"


# -- settings.shared_logs ------------------------------------------------------


def test_run_log_dir_follows_shared_logs(tmp_path: Path) -> None:
    workdir = make_workdir(tmp_path, "r7", f"{USER}_main")
    (workdir / "kptn.yaml").write_text("settings:\n  shared_logs: ../_kptn_logs\n")
    (tmp_path / "r7" / "_kptn_logs").mkdir()

    (entry,) = ProjectRegistry(tmp_path, USER).scan()

    assert entry.run_log_dir == (tmp_path / "r7" / "_kptn_logs" / f"{USER}_main").resolve()
    assert entry.run_log_error is None
    assert not entry.run_log_dir.exists()  # the registry creates nothing


def test_missing_shared_logs_leave_runs_in_dot_kptn(tmp_path: Path) -> None:
    workdir = make_workdir(tmp_path, "r7", f"{USER}_main")
    (workdir / "kptn.yaml").write_text("settings:\n  shared_logs: ../_kptn_logs\n")

    (entry,) = ProjectRegistry(tmp_path, USER).scan()

    assert entry.run_log_dir == workdir.resolve() / ".kptn" / "state" / "runs"
    assert entry.run_log_error is None


def test_shared_logs_outside_the_projects_root_are_not_read(tmp_path: Path) -> None:
    root = tmp_path / "projects"
    workdir = make_workdir(root, "r7", f"{USER}_main")
    (workdir / "kptn.yaml").write_text(f"settings:\n  shared_logs: {tmp_path / 'elsewhere'}\n")
    (tmp_path / "elsewhere").mkdir()

    (entry,) = ProjectRegistry(root, USER).scan()

    assert entry.run_log_error is not None and "outside" in entry.run_log_error
    assert entry.error is None  # still served as a project; only its logs are not read


# -- removed working directories -----------------------------------------------


def _share_logs(workdir: Path) -> Path:
    (workdir / "kptn.yaml").write_text("settings:\n  shared_logs: ../_kptn_logs\n")
    return workdir


def _left_logs(root: Path, release: str, name: str) -> Path:
    logs = root / release / "_kptn_logs" / name
    logs.mkdir(parents=True)
    (logs / "index.json").write_text('{"schema": 1, "runs": []}')
    return logs


def test_a_deleted_directory_with_logs_is_listed_as_removed(tmp_path: Path) -> None:
    _share_logs(make_workdir(tmp_path, "r7", f"{USER}_main"))
    logs = _left_logs(tmp_path, "r7", "bob_old")
    _left_logs(tmp_path, "r7", f"{USER}_main")  # its directory is still there

    entries = {entry.slug: entry for entry in ProjectRegistry(tmp_path, USER).scan()}

    assert set(entries) == {f"{USER}_main", "bob_old"}
    removed = entries["bob_old"]
    assert removed.removed and not removed.owned
    assert removed.database_path is None
    assert removed.run_log_dir == logs.resolve()
    assert not entries[f"{USER}_main"].removed


def test_my_removed_directory_resolves_for_viewing(tmp_path: Path) -> None:
    _share_logs(make_workdir(tmp_path, "r7", f"{USER}_main"))
    _left_logs(tmp_path, "r7", f"{USER}_old")
    registry = ProjectRegistry(tmp_path, USER)
    registry.scan()

    entry = registry.resolve(f"{USER}_old", servable_only=False)

    assert entry is not None and entry.removed and entry.owned


def test_removed_directories_do_not_choose_the_release(tmp_path: Path) -> None:
    make_workdir(tmp_path, "r6", f"{USER}_main")
    _share_logs(make_workdir(tmp_path, "r7", "bob_main"))
    _left_logs(tmp_path, "r7", f"{USER}_old")
    registry = ProjectRegistry(tmp_path, USER)

    registry.scan()

    assert registry.release == "r6"


def test_only_directories_with_an_index_and_no_links_are_listed(tmp_path: Path) -> None:
    _share_logs(make_workdir(tmp_path, "r7", f"{USER}_main"))
    logs = tmp_path / "r7" / "_kptn_logs"
    (logs / "no_index").mkdir(parents=True)
    _left_logs(tmp_path, "r7", "real")
    (logs / "linked").symlink_to(logs / "real")

    slugs = {entry.slug for entry in ProjectRegistry(tmp_path, USER).scan()}

    assert slugs == {f"{USER}_main", "real"}


def test_removed_logs_outside_the_projects_root_are_not_listed(tmp_path: Path) -> None:
    root = tmp_path / "projects"
    workdir = make_workdir(root, "r7", f"{USER}_main")
    (workdir / "kptn.yaml").write_text(f"settings:\n  shared_logs: {tmp_path / 'elsewhere'}\n")
    (tmp_path / "elsewhere" / "ghost").mkdir(parents=True)
    (tmp_path / "elsewhere" / "ghost" / "index.json").write_text("{}")

    assert {entry.slug for entry in ProjectRegistry(root, USER).scan()} == {f"{USER}_main"}
