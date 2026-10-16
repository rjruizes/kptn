"""Two working directories of one release, sharing built tables through kptn.run."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest

import kptn
from kptn.runner.events import EventKind
from kptn.sources.shared import SHARED_CACHE_ENV


class Recorder:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)

    def shared(self, name: str) -> str | None:
        [e] = [e for e in self.events if e.kind is EventKind.TASK_FINISHED and e.task_name == name]
        return e.payload.get("shared")


def _make_project(root: Path) -> None:
    (root / "in" / "a").mkdir(parents=True)
    (root / "kptn.yaml").write_text("settings:\n  db: duckdb\n  shared_cache: ../_kptn_cache\n")
    (root / "in" / "a" / "t.csv").write_text("x\n1\n")


def _pipeline(root: Path) -> tuple[kptn.Pipeline, list[int]]:
    seen: list[int] = []

    def get_engine():
        return duckdb.connect(str(root / "pipeline.db"))

    @kptn.task(outputs=[])
    def consumer(engine) -> None:
        seen.append(engine.sql('SELECT count(*) FROM raw."a_t"').fetchone()[0])

    raw_ingest = kptn.Stage("raw_ingest", kptn.source("a", files="in/a"))
    pipeline = kptn.Pipeline(
        "p",
        kptn.config(duckdb=(get_engine, "engine")) >> raw_ingest >> kptn.source_views(raw_ingest) >> consumer,
    )
    return pipeline, seen


def _run(pipeline: kptn.Pipeline, **kwargs) -> Recorder:
    rec = Recorder()
    kptn.run(pipeline, event_sink=rec, **kwargs)
    return rec


@pytest.fixture
def release(tmp_path: Path, monkeypatch) -> tuple[Path, Path, Path]:
    monkeypatch.delenv(SHARED_CACHE_ENV, raising=False)
    alice = tmp_path / "r6" / "alice_main"
    _make_project(alice)
    bob = tmp_path / "r6" / "bob_feature"
    shutil.copytree(alice, bob)  # copy2 keeps sizes and modification times
    return tmp_path / "r6" / "_kptn_cache", alice, bob


def test_second_working_directory_restores_the_first_ones_build(release, monkeypatch) -> None:
    cache, alice, bob = release
    monkeypatch.chdir(alice)
    pipeline, seen = _pipeline(alice)
    with pytest.warns(UserWarning):  # no check file
        rec = _run(pipeline)
    assert rec.shared("a[t]") == "published" and seen == [1]

    monkeypatch.chdir(bob)
    pipeline, seen = _pipeline(bob)
    rec = _run(pipeline)
    assert rec.shared("a[t]") == "restored"
    assert seen == [1]  # the view reads through the symlink
    assert (bob / "_bronze/a/t.parquet").resolve().is_relative_to(cache.resolve())


def test_env_off_builds_locally(release, monkeypatch) -> None:
    cache, alice, _ = release
    monkeypatch.chdir(alice)
    monkeypatch.setenv(SHARED_CACHE_ENV, "off")
    pipeline, seen = _pipeline(alice)
    with pytest.warns(UserWarning):
        rec = _run(pipeline)
    assert rec.shared("a[t]") is None and seen == [1]
    assert not (alice / "_bronze/a/t.parquet").is_symlink()
    assert not cache.exists()


def test_force_builds_locally_over_a_link(release, monkeypatch) -> None:
    cache, alice, _ = release
    monkeypatch.chdir(alice)
    pipeline, _ = _pipeline(alice)
    with pytest.warns(UserWarning):
        _run(pipeline)
    assert (alice / "_bronze/a/t.parquet").is_symlink()
    pipeline, seen = _pipeline(alice)
    with pytest.warns(UserWarning):
        rec = _run(pipeline, force=True)
    assert rec.shared("a[t]") is None and seen == [1]
    assert not (alice / "_bronze/a/t.parquet").is_symlink()
    assert len(list((cache / "a" / "t").iterdir())) == 1  # nothing new published
