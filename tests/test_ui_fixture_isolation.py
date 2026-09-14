"""The fixture project must arrive clean, whatever the source directory holds.

``tests/fixtures/ui_project/.kptn/`` is git-ignored rather than absent: run
the UI tests once and a task-state database is left sitting in the *source*
project. A plain ``copytree`` then carries it into every copy, which makes
``ui_pipeline.noisy_task`` skip as **cached** -- the run succeeds instantly
having printed nothing, and the acceptance test's ``assert "ordinary output"
in page`` fails.

That failure mode is worth a test of its own because of how it lies: a fresh
``git worktree`` has no ignored files, so the same test passes at baseline
and the diff under review looks guilty. The copy is the one place that can
make the fixture independent of whatever the developer's last run left
behind.
"""

from __future__ import annotations

from pathlib import Path

from tests.conftest import copy_fixture_project


def _source_project(tmp_path: Path) -> Path:
    """A fixture-shaped source, complete with the state a run leaves behind."""
    source = tmp_path / "source"
    (source / ".kptn" / "runs").mkdir(parents=True)
    (source / ".kptn" / "kptn.db").write_bytes(b"stale task state")
    (source / ".kptn" / "ui.db").write_bytes(b"stale run history")
    (source / "__pycache__").mkdir()
    (source / "__pycache__" / "ui_pipeline.pyc").write_bytes(b"stale bytecode")
    (source / "kptn.yaml").write_text("pipeline: fixture\n")
    (source / "ui_pipeline.py").write_text("# the pipeline itself\n")
    return source


def test_copy_leaves_previous_run_state_behind(tmp_path: Path) -> None:
    """The copy starts with no runs and no task cache, every time."""
    project = copy_fixture_project(_source_project(tmp_path), tmp_path, "project")

    assert not (project / ".kptn").exists(), (
        "the copy inherited a previous run's state, so its tasks are cached"
    )


def test_copy_leaves_stale_bytecode_behind(tmp_path: Path) -> None:
    """``__pycache__`` is the same trap one layer down.

    Each copy is a different directory that imports a module of the same
    name, and a stale ``.pyc`` is how one copy's pipeline ends up running in
    another's.
    """
    project = copy_fixture_project(_source_project(tmp_path), tmp_path, "project")

    assert not (project / "__pycache__").exists()


def test_copy_brings_the_project_itself(tmp_path: Path) -> None:
    """The other half: skipping state must not skip the pipeline."""
    project = copy_fixture_project(_source_project(tmp_path), tmp_path, "project")

    assert (project / "kptn.yaml").read_text() == "pipeline: fixture\n"
    assert (project / "ui_pipeline.py").is_file()
