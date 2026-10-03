"""One loaded project at a time.

``kptn.project._prepare_project_imports`` puts a project root on ``sys.path``
and purges from ``sys.modules`` every module belonging to any project root it
has seen. The deployment's working directories are checkouts of the *same*
repository, so two of them define a module of the same name, and loading the
second evicts the first -- leaving the first project's task callables bound
to modules that are no longer loaded. Nothing raises. The wrong code runs.

So these tests use two copies of the fixture project whose pipeline modules
differ observably. That is the actual failure mode, not an approximation of
it: a test with two differently-named modules would pass against a broken
implementation.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from kptn_server.project import ProjectContext, ProjectError
from kptn_server.registry import ProjectEntry
from kptn_server.slot import ProjectSlot
from tests.conftest import FIXTURE_PROJECT, copy_fixture_project


def make_entry(root: Path) -> ProjectEntry:
    resolved = root.resolve()
    return ProjectEntry(
        slug=resolved.name,
        root=resolved,
        release="r1",
        display_name=resolved.name,
        profiles=(),
        database_path=resolved / ".kptn" / "ui.db",
        run_log_dir=resolved / ".kptn" / "runs",
    )


def rename_pipeline(project: Path, name: str) -> None:
    """Make this copy's pipeline distinguishable from the other's.

    The fixture builds its Pipeline with a literal name; rewriting that
    literal is what lets a test tell which checkout is actually loaded.
    """
    source = project / "ui_pipeline.py"
    text = source.read_text()
    assert 'kptn.Pipeline("fixture"' in text, "fixture layout changed; update this helper"
    source.write_text(text.replace('kptn.Pipeline("fixture"', f'kptn.Pipeline("{name}"'))


@pytest.fixture
def two_projects(tmp_path: Path) -> tuple[ProjectEntry, ProjectEntry]:
    first = copy_fixture_project(FIXTURE_PROJECT, tmp_path, "alpha")
    second = copy_fixture_project(FIXTURE_PROJECT, tmp_path, "beta")
    rename_pipeline(first, "alpha-pipeline")
    rename_pipeline(second, "beta-pipeline")
    return make_entry(first), make_entry(second)


def test_loads_a_project_on_first_use(two_projects) -> None:
    alpha, _ = two_projects
    slot = ProjectSlot()

    with slot.use(alpha) as project:
        assert project.root == alpha.root

    assert slot.loaded_root == alpha.root


def test_switching_loads_the_other_project(two_projects) -> None:
    alpha, beta = two_projects
    slot = ProjectSlot()

    with slot.use(alpha) as project:
        assert project.pipeline.name == "alpha-pipeline"
    with slot.use(beta) as project:
        assert project.pipeline.name == "beta-pipeline"


def test_switching_back_reloads_rather_than_returning_a_stale_graph(
    two_projects,
) -> None:
    """The bug this whole design exists to prevent.

    After beta is loaded, alpha's modules have been purged. Handing back the
    cached alpha context would hand back callables bound to evicted modules.
    """
    alpha, beta = two_projects
    slot = ProjectSlot()

    with slot.use(alpha):
        pass
    with slot.use(beta):
        pass
    with slot.use(alpha) as project:
        assert project.pipeline.name == "alpha-pipeline"
        assert project.root == alpha.root


def test_the_same_project_twice_does_not_reload(two_projects) -> None:
    alpha, _ = two_projects
    slot = ProjectSlot()

    with slot.use(alpha) as first:
        pass
    with slot.use(alpha) as second:
        assert first is second


def test_a_preloaded_context_is_not_reloaded(two_projects) -> None:
    """Single-project mode loads at startup to fail fast; the slot honours it."""
    alpha, _ = two_projects
    context = ProjectContext.load(alpha.root)
    slot = ProjectSlot(preloaded=context)

    with slot.use(alpha) as project:
        assert project is context


def test_two_threads_on_different_projects_serialise(two_projects) -> None:
    """No two pipelines live at once, even under concurrent requests."""
    alpha, beta = two_projects
    slot = ProjectSlot()
    inside = threading.Semaphore(0)
    release = threading.Event()
    overlapped = threading.Event()

    def hold() -> None:
        with slot.use(alpha):
            inside.release()
            release.wait(timeout=5)

    def intrude() -> None:
        with slot.use(beta):
            if not release.is_set():
                overlapped.set()

    holder = threading.Thread(target=hold)
    holder.start()
    inside.acquire(timeout=5)

    intruder = threading.Thread(target=intrude)
    intruder.start()
    intruder.join(timeout=0.5)
    assert intruder.is_alive(), "the second project entered while the first was held"

    release.set()
    holder.join(timeout=5)
    intruder.join(timeout=5)
    assert not overlapped.is_set()


def test_a_broken_project_raises_and_leaves_the_slot_empty(tmp_path: Path) -> None:
    """A failed load must not leave a half-loaded project behind as 'current'."""
    from tests.conftest import BROKEN_FIXTURE_PROJECT

    broken = make_entry(copy_fixture_project(BROKEN_FIXTURE_PROJECT, tmp_path, "broken"))
    slot = ProjectSlot()

    with pytest.raises(ProjectError):
        with slot.use(broken):
            pass

    assert slot.loaded_root is None


def _elsewhere(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Put the process somewhere that is not a project, as the proxy does.

    jupyter-server-proxy starts ``kptn ui`` with the Jupyter server's working
    directory -- ``/home/jovyan`` on the box -- not the project's.
    """
    elsewhere = (tmp_path / "elsewhere").resolve()
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    return elsewhere


def test_a_pipeline_that_reads_a_relative_path_at_import_time_loads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failure that surfaced on the box, reduced to its mechanism.

    nph-curation's pipeline imports a setup module that looks for
    ``../../packages`` relative to the working directory. Every other way kptn
    loads a pipeline runs it from the project root -- the CLI because the
    reader ``cd``s there, run workers because they are spawned with
    ``cwd=project_root`` -- so a relative path at import time is a reasonable
    thing for a pipeline to do. The UI must honour the same invariant.
    """
    project = copy_fixture_project(FIXTURE_PROJECT, tmp_path, "relative")
    (project / "marker.txt").write_text("found")
    source = project / "ui_pipeline.py"
    # Appended, not prepended: the fixture opens with a ``from __future__``
    # import, which must stay first. Module-level code still runs at import.
    source.write_text(
        source.read_text()
        + '\nMARKER = open("marker.txt").read()  # relative, as a pipeline may\n'
    )
    _elsewhere(tmp_path, monkeypatch)

    with ProjectSlot().use(make_entry(project)) as loaded:
        assert loaded.root == project.resolve()


def test_the_body_runs_in_the_project_directory(
    two_projects, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rendering a plan or a task runs project code too, not only the import."""
    alpha, _ = two_projects
    elsewhere = _elsewhere(tmp_path, monkeypatch)

    with ProjectSlot().use(alpha):
        assert Path.cwd().resolve() == alpha.root

    assert Path.cwd().resolve() == elsewhere


def test_the_working_directory_is_restored_when_the_body_raises(
    two_projects, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    alpha, _ = two_projects
    elsewhere = _elsewhere(tmp_path, monkeypatch)

    with pytest.raises(RuntimeError):
        with ProjectSlot().use(alpha):
            raise RuntimeError("a handler failed mid-render")

    assert Path.cwd().resolve() == elsewhere


def test_the_working_directory_is_restored_when_the_load_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.conftest import BROKEN_FIXTURE_PROJECT

    broken = make_entry(copy_fixture_project(BROKEN_FIXTURE_PROJECT, tmp_path, "broken"))
    elsewhere = _elsewhere(tmp_path, monkeypatch)

    with pytest.raises(ProjectError):
        with ProjectSlot().use(broken):
            pass

    assert Path.cwd().resolve() == elsewhere
