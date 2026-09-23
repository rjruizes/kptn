"""Tests for the ``kptn ui`` standalone launcher and the module entry point.

The launcher is small but has five properties worth pinning down:

1. It binds **loopback** by default. There is no authentication and no remote
   execution anywhere in this UI, so the default bind address is the only
   thing standing between "a developer's tool" and "an unauthenticated remote
   pipeline runner". A regression here is a security regression.
2. It hands ``uvicorn.run`` the *application object*, not an import string --
   the app is built by a factory bound to ``Path.cwd()``, so an import string
   could not name it. ``--reload`` is the one exception, and has to be: the
   reloader re-imports in a subprocess, so it takes the import string of a
   factory that reads the working directory itself.
3. It opens a browser only once the health endpoint answers, and ``--no-open``
   skips that machinery entirely.
4. ``python -m kptn ui`` works, because jupyter-server-proxy's config invokes
   the notebook environment's interpreter that way rather than relying on a
   console script being on ``PATH``.
5. ``--projects-root`` builds the *multi-project* application instead, from a
   working directory that is no project at all, for whoever ``--user`` (or
   the environment) names. That is the production launch path, and
   :func:`kptn_server.app.create_app_for_env` is the same decision made from
   the environment for the ``--reload`` subprocess.

No test here opens a real browser (an autouse fixture makes that fail loudly),
starts a real server, or synchronizes on ``sleep``: the readiness probe, the
sleep, and the browser opener are all injected.
"""

from __future__ import annotations

import subprocess
import sys
import threading
import webbrowser
from pathlib import Path

import pytest
from typer.testing import CliRunner

from kptn.cli import app
from kptn.cli import commands as commands_module
from tests.conftest import FIXTURE_PROJECT, copy_fixture_project

REPO_ROOT = Path(__file__).resolve().parents[1]

# Opts this module into restore_process_state and reap_spawned_workers; the
# ui_project_cwd fixture comes from tests/conftest.py too.
pytestmark = pytest.mark.ui_hygiene


# -- fixtures --------------------------------------------------------------


@pytest.fixture(autouse=True)
def forbid_real_browser(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail loudly if any code path here would open a real browser window."""

    def explode(*args: object, **kwargs: object) -> bool:
        raise AssertionError(f"a test opened a real browser: {args!r} {kwargs!r}")

    monkeypatch.setattr(webbrowser, "open", explode)


@pytest.fixture(autouse=True)
def stub_uvicorn(monkeypatch: pytest.MonkeyPatch) -> list[tuple[tuple, dict]]:
    """Never actually bind a socket. Records what the launcher asked for."""
    import uvicorn

    calls: list[tuple[tuple, dict]] = []
    monkeypatch.setattr(
        uvicorn, "run", lambda *args, **kwargs: calls.append((args, kwargs))
    )
    return calls


# -- serving defaults ------------------------------------------------------


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    """Pin the ``pytestmark`` opt-in.

    ``restore_process_state`` and ``reap_spawned_workers`` are gated on the
    marker, so deleting the module's ``pytestmark`` line would silently strip
    this module of both -- no error, no failure, just a module that can leak a
    detached worker and pollute ``sys.path`` for everything after it. This
    turns that silent loss into a failure.
    """
    assert request.node.get_closest_marker("ui_hygiene") is not None


def test_ui_command_passes_loopback_defaults(
    ui_project_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called: dict[str, object] = {}
    monkeypatch.setattr("uvicorn.run", lambda *a, **kw: called.update(kw))

    result = CliRunner().invoke(app, ["ui", "--no-open"])

    assert result.exit_code == 0, result.output
    assert called["host"] == "127.0.0.1"
    assert called["port"] == 8000


def test_ui_command_binds_loopback_and_not_all_interfaces(ui_project_cwd: Path) -> None:
    """The default must not be 0.0.0.0: there is no auth on this surface."""
    from kptn.cli.commands import DEFAULT_UI_HOST

    assert DEFAULT_UI_HOST == "127.0.0.1"
    assert DEFAULT_UI_HOST not in {"0.0.0.0", "::", "*", ""}


def test_ui_command_hands_uvicorn_the_application_object(
    ui_project_cwd: Path, stub_uvicorn: list[tuple[tuple, dict]]
) -> None:
    """A factory-built app cannot be named by an import string."""
    from fastapi import FastAPI

    result = CliRunner().invoke(app, ["ui", "--no-open"])

    assert result.exit_code == 0, result.output
    ((args, _kwargs),) = stub_uvicorn
    assert args, "uvicorn.run was given no application"
    assert isinstance(args[0], FastAPI)
    assert args[0].state.project.root == ui_project_cwd.resolve()


def test_ui_command_honours_host_and_port_overrides(
    ui_project_cwd: Path, stub_uvicorn: list[tuple[tuple, dict]]
) -> None:
    result = CliRunner().invoke(
        app, ["ui", "--no-open", "--host", "localhost", "--port", "8731"]
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = stub_uvicorn[0]
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 8731


def test_ui_command_prints_the_complete_url(
    ui_project_cwd: Path, stub_uvicorn: list[tuple[tuple, dict]]
) -> None:
    """A developer has to be able to click or paste it, so print scheme+port."""
    result = CliRunner().invoke(app, ["ui", "--no-open", "--port", "8731"])

    assert result.exit_code == 0, result.output
    assert "http://127.0.0.1:8731/" in result.output


def test_ui_command_reports_a_directory_that_is_not_a_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stub_uvicorn: list
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(app, ["ui", "--no-open"])

    assert result.exit_code != 0
    assert "pyproject.toml" in result.output
    assert stub_uvicorn == [], "a broken project must not start a server"


# -- --reload --------------------------------------------------------------
#
# The app is built by a factory bound to ``Path.cwd()``, which is why the
# launcher normally hands uvicorn the object rather than a name. The reloader
# cannot work that way: it re-imports the application in a fresh subprocess
# after every change, so it needs a name to import. ``--reload`` therefore
# switches to the import string of a factory that reads ``Path.cwd()`` itself
# -- the subprocess inherits the working directory, so it lands on the same
# project.


def test_reload_hands_uvicorn_an_import_string_factory(
    ui_project_cwd: Path, stub_uvicorn: list[tuple[tuple, dict]]
) -> None:
    """The reloader cannot re-import an object, only a name."""
    result = CliRunner().invoke(app, ["ui", "--no-open", "--reload"])

    assert result.exit_code == 0, result.output
    (args, kwargs) = stub_uvicorn[0]
    assert args[0] == "kptn_server.app:create_app_for_env"
    assert kwargs["factory"] is True
    assert kwargs["reload"] is True


def test_reload_watches_the_code_that_serves_the_ui(
    ui_project_cwd: Path, stub_uvicorn: list[tuple[tuple, dict]]
) -> None:
    """Uvicorn would otherwise watch the working directory: the project.

    A pipeline edit is not what this flag is for -- the UI reads the project
    per request already. What a developer wants restarted is the server whose
    Python they just changed.
    """
    import kptn
    import kptn_server

    result = CliRunner().invoke(app, ["ui", "--no-open", "--reload"])

    assert result.exit_code == 0, result.output
    _args, kwargs = stub_uvicorn[0]
    watched = {Path(directory) for directory in kwargs["reload_dirs"]}
    assert watched == {
        Path(kptn_server.__file__).parent,
        Path(kptn.__file__).parent,
    }
    assert ui_project_cwd.resolve() not in watched


def test_without_reload_nothing_changes(
    ui_project_cwd: Path, stub_uvicorn: list[tuple[tuple, dict]]
) -> None:
    """The default path stays the object, and stays un-reloaded."""
    from fastapi import FastAPI

    result = CliRunner().invoke(app, ["ui", "--no-open"])

    assert result.exit_code == 0, result.output
    args, kwargs = stub_uvicorn[0]
    assert isinstance(args[0], FastAPI)
    assert not kwargs.get("reload")


def test_reload_still_refuses_a_directory_that_is_not_a_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stub_uvicorn: list
) -> None:
    """The import string defers the build, so the check has to stay here.

    Without it a reloading server would start, fail to import in its own
    subprocess, and keep retrying -- a loop that reports the mistake far less
    clearly than the message this command already has.
    """
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(app, ["ui", "--no-open", "--reload"])

    assert result.exit_code != 0
    assert "pyproject.toml" in result.output
    assert stub_uvicorn == [], "a broken project must not start a server"


# -- browser opening -------------------------------------------------------


def test_no_open_skips_the_browser_entirely(
    ui_project_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[str] = []
    monkeypatch.setattr(
        commands_module, "_start_browser_opener", lambda url: started.append(url)
    )

    result = CliRunner().invoke(app, ["ui", "--no-open"])

    assert result.exit_code == 0, result.output
    assert started == []


def test_the_default_starts_a_browser_opener(
    ui_project_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The opposite direction, so ``--no-open`` above cannot pass vacuously."""
    started: list[str] = []
    monkeypatch.setattr(
        commands_module, "_start_browser_opener", lambda url: started.append(url)
    )

    result = CliRunner().invoke(app, ["ui", "--port", "8731"])

    assert result.exit_code == 0, result.output
    assert started == ["http://127.0.0.1:8731/"]


def test_browser_opens_only_after_the_health_endpoint_responds() -> None:
    """Not before: a browser aimed at a socket that is not listening yet
    lands the developer on a connection-refused page."""
    log: list[tuple[str, object]] = []
    answers = iter([False, False, True])

    def probe(health_url: str) -> bool:
        answer = next(answers)
        log.append(("probe", health_url if answer else False))
        return answer

    opened = commands_module._open_when_ready(
        "http://127.0.0.1:8731/",
        probe=probe,
        opener=lambda url: log.append(("open", url)) or True,
        sleep=lambda _delay: log.append(("sleep", None)),
    )

    assert opened is True
    assert log == [
        ("probe", False),
        ("sleep", None),
        ("probe", False),
        ("sleep", None),
        ("probe", "http://127.0.0.1:8731/healthz"),
        ("open", "http://127.0.0.1:8731/"),
    ]


def test_browser_never_opens_if_health_never_responds() -> None:
    """And the poll gives up rather than spinning for the process's lifetime."""
    probes = 0

    def probe(_health_url: str) -> bool:
        nonlocal probes
        probes += 1
        return False

    opened = commands_module._open_when_ready(
        "http://127.0.0.1:8731/",
        probe=probe,
        opener=lambda url: pytest.fail(f"opened {url} with no healthy server"),
        sleep=lambda _delay: None,
        attempts=4,
    )

    assert opened is False
    assert probes == 4


def test_browser_opener_runs_on_a_daemon_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``uvicorn.run`` blocks, so the poll needs its own thread -- and it must
    not be able to keep the interpreter alive after the server stops."""
    ran = threading.Event()
    monkeypatch.setattr(
        commands_module,
        "_open_when_ready",
        lambda url: ran.set(),
    )

    thread = commands_module._start_browser_opener("http://127.0.0.1:8000/")

    assert thread.daemon is True
    thread.join(timeout=10)
    assert ran.is_set()
    assert not thread.is_alive()


# -- --projects-root -------------------------------------------------------
#
# This is how the feature actually starts in production: jupyter-server-proxy
# runs ``python -m kptn ui --projects-root ... --root-path {base_url}kptn``
# from the notebook server's working directory, which names no project at
# all. Nothing below binds a socket -- ``stub_uvicorn`` sees to that -- but
# the application object handed to uvicorn is the real one.

USER = "rruizesparza"


@pytest.fixture
def projects_root(tmp_path: Path) -> Path:
    """A releases folder with one of this person's projects, and one of someone else's."""
    root = tmp_path / "shared"
    release = root / "r1"
    release.mkdir(parents=True)
    copy_fixture_project(FIXTURE_PROJECT, release, f"{USER}_main")
    copy_fixture_project(FIXTURE_PROJECT, release, "someoneelse_main")
    return root


def _launched_app(stub_uvicorn: list[tuple[tuple, dict]]):
    ((args, _kwargs),) = stub_uvicorn
    assert args, "uvicorn.run was given no application"
    return args[0]


def test_projects_root_hands_uvicorn_a_multi_project_app(
    tmp_path: Path,
    projects_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_uvicorn: list[tuple[tuple, dict]],
) -> None:
    """The working directory names no project, and that must be fine."""
    from fastapi import FastAPI

    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        ["ui", "--no-open", "--projects-root", str(projects_root), "--user", USER],
    )

    assert result.exit_code == 0, result.output
    application = _launched_app(stub_uvicorn)
    assert isinstance(application, FastAPI)
    # A multi-project app has a registry and no single resolved project.
    assert application.state.registry.projects_root == projects_root
    assert not hasattr(application.state, "project")
    assert [entry.slug for entry in application.state.registry.entries()] == [
        f"{USER}_main"
    ]


def test_projects_root_carries_the_proxy_prefix(
    tmp_path: Path,
    projects_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_uvicorn: list[tuple[tuple, dict]],
) -> None:
    """``--root-path`` and ``--projects-root`` are used together in production."""
    monkeypatch.chdir(tmp_path)
    prefix = "/notebook/user/rruizesparza/kptn"

    result = CliRunner().invoke(
        app,
        [
            "ui",
            "--no-open",
            "--projects-root",
            str(projects_root),
            "--user",
            USER,
            "--root-path",
            prefix,
        ],
    )

    assert result.exit_code == 0, result.output
    assert _launched_app(stub_uvicorn).state.base == prefix


def test_projects_root_defaults_the_user_to_jupyterhub_user(
    tmp_path: Path,
    projects_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_uvicorn: list[tuple[tuple, dict]],
) -> None:
    """Under JupyterHub the single-user server is told whose it is."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("JUPYTERHUB_USER", USER)
    monkeypatch.setenv("USER", "someoneelse")

    result = CliRunner().invoke(
        app, ["ui", "--no-open", "--projects-root", str(projects_root)]
    )

    assert result.exit_code == 0, result.output
    assert _launched_app(stub_uvicorn).state.registry.user == USER


def test_projects_root_falls_back_to_the_shell_user(
    tmp_path: Path,
    projects_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_uvicorn: list[tuple[tuple, dict]],
) -> None:
    """A plain shell has no ``JUPYTERHUB_USER``, and the flag stays optional."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("JUPYTERHUB_USER", raising=False)
    monkeypatch.setenv("USER", USER)

    result = CliRunner().invoke(
        app, ["ui", "--no-open", "--projects-root", str(projects_root)]
    )

    assert result.exit_code == 0, result.output
    assert _launched_app(stub_uvicorn).state.registry.user == USER


def test_an_explicit_user_wins_over_the_environment(
    tmp_path: Path,
    projects_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_uvicorn: list[tuple[tuple, dict]],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("JUPYTERHUB_USER", "someoneelse")

    result = CliRunner().invoke(
        app,
        ["ui", "--no-open", "--projects-root", str(projects_root), "--user", USER],
    )

    assert result.exit_code == 0, result.output
    assert _launched_app(stub_uvicorn).state.registry.user == USER


def test_projects_root_names_the_person_and_the_root_it_serves(
    tmp_path: Path,
    projects_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_uvicorn: list[tuple[tuple, dict]],
) -> None:
    """The one line printed has to say whose directories are on offer."""
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        ["ui", "--no-open", "--projects-root", str(projects_root), "--user", USER],
    )

    assert result.exit_code == 0, result.output
    assert USER in result.output
    assert str(projects_root) in result.output


def test_projects_root_starts_even_when_a_project_is_broken(
    tmp_path: Path,
    projects_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_uvicorn: list[tuple[tuple, dict]],
) -> None:
    """No fail-fast load: the list page reports each project's error instead.

    Single-project mode exits non-zero on an unusable directory. Doing that
    here would let one bad checkout keep a person out of all their others.
    """
    monkeypatch.chdir(tmp_path)
    broken = projects_root / "r1" / f"{USER}_broken"
    broken.mkdir()
    (broken / "pyproject.toml").write_text("not = [valid", encoding="utf-8")

    result = CliRunner().invoke(
        app,
        ["ui", "--no-open", "--projects-root", str(projects_root), "--user", USER],
    )

    assert result.exit_code == 0, result.output
    assert len(stub_uvicorn) == 1


# -- create_app_for_env ----------------------------------------------------
#
# The ``--reload`` subprocess re-imports the app from an import string, which
# carries no arguments, so the environment is the only channel. The launcher
# writes it; this is the other half.


def test_create_app_for_env_reads_the_projects_root_and_user(
    tmp_path: Path, projects_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kptn_server.app import create_app_for_env

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KPTN_UI_PROJECTS_ROOT", str(projects_root))
    monkeypatch.setenv("KPTN_UI_USER", USER)
    monkeypatch.setenv("KPTN_UI_ROOT_PATH", "/notebook/user/rruizesparza/kptn")

    application = create_app_for_env()

    assert application.state.registry.projects_root == projects_root
    assert application.state.registry.user == USER
    assert application.state.base == "/notebook/user/rruizesparza/kptn"
    assert not hasattr(application.state, "project")


def test_create_app_for_env_defaults_the_user_like_the_command_does(
    tmp_path: Path, projects_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kptn_server.app import create_app_for_env

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("KPTN_UI_USER", raising=False)
    monkeypatch.setenv("JUPYTERHUB_USER", USER)
    monkeypatch.setenv("KPTN_UI_PROJECTS_ROOT", str(projects_root))

    assert create_app_for_env().state.registry.user == USER


def test_create_app_for_env_serves_the_working_directory_without_a_root(
    ui_project_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No ``KPTN_UI_PROJECTS_ROOT`` means the historical single-project app."""
    from kptn_server.app import create_app_for_env

    monkeypatch.delenv("KPTN_UI_PROJECTS_ROOT", raising=False)
    monkeypatch.setenv("KPTN_UI_ROOT_PATH", "/notebook/user/rruizesparza/kptn")

    application = create_app_for_env()

    assert application.state.project.root == ui_project_cwd.resolve()
    assert application.state.base == "/notebook/user/rruizesparza/kptn"
    assert not hasattr(application.state, "registry")


def test_create_app_for_env_needs_no_environment_at_all(
    ui_project_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A developer running ``kptn ui --reload`` sets none of these."""
    from kptn_server.app import create_app_for_cwd, create_app_for_env

    for name in ("KPTN_UI_PROJECTS_ROOT", "KPTN_UI_USER", "KPTN_UI_ROOT_PATH"):
        monkeypatch.delenv(name, raising=False)

    application = create_app_for_env()

    assert application.state.project.root == ui_project_cwd.resolve()
    assert application.state.base == ""
    assert create_app_for_cwd is create_app_for_env


# -- entry points ----------------------------------------------------------


def test_module_entry_point_exposes_the_ui_command() -> None:
    """jupyter-server-proxy runs ``python -m kptn ui`` with its interpreter."""
    result = subprocess.run(
        [sys.executable, "-m", "kptn", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stderr
    assert "ui" in result.stdout


def test_ui_is_added_beside_the_existing_terminal_commands() -> None:
    """``kptn run`` and ``kptn plan`` keep their terminal output; ``ui`` is new."""
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0, result.output
    for command in ("run", "plan", "ui"):
        assert command in result.output
