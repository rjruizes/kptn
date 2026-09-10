"""Tests for the ``kptn ui`` standalone launcher and the module entry point.

The launcher is small but has four properties worth pinning down:

1. It binds **loopback** by default. There is no authentication and no remote
   execution anywhere in this UI, so the default bind address is the only
   thing standing between "a developer's tool" and "an unauthenticated remote
   pipeline runner". A regression here is a security regression.
2. It hands ``uvicorn.run`` the *application object*, not an import string --
   the app is built by a factory bound to ``Path.cwd()``, so an import string
   could not name it.
3. It opens a browser only once the health endpoint answers, and ``--no-open``
   skips that machinery entirely.
4. ``python -m kptn ui`` works, because the VS Code extension invokes the
   selected interpreter that way rather than relying on a console script being
   on ``PATH``.

No test here opens a real browser (an autouse fixture makes that fail loudly),
starts a real server, or synchronizes on ``sleep``: the readiness probe, the
sleep, and the browser opener are all injected.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import threading
import warnings
import webbrowser
from pathlib import Path

import pytest
from typer.testing import CliRunner

from kptn.cli import app
from kptn.cli import commands as commands_module

FIXTURE_PROJECT = Path(__file__).parent / "fixtures" / "ui_project"
REPO_ROOT = Path(__file__).resolve().parents[1]


# -- fixtures --------------------------------------------------------------


@pytest.fixture(autouse=True)
def restore_process_state():
    """Undo what loading a project pipeline does to this process."""
    original_cwd = Path.cwd()
    original_path = sys.path.copy()
    original_modules = set(sys.modules)
    original_showwarning = warnings.showwarning
    root = logging.getLogger()
    original_handlers = root.handlers.copy()

    yield

    os.chdir(original_cwd)
    sys.path[:] = original_path
    for name in set(sys.modules) - original_modules:
        sys.modules.pop(name, None)
    warnings.showwarning = original_showwarning
    root.handlers[:] = original_handlers


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


@pytest.fixture
def ui_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    destination = tmp_path / "project"
    shutil.copytree(FIXTURE_PROJECT, destination)
    monkeypatch.chdir(destination)
    return destination


# -- serving defaults ------------------------------------------------------


def test_ui_command_passes_loopback_defaults(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called: dict[str, object] = {}
    monkeypatch.setattr("uvicorn.run", lambda *a, **kw: called.update(kw))

    result = CliRunner().invoke(app, ["ui", "--no-open"])

    assert result.exit_code == 0, result.output
    assert called["host"] == "127.0.0.1"
    assert called["port"] == 8000


def test_ui_command_binds_loopback_and_not_all_interfaces(ui_project: Path) -> None:
    """The default must not be 0.0.0.0: there is no auth on this surface."""
    from kptn.cli.commands import DEFAULT_UI_HOST

    assert DEFAULT_UI_HOST == "127.0.0.1"
    assert DEFAULT_UI_HOST not in {"0.0.0.0", "::", "*", ""}


def test_ui_command_hands_uvicorn_the_application_object(
    ui_project: Path, stub_uvicorn: list[tuple[tuple, dict]]
) -> None:
    """A factory-built app cannot be named by an import string."""
    from fastapi import FastAPI

    result = CliRunner().invoke(app, ["ui", "--no-open"])

    assert result.exit_code == 0, result.output
    ((args, _kwargs),) = stub_uvicorn
    assert args, "uvicorn.run was given no application"
    assert isinstance(args[0], FastAPI)
    assert args[0].state.project.root == ui_project.resolve()


def test_ui_command_honours_host_and_port_overrides(
    ui_project: Path, stub_uvicorn: list[tuple[tuple, dict]]
) -> None:
    result = CliRunner().invoke(
        app, ["ui", "--no-open", "--host", "localhost", "--port", "8731"]
    )

    assert result.exit_code == 0, result.output
    _args, kwargs = stub_uvicorn[0]
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 8731


def test_ui_command_prints_the_complete_url(
    ui_project: Path, stub_uvicorn: list[tuple[tuple, dict]]
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


# -- browser opening -------------------------------------------------------


def test_no_open_skips_the_browser_entirely(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[str] = []
    monkeypatch.setattr(
        commands_module, "_start_browser_opener", lambda url: started.append(url)
    )

    result = CliRunner().invoke(app, ["ui", "--no-open"])

    assert result.exit_code == 0, result.output
    assert started == []


def test_the_default_starts_a_browser_opener(
    ui_project: Path, monkeypatch: pytest.MonkeyPatch
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
        timeout=1.0,
        interval=0.25,
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


# -- entry points ----------------------------------------------------------


def test_module_entry_point_exposes_the_ui_command() -> None:
    """The VS Code extension runs ``python -m kptn ui`` with its interpreter."""
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
