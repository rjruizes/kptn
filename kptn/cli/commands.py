from __future__ import annotations

import os
import threading
import time
import webbrowser
from pathlib import Path
from typing import TYPE_CHECKING, Callable
from urllib.parse import urljoin

import typer

import kptn
from kptn.exceptions import ProfileError, ProjectConfigError
from kptn.graph.pipeline import Pipeline
from kptn.project import load_pipeline
from kptn.runner.api import resolve_pipeline
from kptn.runner.api import run as _run_pipeline
import kptn.runner.plan as runner_plan

if TYPE_CHECKING:
    from kptn_server.run_store import RunStore

app = typer.Typer()


@app.command()
def run(
    profile: str | None = typer.Option(None, "--profile"),
    force: bool = typer.Option(False, "--force"),
    record: bool = typer.Option(
        True,
        "--record/--no-record",
        help=(
            "Keep this run and its log in the project's run history, where "
            "`kptn ui` shows them, as it does for runs it starts itself."
        ),
    ),
) -> None:
    project_root = Path.cwd()
    try:
        pipeline = load_pipeline(project_root)
    except ProjectConfigError as e:
        raise typer.BadParameter(str(e)) from e

    if record:
        exit_code = _run_recorded(pipeline, project_root, profile=profile, force=force)
        if exit_code is not None:
            if exit_code:
                raise typer.Exit(code=exit_code)
            return

    try:
        _run_pipeline(pipeline, profile=profile, force=force)
    except ProfileError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1)
    except Exception:
        raise typer.Exit(code=1)


def _run_recorded(
    pipeline: Pipeline, project_root: Path, *, profile: str | None, force: bool
) -> int | None:
    """Run *pipeline* in this process, recorded as a UI-started run would be.

    The run, its events, and its captured output go to the project's run store
    (``.kptn/ui.db`` and ``.kptn/runs/<run_id>.jsonl``) through the UI worker's
    own :func:`~kptn_server.worker.execute_run`, so ``kptn ui`` lists the run
    and serves its log like any other. Output still reaches the terminal.

    Returns the exit code, or ``None`` when the store cannot be opened -- the
    run then goes ahead unrecorded rather than not at all.
    """
    import sqlite3

    from kptn_server.project import UI_DATABASE_RELATIVE_PATH
    from kptn_server.run_store import ActiveRunError, RunRequest, RunStore
    from kptn_server.worker import execute_run

    database = project_root / UI_DATABASE_RELATIVE_PATH
    try:
        store = RunStore(database)
    except (sqlite3.Error, OSError) as e:
        typer.echo(f"kptn: not recording this run, cannot open {database}: {e}", err=True)
        return None

    request = RunRequest(
        project_root=project_root,
        pipeline=pipeline.name,
        profile=profile,
        force=force,
    )
    try:
        try:
            record = store.create_run(request)
        except ActiveRunError:
            # The run holding the lock may have died with no UI server around
            # to notice; clear it the way that server would, then ask again.
            _reconcile(store)
            record = store.create_run(request)
    except ActiveRunError as e:
        typer.echo(
            f"{e}\nWait for it to finish, stop it from `kptn ui`, or pass "
            "--no-record to run alongside it. A run that was killed releases "
            "the project once its heartbeat is 15 seconds old.",
            err=True,
        )
        return 1

    try:
        return execute_run(store, record, echo=True, pipeline=pipeline)
    finally:
        store.close()


def _reconcile(store: RunStore) -> None:
    """Mark runs whose process is gone as interrupted, releasing their lock."""
    try:
        from kptn_server.processes import RunProcessManager
    except ImportError:  # pragma: no cover - psutil comes with the web extra
        return
    RunProcessManager(store).reconcile()


@app.command()
def version() -> None:
    """Print the installed kptn version."""
    typer.echo(f"kptn {kptn.__version__}")


@app.command()
def plan(
    profile: str | None = typer.Option(None, "--profile"),
) -> None:
    project_root = Path.cwd()
    try:
        pipeline = load_pipeline(project_root)
        resolved, state_store = resolve_pipeline(pipeline, project_root, profile)
    except ProjectConfigError as e:
        raise typer.BadParameter(str(e)) from e
    except ProfileError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1)

    runner_plan.plan(resolved, state_store)


DEFAULT_UI_HOST = "127.0.0.1"
DEFAULT_UI_PORT = 8000

#: How many times the launcher probes the health endpoint before giving up on
#: opening a browser. An attempt count rather than a wall-clock deadline
#: precisely because it is also the test seam: a test that injects ``sleep``
#: gets exactly the production number of attempts without any clock advancing.
#: The elapsed time this corresponds to is not fixed -- each probe can itself
#: block for up to ``_HEALTH_PROBE_TIMEOUT_SECONDS`` inside ``urllib`` (a
#: filtered port, as opposed to a refused connection) -- so the constant is
#: named for what it actually bounds. The server keeps running either way.
BROWSER_READY_PROBE_ATTEMPTS = 300
BROWSER_POLL_INTERVAL_SECONDS = 0.1
_HEALTH_PROBE_TIMEOUT_SECONDS = 1.0


def _probe_health(health_url: str) -> bool:
    """Has the server started answering yet?

    Deliberately stdlib-only: the UI's HTTP client dependency is test-only,
    and a launcher that cannot start because an extra is missing is worse than
    a launcher that polls with ``urllib``.
    """
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(  # noqa: S310 - loopback URL we just built
            health_url, timeout=_HEALTH_PROBE_TIMEOUT_SECONDS
        ) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError, ValueError):
        return False


def _open_when_ready(
    url: str,
    *,
    probe: Callable[[str], bool] = _probe_health,
    opener: Callable[[str], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    attempts: int = BROWSER_READY_PROBE_ATTEMPTS,
    interval: float = BROWSER_POLL_INTERVAL_SECONDS,
) -> bool:
    """Open *url* in a browser once its health endpoint answers.

    Polling is bounded by *attempts* rather than a wall-clock deadline, so a
    caller that injects ``sleep`` (a test) gets exactly the same number of
    attempts as production without any clock having to advance. Returns
    whether a browser was opened; a server that never answers simply leaves
    the developer to click the URL the command printed.
    """
    health_url = urljoin(url, "healthz")
    attempts = max(1, attempts)
    # Resolved at call time, not captured as a default, so a test that
    # forbids the real browser actually forbids it.
    open_url = opener if opener is not None else webbrowser.open

    for attempt in range(attempts):
        if probe(health_url):
            open_url(url)
            return True
        if attempt < attempts - 1:
            sleep(interval)
    return False


def _start_browser_opener(url: str) -> threading.Thread:
    """Poll for readiness on a daemon thread.

    ``uvicorn.run`` blocks the calling thread for the life of the server, so
    the readiness poll cannot happen inline. The thread is a daemon so it can
    never keep the interpreter alive after the server stops.
    """
    thread = threading.Thread(
        target=_open_when_ready,
        args=(url,),
        name="kptn-ui-browser-opener",
        daemon=True,
    )
    thread.start()
    return thread


@app.command()
def ui(
    host: str = typer.Option(
        DEFAULT_UI_HOST, "--host", help="Interface to bind. Loopback by default."
    ),
    port: int = typer.Option(DEFAULT_UI_PORT, "--port", help="Port to bind."),
    open_browser: bool = typer.Option(
        True, "--open/--no-open", help="Open a browser once the server is ready."
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        help="Restart the server when kptn's own source changes (development).",
    ),
    root_path: str = typer.Option(
        "",
        "--root-path",
        help=(
            "Path prefix a reverse proxy strips before requests arrive, e.g. "
            "/notebook/user/me/kptn. Emitted URLs gain it."
        ),
    ),
    projects_root: str = typer.Option(
        "",
        "--projects-root",
        help=(
            "Serve every kptn project you have under this directory, chosen "
            "from the UI, instead of the one in the working directory. For "
            "running behind jupyter-server-proxy, where the working "
            "directory is the notebook server's."
        ),
    ),
    user: str = typer.Option(
        "",
        "--user",
        help="Whose working directories to offer. Default: $JUPYTERHUB_USER, then $USER.",
    ),
) -> None:
    """Serve the pipeline UI for one project, or several under --projects-root.

    Loopback-bound with no authentication: this is a single developer's view
    of their own project, and it must not become an unauthenticated remote
    pipeline runner. Nothing here accepts a project path from the network --
    the served project is always ``Path.cwd()``, or, with ``--projects-root``,
    one of the directories that flag named on the command line.

    ``--projects-root`` exists for jupyter-server-proxy, where the working
    directory belongs to the notebook server and names no project at all: the
    UI then offers the working directories ``--user`` has under that root,
    each at ``/p/<slug>/``.

    ``--reload`` is for working on kptn itself. Jinja already re-reads its
    templates from disk, so without it a long-running server picks up markup
    changes while still serving the Python it started with -- new chrome, old
    behaviour, and a bug hunt that leads nowhere.
    """
    try:
        import uvicorn

        from kptn_server.app import create_app
        from kptn_server.project import ProjectError
    except ImportError as e:  # pragma: no cover - depends on install extras
        typer.echo(
            f"The kptn UI needs the 'web' extra: pip install 'kptn[web]' ({e})",
            err=True,
        )
        raise typer.Exit(code=1)

    url = f"http://{host}:{port}/"

    if projects_root:
        # No fail-fast load here, deliberately: there is no one project whose
        # brokenness should stop a server that offers several, and the list
        # page reports each one's error where it can be read.
        from kptn_server.app import create_multi_app
        from kptn_server.registry import default_user

        chosen_user = user or default_user()
        application = create_multi_app(Path(projects_root), chosen_user, root_path)
        typer.echo(f"kptn UI for {chosen_user} under {projects_root} on {url}")
    else:
        project_root = Path.cwd()
        try:
            application = create_app(project_root, root_path)
        except ProjectError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=1)
        typer.echo(f"kptn UI for {project_root} on {url}")

    if open_browser:
        _start_browser_opener(url)

    if reload:
        # The reloader re-imports the application in a fresh subprocess after
        # every change, so it needs a name rather than the object built above
        # -- and an import string cannot carry the project root. The factory
        # reads Path.cwd(), which the subprocess inherits, so both paths serve
        # the same project. The app built above is discarded, but the build is
        # what proved this directory is servable: without that check a
        # reloading server would start and then fail to import, over and over,
        # reporting the mistake far less clearly than the message above does.
        del application
        # An import string cannot carry the prefix, and the reloader builds
        # the app in a fresh subprocess; the environment is the only channel.
        if root_path:
            os.environ["KPTN_UI_ROOT_PATH"] = root_path
        if projects_root:
            os.environ["KPTN_UI_PROJECTS_ROOT"] = projects_root
            if user:
                os.environ["KPTN_UI_USER"] = user
        uvicorn.run(
            "kptn_server.app:create_app_for_env",
            factory=True,
            reload=True,
            reload_dirs=_ui_source_directories(),
            host=host,
            port=port,
            log_level="warning",
        )
        return

    uvicorn.run(application, host=host, port=port, log_level="warning")


def _ui_source_directories() -> list[str]:
    """The trees ``--reload`` watches: kptn's own source, not the project.

    Uvicorn would otherwise watch the working directory, which here is the
    *served project* -- a pipeline edit is not what this flag is for, since
    the UI reads the project per request already. What a developer wants
    restarted is the server whose Python they just changed.
    """
    import kptn
    import kptn_server

    return [
        str(Path(kptn_server.__file__).parent),
        str(Path(kptn.__file__).parent),
    ]
