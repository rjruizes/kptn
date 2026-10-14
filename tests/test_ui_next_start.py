"""The run page's state card: what the next run starts from.

A small DuckDB project is run for real, so its database holds kptn's task
state and a checkpoint sits beside it; the UI then reads what the next run
starts from, and deletes it a file at a time.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import STATIC_DIR, create_app
from kptn_server.run_store import STATUS_SUCCEEDED, RunRequest, RunStore

pytestmark = pytest.mark.ui_hygiene

PIPELINE = '''
import duckdb
import kptn


def get_engine():
    return duckdb.connect("pipeline.db")


@kptn.task(outputs=[], duckdb_checkpoint=True)
def init_database(engine):
    engine.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")


@kptn.task(outputs=[])
def report(engine):
    engine.execute("SELECT count(*) FROM t").fetchone()


pipeline = kptn.Pipeline(
    "duck",
    kptn.config(duckdb=(get_engine, "engine")) >> init_database >> report,
)
'''

HX = {"HX-Request": "true"}


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "duck"
    root.mkdir()
    (root / "pyproject.toml").write_text(
        '[project]\nname = "duck"\nversion = "0.0.0"\n\n[tool.kptn]\npipeline = "duck_pipeline"\n'
    )
    (root / "kptn.yaml").write_text("settings:\n  db: duckdb\n")
    (root / "duck_pipeline.py").write_text(PIPELINE)
    subprocess.run(
        [sys.executable, "-c", "import kptn, duck_pipeline; kptn.run(duck_pipeline.pipeline)"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    assert (root / "pipeline.db").exists()
    assert (root / "pipeline.init_database.backup.db").exists()
    return root


@pytest.fixture
def app(project: Path):
    return create_app(project)


@pytest.fixture
def store(app) -> RunStore:
    return app.state.store


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def _report(store: RunStore, root: Path, *, finish: bool = True) -> None:
    """A run that reported opening the project's database."""
    record = store.create_run(RunRequest(project_root=root, pipeline="duck", profile=None))
    store.append_event(record.run_id, "run_started")
    store.append_event(
        record.run_id, "database", payload={"path": str(root / "pipeline.db"), "checkpoint": None}
    )
    if finish:
        store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
        store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)


def _card(html: str) -> tuple[str, str, str | None]:
    """The card's label, name, and the file its trash deletes."""
    label = re.search(r'<span class="st-lbl">([^<]*)</span>', html)
    name = re.search(r'<span class="st-db__name">([^<]*)</span>', html)
    assert label and name, "no card"
    file = re.search(r'name="name" value="([^"]*)"', html)
    return label.group(1), name.group(1), file.group(1) if file else None


def _delete(client: TestClient, name: str):
    return client.post("/next-start/delete", data={"name": name, "profile": ""}, headers=HX)


def test_the_card_names_the_database_the_next_run_continues(app, store, client, project) -> None:
    _report(store, project)

    assert _card(client.get("/next-start").text) == (
        "Continues from",
        "pipeline.db",
        "pipeline.db",
    )


def test_deleting_the_database_hands_the_card_to_its_checkpoint(app, store, client, project) -> None:
    _report(store, project)

    response = _delete(client, "pipeline.db")

    assert response.status_code == 200
    assert not (project / "pipeline.db").exists()
    assert _card(response.text) == (
        "Restores",
        "init_database checkpoint",
        "pipeline.init_database.backup.db",
    )


def test_with_nothing_left_there_is_no_card(app, store, client, project) -> None:
    """Nothing to start from, nothing to say: the pane shows Run and Plan."""
    _report(store, project)
    _delete(client, "pipeline.db")

    response = _delete(client, "pipeline.init_database.backup.db")

    assert response.status_code == 200
    assert not (project / "pipeline.init_database.backup.db").exists()
    assert response.text.strip() == ""


def test_a_stale_card_cannot_delete_what_took_its_place(app, store, client, project) -> None:
    _report(store, project)
    _delete(client, "pipeline.db")

    response = _delete(client, "pipeline.db")  # a second click from an old card

    assert response.status_code == 409
    assert (project / "pipeline.init_database.backup.db").exists()
    assert _card(response.text)[0] == "Restores"


def test_nothing_is_deleted_while_a_run_is_in_progress(app, store, client, project) -> None:
    _report(store, project, finish=False)

    card = client.get("/next-start").text
    response = _delete(client, "pipeline.db")

    assert _card(card)[2] is None, "the card offers no delete during a run"
    assert response.status_code == 409
    assert (project / "pipeline.db").exists()


def test_before_any_run_reports_there_is_no_card(app, client) -> None:
    assert client.get("/next-start").text.strip() == ""


def test_a_pipeline_without_duckdb_has_no_card(ui_project: Path) -> None:
    assert TestClient(create_app(ui_project)).get("/next-start").text.strip() == ""


def test_a_plain_form_post_returns_to_the_page(app, store, client, project) -> None:
    _report(store, project)

    response = client.post(
        "/next-start/delete",
        data={"name": "pipeline.db", "profile": ""},
        headers={"referer": "http://testserver/runs/abc"},
        follow_redirects=False,
    )

    assert response.status_code == 303 and response.headers["location"] == "/runs/abc"


def test_the_card_heads_the_sidebars_next_run_pane(app, store, client, project) -> None:
    """Above Run and Plan, in the first of the sidebar's panes."""
    _report(store, project)
    run_id = store.list_runs(project)[0].run_id

    html = client.get(f"/runs/{run_id}").text
    side = html[html.index('<aside class="side"') : html.index("</aside>")]
    pane = side[side.index('<section class="pane pane--next"') : side.index("</section>")]

    assert side.index("pane--next") < side.index("pane--runs") < side.index("pane--run\"")
    assert pane.index('id="next-start"') < pane.index('id="run-form"')
    assert 'data-url="/next-start"' in pane


def test_the_trash_takes_two_clicks() -> None:
    script = (STATIC_DIR / "app.js").read_text()
    assert "var ARM_MS = 3000;" in script
    assert 'slot.addEventListener("focusout", disarm)' in script


# -- the plan page reads the same file ---------------------------------------


def _actions(html: str) -> dict[str, str]:
    return dict(re.findall(r'<tr data-task="([^"]+)" data-action="([^"]+)">', html))


def test_the_plan_reads_task_state_from_the_pipelines_database(app, store, client, project) -> None:
    _report(store, project)

    html = client.get("/plan").text

    assert _actions(html) == {"init_database": "SKIP", "report": "SKIP"}
    assert "Task state read from pipeline.db." in html
    assert not (project / ".kptn" / "kptn.db").exists(), "the plan made a state file"


def test_without_the_database_the_plan_reads_the_checkpoint_it_restores(
    app, store, client, project
) -> None:
    _report(store, project)
    (project / "pipeline.db").unlink()

    html = client.get("/plan").text

    # The checkpoint was saved after init_database, before report ran.
    assert _actions(html) == {"init_database": "SKIP", "report": "RUN"}
    assert "the init_database checkpoint, which the next run restores" in html


def test_before_any_run_reports_the_plan_says_why_nothing_is_cached(client) -> None:
    html = client.get("/plan").text

    assert set(_actions(html).values()) == {"RUN"}
    assert "No run has reported where this pipeline&#39;s database is yet" in html


def test_the_plan_waits_for_a_run_in_progress(app, store, client, project) -> None:
    _report(store, project, finish=False)

    response = client.get("/plan")

    assert response.status_code == 409
    assert "A run is in progress" in response.text


def test_the_plan_lists_a_config_driven_sources_tables(tmp_path: Path) -> None:
    """The plan gets the config's values, as ``kptn plan`` does: a source
    whose files come from config is planned table by table. The config
    reads a relative path, so this also pins the project's directory."""
    root = tmp_path / "sourced"
    (root / "in" / "labs").mkdir(parents=True)
    (root / "in" / "labs" / "a.csv").write_text("x\n1\n")
    (root / "in" / "labs" / "b.csv").write_text("x\n2\n")
    (root / "config.json").write_text('{"labs": "in/labs"}')
    (root / "pyproject.toml").write_text(
        '[project]\nname = "sourced"\nversion = "0.0.0"\n\n[tool.kptn]\npipeline = "sourced_pipeline"\n'
    )
    (root / "sourced_pipeline.py").write_text(
        "import json\nfrom pathlib import Path\n\nimport kptn\n\n\n"
        "def get_config():\n    return json.loads(Path('config.json').read_text())\n\n\n"
        "labs = kptn.source('labs', files=lambda config: config['labs'])\n\n"
        "pipeline = kptn.Pipeline('sourced', kptn.config(config=get_config) >> labs)\n"
    )

    html = TestClient(create_app(root)).get("/plan").text

    assert set(_actions(html)) == {"labs[a]", "labs[b]"}
    assert "files depend on config" not in html
