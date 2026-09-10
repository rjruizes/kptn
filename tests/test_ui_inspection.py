"""Tests for the plan view, the pipeline walkthrough, and its docs panel.

Four properties carry the weight in this layer.

1. **The plan the page shows is the plan the CLI would run.** ``/plan``
   renders :func:`kptn.runner.plan.build_plan`'s entries, and the walkthrough
   renders :func:`kptn.inspection.inspect_pipeline`'s order. Neither page may
   re-derive either one: a second opinion about what runs, or in what order,
   is a page that can disagree with ``kptn run``.

2. **Documentation is confined to the project root.** A ``docs`` reference
   that escapes the project -- by ``..``, by an absolute path, or through a
   symlink -- must be refused, not read. This is the only place in the UI that
   opens a file whose path a project author chose.

3. **A docs file is data, not markup.** The renderer is built with
   ``{"html": False}`` and its output is the *only* string this module wraps
   in ``Markup``. Raw HTML in a Markdown file has to arrive as visible text.

4. **The walkthrough works with JavaScript switched off.** htmx loads one task
   detail at a time, but every one of those URLs is a real page: the fragment
   is the exception, chosen only for an ``HX-Request``.

Nothing here synchronizes on ``sleep`` and nothing here starts a worker: every
route under test is read-only.
"""

from __future__ import annotations

import re
import urllib.parse
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kptn_server.app import create_app
from kptn_server.markdown import DocumentationError, render_project_markdown
from kptn_server.routes import inspect as inspect_routes

# Opts this module into restore_process_state and reap_spawned_workers; the
# ui_project fixture comes from tests/conftest.py too.
pytestmark = pytest.mark.ui_hygiene


# -- fixtures --------------------------------------------------------------


@pytest.fixture
def app(ui_project: Path):
    """The UI app for a private copy of the shared fixture project."""
    return create_app(ui_project)


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


PYPROJECT = """\
[project]
name = "{name}"
version = "0.0.0"

[tool.kptn]
pipeline = "{module}"
"""


def write_project(
    tmp_path: Path, name: str, module_source: str, config: str = ""
) -> Path:
    """A throwaway project with a pipeline of this test's own design.

    The shared ``ui_project`` fixture is deliberately *not* the place to add a
    task per assertion: six other test modules run its pipeline for real.
    Cases that need a hostile description, a docs path that escapes the
    project, declared outputs, or a ``start_from`` profile get their own
    one-file project here instead.
    """
    root = tmp_path / name
    root.mkdir()
    module = f"{name}_pipeline"
    (root / "pyproject.toml").write_text(
        PYPROJECT.format(name=name, module=module), encoding="utf-8"
    )
    (root / f"{module}.py").write_text(module_source, encoding="utf-8")
    if config:
        (root / "kptn.yaml").write_text(config, encoding="utf-8")
    return root


def project_client(root: Path) -> TestClient:
    return TestClient(create_app(root))


# -- the opt-in itself -----------------------------------------------------


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    """Pin the ``pytestmark`` opt-in.

    Deleting the module's ``pytestmark`` line would silently strip both
    hygiene fixtures with no error and no failure -- and this module loads a
    different project pipeline in almost every test, so losing
    ``restore_process_state`` would make the outcomes order-dependent.
    """
    assert request.node.get_closest_marker("ui_hygiene") is not None


# -- the brief's two named tests -------------------------------------------


def test_plan_view_uses_structured_plan(client: TestClient) -> None:
    response = client.get("/plan?profile=success")
    assert response.status_code == 200
    assert "noisy_task" in response.text
    assert "RUN" in response.text


def test_walkthrough_orders_resolved_tasks_and_renders_docs(
    client: TestClient,
) -> None:
    response = client.get("/walkthrough?profile=success")
    assert response.text.index("setup_task") < response.text.index("noisy_task")
    detail = client.get("/walkthrough/task/noisy_task?profile=success")
    assert "Emit output and warnings." in detail.text
    assert "Detailed fixture documentation" in detail.text


# -- the plan comes from build_plan ----------------------------------------


def test_plan_view_renders_the_plan_builders_entries(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The page must render ``build_plan``'s answer, not its own walk.

    A route that walked the graph itself would produce a plausible table for
    this project while silently disagreeing with ``kptn plan`` about staleness.
    Replacing the builder with one that returns a recognisable entry proves
    the page's rows come from it.
    """
    from kptn.runner.plan import PlanAction, PlanEntry

    monkeypatch.setattr(
        inspect_routes,
        "build_plan",
        lambda resolved, state_store: [
            PlanEntry("only_from_build_plan", PlanAction.SKIP, reason="cached"),
        ],
    )

    response = client.get("/plan?profile=success")

    assert response.status_code == 200
    assert "only_from_build_plan" in response.text
    assert "SKIP" in response.text
    assert "cached" in response.text
    # The real plan's tasks are gone, because the page did not walk the graph.
    assert "noisy_task" not in response.text


def test_plan_view_reports_every_plan_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ui_project: Path
) -> None:
    """RUN, SKIP and MAP each have to reach the page, MAP with its provider."""
    from kptn.runner.plan import PlanAction, PlanEntry

    monkeypatch.setattr(
        inspect_routes,
        "build_plan",
        lambda resolved, state_store: [
            PlanEntry("runs_now", PlanAction.RUN, reason="no cached hash"),
            PlanEntry("stays_cached", PlanAction.SKIP, reason="cached"),
            PlanEntry("fans_out", PlanAction.MAP, provider="upstream"),
        ],
    )

    body = TestClient(create_app(ui_project)).get("/plan").text

    for expected in (
        "runs_now",
        "RUN",
        "no cached hash",
        "stays_cached",
        "SKIP",
        "fans_out",
        "MAP",
        "upstream",
    ):
        assert expected in body, expected


def test_plan_view_does_not_create_the_projects_state_store(
    client: TestClient, ui_project: Path
) -> None:
    """Reading a plan must not write to the project.

    ``kptn``'s own SQLite state store creates its file and its table on
    construction. Building one per page view would mean opening ``/plan`` on a
    project that has never run leaves a database behind -- and, because the
    configured path is relative, possibly not even inside the project.
    """
    state_db = ui_project / ".kptn" / "kptn.db"
    assert not state_db.exists()

    assert client.get("/plan?profile=success").status_code == 200

    assert not state_db.exists()


def test_plan_view_reads_an_existing_state_store(
    client: TestClient, ui_project: Path
) -> None:
    """A task whose recorded hash still matches is reported as cached.

    This is the other half of the test above: not creating the store must not
    become not *reading* it, which would make every task look stale forever.
    """
    from kptn.change_detector.detector import _hash_code
    from kptn.graph.nodes import TaskNode
    from kptn.state_store.sqlite import SqliteBackend

    project = create_app(ui_project).state.project
    node = next(
        n
        for n in project.pipeline.nodes
        if isinstance(n, TaskNode) and n.name == "noisy_task"
    )
    store = SqliteBackend(path=str(ui_project / ".kptn" / "kptn.db"))
    store.write_hash(".kptn/kptn.db", "fixture", "noisy_task", _hash_code(node))

    body = client.get("/plan?profile=success").text

    row = _row_for(body, "noisy_task")
    assert "SKIP" in row, row
    assert "cached" in row


def _row_for(body: str, task_name: str) -> str:
    """The one plan/walkthrough row carrying ``data-task="<task_name>"``.

    The opening ``<tr>`` is part of the match: the row's ``data-sequence`` and
    its classes are exactly what several of these tests are about.
    """
    match = re.search(
        r'(<tr[^>]*data-task="' + re.escape(task_name) + r'"[^>]*>.*?</tr>)',
        body,
        re.DOTALL,
    )
    assert match is not None, f"no row for {task_name} in:\n{body}"
    return match.group(1)


# -- unknown profile and unknown task --------------------------------------


@pytest.mark.parametrize("path", ["/plan", "/walkthrough"])
def test_unknown_profile_is_refused_with_the_declared_profiles(
    client: TestClient, path: str
) -> None:
    response = client.get(f"{path}?profile=not-a-profile")

    assert response.status_code == 400
    assert "not-a-profile" in response.text
    assert "success" in response.text
    # A page, not an orphan fragment: this is a plain browser navigation.
    assert response.text.lstrip().startswith("<!DOCTYPE html>")


def test_unknown_profile_on_a_task_detail_is_refused(client: TestClient) -> None:
    response = client.get("/walkthrough/task/noisy_task?profile=not-a-profile")

    assert response.status_code == 400
    assert "not-a-profile" in response.text


def test_unknown_task_is_a_404_naming_the_task(client: TestClient) -> None:
    response = client.get("/walkthrough/task/no_such_task?profile=success")

    assert response.status_code == 404
    assert "no_such_task" in response.text


def test_a_task_pruned_by_the_profile_is_not_reachable(tmp_path: Path) -> None:
    """The profile decides what exists, so a stopped-after task is a 404.

    ``stop_after`` removes a task from the resolved graph entirely. A detail
    route that looked the name up on the *unresolved* pipeline would happily
    describe a task this profile will never reach.
    """
    root = write_project(
        tmp_path,
        "stopped",
        module_source=(
            "import kptn\n\n\n"
            '@kptn.task(outputs=[], description="First.")\n'
            "def first() -> None:\n    return None\n\n\n"
            '@kptn.task(outputs=[], description="Second.")\n'
            "def second() -> None:\n    return None\n\n\n"
            'pipeline = kptn.Pipeline("stopped", first >> second)\n'
        ),
        config="profiles:\n  early:\n    stop_after: first\n",
    )
    client = project_client(root)

    assert client.get("/walkthrough/task/first?profile=early").status_code == 200
    assert client.get("/walkthrough/task/second?profile=early").status_code == 404
    # ...and without the profile, the whole pipeline is inspectable.
    assert client.get("/walkthrough/task/second").status_code == 200


# -- missing metadata ------------------------------------------------------


def test_missing_metadata_reads_as_not_documented(tmp_path: Path) -> None:
    root = write_project(
        tmp_path,
        "bare",
        module_source=(
            "import kptn\n\n\n"
            "@kptn.task(outputs=[])\n"
            "def undocumented() -> None:\n"
            "    return None\n\n\n"
            'pipeline = kptn.Pipeline("bare", undocumented)\n'
        ),
    )

    body = project_client(root).get("/walkthrough/task/undocumented").text

    assert "Not documented." in body
    # One placeholder per empty field: description, inputs, outputs, docs.
    assert body.count("Not documented.") >= 4


def test_a_docstring_is_shown_when_no_description_was_declared(
    tmp_path: Path,
) -> None:
    """Description precedence belongs to ``inspect_pipeline``, not the page.

    A page that only rendered explicit metadata would show "Not documented."
    for the many tasks whose documentation is their docstring.
    """
    root = write_project(
        tmp_path,
        "docstringed",
        module_source=(
            "import kptn\n\n\n"
            "@kptn.task(outputs=[])\n"
            "def from_docstring() -> None:\n"
            '    """The docstring is the description."""\n'
            "    return None\n\n\n"
            'pipeline = kptn.Pipeline("docstringed", from_docstring)\n'
        ),
    )

    body = project_client(root).get("/walkthrough/task/from_docstring").text

    assert "The docstring is the description." in body


# -- the docs panel: missing, escaping, and raw HTML -----------------------


def test_a_missing_markdown_file_is_reported_visibly(tmp_path: Path) -> None:
    """A broken docs reference must be *said*, not swallowed.

    A blank panel is indistinguishable from a task with no documentation, so
    the author never learns the path is wrong.
    """
    root = write_project(
        tmp_path,
        "absent",
        module_source=(
            "import kptn\n\n\n"
            '@kptn.task(outputs=[], description="Has a dangling docs path.",\n'
            '           docs="docs/absent.md")\n'
            "def dangling() -> None:\n    return None\n\n\n"
            'pipeline = kptn.Pipeline("absent", dangling)\n'
        ),
    )

    response = project_client(root).get("/walkthrough/task/dangling")

    assert response.status_code == 200
    assert "docs/absent.md" in response.text
    assert "does not exist" in response.text
    # The rest of the detail still renders: one broken file is not a dead page.
    assert "Has a dangling docs path." in response.text


def test_render_project_markdown_reads_a_project_file(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "note.md").write_text("# Title\n\nBody text.\n")

    html = render_project_markdown(tmp_path, "docs/note.md")

    assert "<h1>Title</h1>" in html
    assert "Body text." in html


def test_render_project_markdown_ignores_the_anchor(tmp_path: Path) -> None:
    (tmp_path / "note.md").write_text("# Title\n\nBody text.\n")

    assert "Body text." in render_project_markdown(tmp_path, "note.md#section")


def test_render_project_markdown_raises_on_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(DocumentationError, match="does not exist"):
        render_project_markdown(tmp_path, "docs/absent.md")


def test_render_project_markdown_raises_on_a_directory(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()

    with pytest.raises(DocumentationError):
        render_project_markdown(tmp_path, "docs")


def test_render_project_markdown_raises_on_undecodable_bytes(tmp_path: Path) -> None:
    (tmp_path / "binary.md").write_bytes(b"\xff\xfe not utf-8 \x00")

    with pytest.raises(DocumentationError):
        render_project_markdown(tmp_path, "binary.md")


SECRET = "TOP-SECRET-OUTSIDE-THE-PROJECT"


@pytest.mark.parametrize(
    "escape",
    [
        "../outside.md",
        "docs/../../outside.md",
        "./docs/../../outside.md",
        "/etc/hosts",
    ],
)
def test_render_project_markdown_refuses_a_path_outside_the_project(
    tmp_path: Path, escape: str
) -> None:
    """Traversal is refused before the file is opened."""
    project = tmp_path / "project"
    (project / "docs").mkdir(parents=True)
    (tmp_path / "outside.md").write_text(f"# Secret\n\n{SECRET}\n")

    with pytest.raises(DocumentationError, match="must stay within project root"):
        render_project_markdown(project, escape)


def test_render_project_markdown_refuses_a_symlink_out_of_the_project(
    tmp_path: Path,
) -> None:
    """A link *inside* the project pointing outside it is still outside it.

    ``..`` is the obvious escape; a symlink is the one a string check misses.
    Containment is decided after ``Path.resolve()`` for exactly this case.
    """
    project = tmp_path / "project"
    project.mkdir()
    (tmp_path / "outside.md").write_text(f"# Secret\n\n{SECRET}\n")
    link = project / "linked.md"
    link.symlink_to(tmp_path / "outside.md")
    assert link.read_text().count(SECRET) == 1, "the symlink itself must work"

    with pytest.raises(DocumentationError, match="must stay within project root"):
        render_project_markdown(project, "linked.md")


def test_task_detail_refuses_documentation_outside_the_project(
    tmp_path: Path,
) -> None:
    """End to end: a project whose ``docs`` escapes gets an error, not a file."""
    (tmp_path / "outside.md").write_text(f"# Secret\n\n{SECRET}\n")
    root = write_project(
        tmp_path,
        "escaper",
        module_source=(
            "import kptn\n\n\n"
            '@kptn.task(outputs=[], description="Points out of the project.",\n'
            '           docs="../../outside.md")\n'
            "def escaping() -> None:\n    return None\n\n\n"
            'pipeline = kptn.Pipeline("escaper", escaping)\n'
        ),
    )
    client = project_client(root)

    for path in ("/walkthrough", "/walkthrough/task/escaping"):
        response = client.get(path)
        assert response.status_code == 500, path
        assert SECRET not in response.text, path
        assert "must stay within project root" in response.text, path


def test_render_project_markdown_escapes_raw_html(tmp_path: Path) -> None:
    """``{"html": False}`` means a docs file cannot inject live markup."""
    (tmp_path / "hostile.md").write_text(
        "# Hostile\n\n"
        '<script>alert("xss")</script>\n\n'
        "<img src=x onerror=\"alert('xss')\">\n\n"
        '<div onclick="steal()">click</div>\n',
        encoding="utf-8",
    )

    html = str(render_project_markdown(tmp_path, "hostile.md"))

    # No live markup: not the tags, and not the inline handlers, whose
    # quoting survives escaping as &quot; and can therefore never open an
    # attribute value.
    assert "<script>" not in html
    assert "<img" not in html
    assert "<div" not in html
    assert 'onerror="' not in html
    assert 'onclick="' not in html
    assert "&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;" in html
    # The renderer's own markup is still markup, or nothing would render.
    assert "<h1>Hostile</h1>" in html


def test_task_detail_escapes_raw_html_from_a_docs_file(client: TestClient) -> None:
    """The fixture's own docs file carries a script tag and an inline handler."""
    body = client.get("/walkthrough/task/noisy_task?profile=success").text

    assert "<script>alert" not in body
    assert '<img src=x onerror="' not in body
    assert "&lt;script&gt;" in body
    assert "&lt;img src=x onerror=&quot;" in body


def test_task_detail_escapes_project_authored_text(tmp_path: Path) -> None:
    """Descriptions, inputs and outputs are project text, not markup."""
    root = write_project(
        tmp_path,
        "hostile",
        module_source=(
            "import kptn\n\n\n"
            "@kptn.task(\n"
            '    outputs=["<img src=x onerror=alert(1)>"],\n'
            '    inputs=["<b>bold input</b>"],\n'
            "    description=\"<script>alert('desc')</script>\",\n"
            ")\n"
            "def hostile_task() -> None:\n    return None\n\n\n"
            'pipeline = kptn.Pipeline("hostile", hostile_task)\n'
        ),
    )

    body = project_client(root).get("/walkthrough/task/hostile_task").text

    assert "<script>alert('desc')</script>" not in body
    assert "<b>bold input</b>" not in body
    # The brackets are what make an attribute live, and they are escaped.
    assert "<img src=x" not in body
    assert "&lt;script&gt;" in body
    assert "&lt;b&gt;bold input&lt;/b&gt;" in body
    assert "&lt;img src=x onerror=alert(1)&gt;" in body


# -- bypassed tasks, inputs/outputs, source ---------------------------------


BYPASS_PROJECT = (
    "import kptn\n\n\n"
    '@kptn.task(outputs=[], description="Skipped by the cursor.")\n'
    "def already_done() -> None:\n    return None\n\n\n"
    '@kptn.task(outputs=[], description="The one that still runs.")\n'
    "def still_to_run() -> None:\n    return None\n\n\n"
    'pipeline = kptn.Pipeline("cursored", already_done >> still_to_run)\n'
)


def test_walkthrough_marks_bypassed_tasks_and_numbers_only_the_rest(
    tmp_path: Path,
) -> None:
    """``start_from`` bypasses a task: it is shown, marked, and unnumbered.

    Hiding it would make the page disagree with the graph; numbering it would
    make the sequence numbers disagree with what the runner will do.
    """
    root = write_project(
        tmp_path,
        "cursored",
        module_source=BYPASS_PROJECT,
        config="profiles:\n  resume:\n    start_from: still_to_run\n",
    )
    client = project_client(root)

    body = client.get("/walkthrough?profile=resume").text

    bypassed = _row_for(body, "already_done")
    running = _row_for(body, "still_to_run")
    assert "bypassed" in bypassed
    assert 'data-sequence=""' in bypassed
    assert "bypassed" not in running
    assert 'data-sequence="1"' in running

    detail = client.get("/walkthrough/task/already_done?profile=resume")
    assert "bypassed" in detail.text

    # Without the cursor, both are numbered and neither is bypassed.
    plain = client.get("/walkthrough").text
    assert 'data-sequence="1"' in _row_for(plain, "already_done")
    assert 'data-sequence="2"' in _row_for(plain, "still_to_run")
    assert "bypassed" not in _row_for(plain, "already_done")


def test_bypassed_tasks_are_absent_from_the_plan(tmp_path: Path) -> None:
    """``build_plan`` drops them, and the plan page must not add them back."""
    root = write_project(
        tmp_path,
        "cursored_plan",
        module_source=BYPASS_PROJECT,
        config="profiles:\n  resume:\n    start_from: still_to_run\n",
    )

    body = project_client(root).get("/plan?profile=resume").text

    assert "still_to_run" in body
    assert "already_done" not in body


def test_task_detail_lists_declared_inputs_and_outputs(tmp_path: Path) -> None:
    root = write_project(
        tmp_path,
        "declared",
        module_source=(
            "import kptn\n\n\n"
            "@kptn.task(\n"
            '    outputs=["main.widgets", "main.gadgets"],\n'
            '    inputs=["raw.orders"],\n'
            '    description="Declares both sides.",\n'
            ")\n"
            "def declares() -> None:\n    return None\n\n\n"
            'pipeline = kptn.Pipeline("declared", declares)\n'
        ),
    )

    body = project_client(root).get("/walkthrough/task/declares").text

    assert "raw.orders" in body
    assert "main.widgets" in body
    assert "main.gadgets" in body
    assert "Not documented." not in body.split("Outputs")[-1].split("Source")[0]


def test_task_detail_shows_the_source_path_and_line(
    client: TestClient, ui_project: Path
) -> None:
    """The reader has to be able to open the code the page is describing."""
    source = (ui_project / "ui_pipeline.py").read_text(encoding="utf-8")
    lines = source.splitlines()
    definition = next(
        i for i, line in enumerate(lines) if line.startswith("def noisy_task(")
    )
    decorator = next(
        i for i in range(definition, -1, -1) if lines[i].startswith("@kptn.task(")
    )
    expected_line = decorator + 1

    body = client.get("/walkthrough/task/noisy_task?profile=success").text

    assert "ui_pipeline.py" in body
    assert f"ui_pipeline.py:{expected_line}" in body


def test_task_detail_says_not_documented_when_there_is_no_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A node whose source cannot be located must not render a bare colon."""
    root = write_project(
        tmp_path,
        "nosource",
        module_source=(
            "import kptn\n\n\n"
            '@kptn.task(outputs=[], description="Sourceless.")\n'
            "def sourceless() -> None:\n    return None\n\n\n"
            'pipeline = kptn.Pipeline("nosource", sourceless)\n'
        ),
    )
    client = project_client(root)
    monkeypatch.setattr(
        "inspect.getsourcefile", lambda _obj: (_ for _ in ()).throw(TypeError("no"))
    )

    body = client.get("/walkthrough/task/sourceless").text

    assert "Sourceless." in body
    assert "Not documented." in body


# -- structure of the walkthrough page -------------------------------------


def test_walkthrough_shows_the_pipeline_heading_and_the_sequence(
    client: TestClient,
) -> None:
    body = client.get("/walkthrough?profile=success").text

    assert "fixture" in body
    assert 'data-sequence="1"' in _row_for(body, "setup_task")
    assert 'data-sequence="2"' in _row_for(body, "noisy_task")


def test_walkthrough_renders_stage_headings(tmp_path: Path) -> None:
    """Structural nodes are headings, not numbered steps."""
    root = write_project(
        tmp_path,
        "staged",
        module_source=(
            "import kptn\n\n\n"
            '@kptn.task(outputs=[], description="In a stage.")\n'
            "def staged_task() -> None:\n    return None\n\n\n"
            'pipeline = kptn.Pipeline("staged", kptn.Stage("ingest", staged_task))\n'
        ),
    )

    body = project_client(root).get("/walkthrough").text

    assert "staged_task" in body
    assert 'data-kind="stage"' in body
    assert "ingest" in body
    # A stage is a heading, not a step: it takes no sequence number.
    assert 'data-sequence=""' in _row_for(body, "ingest")


# -- the no-JavaScript fallback --------------------------------------------


def test_task_detail_is_a_whole_page_for_a_direct_request(
    client: TestClient,
) -> None:
    response = client.get("/walkthrough/task/noisy_task?profile=success")

    assert response.status_code == 200
    assert response.text.lstrip().startswith("<!DOCTYPE html>")
    assert "app-bar__nav" in response.text
    assert "Emit output and warnings." in response.text


def test_task_detail_is_a_bare_fragment_for_htmx(client: TestClient) -> None:
    response = client.get(
        "/walkthrough/task/noisy_task?profile=success",
        headers={"HX-Request": "true"},
    )

    assert response.status_code == 200
    assert "<!DOCTYPE html>" not in response.text
    assert "app-bar__nav" not in response.text
    assert "Emit output and warnings." in response.text


def test_every_walkthrough_task_link_is_a_real_page(client: TestClient) -> None:
    """The htmx targets are ordinary links, and each one resolves.

    This is the whole no-JavaScript contract in one assertion: whatever the
    page asks htmx to fetch, a browser with scripting off can follow.
    """
    body = client.get("/walkthrough?profile=success").text
    hrefs = re.findall(r'href="(/walkthrough/task/[^"]+)"', body)

    assert len(hrefs) == 2, hrefs
    for href in hrefs:
        page = client.get(href.replace("&amp;", "&"))
        assert page.status_code == 200, href
        assert page.text.lstrip().startswith("<!DOCTYPE html>"), href


def test_walkthrough_task_rows_carry_the_htmx_attributes(
    client: TestClient,
) -> None:
    """htmx loads one detail at a time, targeting the shared panel."""
    body = client.get("/walkthrough?profile=success").text
    row = _row_for(body, "noisy_task")

    assert "hx-get=" in row
    assert 'hx-target="#task-detail"' in row


def test_the_profile_travels_with_every_link(client: TestClient) -> None:
    """A walkthrough read under a profile must not drop it on the next click."""
    body = client.get("/walkthrough?profile=success").text

    for href in re.findall(r'href="(/walkthrough/task/[^"]+)"', body):
        assert "profile=success" in href, href


def test_the_navigation_points_at_the_routes_that_exist(client: TestClient) -> None:
    """Every nav link on these pages has to resolve.

    The nav is authored in ``base.html`` by hand, so a route rename shows up
    as a 404 the reader finds, not a test failure.
    """
    body = client.get("/plan").text
    for href in set(re.findall(r'<a href="(/[^"]*)"', body)):
        assert client.get(href).status_code == 200, href


# -- lineage and table-preview links ----------------------------------------


LINKED_PROJECT = (
    "import kptn\n\n\n"
    '@kptn.task(outputs=["main.widgets"], description="Builds a table.")\n'
    "def build_widgets() -> None:\n    return None\n\n\n"
    'pipeline = kptn.Pipeline("linked", build_widgets)\n'
)

LINKED_CONFIG = """\
tasks:
  build_widgets:
    file: build_widgets.sql
    outputs:
      - main.widgets
"""


def _stub_resolver(config_path_to_map: dict[str, str]):
    """Stand in for the retained lineage service's table-file mapping.

    The real resolver *is* importable now that the ``web`` extra declares
    ``sqlglot`` -- see
    :func:`test_the_resolver_seam_resolves_now_that_sqlglot_ships` -- but it
    reads whatever ``kptn.yaml`` the project on disk happens to have. Patching
    the seam pins the part these tests own: whether a resolvable output is
    linked, whether an unresolvable one is left alone, and whether a target
    this app does not serve is offered at all.

    The normalizer mirrors ``service._normalize_table_name`` for these inputs:
    last dotted segment, lowercased.
    """

    def resolver():
        return (
            lambda config_path: dict(config_path_to_map),
            lambda name: name.split(".")[-1].lower() or None,
        )

    return resolver


def test_the_resolver_seam_resolves_now_that_sqlglot_ships(tmp_path: Path) -> None:
    """The retained service imports, and resolves a declared output.

    This used to assert ``_resolver() is None``: ``sqlglot`` was in no extra,
    so the lineage stack was unimportable and the walkthrough's two links were
    dead code in every environment the UI shipped to. The ``web`` extra now
    declares it, and this is the test that would fail if it were dropped
    again.
    """
    resolution = inspect_routes._resolver()

    assert resolution is not None
    build_table_file_map, normalize = resolution
    config = tmp_path / "kptn.yaml"
    config.write_text(LINKED_CONFIG, encoding="utf-8")

    assert normalize("main.widgets") == "widgets"
    assert "widgets" in build_table_file_map(config)


def test_the_lineage_targets_are_served_by_the_shared_app(ui_project: Path) -> None:
    """The link targets are real routes on the one application.

    ``output_links`` refuses to render a link whose path this app does not
    serve, so if the lineage router were dropped from ``register_routers`` the
    UI would silently stop offering lineage and previews rather than fail.
    """
    served = {
        route.path
        for route in create_app(ui_project).router.routes
        if isinstance(getattr(route, "path", None), str)
    }

    assert inspect_routes.LINEAGE_PATH in served
    assert inspect_routes.TABLE_PREVIEW_PATH in served


def test_output_links_are_omitted_when_nothing_resolves(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shared fixture declares no table files, so it gets no links."""
    monkeypatch.setattr(inspect_routes, "_resolver", _stub_resolver({}))

    body = client.get("/walkthrough/task/noisy_task?profile=success").text

    assert "/lineage-page" not in body
    assert "/table-preview" not in body


def test_output_links_are_omitted_when_the_app_does_not_serve_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A resolvable table is still not a link if nothing serves the target.

    Rendering the link anyway would put a guaranteed 404 in front of the
    reader. The shared app does serve both targets, so this strips them off
    the built app -- the condition under test is the served-path check itself,
    not the current router list.
    """
    root = write_project(
        tmp_path, "unserved", module_source=LINKED_PROJECT, config=LINKED_CONFIG
    )
    monkeypatch.setattr(
        inspect_routes, "_resolver", _stub_resolver({"widgets": "build_widgets.sql"})
    )
    app = create_app(root)
    app.router.routes[:] = [
        route
        for route in app.router.routes
        if getattr(route, "path", None)
        not in {inspect_routes.LINEAGE_PATH, inspect_routes.TABLE_PREVIEW_PATH}
    ]

    body = TestClient(app).get("/walkthrough/task/build_widgets").text

    assert "main.widgets" in body
    assert inspect_routes.LINEAGE_PATH not in body
    assert inspect_routes.TABLE_PREVIEW_PATH not in body


def test_output_links_appear_when_resolvable_and_served(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = write_project(
        tmp_path, "served", module_source=LINKED_PROJECT, config=LINKED_CONFIG
    )
    monkeypatch.setattr(
        inspect_routes, "_resolver", _stub_resolver({"widgets": "build_widgets.sql"})
    )
    # No stand-in routes: the shared app serves both targets for real.
    app = create_app(root)

    body = (
        TestClient(app)
        .get("/walkthrough/task/build_widgets")
        .text.replace("&amp;", "&")
    )

    assert inspect_routes.LINEAGE_PATH in body
    assert inspect_routes.TABLE_PREVIEW_PATH in body
    assert "table=main.widgets" in body
    # Both links carry the project's own kptn.yaml, which is what the retained
    # endpoints take as their ``configPath``.
    assert f"configPath={urllib.parse.quote(str(root / 'kptn.yaml'), safe='')}" in body


def test_output_links_are_omitted_for_an_unmapped_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the declared outputs the service can resolve get a link."""
    root = write_project(
        tmp_path,
        "partly",
        module_source=(
            "import kptn\n\n\n"
            '@kptn.task(outputs=["main.widgets", "main.unmapped"],\n'
            '           description="Builds two tables, one undeclared.")\n'
            "def build_widgets() -> None:\n    return None\n\n\n"
            'pipeline = kptn.Pipeline("partly", build_widgets)\n'
        ),
        config=LINKED_CONFIG,
    )
    monkeypatch.setattr(
        inspect_routes, "_resolver", _stub_resolver({"widgets": "build_widgets.sql"})
    )
    app = create_app(root)

    body = (
        TestClient(app)
        .get("/walkthrough/task/build_widgets")
        .text.replace("&amp;", "&")
    )

    assert "table=main.widgets" in body
    assert "table=main.unmapped" not in body
    assert "main.unmapped" in body


# -- nothing here runs anything --------------------------------------------


def test_the_inspection_routes_never_start_a_run(client: TestClient) -> None:
    """These pages are read-only: no run row, no lock, no worker."""
    store = client.app.state.store
    project = client.app.state.project

    client.get("/plan?profile=success")
    client.get("/walkthrough?profile=success")
    client.get("/walkthrough/task/noisy_task?profile=success")

    assert store.list_runs(project.root) == []
    assert store.active_run(project.root) is None
