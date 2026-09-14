"""Tests for the resumable SSE event stream.

The stream is the one part of this UI that has to survive everything: the
browser reloading, VS Code quitting, the FastAPI server being stopped and
started again. It can therefore hold *no* per-connection state -- the cursor
arrives from the client on every connection (``Last-Event-ID``, or ``?after=``) and
every event is read back out of the durable store.

Two failure modes get explicit guards here:

* **Waiting for ``run_finished``.** Task 6's ``reconcile()`` writes status only
  and appends no event, so an interrupted run's stream would never see a
  finish event. A stream that closed on the event rather than the status would
  hang that connection forever.
* **Raw log text.** ``log`` payloads carry byte offsets, not text; the server
  reads the slice and renders it through Jinja. The frames must carry escaped
  HTML, because the browser puts them into the DOM.

Nothing here synchronizes on ``sleep``. The stream's poll and heartbeat sleeps
are injected, and the fake that replaces them drives a fake clock, so a
16-second idle stream is observed in microseconds.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import AsyncIterator

import pytest
from fastapi.testclient import TestClient

from kptn.runner.events import EventKind
from kptn_server.app import STATIC_DIR, create_app
from kptn_server.routes import runs as runs_module
from kptn_server.run_store import (
    STATUS_INTERRUPTED,
    STATUS_SUCCEEDED,
    RunRecord,
    RunRequest,
    RunStore,
)

# Opts this module into restore_process_state and reap_spawned_workers.
pytestmark = pytest.mark.ui_hygiene

#: A tick budget that a correct stream never reaches. Exceeding it means the
#: generator is not making progress, and the fake sleep fails the test rather
#: than letting pytest hang.
MAX_TICKS = 5000


# -- fixtures --------------------------------------------------------------


@pytest.fixture
def app(ui_project: Path):
    return create_app(ui_project)


@pytest.fixture
def store(app) -> RunStore:
    return app.state.store


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


def _start_run(store: RunStore, project_root: Path, *, profile: str = "success"):
    record = store.create_run(
        RunRequest(project_root=project_root, pipeline="fixture", profile=profile)
    )
    store.append_event(record.run_id, "run_started")
    return record


def _append_log(store: RunStore, record: RunRecord, text: str) -> None:
    """Append a ``log`` event the way the capture layer does: offsets only."""
    data = text.encode("utf-8")
    log_path = Path(record.log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "ab") as handle:
        start = handle.tell()
        handle.write(data)
    store.append_event(
        record.run_id,
        "log",
        task_name="alpha",
        payload={"stream": "stdout", "severity": "output"},
        log_start=start,
        log_end=start + len(data),
    )


@pytest.fixture
def seeded_events(app, store: RunStore) -> RunRecord:
    """A *terminal* run with four events, so a stream over it closes.

    Terminal on purpose: the streaming response then ends by itself and the
    test never has to wait for, or interrupt, a live connection.

    Sequences: 1 run_started, 2 task_started, 3 log, 4 run_finished.
    """
    record = _start_run(store, app.state.project.root)
    store.append_event(record.run_id, "task_started", task_name="alpha")
    _append_log(store, record, "hello from the pipeline\n")
    store.append_event(record.run_id, "run_finished", payload={"status": "succeeded"})
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)
    return record


def _frames(text: str) -> list[str]:
    return [frame for frame in text.split("\n\n") if frame.strip()]


def _data_payloads(text: str) -> list[dict]:
    return [
        json.loads(line[len("data: ") :])
        for line in text.splitlines()
        if line.startswith("data: ")
    ]


class FakeClock:
    """Drives the stream's poll and heartbeat sleeps without real time.

    Each call advances a fake monotonic clock by the delay the stream asked
    for and records it, so a test can assert *which interval* was used. An
    optional ``on_tick`` hook lets a test change the world (finish the run,
    append an event) at a chosen tick.
    """

    def __init__(self, on_tick=None) -> None:
        self.delays: list[float] = []
        self.now = 0.0
        self._on_tick = on_tick

    async def sleep(self, delay: float) -> None:
        self.delays.append(delay)
        self.now += delay
        assert len(self.delays) <= MAX_TICKS, (
            "the event stream never stopped polling; it is not making progress"
        )
        if self._on_tick is not None:
            self._on_tick(len(self.delays))

    def monotonic(self) -> float:
        return self.now


def _install_clock(monkeypatch: pytest.MonkeyPatch, clock: FakeClock) -> None:
    monkeypatch.setattr(runs_module, "_sleep", clock.sleep)
    monkeypatch.setattr(runs_module, "_monotonic", clock.monotonic)


def _drain(gen: AsyncIterator[str]) -> list[str]:
    async def collect() -> list[str]:
        return [chunk async for chunk in gen]

    return asyncio.run(collect())


# -- the opt-in itself -----------------------------------------------------


def test_module_opts_into_the_ui_hygiene_fixtures(
    request: pytest.FixtureRequest,
) -> None:
    assert request.node.get_closest_marker("ui_hygiene") is not None


# -- cursors ---------------------------------------------------------------


def test_event_stream_resumes_after_last_event_id(
    client: TestClient, seeded_events: RunRecord
) -> None:
    response = client.get(
        f"/runs/{seeded_events.run_id}/events",
        headers={"Last-Event-ID": "2"},
    )

    assert "id: 3\n" in response.text
    assert "id: 1\n" not in response.text
    assert "id: 2\n" not in response.text


def test_event_stream_falls_back_to_the_after_parameter(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """The first connection has no ``Last-Event-ID``, only what the page knows."""
    response = client.get(f"/runs/{seeded_events.run_id}/events?after=3")

    assert "id: 4\n" in response.text
    assert "id: 3\n" not in response.text


def test_event_stream_replays_everything_from_zero(
    client: TestClient, seeded_events: RunRecord
) -> None:
    response = client.get(f"/runs/{seeded_events.run_id}/events?after=0")

    for sequence in (1, 2, 3, 4):
        assert f"id: {sequence}\n" in response.text


def test_event_stream_prefers_last_event_id_over_after(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """A reconnecting ``EventSource`` sends the header and keeps the old query.

    The browser reuses the URL it was constructed with -- ``?after=0`` and all
    -- and adds the header. Honouring the query would replay the whole run
    into the console on every reconnect.
    """
    response = client.get(
        f"/runs/{seeded_events.run_id}/events?after=0",
        headers={"Last-Event-ID": "3"},
    )

    assert "id: 4\n" in response.text
    assert "id: 1\n" not in response.text


def test_event_stream_ignores_an_unparseable_last_event_id(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """A garbled header must not 500 the stream, and must not skip events."""
    response = client.get(
        f"/runs/{seeded_events.run_id}/events?after=2",
        headers={"Last-Event-ID": "not-a-number"},
    )

    assert response.status_code == 200
    assert "id: 3\n" in response.text


@pytest.mark.parametrize("after", ["not-a-number", "", "  ", "3.5"])
def test_event_stream_tolerates_an_unparseable_after(
    client: TestClient, seeded_events: RunRecord, after: str
) -> None:
    """``?after=`` is parsed by the same tolerant rule as the header.

    A declared ``int`` parameter would hand a garbled query string FastAPI's
    422 JSON body while the identically garbled ``Last-Event-ID`` fell back to
    0 -- two different behaviours for the same malformed cursor, on the same
    endpoint. A cursor is a resume hint: not understanding one costs a replay,
    never an error page in place of the stream.
    """
    response = client.get(f"/runs/{seeded_events.run_id}/events?after={after}")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert "id: 1\n" in response.text


def test_event_stream_clamps_a_negative_after(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """FastAPI accepts ``after=-5`` happily, so the clamp has to be real."""
    response = client.get(f"/runs/{seeded_events.run_id}/events?after=-5")

    assert response.status_code == 200
    assert "id: 1\n" in response.text


def test_event_stream_404s_for_an_unknown_run(client: TestClient) -> None:
    response = client.get("/runs/deadbeef/events")

    assert response.status_code == 404


# -- frame shape -----------------------------------------------------------


def test_event_stream_is_an_event_stream(
    client: TestClient, seeded_events: RunRecord
) -> None:
    response = client.get(f"/runs/{seeded_events.run_id}/events")

    assert response.headers["content-type"].startswith("text/event-stream")
    # A cached event stream is a stream that never delivers anything.
    assert "no-cache" in response.headers["cache-control"]


def test_event_stream_frames_name_the_event_kind(
    client: TestClient, seeded_events: RunRecord
) -> None:
    response = client.get(f"/runs/{seeded_events.run_id}/events")

    for kind in ("run_started", "task_started", "log", "run_finished"):
        assert f"event: {kind}\n" in response.text


def test_event_stream_frames_are_single_line_and_ordered(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """SSE has no escaping: a raw newline inside ``data:`` splits the frame.

    Rendered HTML is multi-line by nature, so the payload must be JSON (which
    escapes newlines) rather than the fragment itself.
    """
    text = client.get(f"/runs/{seeded_events.run_id}/events").text

    ids = [int(match) for match in re.findall(r"^id: (\d+)$", text, re.M)]
    assert ids == sorted(ids)
    for frame in _frames(text):
        data_lines = [line for line in frame.splitlines() if line.startswith("data: ")]
        assert len(data_lines) <= 1, f"frame carries a split payload: {frame!r}"


def test_event_stream_carries_rendered_html_for_each_event(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """The browser appends server-rendered fragments; it never formats text."""
    payloads = _data_payloads(client.get(f"/runs/{seeded_events.run_id}/events").text)

    rendered = [p for p in payloads if p.get("kind") == "log"]
    assert rendered, f"no log frame in {payloads}"
    assert 'data-sequence="3"' in rendered[0]["html"]
    assert "hello from the pipeline" in rendered[0]["html"]


def test_event_stream_escapes_captured_log_text(
    app, store: RunStore, client: TestClient
) -> None:
    """The fragment goes into the DOM, so the text in it must be inert.

    This is the one defect in this task that is a security bug rather than a
    cosmetic one: a ``| safe`` here executes pipeline output in the
    developer's browser.
    """
    record = _start_run(store, app.state.project.root)
    _append_log(store, record, "<img src=x onerror=alert(1)>\n")
    store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    text = client.get(f"/runs/{record.run_id}/events").text

    assert "<img src=x onerror=alert(1)>" not in text
    assert "&lt;img src=x onerror=alert(1)&gt;" in json.dumps(_data_payloads(text))


# -- closing ---------------------------------------------------------------


def test_event_stream_closes_on_a_terminal_run(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """The response body ends; nothing has to interrupt the connection."""
    text = client.get(f"/runs/{seeded_events.run_id}/events").text

    assert "event: run_status" in text
    assert STATUS_SUCCEEDED in text


def test_event_stream_status_frame_carries_no_id(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """The status frame must never become the client's resume cursor.

    ``run_status`` is not a stored event and has no sequence of its own. An
    ``id:`` field on it would set ``Last-Event-ID`` to a number that does not
    correspond to any row, and the next reconnect would resume from there --
    silently skipping, or silently replaying, real events.
    """
    text = client.get(f"/runs/{seeded_events.run_id}/events").text

    status_frames = [frame for frame in _frames(text) if "event: run_status" in frame]
    assert status_frames, f"no status frame in {text!r}"
    for frame in status_frames:
        assert not any(line.startswith("id:") for line in frame.splitlines()), (
            f"the status frame carries a resume id: {frame!r}"
        )


def test_event_stream_closes_on_a_terminal_run_with_no_run_finished_event(
    app, store: RunStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The interrupted case, which has no finish event anywhere.

    ``reconcile()`` writes status only. A stream keyed on ``run_finished``
    would poll this run forever -- so the sleep is replaced with one that
    fails on its first call: a terminal run must be recognised from its
    status, before any further polling.
    """
    record = _start_run(store, app.state.project.root, profile="slow")
    store.finish_run(record.run_id, STATUS_INTERRUPTED)

    def refuse(_tick: int) -> None:  # pragma: no cover - reached only on regression
        raise AssertionError(
            "the stream polled again for a run that is already terminal"
        )

    clock = FakeClock(on_tick=refuse)
    _install_clock(monkeypatch, clock)

    frames = _drain(
        runs_module.event_frames(store, app.state.templates, record.run_id, after=0)
    )

    assert clock.delays == [], "a terminal run's stream must close without polling"
    joined = "".join(frames)
    assert "event: run_status" in joined
    assert STATUS_INTERRUPTED in joined


def test_event_stream_streams_events_appended_while_it_is_open(
    app, store: RunStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The live case: an event appended after the connection opened arrives."""
    record = _start_run(store, app.state.project.root)

    def progress(tick: int) -> None:
        if tick == 1:
            store.append_event(record.run_id, "task_started", task_name="alpha")
        elif tick == 2:
            store.append_event(
                record.run_id, "run_finished", payload={"status": "succeeded"}
            )
            store.finish_run(record.run_id, STATUS_SUCCEEDED, exit_code=0)

    _install_clock(monkeypatch, FakeClock(on_tick=progress))

    joined = "".join(
        _drain(
            runs_module.event_frames(store, app.state.templates, record.run_id, after=0)
        )
    )

    assert "id: 1\n" in joined
    assert "id: 2\n" in joined
    assert "id: 3\n" in joined


# -- idle behaviour --------------------------------------------------------


def test_event_stream_polls_and_heartbeats_on_its_declared_intervals(
    app, store: RunStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """100 ms polls, one comment heartbeat per 15 idle seconds.

    A proxy or a laptop sleeping will drop a connection that says nothing for
    long enough, and the client can only resume what it knows it lost -- so an
    idle stream has to keep talking.
    """
    assert runs_module.POLL_INTERVAL_SECONDS == 0.1
    assert runs_module.HEARTBEAT_INTERVAL_SECONDS == 15.0

    record = _start_run(store, app.state.project.root, profile="slow")

    def finish_eventually(tick: int) -> None:
        if tick == 200:  # 20 fake seconds: one heartbeat due, at 15.0s
            store.finish_run(record.run_id, STATUS_INTERRUPTED)

    clock = FakeClock(on_tick=finish_eventually)
    _install_clock(monkeypatch, clock)

    frames = _drain(
        runs_module.event_frames(store, app.state.templates, record.run_id, after=0)
    )

    assert clock.delays and set(clock.delays) == {0.1}
    heartbeats = [frame for frame in frames if frame.startswith(":")]
    assert len(heartbeats) == 1, f"expected one heartbeat, saw {len(heartbeats)}"


def test_event_stream_reads_its_intervals_from_the_module_constants(
    app, store: RunStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The intervals must be named, not literals inlined in the loop.

    Both are replaced with unmistakable sentinels: a loop sleeping on a
    hardcoded ``0.1`` records 0.1 here, and one comparing against a hardcoded
    15 emits no heartbeat at all within the sentinel window.
    """
    monkeypatch.setattr(runs_module, "POLL_INTERVAL_SECONDS", 2.5)
    monkeypatch.setattr(runs_module, "HEARTBEAT_INTERVAL_SECONDS", 5.0)

    record = _start_run(store, app.state.project.root, profile="slow")

    def finish_eventually(tick: int) -> None:
        if tick == 5:  # 12.5 sentinel seconds: two heartbeat windows elapsed
            store.finish_run(record.run_id, STATUS_INTERRUPTED)

    clock = FakeClock(on_tick=finish_eventually)
    _install_clock(monkeypatch, clock)

    frames = _drain(
        runs_module.event_frames(store, app.state.templates, record.run_id, after=0)
    )

    assert set(clock.delays) == {2.5}
    heartbeats = [frame for frame in frames if frame.startswith(":")]
    assert len(heartbeats) == 2, f"expected two heartbeats, saw {len(heartbeats)}"


# -- the browser's half of the contract ------------------------------------


def test_app_js_listens_for_every_event_kind() -> None:
    """The JS event-name list must match ``EventKind`` exactly.

    ``EventSource`` dispatches by event name, so a kind the browser does not
    listen for arrives and is dropped in silence -- no console error, no
    missing-frame symptom, just an event that never appears in the console.
    Tasks 9-10 add surfaces to this same app; if one adds a kind, this fails
    instead of the console quietly going incomplete.
    """
    source = (STATIC_DIR / "app.js").read_text()

    match = re.search(r"var EVENT_KINDS = \[(.*?)\];", source, re.S)
    assert match, "EVENT_KINDS is no longer a literal array in app.js"
    listed = re.findall(r'"([a-z_]+)"', match.group(1))

    assert listed == [kind.value for kind in EventKind], (
        "app.js and kptn.runner.events.EventKind have diverged"
    )


# -- the regions the stream settles at terminal ----------------------------
#
# The status pill is not the only thing on the run page that comes from the
# run *row* rather than from the event list. The header's Stop control and the
# force-finish hatch do too, and a run that goes terminal under an open stream
# leaves both offering to stop a process that is already gone, until somebody
# reloads. So the closing pass sends that region as a rendered fragment, the
# same way it sends the status.


def _region_frame(text: str, name: str) -> dict:
    frames = [frame for frame in _frames(text) if f"event: {name}" in frame]
    assert frames, f"no {name} frame in {text!r}"
    assert len(frames) == 1, f"{len(frames)} {name} frames; expected exactly one"
    for line in frames[0].splitlines():
        if line.startswith("data: "):
            return json.loads(line[len("data: ") :])
    raise AssertionError(f"the {name} frame carries no data: {frames[0]!r}")


def test_event_stream_settles_the_run_header_on_a_terminal_run(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """The closing header fragment offers no way to stop a finished run.

    Both controls are correct for the page that opened the stream and wrong
    the moment the run ends, and neither lives inside the status fragment --
    which is the whole reason this region is replaceable.
    """
    payload = _region_frame(
        client.get(f"/runs/{seeded_events.run_id}/events").text, "run_header"
    )

    assert payload["target"] == "run-header"
    assert f'action="/runs/{seeded_events.run_id}/stop"' not in payload["html"]
    assert "/force-finish" not in payload["html"]


def test_event_stream_run_header_still_carries_the_run(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """Sending an empty fragment would also pass the test above.

    The header's timestamps and log path are gone, so the status line is what
    is left to prove the frame is a rendered header rather than nothing.
    """
    payload = _region_frame(
        client.get(f"/runs/{seeded_events.run_id}/events").text, "run_header"
    )

    assert f'data-run-id="{seeded_events.run_id}"' in payload["html"]


def test_event_stream_region_frames_carry_no_id(
    client: TestClient, seeded_events: RunRecord
) -> None:
    """Same reason the status frame carries none: these are not stored events.

    An ``id:`` here would set ``Last-Event-ID`` to a number that matches no
    row, and the next reconnect would resume from it -- skipping or replaying
    real events.
    """
    text = client.get(f"/runs/{seeded_events.run_id}/events").text

    for name in runs_module.REGION_EVENT_TARGETS:
        frames = [frame for frame in _frames(text) if f"event: {name}" in frame]
        assert frames, f"no {name} frame in {text!r}"
        for frame in frames:
            assert not any(line.startswith("id:") for line in frame.splitlines()), (
                f"the {name} frame carries a resume id: {frame!r}"
            )


def test_app_js_swaps_every_region_the_server_sends() -> None:
    """The JS region map must match the server's region list exactly.

    ``EventSource`` dispatches by name, so a region the server settles and the
    browser does not listen for arrives and is dropped in silence -- which is
    precisely the stale header this frame exists to fix, back again with no
    symptom to notice it by.
    """
    source = (STATIC_DIR / "app.js").read_text()

    match = re.search(r"var REGION_EVENTS = \{(.*?)\};", source, re.S)
    assert match, "REGION_EVENTS is no longer a literal object in app.js"
    listed = dict(re.findall(r'(\w+):\s*"([a-z-]+)"', match.group(1)))

    assert listed == dict(runs_module.REGION_EVENT_TARGETS), (
        "app.js and runs.REGION_EVENT_TARGETS have diverged"
    )
