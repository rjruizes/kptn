from __future__ import annotations

import kptn
from kptn.runner.plan import emit_run  # noqa: F401  (ensures plan module import path is valid)

CALLS: list[str] = []


@kptn.task(outputs=[])
def expensive() -> None:
    CALLS.append("expensive")


@kptn.task(outputs=[], requires=[expensive])
def needs_it() -> None:
    CALLS.append("needs_it")


@kptn.task(outputs=[])
def standalone() -> None:
    CALLS.append("standalone")


def test_required_prereq_runs_once_when_consumer_present(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    CALLS.clear()
    pipe = kptn.Pipeline("p", needs_it)
    pipe.run(no_cache=True)
    assert CALLS.count("expensive") == 1
    assert CALLS.index("expensive") < CALLS.index("needs_it")


def test_disjunctive_consumer_dropped_end_to_end(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    CALLS.clear()

    @kptn.task(outputs=[])
    def src() -> None:
        CALLS.append("src")

    @kptn.task(outputs=[], requires=[kptn.any_of(src)])
    def gated() -> None:
        CALLS.append("gated")

    # `src` is NOT in the pipeline → gated must be dropped, never executed
    pipe = kptn.Pipeline("p", standalone >> gated)
    pipe.run(no_cache=True)
    assert "standalone" in CALLS
    assert "gated" not in CALLS
