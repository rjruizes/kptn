from __future__ import annotations

import textwrap

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


def test_conjunctive_prereq_does_not_revive_pruned_branch(tmp_path, monkeypatch) -> None:
    """A pruned stage branch must stay pruned even when its tasks conjunctively
    require a shared prerequisite that survives (demand-driven by a selected branch).

    Regression: ``requires=[shared]`` on tasks in two sibling Pipelines caused the
    shared prereq to be injected into each Pipeline (stealing its structural head)
    and then coalesced into one node. The surviving prereq's requires-edge then
    kept the *unselected* branch's tasks alive through profile pruning.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "kptn.yaml").write_text(
        textwrap.dedent(
            """
            settings:
              db_path: .kptn/kptn.db
            profiles:
              only_a:
                stage_selections:
                  datasets: [load_a]
            """
        ).strip()
    )

    calls: list[str] = []

    @kptn.task(outputs=[])
    def shared() -> None:
        calls.append("shared")

    @kptn.task(outputs=[], requires=[shared])
    def a_reports() -> None:
        calls.append("a_reports")

    @kptn.task(outputs=[], requires=[shared])
    def b_reports() -> None:
        calls.append("b_reports")

    load_a = kptn.Pipeline("load_a", a_reports)
    load_b = kptn.Pipeline("load_b", b_reports)
    load_shared = kptn.Pipeline("load_shared", shared)

    pipe = kptn.Pipeline("p", kptn.Stage("datasets", load_a, load_b, load_shared))

    # Profile selects only load_a. a_reports pulls in shared (demand-driven), but
    # the unselected load_b branch must NOT run.
    pipe.run(profile="only_a")
    assert "a_reports" in calls
    assert "shared" in calls
    assert "b_reports" not in calls


def test_disjunctive_satisfied_by_selected_stage_branch(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "kptn.yaml").write_text(
        textwrap.dedent(
            """
            settings:
              db_path: .kptn/kptn.db
            profiles:
              use_a:
                stage_selections:
                  src_stage: [branch_a]
              use_b:
                stage_selections:
                  src_stage: [branch_b]
            """
        ).strip()
    )

    calls: list[str] = []

    @kptn.task(outputs=[])
    def branch_a() -> None:
        calls.append("branch_a")

    @kptn.task(outputs=[])
    def branch_b() -> None:
        calls.append("branch_b")

    @kptn.task(outputs=[], requires=[kptn.any_of(branch_a, branch_b)])
    def consume() -> None:
        calls.append("consume")

    pipe = kptn.Pipeline(
        "p",
        kptn.Stage("src_stage", branch_a, branch_b) >> consume,
    )

    # Profile selects branch_a → consume's any_of is satisfied → consume runs.
    pipe.run(profile="use_a")
    assert "branch_a" in calls
    assert "consume" in calls
    assert "branch_b" not in calls
