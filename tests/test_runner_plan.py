"""Tests for kptn/runner/plan.py — Story 3.6: kptn plan command & semver-stable output."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from kptn.cli.commands import app
from kptn.exceptions import HashError, ProfileError
from kptn.graph.decorators import TaskSpec
from kptn.graph.graph import Graph
from kptn.graph.nodes import (
    ConfigNode,
    MapNode,
    NoopNode,
    ParallelNode,
    PipelineNode,
    StageNode,
    TaskNode,
)
from kptn.graph.pipeline import Pipeline
from kptn.profiles.resolved import ResolvedGraph
from kptn.profiles.schema import KptnConfig
from kptn.runner.plan import PlanAction, PlanEntry, build_plan, emit_map_plan, emit_run, emit_skip, plan, render_plan
from tests.fakes import FakeStateStore


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_task_node(name: str, outputs: list[str] | None = None) -> TaskNode:
    fn = MagicMock(return_value=None)
    fn.__name__ = name
    spec = TaskSpec(outputs=outputs or [])
    return TaskNode(fn=fn, spec=spec, name=name)


def _make_map_node(name: str, over: str) -> MapNode:
    task = MagicMock()
    task.__name__ = name
    return MapNode(task=task, over=over, name=name)


def _make_resolved(
    graph: Graph,
    bypassed_names: frozenset[str] | None = None,
) -> ResolvedGraph:
    return ResolvedGraph(
        graph=graph,
        pipeline="default",
        storage_key="kptn",
        bypassed_names=bypassed_names or frozenset(),
    )


# ─── AC-1: Plan output per task type ──────────────────────────────────────────


def test_plan_emits_run_for_stale_task(capsys: pytest.CaptureFixture) -> None:
    """Task with no stored hash → [RUN] task_a (AC-1)."""
    node = _make_task_node("task_a")
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    plan(resolved, store)

    captured = capsys.readouterr()
    assert "[RUN] task_a" in captured.out


def test_plan_emits_skip_for_cached_task(capsys: pytest.CaptureFixture) -> None:
    """Patched is_stale returning (False, 'cached') → [SKIP] task_a — cached (AC-1)."""
    node = _make_task_node("task_a", outputs=["out.csv"])
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    with patch("kptn.runner.plan.is_stale", return_value=(False, "cached")):
        plan(resolved, store)

    captured = capsys.readouterr()
    assert "[SKIP] task_a \u2014 cached" in captured.out


def test_plan_emits_map_for_map_node(capsys: pytest.CaptureFixture) -> None:
    """MapNode with over='provider_task.result' → [MAP] expand_items — dynamic, expands after provider_task (AC-1, AC-2)."""
    node = _make_map_node("expand_items", "provider_task.result")
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)
    store = FakeStateStore()

    plan(resolved, store)

    captured = capsys.readouterr()
    assert "[MAP] expand_items \u2014 dynamic, expands after provider_task" in captured.out


# ─── AC-2: MapNode provider resolution ────────────────────────────────────────


def test_plan_map_provider_is_first_dot_segment_dotted(capsys: pytest.CaptureFixture) -> None:
    """over='ctx.states' → provider=ctx (AC-2)."""
    node = _make_map_node("process_state", "ctx.states")
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)

    plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    assert "expands after ctx" in captured.out


def test_plan_map_provider_is_first_dot_segment_undotted(capsys: pytest.CaptureFixture) -> None:
    """over='get_active_states' → provider=get_active_states (AC-2)."""
    node = _make_map_node("process_state", "get_active_states")
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)

    plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    assert "expands after get_active_states" in captured.out


# ─── AC-3 & AC-5: Semver-stable exact format contract ─────────────────────────


def test_plan_run_exact_format(capsys: pytest.CaptureFixture) -> None:
    """[RUN] format is exactly '[RUN] task_name\\n' — semver contract (AC-3, AC-5)."""
    node = _make_task_node("task_name")
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)

    plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    assert "[RUN] task_name\n" in captured.out


def test_plan_skip_exact_format(capsys: pytest.CaptureFixture) -> None:
    """[SKIP] format is exactly '[SKIP] task_name — cached\\n' with em dash (AC-3, AC-5)."""
    node = _make_task_node("task_name", outputs=["out.csv"])
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)

    with patch("kptn.runner.plan.is_stale", return_value=(False, "cached")):
        plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    assert "[SKIP] task_name \u2014 cached\n" in captured.out


def test_plan_map_exact_format(capsys: pytest.CaptureFixture) -> None:
    """[MAP] format is exactly '[MAP] task_name — dynamic, expands after provider\\n' with em dash (AC-3, AC-5)."""
    node = _make_map_node("task_name", "provider")
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)

    plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    assert "[MAP] task_name \u2014 dynamic, expands after provider\n" in captured.out


# ─── AC-1: Non-exec nodes produce no output ───────────────────────────────────


def test_plan_skips_non_exec_nodes(capsys: pytest.CaptureFixture) -> None:
    """ParallelNode, StageNode, NoopNode, PipelineNode, ConfigNode produce no output lines (AC-1)."""
    nodes = [
        ParallelNode(),
        StageNode(name="stage_a"),
        NoopNode(),
        PipelineNode(name="pipeline_a"),
        ConfigNode(spec={}, name="config"),
    ]
    graph = Graph(nodes=nodes, edges=[])
    resolved = _make_resolved(graph)

    plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    assert captured.out == ""


# ─── AC-1: Bypassed nodes are absent ──────────────────────────────────────────


def test_plan_skips_bypassed_nodes(capsys: pytest.CaptureFixture) -> None:
    """Task in resolved.bypassed_names → absent from output (AC-1)."""
    node = _make_task_node("bypassed_task")
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph, bypassed_names=frozenset({"bypassed_task"}))

    plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    assert "bypassed_task" not in captured.out


# ─── AC-1: HashError treated as RUN ───────────────────────────────────────────


def test_plan_hash_error_treated_as_run(capsys: pytest.CaptureFixture) -> None:
    """is_stale raising HashError → [RUN] emitted (not crash) (AC-1)."""
    node = _make_task_node("task_a", outputs=["out.csv"])
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)

    with patch("kptn.runner.plan.is_stale", side_effect=HashError("unreadable")):
        plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    assert "[RUN] task_a" in captured.out


# ─── AC-1: Topological order ──────────────────────────────────────────────────


def test_plan_topological_order(capsys: pytest.CaptureFixture) -> None:
    """Two-node sequential graph: plan output respects dependency order (AC-1)."""
    node_a = _make_task_node("task_a")
    node_b = _make_task_node("task_b")
    graph = Graph(nodes=[node_a, node_b], edges=[(node_a, node_b)])
    resolved = _make_resolved(graph)

    plan(resolved, FakeStateStore())

    captured = capsys.readouterr()
    lines = [ln for ln in captured.out.splitlines() if ln]
    assert lines.index("[RUN] task_a") < lines.index("[RUN] task_b")


def test_build_plan_returns_ordered_reasons() -> None:
    """Structured plan entries preserve task order, action, and reason."""
    node_a = _make_task_node("task_a")
    node_b = _make_task_node("task_b", outputs=["b.csv"])
    resolved = _make_resolved(Graph(nodes=[node_a, node_b], edges=[(node_a, node_b)]))

    with patch(
        "kptn.runner.plan.is_stale",
        side_effect=[(True, "source changed"), (False, "cached")],
    ):
        entries = build_plan(resolved, FakeStateStore())

    assert [(e.task_name, e.action, e.reason) for e in entries] == [
        ("task_a", PlanAction.RUN, "source changed"),
        ("task_b", PlanAction.SKIP, "cached"),
    ]


def test_render_plan_preserves_exact_cli_output(capsys: pytest.CaptureFixture) -> None:
    """Rendering the structured plan preserves the existing terminal contract."""
    render_plan([PlanEntry("task_name", PlanAction.SKIP, reason="cached")])

    captured = capsys.readouterr()
    assert captured.out == "[SKIP] task_name — cached\n"


@pytest.mark.parametrize(
    ("emitter", "kwargs", "action", "task_name", "provider"),
    [
        (emit_run, {"task_name": "task_name"}, PlanAction.RUN, "task_name", None),
        (emit_skip, {"task_name": "task_name"}, PlanAction.SKIP, "task_name", None),
        (
            emit_map_plan,
            {"task_name": "task_name", "provider": "provider_task"},
            PlanAction.MAP,
            "task_name",
            "provider_task",
        ),
    ],
)
def test_status_emitters_use_shared_formatter(
    capsys: pytest.CaptureFixture,
    emitter,
    kwargs: dict[str, str],
    action: PlanAction,
    task_name: str,
    provider: str | None,
) -> None:
    """Status emitters delegate to one shared formatter for exact output."""
    with patch(
        "kptn.runner.plan._format_plan_status_line",
        return_value=f"[{action.name}] {task_name}",
    ) as mock_format:
        emitter(**kwargs)

    captured = capsys.readouterr()
    assert captured.out == f"[{action.name}] {task_name}\n"
    mock_format.assert_called_once_with(action, task_name, provider=provider, timestamp=False)


def test_render_plan_uses_shared_formatter(capsys: pytest.CaptureFixture) -> None:
    """render_plan uses the same formatter as the live status emitters."""
    with patch(
        "kptn.runner.plan._format_plan_status_line",
        return_value="[SKIP] task_name — cached",
    ) as mock_format:
        render_plan([PlanEntry("task_name", PlanAction.SKIP, reason="cached")])

    captured = capsys.readouterr()
    assert captured.out == "[SKIP] task_name — cached\n"
    mock_format.assert_called_once_with(
        PlanAction.SKIP,
        "task_name",
        provider=None,
        timestamp=False,
    )


# ─── AC-4: ProfileError exits non-zero with stderr, no partial output ─────────


def test_plan_profile_error_exits_nonzero_stderr_no_partial_output() -> None:
    """AC-4: ProfileError during compile → exit 1, message on stderr, no partial plan output."""
    runner = CliRunner()
    pipeline = Pipeline("default", Graph(nodes=[], edges=[]))

    with (
        patch("kptn.cli.commands.load_pipeline", return_value=pipeline),
        patch("kptn.cli.commands.resolve_pipeline") as mock_resolver,
    ):
        mock_resolver.side_effect = ProfileError("stale ref: 'missing_branch'")
        result = runner.invoke(app, ["plan", "--profile", "dev"])

    assert result.exit_code == 1
    assert "stale ref: 'missing_branch'" in result.stderr
    assert result.stdout == ""


def test_plan_command_translates_pipeline_import_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """plan command reports pipeline import failures as a BadParameter error."""
    (tmp_path / "pyproject.toml").write_text('[tool.kptn]\npipeline = "demo_pkg.pipeline"\n')
    (tmp_path / "demo_pkg").mkdir()
    (tmp_path / "demo_pkg" / "__init__.py").write_text("")
    (tmp_path / "demo_pkg" / "pipeline.py").write_text("from .missing import PIPELINE_NAME\n")
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    result = runner.invoke(app, ["plan"])

    assert result.exit_code == 2
    assert "Could not import pipeline module 'demo_pkg.pipeline'" in result.stderr


# ─── P1 guard: MapNode.over = "" raises ValueError ────────────────────────────


def test_plan_map_empty_over_raises_value_error() -> None:
    """MapNode with over='' raises ValueError — prevents malformed [MAP] output."""
    node = _make_map_node("bad_node", "")
    graph = Graph(nodes=[node], edges=[])
    resolved = _make_resolved(graph)

    with pytest.raises(ValueError, match="empty 'over' expression"):
        plan(resolved, FakeStateStore())
