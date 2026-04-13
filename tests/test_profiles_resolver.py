from __future__ import annotations

import pytest

import kptn
from kptn.exceptions import ProfileError
from kptn.graph.graph import Graph
from kptn.graph.nodes import StageNode
from kptn.graph.pipeline import Pipeline
from kptn.profiles.resolved import ResolvedGraph
from kptn.profiles.resolver import ProfileResolver
from kptn.profiles.schema import KptnConfig, KptnSettings, ProfileSpec


def test_resolve_no_extends_returns_spec_unchanged():
    spec = ProfileSpec(stage_selections={"data_sources": ["A"]})
    config = KptnConfig(profiles={"base": spec})
    result = ProfileResolver(config).resolve("base")
    assert result.stage_selections == {"data_sources": ["A"]}
    assert result.extends is None


def test_resolve_single_parent_stage_selection_inherited():
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(stage_selections={"data_sources": ["A"]}),
            "child": ProfileSpec(extends="base"),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.stage_selections["data_sources"] == ["A"]


def test_resolve_stage_selections_additive():
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(stage_selections={"data_sources": ["A"]}),
            "child": ProfileSpec(extends="base", stage_selections={"data_sources": ["B"]}),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.stage_selections["data_sources"] == ["A", "B"]


def test_resolve_args_deep_merge_child_wins():
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(args={"my_task": {"param1": "parent_val", "param2": "other"}}),
            "child": ProfileSpec(extends="base", args={"my_task": {"param1": "child_val"}}),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.args["my_task"]["param1"] == "child_val"
    assert result.args["my_task"]["param2"] == "other"


def test_resolve_multiple_parents_rightmost_wins():
    config = KptnConfig(
        profiles={
            "base1": ProfileSpec(args={"my_task": {"param1": "from_base1"}}),
            "base2": ProfileSpec(args={"my_task": {"param1": "from_base2"}}),
            "child": ProfileSpec(extends=["base1", "base2"]),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.args["my_task"]["param1"] == "from_base2"


def test_resolve_multiple_parents_stage_selections_additive():
    config = KptnConfig(
        profiles={
            "base1": ProfileSpec(stage_selections={"data_sources": ["A"]}),
            "base2": ProfileSpec(stage_selections={"data_sources": ["B"]}),
            "child": ProfileSpec(extends=["base1", "base2"], stage_selections={"data_sources": ["C"]}),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.stage_selections["data_sources"] == ["A", "B", "C"]


def test_resolve_unknown_profile_name_raises_profile_error():
    config = KptnConfig(profiles={})
    with pytest.raises(ProfileError, match="Unknown profile 'missing'"):
        ProfileResolver(config).resolve("missing")


def test_resolve_extends_unknown_parent_raises_profile_error():
    config = KptnConfig(
        profiles={
            "child": ProfileSpec(extends="nonexistent"),
        }
    )
    with pytest.raises(ProfileError, match="nonexistent"):
        ProfileResolver(config).resolve("child")


def test_resolve_transitive_extends():
    config = KptnConfig(
        profiles={
            "grandparent": ProfileSpec(stage_selections={"data_sources": ["GP"]}),
            "parent": ProfileSpec(extends="grandparent", stage_selections={"data_sources": ["P"]}),
            "child": ProfileSpec(extends="parent", stage_selections={"data_sources": ["C"]}),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.stage_selections["data_sources"] == ["GP", "P", "C"]


def test_resolve_circular_extends_raises_profile_error():
    config = KptnConfig(
        profiles={
            "a": ProfileSpec(extends="b"),
            "b": ProfileSpec(extends="a"),
        }
    )
    with pytest.raises(ProfileError, match="[Cc]ircular"):
        ProfileResolver(config).resolve("a")


def test_resolve_optional_groups_inherited():
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(optional_groups={"*.validate": False}),
            "child": ProfileSpec(extends="base"),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.optional_groups["*.validate"] is False


def test_resolve_optional_groups_child_wins():
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(optional_groups={"*.validate": False}),
            "child": ProfileSpec(extends="base", optional_groups={"*.validate": True}),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.optional_groups["*.validate"] is True


def test_resolve_extends_str_same_as_single_element_list():
    config_str = KptnConfig(
        profiles={
            "base": ProfileSpec(stage_selections={"data_sources": ["A"]}),
            "child": ProfileSpec(extends="base"),
        }
    )
    config_list = KptnConfig(
        profiles={
            "base": ProfileSpec(stage_selections={"data_sources": ["A"]}),
            "child": ProfileSpec(extends=["base"]),
        }
    )
    result_str = ProfileResolver(config_str).resolve("child")
    result_list = ProfileResolver(config_list).resolve("child")
    assert result_str.stage_selections == result_list.stage_selections


def test_resolve_start_from_and_stop_after_not_inherited():
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(start_from="task_a", stop_after="task_z"),
            "child": ProfileSpec(extends="base"),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.start_from is None
    assert result.stop_after is None


def test_resolve_start_from_stop_after_preserved_on_child():
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(start_from="task_a", stop_after="task_z"),
            "child": ProfileSpec(extends="base", start_from="task_b", stop_after="task_y"),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.start_from == "task_b"
    assert result.stop_after == "task_y"


def test_resolve_extends_set_to_none_on_result():
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(),
            "child": ProfileSpec(extends="base"),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.extends is None


def test_resolve_diamond_no_duplicate_stage_selections():
    """Diamond: child → [left, right], both extend base. base contributes exactly once."""
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(stage_selections={"data_sources": ["BASE"]}),
            "left": ProfileSpec(extends="base", stage_selections={"data_sources": ["LEFT"]}),
            "right": ProfileSpec(extends="base", stage_selections={"data_sources": ["RIGHT"]}),
            "child": ProfileSpec(extends=["left", "right"]),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.stage_selections["data_sources"] == ["BASE", "LEFT", "RIGHT"]


def test_resolve_duplicate_parent_name_in_extends():
    """extends: ['base', 'base'] — base contributes exactly once."""
    config = KptnConfig(
        profiles={
            "base": ProfileSpec(stage_selections={"data_sources": ["A"]}),
            "child": ProfileSpec(extends=["base", "base"]),
        }
    )
    result = ProfileResolver(config).resolve("child")
    assert result.stage_selections["data_sources"] == ["A"]


def test_resolve_cycle_message_shows_ordered_path():
    """Cycle error message must reflect the actual traversal path, not arbitrary set order."""
    config = KptnConfig(
        profiles={
            "a": ProfileSpec(extends="b"),
            "b": ProfileSpec(extends="a"),
        }
    )
    with pytest.raises(ProfileError, match="a → b → a"):
        ProfileResolver(config).resolve("a")


# ---------------------------------------------------------------------------
# Compile tests — Story 3.3
# ---------------------------------------------------------------------------

@kptn.task(outputs=[])
def task_a(): pass


@kptn.task(outputs=[])
def task_b(): pass


@kptn.task(outputs=[])
def task_c(): pass


@kptn.task(outputs=[])
def task_d(): pass


@kptn.task(outputs=[], optional="validate")
def validate_task(): pass


@kptn.task(outputs=[], optional="opt_grp")
def opt_task_1(): pass


@kptn.task(outputs=[], optional="opt_grp")
def opt_task_2(): pass


def test_compile_stage_branch_pruned():
    """AC-1: inactive branch B is absent from the resolved graph."""
    stage_g = kptn.Stage("data_sources", task_a, task_b)
    pipeline = Pipeline("test", stage_g)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["task_a"]})})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_a" in node_names
    assert "task_b" not in node_names


def test_compile_stage_active_branch_present():
    """AC-1: active branch A and its edges are present."""
    stage_g = kptn.Stage("data_sources", task_a, task_b)
    pipeline = Pipeline("test", stage_g)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["task_a"]})})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_a" in node_names
    edge_dsts = {d.name for _, d in result.graph.edges}
    assert "task_a" in edge_dsts


def test_compile_stage_downstream_of_dead_branch_pruned():
    """Empty stage selection prunes all branches and forward-propagates to downstream nodes.

    stage_selections=[] deactivates all branches; task_b and its successor task_c are
    both removed from the resolved graph (forward-propagation).

    Note: the previous version of this test used stage_selections={"data_sources": ["task_a"]}
    where task_a was not a branch of the stage (only task_b was). That scenario now raises
    ProfileError via _validate_stage_refs — see test_compile_invalid_branch_ref_raises.
    """
    stage_g = kptn.Stage("data_sources", task_b)
    pipeline = Pipeline("test", stage_g >> task_c)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": []})})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_b" not in node_names
    assert "task_c" not in node_names


def test_compile_invalid_branch_ref_raises():
    """An invalid (non-existent) branch ref in stage_selections raises ProfileError.

    Prior to Story 3.5, referencing a non-existent branch silently selected nothing
    (equivalent to an empty selection). _validate_stage_refs now catches this at
    compile time with an actionable error.
    """
    stage_g = kptn.Stage("data_sources", task_b)
    pipeline = Pipeline("test", stage_g >> task_c)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["task_a"]})})
    with pytest.raises(ProfileError, match="task_a"):
        ProfileResolver(config).compile(pipeline, "ci")


def test_compile_fan_in_with_one_active_branch_survives():
    """AC-5: fan-in node reachable from active branch survives; dead branch edge removed."""
    stage_g = kptn.Stage("data_sources", task_a, task_b)
    fanin_g = stage_g >> task_c
    pipeline = Pipeline("test", fanin_g)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["task_a"]})})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_a" in node_names
    assert "task_b" not in node_names
    assert "task_c" in node_names
    edge_pairs = {(s.name, d.name) for s, d in result.graph.edges}
    assert ("task_b", "task_c") not in edge_pairs
    assert ("task_a", "task_c") in edge_pairs


def test_compile_optional_task_excluded_when_group_off():
    """AC-2: optional task excluded when group is not enabled."""
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    pipeline = Pipeline("test", Graph._from_node(validate_task))
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert all(n.name != "validate_task" for n in result.graph.nodes)


def test_compile_optional_task_included_when_group_on():
    """AC-3: optional task included when group is enabled."""
    config = KptnConfig(profiles={"ci": ProfileSpec(optional_groups={"*.validate": True})})
    pipeline = Pipeline("test", Graph._from_node(validate_task))
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert any(n.name == "validate_task" for n in result.graph.nodes)


def test_compile_parallel_branches_never_pruned():
    """AC-4: ParallelNode branches are always present regardless of profile."""
    par_g = kptn.parallel(task_a, task_b)
    pipeline = Pipeline("test", par_g)
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_a" in node_names
    assert "task_b" in node_names


def test_compile_no_stage_selection_keeps_all_branches():
    """Profile with no stage key for 'data_sources' → both branches present."""
    stage_g = kptn.Stage("data_sources", task_a, task_b)
    pipeline = Pipeline("test", stage_g)
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_a" in node_names
    assert "task_b" in node_names


def test_compile_profile_args_in_resolved_graph():
    """resolved_graph.profile_args == profile.args."""
    config = KptnConfig(profiles={"ci": ProfileSpec(args={"task_a": {"k": "v"}})})
    pipeline = Pipeline("test", Graph._from_node(task_a))
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert result.profile_args == {"task_a": {"k": "v"}}


def test_compile_storage_key_default():
    """No db_path in settings → storage_key == '.kptn/kptn.db'."""
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    pipeline = Pipeline("test", Graph._from_node(task_a))
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert result.storage_key == ".kptn/kptn.db"


def test_compile_storage_key_from_settings():
    """db_path='my.db' in settings → storage_key == 'my.db'."""
    config = KptnConfig(
        settings=KptnSettings(db_path="my.db"),
        profiles={"ci": ProfileSpec()},
    )
    pipeline = Pipeline("test", Graph._from_node(task_a))
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert result.storage_key == "my.db"


def test_compile_bypassed_names_empty():
    """Story 3.3 never sets bypassed_names — always frozenset()."""
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    pipeline = Pipeline("test", Graph._from_node(task_a))
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert result.bypassed_names == frozenset()


def test_compile_pipeline_name_from_pipeline():
    """resolved_graph.pipeline == pipeline.name."""
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    pipeline = Pipeline("my_pipeline", Graph._from_node(task_a))
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert result.pipeline == "my_pipeline"


def test_compile_unknown_profile_raises_profile_error():
    """compile() with unknown profile_name → ProfileError."""
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    pipeline = Pipeline("test", Graph._from_node(task_a))
    with pytest.raises(ProfileError, match="Unknown profile 'unknown'"):
        ProfileResolver(config).compile(pipeline, "unknown")


def test_compile_parallel_branches_survive_active_stage_pruning():
    """AC-4: parallel branches are present even when profile actively prunes a Stage branch."""
    stage_g = kptn.Stage("data_sources", task_a, task_b)
    par_g = kptn.parallel(task_c, task_d)
    pipeline = Pipeline("test", stage_g >> par_g)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["task_a"]})})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_a" in node_names       # active stage branch
    assert "task_b" not in node_names   # pruned stage branch
    assert "task_c" in node_names       # parallel branch 1 — must survive
    assert "task_d" in node_names       # parallel branch 2 — must survive


# ---------------------------------------------------------------------------
# Cursor tests — Story 3.4
# ---------------------------------------------------------------------------

def test_compile_start_from_bypasses_upstream_nodes():
    """AC-1: start_from: task_b bypasses pipeline sentinel + task_a; all nodes remain in graph."""
    pipeline = Pipeline("p", task_a >> task_b >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_b")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    # PipelineNode("p") and task_a are topologically before task_b
    assert resolved.bypassed_names == frozenset({"p", "task_a"})
    node_names = {n.name for n in resolved.graph.nodes}
    assert "task_a" in node_names   # bypass-flagged but still present
    assert "task_b" in node_names
    assert "task_c" in node_names


def test_compile_start_from_cursor_node_not_bypassed():
    """start_from cursor node itself is NOT in bypassed_names."""
    pipeline = Pipeline("p", task_a >> task_b >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_b")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    assert "task_b" not in resolved.bypassed_names


def test_compile_start_from_source_node_no_bypass():
    """start_from the first task: the pipeline sentinel is bypassed, but task_a/b/c are not."""
    pipeline = Pipeline("p", task_a >> task_b >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_a")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    assert "task_a" not in resolved.bypassed_names
    assert "task_b" not in resolved.bypassed_names
    assert "task_c" not in resolved.bypassed_names
    # PipelineNode("p") is topologically before task_a and IS bypassed
    assert "p" in resolved.bypassed_names


def test_compile_stop_after_prunes_downstream():
    """AC-2: stop_after: task_b prunes task_c; A and B present; bypassed_names empty."""
    pipeline = Pipeline("p", task_a >> task_b >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(stop_after="task_b")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    node_names = {n.name for n in resolved.graph.nodes}
    assert "task_c" not in node_names
    assert "task_a" in node_names
    assert "task_b" in node_names
    assert resolved.bypassed_names == frozenset()


def test_compile_stop_after_last_node_no_prune():
    """stop_after the last node: all nodes are present."""
    pipeline = Pipeline("p", task_a >> task_b >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(stop_after="task_c")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    node_names = {n.name for n in resolved.graph.nodes}
    assert "task_a" in node_names
    assert "task_b" in node_names
    assert "task_c" in node_names


def test_compile_stop_after_bypassed_names_empty():
    """stop_after alone never sets bypassed_names."""
    pipeline = Pipeline("p", task_a >> task_b >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(stop_after="task_b")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    assert resolved.bypassed_names == frozenset()


def test_compile_start_from_stage_group_cursor_unit():
    """AC-3: start_from a Stage branch → StageNode sentinel used as cursor anchor.

    Graph: PipelineNode → StageNode("ds") → task_a, task_b → task_c
    start_from: task_a (inside Stage "ds") → cursor anchor = StageNode("ds") at index 1
    bypassed_names = {PipelineNode("p")} only; StageNode, task_a, task_b, task_c NOT bypassed.
    """
    stage_g = kptn.Stage("ds", task_a, task_b)
    pipeline = Pipeline("p", stage_g >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_a")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    assert "ds" not in resolved.bypassed_names
    assert "task_a" not in resolved.bypassed_names
    assert "task_b" not in resolved.bypassed_names
    assert "task_c" not in resolved.bypassed_names
    # The PipelineNode sentinel ("p") is before the StageNode, so it is bypassed
    assert "p" in resolved.bypassed_names


def test_compile_start_from_stage_group_no_upstream():
    """Stage is the first node after pipeline sentinel; start_from a branch → no task bypassed."""
    stage_g = kptn.Stage("ds", task_a, task_b)
    pipeline = Pipeline("p", stage_g)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_a")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    assert "ds" not in resolved.bypassed_names
    assert "task_a" not in resolved.bypassed_names
    assert "task_b" not in resolved.bypassed_names
    # Cursor anchor = StageNode("ds"); PipelineNode("p") precedes it and IS bypassed
    assert "p" in resolved.bypassed_names


def test_compile_start_from_unknown_raises_profile_error():
    """AC-4: start_from references a node not in the graph → ProfileError with node name."""
    pipeline = Pipeline("p", task_a >> task_b)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="nonexistent")})
    with pytest.raises(ProfileError, match="nonexistent"):
        ProfileResolver(config).compile(pipeline, "dev")


def test_compile_start_from_unknown_did_you_mean():
    """AC-4: start_from close match → 'Did you mean' suggestion in error message."""
    pipeline = Pipeline("p", task_a >> task_b)
    # "task_b_typo" is close enough to "task_b" (cutoff 0.6)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_b_typo")})
    with pytest.raises(ProfileError, match="Did you mean"):
        ProfileResolver(config).compile(pipeline, "dev")


def test_compile_start_from_excluded_by_stage_raises():
    """AC-4: start_from a node excluded by Stage selection → ProfileError (not in pruned graph)."""
    stage_g = kptn.Stage("ds", task_a, task_b)
    pipeline = Pipeline("p", stage_g)
    # task_b is pruned by stage selection; start_from: task_b → ProfileError
    config = KptnConfig(profiles={
        "dev": ProfileSpec(
            stage_selections={"ds": ["task_a"]},
            start_from="task_b",
        )
    })
    with pytest.raises(ProfileError, match="task_b"):
        ProfileResolver(config).compile(pipeline, "dev")


def test_compile_stop_after_unknown_raises_profile_error():
    """stop_after references an unknown node → ProfileError."""
    pipeline = Pipeline("p", task_a >> task_b)
    config = KptnConfig(profiles={"dev": ProfileSpec(stop_after="nonexistent")})
    with pytest.raises(ProfileError, match="nonexistent"):
        ProfileResolver(config).compile(pipeline, "dev")


def test_compile_conflict_stop_before_start_raises():
    """AC-5: stop_after is topologically before start_from → ProfileError with both names."""
    pipeline = Pipeline("p", task_a >> task_b >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_c", stop_after="task_a")})
    with pytest.raises(ProfileError, match="task_a") as exc_info:
        ProfileResolver(config).compile(pipeline, "dev")
    assert "task_c" in str(exc_info.value)


def test_compile_both_cursors_valid():
    """start_from: task_b + stop_after: task_c in A >> B >> C >> D → bypass A, prune D."""
    pipeline = Pipeline("p", task_a >> task_b >> task_c >> task_d)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_b", stop_after="task_c")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    # bypassed: pipeline sentinel "p" + task_a
    assert "task_a" in resolved.bypassed_names
    assert "p" in resolved.bypassed_names
    assert "task_b" not in resolved.bypassed_names
    # task_d pruned by stop_after
    node_names = {n.name for n in resolved.graph.nodes}
    assert "task_d" not in node_names
    assert "task_b" in node_names
    assert "task_c" in node_names


def test_compile_stop_after_unknown_did_you_mean():
    """AC-4 (stop_after): close match → 'Did you mean' suggestion in error message."""
    pipeline = Pipeline("p", task_a >> task_b)
    config = KptnConfig(profiles={"dev": ProfileSpec(stop_after="task_b_typo")})
    with pytest.raises(ProfileError, match="Did you mean"):
        ProfileResolver(config).compile(pipeline, "dev")


def test_compile_stop_after_stage_member_cursor():
    """stop_after a Stage branch → Stage atomicity: stop after last branch in topo order.

    Graph: PipelineNode → StageNode("ds") → task_a, task_b → task_c
    stop_after: task_a (inside Stage "ds") → cursor = max(idx_of[task_a], idx_of[task_b])
    Both task_a and task_b are retained; task_c is pruned.
    """
    stage_g = kptn.Stage("ds", task_a, task_b)
    pipeline = Pipeline("p", stage_g >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(stop_after="task_a")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    node_names = {n.name for n in resolved.graph.nodes}
    assert "task_a" in node_names
    assert "task_b" in node_names   # sibling kept by Stage atomicity
    assert "task_c" not in node_names
    assert resolved.bypassed_names == frozenset()


def test_compile_stop_after_stage_sentinel_cursor():
    """stop_after the StageNode name directly → Stage atomicity: all branches retained.

    Graph: PipelineNode → StageNode("ds") → task_a, task_b → task_c
    stop_after: "ds" (StageNode sentinel) → cursor = max(idx_of[task_a], idx_of[task_b])
    Both branches are retained; task_c is pruned.
    """
    stage_g = kptn.Stage("ds", task_a, task_b)
    pipeline = Pipeline("p", stage_g >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(stop_after="ds")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    node_names = {n.name for n in resolved.graph.nodes}
    assert "ds" in node_names
    assert "task_a" in node_names
    assert "task_b" in node_names
    assert "task_c" not in node_names
    assert resolved.bypassed_names == frozenset()


def test_compile_single_node_slice():
    """start_from == stop_after: valid 'single-node slice' idiom.

    Predecessors are bypass-flagged; successors are pruned; only the named node runs.
    Graph: p → task_a → task_b → task_c; start_from=stop_after="task_b"
    """
    pipeline = Pipeline("p", task_a >> task_b >> task_c)
    config = KptnConfig(profiles={"dev": ProfileSpec(start_from="task_b", stop_after="task_b")})
    resolved = ProfileResolver(config).compile(pipeline, "dev")
    # Predecessors bypassed
    assert "p" in resolved.bypassed_names
    assert "task_a" in resolved.bypassed_names
    assert "task_b" not in resolved.bypassed_names
    # Successors pruned
    node_names = {n.name for n in resolved.graph.nodes}
    assert "task_c" not in node_names
    # task_b and its predecessors present in graph
    assert "task_b" in node_names
    assert "task_a" in node_names


# ---------------------------------------------------------------------------
# Story 3.5 — Stale reference detection with did-you-mean
# ---------------------------------------------------------------------------


def test_compile_stale_stage_branch_raises_profile_error():
    """AC-1: stale branch ref raises ProfileError containing the unknown name."""
    @kptn.task(outputs=[])
    def load_source(): ...

    @kptn.task(outputs=[])
    def downstream(): ...

    pipeline = Pipeline("p", kptn.Stage("data_sources", load_source) >> downstream)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["load_raw"]})})
    with pytest.raises(ProfileError, match="load_raw"):
        ProfileResolver(config).compile(pipeline, "ci")


def test_compile_stale_stage_branch_did_you_mean():
    """AC-1: when a close match exists, the error includes 'Did you mean ...'."""
    @kptn.task(outputs=[])
    def load_source(): ...

    @kptn.task(outputs=[])
    def downstream(): ...

    pipeline = Pipeline("p", kptn.Stage("data_sources", load_source) >> downstream)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["load_raw"]})})
    with pytest.raises(ProfileError, match="Did you mean 'load_source'"):
        ProfileResolver(config).compile(pipeline, "ci")


def test_compile_stale_stage_branch_message_format():
    """AC-1: full error message matches the exact prescribed format."""
    @kptn.task(outputs=[])
    def load_source(): ...

    @kptn.task(outputs=[])
    def downstream(): ...

    pipeline = Pipeline("p", kptn.Stage("data_sources", load_source) >> downstream)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["load_raw"]})})
    with pytest.raises(ProfileError) as exc_info:
        ProfileResolver(config).compile(pipeline, "ci")
    assert str(exc_info.value) == (
        "profile 'ci' stage 'data_sources' references unknown pipeline 'load_raw'."
        " Did you mean 'load_source'?"
    )


def test_compile_stale_stage_branch_no_suggestion():
    """AC-2: when no close match exists, 'Did you mean' is absent."""
    @kptn.task(outputs=[])
    def load_source(): ...

    @kptn.task(outputs=[])
    def downstream(): ...

    pipeline = Pipeline("p", kptn.Stage("data_sources", load_source) >> downstream)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["xyzzy_no_match"]})})
    with pytest.raises(ProfileError) as exc_info:
        ProfileResolver(config).compile(pipeline, "ci")
    assert "Did you mean" not in str(exc_info.value)


def test_compile_stale_stage_branch_no_close_match_message():
    """AC-2: error message still contains the exact unknown name when no suggestion."""
    @kptn.task(outputs=[])
    def load_source(): ...

    @kptn.task(outputs=[])
    def downstream(): ...

    pipeline = Pipeline("p", kptn.Stage("data_sources", load_source) >> downstream)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["xyzzy_no_match"]})})
    with pytest.raises(ProfileError, match="xyzzy_no_match"):
        ProfileResolver(config).compile(pipeline, "ci")


def test_compile_valid_stage_refs_no_error():
    """AC-3: all refs valid → no exception, returns ResolvedGraph."""
    @kptn.task(outputs=[])
    def load_source(): ...

    @kptn.task(outputs=[])
    def downstream(): ...

    pipeline = Pipeline("p", kptn.Stage("data_sources", load_source) >> downstream)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["load_source"]})})
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert isinstance(result, ResolvedGraph)


def test_compile_stage_selection_for_absent_stage_no_error():
    """Stage absent from graph → silently skipped (no ProfileError), consistent with _prune."""
    pipeline = Pipeline("p", task_a >> task_b)
    config = KptnConfig(
        profiles={"ci": ProfileSpec(stage_selections={"nonexistent_stage": ["branch"]})}
    )
    result = ProfileResolver(config).compile(pipeline, "ci")
    assert isinstance(result, ResolvedGraph)


def test_compile_stale_ref_detected_before_prune():
    """Stale ref is detected even when mixed with valid refs in a multi-branch selection.

    Verifies that _validate_stage_refs catches a stale ref ("old_name") alongside a
    valid ref ("branch_active") in the same stage_selections list.

    Note on ordering invariant: calling _validate_stage_refs before _prune() ensures
    the full set of stage branches is visible when building the did-you-mean candidate
    pool. If called post-prune, unselected branches would be absent from the graph and
    could not appear as suggestions for misspelled refs.
    """
    @kptn.task(outputs=[])
    def branch_active(): ...

    @kptn.task(outputs=[])
    def branch_inactive(): ...

    @kptn.task(outputs=[])
    def downstream(): ...

    pipeline = Pipeline(
        "p",
        kptn.Stage("data_sources", branch_active, branch_inactive) >> downstream,
    )
    config = KptnConfig(
        profiles={
            "ci": ProfileSpec(
                stage_selections={"data_sources": ["branch_active", "old_name"]}
            )
        }
    )
    with pytest.raises(ProfileError, match="old_name"):
        ProfileResolver(config).compile(pipeline, "ci")


# ---------------------------------------------------------------------------
# Bypass tests — Phase 1 fix (OPT-01 through OPT-04)
# ---------------------------------------------------------------------------


def test_bypass_single_optional_in_chain():
    """OPT-01/D-01: disabled optional in A→B_opt→C reconnects A directly to C."""
    graph = task_a >> opt_task_1 >> task_c
    pipeline = Pipeline("test", graph)
    config = KptnConfig(profiles={"ci": ProfileSpec()})  # opt_grp absent → disabled
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "opt_task_1" not in node_names, "optional node must be removed"
    assert "task_a" in node_names
    assert "task_c" in node_names
    edge_pairs = {(s.name, d.name) for s, d in result.graph.edges}
    assert ("task_a", "task_c") in edge_pairs, "bypass edge task_a→task_c must exist"
    assert ("task_a", "opt_task_1") not in edge_pairs
    assert ("opt_task_1", "task_c") not in edge_pairs


def test_bypass_source_optional_successor_becomes_source():
    """OPT-03: source optional (no predecessors) — successor survives with no incoming edges."""
    graph = opt_task_1 >> task_c
    pipeline = Pipeline("test", graph)
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "opt_task_1" not in node_names
    assert "task_c" in node_names
    incoming_to_c = [s.name for s, d in result.graph.edges if d.name == "task_c"]
    assert incoming_to_c == [], f"task_c must have no predecessors (is source), got {incoming_to_c}"


def test_bypass_sink_optional_predecessor_becomes_tail():
    """OPT-03: sink optional (no successors) — predecessor remains in graph as tail node."""
    graph = task_a >> opt_task_1
    pipeline = Pipeline("test", graph)
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "opt_task_1" not in node_names
    assert "task_a" in node_names
    outgoing_from_a = [d.name for s, d in result.graph.edges if s.name == "task_a"]
    assert outgoing_from_a == [], f"task_a must have no successors (is tail), got {outgoing_from_a}"


def test_bypass_adjacent_optionals_transitive():
    """D-03: X→A_opt→B_opt→C — transitive BFS produces X→C (not two separate bypasses)."""
    graph = task_a >> opt_task_1 >> opt_task_2 >> task_c
    pipeline = Pipeline("test", graph)
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "opt_task_1" not in node_names
    assert "opt_task_2" not in node_names
    assert "task_a" in node_names
    assert "task_c" in node_names
    edge_pairs = {(s.name, d.name) for s, d in result.graph.edges}
    assert ("task_a", "task_c") in edge_pairs, "transitive bypass task_a→task_c must exist"
    # No dangling edges to/from removed optional nodes
    all_names = {n for pair in edge_pairs for n in pair}
    assert "opt_task_1" not in all_names
    assert "opt_task_2" not in all_names


def test_bypass_fanin_optional_both_preds_reconnected():
    """OPT-01: optional with multiple predecessors (A→C_opt←B; C_opt→D) — both A and B connect to D."""
    # Manual construction avoids duplicate TaskNode instances (M-1 concern)
    a_node = Graph._from_node(task_a).nodes[0]
    b_node = Graph._from_node(task_b).nodes[0]
    opt_node = Graph._from_node(opt_task_1).nodes[0]
    d_node = Graph._from_node(task_d).nodes[0]
    graph = Graph(
        nodes=[a_node, b_node, opt_node, d_node],
        edges=[(a_node, opt_node), (b_node, opt_node), (opt_node, d_node)],
    )
    pipeline = Pipeline("test", graph)
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "opt_task_1" not in node_names
    assert "task_a" in node_names
    assert "task_b" in node_names
    assert "task_d" in node_names
    edge_pairs = {(s.name, d.name) for s, d in result.graph.edges}
    assert ("task_a", "task_d") in edge_pairs, "bypass edge task_a→task_d must exist"
    assert ("task_b", "task_d") in edge_pairs, "bypass edge task_b→task_d must exist"


def test_bypass_fanout_optional_all_succs_reconnected():
    """OPT-01: optional with multiple successors (A→B_opt→C, A→B_opt→D) — A connects to both C and D."""
    # Manual construction to express fan-out from one optional node
    a_node = Graph._from_node(task_a).nodes[0]
    opt_node = Graph._from_node(opt_task_1).nodes[0]
    c_node = Graph._from_node(task_c).nodes[0]
    d_node = Graph._from_node(task_d).nodes[0]
    graph = Graph(
        nodes=[a_node, opt_node, c_node, d_node],
        edges=[(a_node, opt_node), (opt_node, c_node), (opt_node, d_node)],
    )
    pipeline = Pipeline("test", graph)
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "opt_task_1" not in node_names
    assert "task_a" in node_names
    assert "task_c" in node_names
    assert "task_d" in node_names
    edge_pairs = {(s.name, d.name) for s, d in result.graph.edges}
    assert ("task_a", "task_c") in edge_pairs, "bypass edge task_a→task_c must exist"
    assert ("task_a", "task_d") in edge_pairs, "bypass edge task_a→task_d must exist"


def test_bypass_stage_dead_plus_optional_cascade_boundary():
    """OPT-02/D-02: stage_dead→A, A→B_opt→C — stage cascade does NOT jump the optional boundary; C survives as source."""
    # Manual construction: StageNode("env") → task_a (stage-dead) → opt_task_1 (optional-dead) → task_c
    # task_b is the active branch (just to satisfy _validate_stage_refs)
    stage_node = StageNode("env")
    a_node = Graph._from_node(task_a).nodes[0]   # will be stage-dead
    b_node = Graph._from_node(task_b).nodes[0]   # active branch (goes nowhere else)
    opt_node = Graph._from_node(opt_task_1).nodes[0]
    c_node = Graph._from_node(task_c).nodes[0]
    graph = Graph(
        nodes=[stage_node, a_node, b_node, opt_node, c_node],
        edges=[
            (stage_node, a_node),   # StageNode → task_a (stage branch)
            (stage_node, b_node),   # StageNode → task_b (active branch)
            (a_node, opt_node),     # task_a → opt_task_1
            (opt_node, c_node),     # opt_task_1 → task_c
        ],
    )
    pipeline = Pipeline("test", graph)
    # stage_selections: task_b is active → task_a is stage-dead
    config = KptnConfig(
        profiles={"ci": ProfileSpec(stage_selections={"env": ["task_b"]})}
    )
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_a" not in node_names, "stage-dead node task_a must be removed"
    assert "opt_task_1" not in node_names, "optional-dead node must be removed"
    assert "task_c" in node_names, "task_c must survive (optional firewall)"
    # task_c must have no predecessors — it becomes a source node
    # (no bypass: opt_task_1's only pred is stage-dead task_a, BFS yields empty surv_preds)
    incoming_to_c = [s.name for s, d in result.graph.edges if d.name == "task_c"]
    assert incoming_to_c == [], f"task_c must be a source node, got predecessors: {incoming_to_c}"


def test_bypass_edge_dedup_no_duplicate_edges():
    """D-04: bypass injection skips edge if it already exists in the original graph (seen_bypass dedup)."""
    # Graph: task_a → opt_task_1 → task_c   AND   task_a → task_c (direct edge already exists)
    a_node = Graph._from_node(task_a).nodes[0]
    opt_node = Graph._from_node(opt_task_1).nodes[0]
    c_node = Graph._from_node(task_c).nodes[0]
    graph = Graph(
        nodes=[a_node, opt_node, c_node],
        edges=[(a_node, opt_node), (opt_node, c_node), (a_node, c_node)],
    )
    pipeline = Pipeline("test", graph)
    config = KptnConfig(profiles={"ci": ProfileSpec()})
    result = ProfileResolver(config).compile(pipeline, "ci")
    # task_a → task_c must appear EXACTLY ONCE
    ac_edges = [
        (s.name, d.name)
        for s, d in result.graph.edges
        if s.name == "task_a" and d.name == "task_c"
    ]
    assert len(ac_edges) == 1, f"task_a→task_c must appear exactly once, got {len(ac_edges)}: {ac_edges}"
