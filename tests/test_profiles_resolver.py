from __future__ import annotations

import pytest

import kptn
from kptn.exceptions import ProfileError
from kptn.graph.graph import Graph
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
    """Node reachable only via dead branch B is also pruned (forward propagation)."""
    stage_g = kptn.Stage("data_sources", task_b)
    pipeline = Pipeline("test", stage_g >> task_c)
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"data_sources": ["task_a"]})})
    result = ProfileResolver(config).compile(pipeline, "ci")
    node_names = {n.name for n in result.graph.nodes}
    assert "task_b" not in node_names
    assert "task_c" not in node_names


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
