import pytest

from kapten.codegen.lib.stepfunctions import (
    DEFAULT_STEP_FUNCTION_RESOURCE_ARN,
    build_state_machine_definition,
)


def test_parallel_branches_are_grouped_with_trailing_tasks():
    deps_lookup = {
        "A": None,
        "B": "A",
        "C": None,
        "D": ["B", "C"],
    }

    state_machine = build_state_machine_definition(
        "example",
        deps_lookup,
        task_order=["A", "B", "C", "D"],
    )

    assert state_machine["StartAt"] == "ParallelRoot"
    parallel_state = state_machine["States"]["ParallelRoot"]
    assert parallel_state["Type"] == "Parallel"
    assert parallel_state["Branches"][0]["States"]["A"]["Next"] == "B"
    assert parallel_state["Branches"][0]["States"]["B"]["End"] is True
    assert parallel_state["Branches"][1]["StartAt"] == "C"
    assert parallel_state["Branches"][1]["States"]["C"]["End"] is True
    assert parallel_state["Next"] == "D"

    d_state = state_machine["States"]["D"]
    assert d_state["Type"] == "Task"
    assert d_state["Resource"] == DEFAULT_STEP_FUNCTION_RESOURCE_ARN
    params = d_state["Parameters"]
    assert params["Cluster"] == "${ecs_cluster_arn}"
    assert params["TaskDefinition"] == "${ecs_task_definition_arn}"
    network_conf = params["NetworkConfiguration"]["AwsvpcConfiguration"]
    assert network_conf["Subnets"] == "${subnet_ids}"
    assert network_conf["SecurityGroups"] == "${security_group_ids}"
    container_override = params["Overrides"]["ContainerOverrides"][0]
    assert container_override["Name"] == "${container_name}"
    env_vars = {env["Name"]: env["Value"] for env in container_override["Environment"]}
    assert env_vars["KAPTEN_TASK"] == "D"
    assert env_vars["KAPTEN_PIPELINE"] == "example"
    assert d_state["ResultPath"] is None
    assert d_state["End"] is True


def test_sequential_pipeline_maps_to_linear_states():
    deps_lookup = {
        "Extract": None,
        "Transform": "Extract",
        "Load": "Transform",
    }

    state_machine = build_state_machine_definition(
        "etl",
        deps_lookup,
        task_order=["Extract", "Transform", "Load"],
    )

    assert state_machine["StartAt"] == "Extract"
    extract_state = state_machine["States"]["Extract"]
    assert extract_state["Next"] == "Transform"

    transform_state = state_machine["States"]["Transform"]
    assert transform_state["Next"] == "Load"

    load_state = state_machine["States"]["Load"]
    assert load_state["End"] is True
    assert load_state["ResultPath"] is None


def test_cycle_detection_raises_value_error():
    deps_lookup = {
        "A": "C",
        "B": "A",
        "C": "B",
    }

    with pytest.raises(ValueError):
        build_state_machine_definition(
            "cycle",
            deps_lookup,
            task_order=["A", "B", "C"],
        )
