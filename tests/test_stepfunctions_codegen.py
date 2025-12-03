import pytest

from kptn.codegen.lib.stepfunctions import (
    DEFAULT_BATCH_RESOURCE_ARN,
    DEFAULT_STEP_FUNCTION_RESOURCE_ARN,
    build_state_machine_definition,
)


def _build_state_machine(deps_lookup, tasks, order):
    return build_state_machine_definition(
        "example",
        deps_lookup,
        tasks=tasks,
        task_order=order,
        decider_lambda_arn="arn:aws:lambda:us-east-1:123456789012:function:decider",
    )


def test_parallel_branches_are_grouped_with_decider_and_trailing_tasks():
    deps_lookup = {
        "A": None,
        "B": "A",
        "C": None,
        "D": ["B", "C"],
    }
    tasks = {name: {} for name in deps_lookup}

    state_machine = _build_state_machine(deps_lookup, tasks, ["A", "B", "C", "D"])

    start_state = state_machine["StartAt"]
    assert start_state == "Lane0Parallel"
    parallel_state = state_machine["States"][start_state]
    assert parallel_state["Type"] == "Parallel"

    first_branch = parallel_state["Branches"][0]
    assert first_branch["StartAt"] == "A_Decide"
    branch_states = first_branch["States"]
    assert branch_states["A_Decide"]["Resource"] == "arn:aws:states:::lambda:invoke"
    assert branch_states["A_Decide"]["Parameters"]["FunctionName"] == "arn:aws:lambda:us-east-1:123456789012:function:decider"
    payload_params = branch_states["A_Decide"]["Parameters"]["Payload"]
    assert payload_params["state.$"] == "$"
    assert payload_params["task_name"] == "A"
    assert payload_params["task_list.$"] == "$.tasks"
    assert payload_params["ignore_cache.$"] == "$.force"
    assert payload_params["execution_mode"] == "ecs"
    assert payload_params["TASKS_CONFIG_PATH"] == "kptn.yaml"
    assert payload_params["PIPELINE_NAME"] == "example"
    assert branch_states["A_Choice"]["Type"] == "Choice"
    assert branch_states["A_RunEcs"]["End"] is True

    second_branch = parallel_state["Branches"][1]
    assert second_branch["StartAt"] == "C_Decide"
    assert second_branch["States"]["C_RunEcs"]["End"] is True

    assert parallel_state["Next"] == "B_Decide"

    states = state_machine["States"]
    assert states["B_RunEcs"]["Next"] == "D_Decide"
    assert states["B_Skip"]["Next"] == "D_Decide"
    assert states["D_Decide"]["Parameters"]["FunctionName"] == "arn:aws:lambda:us-east-1:123456789012:function:decider"
    d_run_ecs = states["D_RunEcs"]
    assert d_run_ecs["Type"] == "Task"
    assert d_run_ecs["Resource"] == DEFAULT_STEP_FUNCTION_RESOURCE_ARN
    params = d_run_ecs["Parameters"]
    assert params["Cluster"] == "${ecs_cluster_arn}"
    assert params["TaskDefinition"] == "${ecs_task_definition_arn}"
    network_conf = params["NetworkConfiguration"]["AwsvpcConfiguration"]
    assert network_conf["Subnets"] == "${subnet_ids}"
    assert network_conf["SecurityGroups"] == "${security_group_ids}"
    container_override = params["Overrides"]["ContainerOverrides"][0]
    assert container_override["Name"] == "${container_name}"
    env_vars = {env["Name"]: env.get("Value") or env.get("Value.$") for env in container_override["Environment"]}
    assert env_vars["KAPTEN_TASK"] == "D"
    assert env_vars["KAPTEN_PIPELINE"] == "example"
    assert d_run_ecs["ResultPath"] is None
    assert d_run_ecs["End"] is True


def test_sequential_pipeline_maps_to_linear_states():
    deps_lookup = {
        "Extract": None,
        "Transform": "Extract",
        "Load": "Transform",
    }
    tasks = {name: {} for name in deps_lookup}

    state_machine = _build_state_machine(deps_lookup, tasks, ["Extract", "Transform", "Load"])

    assert state_machine["StartAt"] == "Extract_Decide"
    states = state_machine["States"]
    assert states["Extract_RunEcs"]["Next"] == "Transform_Decide"
    assert states["Transform_RunEcs"]["Next"] == "Load_Decide"
    assert states["Load_RunEcs"]["End"] is True


def test_mapped_task_uses_batch_branch():
    deps_lookup = {"List": None, "Process": "List"}
    tasks = {
        "List": {"cache_result": True},
        "Process": {"map_over": "item"},
    }

    state_machine = _build_state_machine(deps_lookup, tasks, ["List", "Process"])
    states = state_machine["States"]

    process_choice = states["Process_Choice"]
    batch_choice = next(
        choice
        for choice in process_choice["Choices"]
        if choice.get("Next") == "Process_RunBatch"
    )
    assert batch_choice["And"][0]["Variable"] == "$.last_decision.Payload.should_run"
    assert batch_choice["And"][1]["Variable"] == "$.last_decision.Payload.execution_mode"
    assert batch_choice["And"][1]["StringEquals"] == "batch_array"

    batch_state = states["Process_RunBatch"]
    assert batch_state["Resource"] == DEFAULT_BATCH_RESOURCE_ARN
    params = batch_state["Parameters"]
    assert params["JobQueue"] == "${batch_job_queue_arn}"
    assert params["JobDefinition"] == "${batch_job_definition_arn}"
    assert params["ArrayProperties"]["Size.$"] == "$.last_decision.Payload.array_size"


def test_cycle_detection_raises_value_error():
    deps_lookup = {
        "A": "C",
        "B": "A",
        "C": "B",
    }
    tasks = {name: {} for name in deps_lookup}

    with pytest.raises(ValueError):
        _build_state_machine(deps_lookup, tasks, ["A", "B", "C"])
