import pytest

from kptn.cli.run_aws import parse_tasks_arg


def test_parse_tasks_arg_handles_values_and_empty() -> None:
    assert parse_tasks_arg("a,b , c") == ["a", "b", "c"]
    assert parse_tasks_arg(None) == []
    assert parse_tasks_arg("") == []
    with pytest.raises(ValueError):
        parse_tasks_arg(" , , ")


def test_choose_state_machine_prefers_pipeline_key() -> None:
    from kptn.cli.run_aws import choose_state_machine_arn

    stack_info = {
        "state_machine_arns": {
            "alpha": "arn:alpha",
            "beta": "arn:beta",
        },
        "state_machine_arn": "arn:default",
    }

    # pipeline match should win over default and sorted-first fallback
    assert choose_state_machine_arn(stack_info, pipeline="beta") == "arn:beta"
    # preferred key still wins when provided
    assert choose_state_machine_arn(stack_info, preferred_key="alpha", pipeline="beta") == "arn:alpha"
