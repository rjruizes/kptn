from textwrap import dedent

from kptn.cli.run_aws import DirectRunConfig, run_ecs_task, task_execution_mode


def test_task_execution_mode_defaults_and_overrides(tmp_path, monkeypatch):
    config = dedent(
        """
        tasks:
          simple:
            file: tasks/simple.py
          mapped:
            file: tasks/mapped.py
            map_over: item
          explicit_batch:
            file: tasks/batch.py
            execution:
              mode: batch_array
        """
    )
    (tmp_path / "kptn.yaml").write_text(config)
    monkeypatch.chdir(tmp_path)

    assert task_execution_mode("simple") == "ecs"
    assert task_execution_mode("mapped") == "batch_array"
    assert task_execution_mode("explicit_batch") == "batch_array"
    assert task_execution_mode("missing") is None


def test_task_execution_mode_without_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert task_execution_mode("anything") is None


def test_run_ecs_task_applies_compute_overrides():
    captured = {}

    class RecordingEcsClient:
        def run_task(self, **kwargs):
            captured["kwargs"] = kwargs
            return {"tasks": [{"taskArn": "task-arn"}], "failures": []}

    class Session:
        def client(self, name):
            assert name == "ecs"
            return RecordingEcsClient()

    stack_info = {
        "cluster_arn": "cluster",
        "task_definition_arn": "taskdef",
        "task_definition_container_name": "container",
    }

    compute = {"cpu": 1024, "memory": 4096}

    response = run_ecs_task(
        session=Session(),
        stack_info=stack_info,
        pipeline="pipe",
        task="task",
        config=DirectRunConfig(),
        compute=compute,
    )

    assert response["tasks"][0]["taskArn"] == "task-arn"
    overrides = captured["kwargs"]["overrides"]
    assert overrides["cpu"] == "1024"
    assert overrides["memory"] == "4096"
    container_override = overrides["containerOverrides"][0]
    assert container_override["cpu"] == 1024
    assert container_override["memory"] == 4096
