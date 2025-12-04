from textwrap import dedent

from kptn.cli.run_aws import DirectRunConfig, run_ecs_task, submit_batch_job, task_execution_mode


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


def test_submit_batch_job_builds_array_properties():
    captured = {}

    class RecordingBatchClient:
        def submit_job(self, **kwargs):
            captured["kwargs"] = kwargs
            return {"jobId": "job-id", "jobName": kwargs["jobName"]}

    class Session:
        def client(self, name):
            assert name == "batch"
            return RecordingBatchClient()

    stack_info = {
        "batch_job_queue_arn": "queue-arn",
        "batch_job_definition_arn": "definition-arn",
        "dynamodb_table_name": "table-name",
    }

    response = submit_batch_job(
        session=Session(),
        stack_info=stack_info,
        pipeline="pipe",
        task="task",
        resource_requirements=[{"type": "VCPU", "value": "2"}],
        array_size=3,
        decision_reason="No cached state",
    )

    kwargs = captured["kwargs"]
    env_lookup = {entry["name"]: entry["value"] for entry in kwargs["containerOverrides"]["environment"]}

    assert kwargs["arrayProperties"] == {"size": 3}
    assert env_lookup["ARRAY_SIZE"] == "3"
    assert env_lookup["KAPTEN_DECISION_REASON"] == "No cached state"
    assert env_lookup["DYNAMODB_TABLE_NAME"] == "table-name"
    assert response["jobId"] == "job-id"
