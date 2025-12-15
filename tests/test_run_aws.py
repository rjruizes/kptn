from textwrap import dedent

from kptn.cli.run_aws import (
    DirectRunConfig,
    ecs_task_console_url,
    follow_ecs_task_logs,
    run_ecs_task,
    submit_batch_job,
    task_execution_mode,
)


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


def test_ecs_task_console_url_from_task_arn():
    arn = "arn:aws:ecs:us-east-1:123456789012:task/sample-cluster/36cfcc29e7f943d7bce6960982ecd565"
    url = ecs_task_console_url(arn)

    assert (
        url
        == "https://us-east-1.console.aws.amazon.com/ecs/v2/clusters/"
        "sample-cluster/tasks/36cfcc29e7f943d7bce6960982ecd565/configuration"
    )


def test_follow_ecs_task_logs_streams_messages(capsys):
    class RecordingLogsClient:
        def __init__(self):
            self.calls = 0

        def get_log_events(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "events": [{"message": "first line"}],
                    "nextForwardToken": "token-1",
                }
            return {"events": [], "nextForwardToken": "token-1"}

    class RecordingEcsClient:
        def describe_task_definition(self, taskDefinition):
            return {
                "taskDefinition": {
                    "containerDefinitions": [
                        {
                            "name": "container",
                            "logConfiguration": {
                                "logDriver": "awslogs",
                                "options": {
                                    "awslogs-group": "/aws/ecs/test",
                                    "awslogs-stream-prefix": "prefix",
                                },
                            },
                        }
                    ]
                }
            }

        def describe_tasks(self, cluster, tasks):
            return {"tasks": [{"lastStatus": "STOPPED"}]}

    class Session:
        def __init__(self):
            self._ecs = RecordingEcsClient()
            self._logs = RecordingLogsClient()

        def client(self, name):
            if name == "ecs":
                return self._ecs
            if name == "logs":
                return self._logs
            raise AssertionError(f"Unexpected client {name}")

    stack_info = {
        "task_definition_arn": "arn:aws:ecs:us-east-1:123456789012:task-definition/example",
        "cluster_arn": "arn:aws:ecs:us-east-1:123456789012:cluster/sample",
    }

    follow_ecs_task_logs(
        session=Session(),
        task_arn="arn:aws:ecs:us-east-1:123456789012:task/sample/abcdef",
        stack_info=stack_info,
        poll_interval=0,
        max_polls=3,
    )

    captured = capsys.readouterr()
    assert "first line" in captured.out
    assert "No log events" not in captured.err


def test_follow_ecs_task_logs_handles_missing_log_config(capsys):
    class NoLogEcsClient:
        def describe_task_definition(self, taskDefinition):
            return {"taskDefinition": {"containerDefinitions": [{"name": "container"}]}}

        def describe_tasks(self, cluster, tasks):
            return {"tasks": [{"lastStatus": "STOPPED"}]}

    class Session:
        def client(self, name):
            if name == "ecs":
                return NoLogEcsClient()
            if name == "logs":
                raise AssertionError("Logs client should not be used when log config is missing")
            raise AssertionError(f"Unexpected client {name}")

    stack_info = {
        "task_definition_arn": "arn:aws:ecs:us-east-1:123456789012:task-definition/example",
        "cluster_arn": "arn:aws:ecs:us-east-1:123456789012:cluster/sample",
    }

    follow_ecs_task_logs(
        session=Session(),
        task_arn="arn:aws:ecs:us-east-1:123456789012:task/sample/abcdef",
        stack_info=stack_info,
        poll_interval=0,
        max_polls=1,
    )

    captured = capsys.readouterr()
    assert "Log streaming not available" in captured.err


def test_follow_ecs_task_logs_waits_for_stream_creation(capsys):
    class ResourceNotFound(Exception):
        def __init__(self):
            self.response = {"Error": {"Code": "ResourceNotFoundException"}}

    class LogsClient:
        def __init__(self):
            self.calls = 0

        def get_log_events(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise ResourceNotFound()
            return {
                "events": [{"message": "hello world"}],
                "nextForwardToken": "token-1",
            }

    class EcsClient:
        def describe_task_definition(self, taskDefinition):
            return {
                "taskDefinition": {
                    "containerDefinitions": [
                        {
                            "name": "container",
                            "logConfiguration": {
                                "logDriver": "awslogs",
                                "options": {
                                    "awslogs-group": "/aws/ecs/test",
                                    "awslogs-stream-prefix": "prefix",
                                },
                            },
                        }
                    ]
                }
            }

        def describe_tasks(self, cluster, tasks):
            return {"tasks": [{"lastStatus": "STOPPED"}]}

    class Session:
        def __init__(self):
            self._ecs = EcsClient()
            self._logs = LogsClient()

        def client(self, name):
            if name == "ecs":
                return self._ecs
            if name == "logs":
                return self._logs
            raise AssertionError(f"Unexpected client {name}")

    stack_info = {
        "task_definition_arn": "arn:aws:ecs:us-east-1:123456789012:task-definition/example",
        "cluster_arn": "arn:aws:ecs:us-east-1:123456789012:cluster/sample",
    }

    follow_ecs_task_logs(
        session=Session(),
        task_arn="arn:aws:ecs:us-east-1:123456789012:task/sample/abcdef",
        stack_info=stack_info,
        poll_interval=0,
        max_polls=3,
    )

    captured = capsys.readouterr()
    assert "hello world" in captured.out
    assert "Failed to fetch log events" not in captured.err
    assert "No log events were available" not in captured.err


def test_follow_ecs_task_logs_not_found_after_stop(capsys):
    class ResourceNotFound(Exception):
        def __init__(self):
            self.response = {"Error": {"Code": "ResourceNotFoundException"}}

    class LogsClient:
        def get_log_events(self, **kwargs):
            raise ResourceNotFound()

    class EcsClient:
        def describe_task_definition(self, taskDefinition):
            return {
                "taskDefinition": {
                    "containerDefinitions": [
                        {
                            "name": "container",
                            "logConfiguration": {
                                "logDriver": "awslogs",
                                "options": {
                                    "awslogs-group": "/aws/ecs/test",
                                    "awslogs-stream-prefix": "prefix",
                                },
                            },
                        }
                    ]
                }
            }

        def describe_tasks(self, cluster, tasks):
            return {"tasks": [{"lastStatus": "STOPPED"}]}

    class Session:
        def __init__(self):
            self._ecs = EcsClient()
            self._logs = LogsClient()

        def client(self, name):
            if name == "ecs":
                return self._ecs
            if name == "logs":
                return self._logs
            raise AssertionError(f"Unexpected client {name}")

    stack_info = {
        "task_definition_arn": "arn:aws:ecs:us-east-1:123456789012:task-definition/example",
        "cluster_arn": "arn:aws:ecs:us-east-1:123456789012:cluster/sample",
    }

    follow_ecs_task_logs(
        session=Session(),
        task_arn="arn:aws:ecs:us-east-1:123456789012:task/sample/abcdef",
        stack_info=stack_info,
        poll_interval=0,
        max_polls=2,
    )

    captured = capsys.readouterr()
    assert "Log stream not found for task" in captured.err
    assert "No log events were available" in captured.err


def test_ecs_task_console_url_with_cluster_fallback():
    task_arn = "arn:aws:ecs:us-east-1:123456789012:task/36cfcc29e7f943d7bce6960982ecd565"
    cluster_arn = "arn:aws:ecs:us-east-1:123456789012:cluster/sample-cluster"

    url = ecs_task_console_url(task_arn, cluster_arn)

    assert (
        url
        == "https://us-east-1.console.aws.amazon.com/ecs/v2/clusters/"
        "sample-cluster/tasks/36cfcc29e7f943d7bce6960982ecd565/configuration"
    )
