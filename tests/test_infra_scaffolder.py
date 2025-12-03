from pathlib import Path

from kptn.codegen.infra_scaffolder import scaffold_stepfunctions_infra


def test_scaffold_creates_expected_files(tmp_path: Path) -> None:
    flows_dir = tmp_path / "flows"
    flows_dir.mkdir()
    pipeline_name = "basic"
    definition_path = flows_dir / f"{pipeline_name}.json"
    definition_path.write_text("{\n  \"StartAt\": \"A\",\n  \"States\": {}\n}\n", encoding="utf-8")

    output_dir = tmp_path / "infra" / "stepfunctions"
    tfvars_values = {
        "create_networking": False,
        "create_security_group": False,
        "create_ecr_repository": False,
        "create_task_definition": False,
        "create_task_execution_role": False,
        "subnet_ids": ["subnet-123456"],
        "security_group_ids": ["sg-123456"],
        "create_ecs_cluster": False,
        "ecs_cluster_arn": "arn:aws:ecs:us-east-1:123456789012:cluster/sample",
        "ecs_task_definition_arn": "arn:aws:ecs:us-east-1:123456789012:task-definition/sample:1",
        "ecs_task_execution_role_arn": "arn:aws:iam::123456789012:role/sample",
    }

    report = scaffold_stepfunctions_infra(
        output_dir=output_dir,
        pipeline_name=pipeline_name,
        flows_dir=flows_dir,
        force=False,
        tfvars_values=tfvars_values,
    )

    assert (output_dir / "main.tf").exists()
    assert (output_dir / "networking.tf").exists()
    assert (output_dir / "ecs.tf").exists()
    assert (output_dir / "task_definition.tf").exists()
    assert (output_dir / "ecr.tf").exists()
    assert (output_dir / "task_execution_role.tf").exists()
    assert (output_dir / "locals.tf").exists()
    assert (output_dir / "stack_info.tf").exists()
    assert (output_dir / "variables.tf").exists()
    assert (output_dir / "outputs.tf").exists()
    assert (output_dir / "README.md").exists()
    assert (output_dir / "terraform.tfvars").exists()

    variables_content = (output_dir / "variables.tf").read_text(encoding="utf-8")
    assert "basic.json" in variables_content

    tfvars_content = (output_dir / "terraform.tfvars").read_text(encoding="utf-8")
    assert "create_networking = false" in tfvars_content
    assert "create_task_definition = false" in tfvars_content
    assert "create_security_group = false" in tfvars_content
    assert "create_ecr_repository = false" in tfvars_content
    assert "create_task_execution_role = false" in tfvars_content
    assert '"sg-123456"' in tfvars_content

    assert report.state_machine_file == definition_path.resolve()
    assert report.state_machine_file_exists is True
    assert report.terraform_tfvars_path == (output_dir / "terraform.tfvars")
    assert report.warnings == []


def test_scaffold_skips_existing_without_force(tmp_path: Path) -> None:
    flows_dir = tmp_path / "flows"
    flows_dir.mkdir()
    (flows_dir / "pipe.json").write_text("{}\n", encoding="utf-8")

    output_dir = tmp_path / "infra"
    output_dir.mkdir(parents=True)
    existing_file = output_dir / "main.tf"
    existing_file.write_text("# existing\n", encoding="utf-8")

    tfvars_values = {
        "create_networking": False,
        "create_security_group": False,
        "create_ecr_repository": False,
        "create_task_definition": False,
        "create_task_execution_role": False,
        "subnet_ids": ["subnet-1"],
        "security_group_ids": ["sg-1"],
        "create_ecs_cluster": False,
        "ecs_cluster_arn": "arn:aws:ecs:us-east-1:123456789012:cluster/sample",
        "ecs_task_definition_arn": "arn:aws:ecs:us-east-1:123456789012:task-definition/sample:1",
        "ecs_task_execution_role_arn": "arn:aws:iam::123456789012:role/sample",
    }

    existing_tfvars = output_dir / "terraform.tfvars"
    existing_tfvars.write_text("create_networking = false\n", encoding="utf-8")

    report = scaffold_stepfunctions_infra(
        output_dir=output_dir,
        pipeline_name="pipe",
        flows_dir=flows_dir,
        force=False,
        tfvars_values=tfvars_values,
    )

    assert existing_file in report.skipped
    assert existing_file not in report.created
    assert existing_tfvars in report.skipped

    report_force = scaffold_stepfunctions_infra(
        output_dir=output_dir,
        pipeline_name="pipe",
        flows_dir=flows_dir,
        force=True,
        tfvars_values=tfvars_values,
    )

    assert existing_file in report_force.created
    assert "# existing" not in existing_file.read_text(encoding="utf-8")
    tfvars_text = existing_tfvars.read_text(encoding="utf-8")
    assert "create_networking = false" in tfvars_text


def test_scaffold_with_task_definition_creation(tmp_path: Path) -> None:
    flows_dir = tmp_path / "flows"
    flows_dir.mkdir()
    (flows_dir / "pipe.json").write_text("{}\n", encoding="utf-8")

    output_dir = tmp_path / "infra"

    tfvars_values = {
        "create_networking": False,
        "create_security_group": False,
        "create_ecr_repository": True,
        "create_task_definition": True,
        "create_task_execution_role": True,
        "create_ecs_cluster": False,
        "task_definition_family": "kptn-pipe",
        "task_definition_cpu": "256",
        "task_definition_memory": "512",
        "task_definition_network_mode": "awsvpc",
        "task_definition_container_name": "pipe",
        "task_definition_container_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/pipe:latest",
        "task_definition_container_command": ["python", "-m", "pipeline"],
        "task_definition_container_environment": {"ENV": "prod"},
        "task_definition_requires_compatibilities": ["FARGATE"],
        "ecr_repository_name": "kptn-pipe",
        "task_execution_role_name_prefix": "kptn-pipe-task-exec",
    }

    report = scaffold_stepfunctions_infra(
        output_dir=output_dir,
        pipeline_name="pipe",
        flows_dir=flows_dir,
        force=False,
        tfvars_values=tfvars_values,
    )

    task_definition_tf = (output_dir / "task_definition.tf").read_text(encoding="utf-8")
    assert "aws_ecs_task_definition" in task_definition_tf
    assert report.terraform_tfvars_path is not None

    iam_tf = (output_dir / "task_execution_role.tf").read_text(encoding="utf-8")
    assert "aws_iam_role" in iam_tf

    ecr_tf = (output_dir / "ecr.tf").read_text(encoding="utf-8")
    assert "aws_ecr_repository" in ecr_tf

    tfvars_content = report.terraform_tfvars_path.read_text(encoding="utf-8")
    assert "create_task_definition = true" in tfvars_content
    assert "task_definition_container_image" in tfvars_content
    assert "create_ecr_repository = true" in tfvars_content
    assert "create_task_execution_role = true" in tfvars_content
