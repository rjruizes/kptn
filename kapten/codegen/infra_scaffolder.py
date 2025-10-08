from __future__ import annotations

import os
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

MAIN_TF_TEMPLATE = """\
terraform {
  required_version = ">= 1.4.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_region" "current" {}

data "aws_partition" "current" {}

data "aws_caller_identity" "current" {}

locals {
  pipeline_name      = var.pipeline_name
  state_machine_name = "${var.pipeline_name}-state-machine"
  state_machine_definition = length(trimspace(var.state_machine_definition)) > 0 ? var.state_machine_definition : templatefile(
    "${path.module}/${var.state_machine_definition_file}",
    {
      ecs_cluster_arn             = local.ecs_cluster_arn_effective
      ecs_task_definition_arn     = local.ecs_task_definition_arn_effective
      ecs_task_execution_role_arn = local.ecs_task_execution_role_arn_effective
      subnet_ids                  = jsonencode(local.subnet_ids_effective)
      security_group_ids          = jsonencode(local.security_group_ids_effective)
      assign_public_ip            = var.assign_public_ip ? "ENABLED" : "DISABLED"
      launch_type                 = var.ecs_launch_type
      container_name              = var.task_definition_container_name
      pipeline_name               = var.pipeline_name
    }
  )
}

resource "aws_cloudwatch_log_group" "step_function" {
  name              = "/aws/vendedlogs/states/${local.pipeline_name}"
  retention_in_days = var.log_retention_in_days
  tags              = var.tags
}

resource "aws_iam_role" "step_function" {
  name_prefix = "${local.pipeline_name}-sfn-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "step_function" {
  name = "${local.pipeline_name}-sfn-policy"
  role = aws_iam_role.step_function.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
          "ecs:DescribeTasks",
          "ecs:StopTask"
        ]
        Resource = [
          local.ecs_task_definition_arn_effective
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = local.ecs_task_execution_role_arn_effective
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.step_function.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule"
        ]
        Resource = [
          "arn:${data.aws_partition.current.partition}:events:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForECSTaskRule",
          "arn:${data.aws_partition.current.partition}:events:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"
        ]
      }
    ]
  })
}

resource "aws_sfn_state_machine" "this" {
  name       = local.state_machine_name
  role_arn   = aws_iam_role.step_function.arn
  definition = local.state_machine_definition

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_function.arn}:*"
    include_execution_data = true
    level                  = var.logging_level
  }

  tags = var.tags
}
"""

NETWORKING_TF_TEMPLATE = """\
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "kapten" {
  for_each = var.create_networking ? { main = true } : {}

  cidr_block           = var.new_vpc_cidr_block
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = merge(var.tags, { Name = "${var.pipeline_name}-vpc" })
}

resource "aws_subnet" "kapten" {
  for_each = var.create_networking ? { for idx, cidr in var.new_subnet_cidr_blocks : idx => cidr } : {}

  vpc_id            = aws_vpc.kapten["main"].id
  cidr_block        = each.value
  availability_zone = length(var.new_subnet_availability_zones) > each.key ? var.new_subnet_availability_zones[each.key] : data.aws_availability_zones.available.names[each.key % length(data.aws_availability_zones.available.names)]
  map_public_ip_on_launch = true
  tags = merge(var.tags, { Name = format("%s-subnet-%02d", var.pipeline_name, each.key + 1) })
}

resource "aws_security_group" "kapten" {
  for_each = var.create_security_group ? { main = true } : {}

  name_prefix = "${var.pipeline_name}-tasks-"
  description = var.new_security_group_description
  vpc_id      = var.create_networking ? aws_vpc.kapten["main"].id : var.vpc_id
  tags        = merge(var.tags, { Name = "${var.pipeline_name}-tasks-sg" })
}

resource "aws_security_group_rule" "kapten_ingress" {
  for_each = var.create_security_group ? { for idx, cidr in var.new_security_group_ingress_cidr_blocks : idx => cidr } : {}

  security_group_id = aws_security_group.kapten["main"].id
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = [each.value]
}

resource "aws_security_group_rule" "kapten_egress" {
  for_each = var.create_security_group ? { for idx, cidr in var.new_security_group_egress_cidr_blocks : idx => cidr } : {}

  security_group_id = aws_security_group.kapten["main"].id
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = [each.value]
}

locals {
  subnet_ids_effective = var.create_networking ? sort([for subnet in values(aws_subnet.kapten) : subnet.id]) : var.subnet_ids

  security_group_ids_effective = var.create_security_group ? concat([aws_security_group.kapten["main"].id], var.security_group_ids) : var.security_group_ids
}
"""

ECS_TF_TEMPLATE = """\
resource "aws_ecs_cluster" "kapten" {
  for_each = var.create_ecs_cluster ? { main = true } : {}

  name = "${var.pipeline_name}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = var.tags
}
"""

TASK_DEFINITION_TF_TEMPLATE = """\
resource "aws_ecs_task_definition" "kapten" {
  for_each = var.create_task_definition ? { main = true } : {}

  family                   = var.task_definition_family
  cpu                      = var.task_definition_cpu
  memory                   = var.task_definition_memory
  network_mode             = var.task_definition_network_mode
  requires_compatibilities = var.task_definition_requires_compatibilities
  execution_role_arn       = local.ecs_task_execution_role_arn_effective
  task_role_arn            = var.task_definition_task_role_arn

  container_definitions = jsonencode([
    {
      name        = var.task_definition_container_name
      image       = var.task_definition_container_image
      essential   = true
      command     = var.task_definition_container_command
      environment = [for k, v in var.task_definition_container_environment : { name = k, value = v }]
    }
  ])
}
"""

ECR_TF_TEMPLATE = """\
resource "aws_ecr_repository" "kapten" {
  for_each = var.create_ecr_repository ? { main = true } : {}

  name                 = var.ecr_repository_name
  image_tag_mutability = var.ecr_repository_image_tag_mutability

  image_scanning_configuration {
    scan_on_push = var.ecr_repository_scan_on_push
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = var.tags
}
"""

TASK_EXECUTION_ROLE_TF_TEMPLATE = """\
resource "aws_iam_role" "kapten_execution" {
  for_each = var.create_task_execution_role ? { main = true } : {}

  name_prefix = var.task_execution_role_name_prefix

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "kapten_execution_managed" {
  for_each = var.create_task_execution_role ? { for idx, policy in var.task_execution_role_managed_policies : idx => policy } : {}

  role       = aws_iam_role.kapten_execution["main"].name
  policy_arn = each.value
}
"""

EFFECTIVE_LOCALS_TEMPLATE = """\
locals {
  ecs_cluster_arn_effective = var.create_ecs_cluster ? aws_ecs_cluster.kapten["main"].arn : var.ecs_cluster_arn

  ecs_task_definition_arn_effective = var.create_task_definition ? aws_ecs_task_definition.kapten["main"].arn : var.ecs_task_definition_arn

  ecs_task_execution_role_arn_effective = var.create_task_execution_role ? aws_iam_role.kapten_execution["main"].arn : var.ecs_task_execution_role_arn

  ecr_repository_url_effective = var.create_ecr_repository ? aws_ecr_repository.kapten["main"].repository_url : var.ecr_repository_url
}
"""

VARIABLES_TF_TEMPLATE = """\
variable "region" {
  type        = string
  description = "AWS region to deploy Step Functions resources into"
  default     = "us-east-1"
}

variable "pipeline_name" {
  type        = string
  description = "Kapten pipeline name represented by this state machine"
}

variable "state_machine_definition" {
  type        = string
  description = "Optional raw JSON definition override for the state machine"
  default     = ""
}

variable "state_machine_definition_file" {
  type        = string
  description = "Relative path to the generated Kapten Step Functions JSON definition"
  default     = "__STATE_MACHINE_DEFINITION_FILE__"
}

variable "create_networking" {
  type        = bool
  description = "Set to true to provision a new VPC and subnets"
  default     = false
}

variable "create_security_group" {
  type        = bool
  description = "Set to true to provision a new security group"
  default     = false
}

variable "create_ecr_repository" {
  type        = bool
  description = "Set to true to provision an ECR repository"
  default     = false
}

variable "vpc_id" {
  type        = string
  description = "Existing VPC ID used when provisioning only a security group"
  default     = null
}

variable "ecr_repository_name" {
  type        = string
  description = "Name assigned to the generated ECR repository"
  default     = ""
}

variable "ecr_repository_image_tag_mutability" {
  type        = string
  description = "Image tag mutability for the generated ECR repository"
  default     = "MUTABLE"
}

variable "ecr_repository_scan_on_push" {
  type        = bool
  description = "Enable image scan on push for the generated ECR repository"
  default     = true
}

variable "ecr_repository_url" {
  type        = string
  description = "Existing ECR repository URL to reuse"
  default     = null
}

variable "new_vpc_cidr_block" {
  type        = string
  description = "CIDR block for a newly provisioned VPC"
  default     = "10.0.0.0/16"
}

variable "new_subnet_cidr_blocks" {
  type        = list(string)
  description = "CIDR blocks for subnets created when networking is provisioned"
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "new_subnet_availability_zones" {
  type        = list(string)
  description = "Optional AZ overrides for new subnets; leave empty to auto-select"
  default     = []
}

variable "new_security_group_ingress_cidr_blocks" {
  type        = list(string)
  description = "CIDR blocks allowed to reach ECS tasks when provisioning networking"
  default     = ["0.0.0.0/0"]
}

variable "new_security_group_egress_cidr_blocks" {
  type        = list(string)
  description = "CIDR blocks reachable from ECS tasks when provisioning networking"
  default     = ["0.0.0.0/0"]
}

variable "new_security_group_description" {
  type        = string
  description = "Description applied to the generated security group"
  default     = "Kapten Step Functions tasks"
}

variable "subnet_ids" {
  type        = list(string)
  description = "Subnet IDs to reuse when not provisioning networking"
  default     = []
}

variable "security_group_ids" {
  type        = list(string)
  description = "Security group IDs to reuse when not provisioning networking"
  default     = []
}

variable "create_ecs_cluster" {
  type        = bool
  description = "Set to true to provision a dedicated ECS cluster"
  default     = false
}

variable "ecs_cluster_arn" {
  type        = string
  description = "Existing ECS cluster ARN to reuse when not creating one"
  default     = null
}

variable "ecs_task_definition_arn" {
  type        = string
  description = "Existing ECS task definition ARN to reuse when not creating one"
  default     = null
}

variable "create_task_definition" {
  type        = bool
  description = "Set to true to provision an ECS task definition"
  default     = false
}

variable "task_definition_family" {
  type        = string
  description = "Family name assigned to the generated ECS task definition"
  default     = "kapten-task"
}

variable "task_definition_cpu" {
  type        = string
  description = "CPU units assigned to the ECS task definition"
  default     = "512"
}

variable "task_definition_memory" {
  type        = string
  description = "Memory (MiB) assigned to the ECS task definition"
  default     = "1024"
}

variable "task_definition_network_mode" {
  type        = string
  description = "Network mode used by the ECS task definition"
  default     = "awsvpc"
}

variable "task_definition_requires_compatibilities" {
  type        = list(string)
  description = "Launch types supported by the ECS task definition"
  default     = ["FARGATE"]
}

variable "task_definition_task_role_arn" {
  type        = string
  description = "IAM task role ARN used by containers in the ECS task definition"
  default     = null
}

variable "task_definition_container_name" {
  type        = string
  description = "Name assigned to the container in the generated task definition"
  default     = "kapten"
}

variable "task_definition_container_image" {
  type        = string
  description = "Container image URI for the ECS task definition"
  default     = "public.ecr.aws/amazonlinux/amazonlinux:latest"
}

variable "task_definition_container_command" {
  type        = list(string)
  description = "Command override for the container (leave empty to use image defaults)"
  default     = []
}

variable "task_definition_container_environment" {
  type        = map(string)
  description = "Environment variables injected into the container"
  default     = {}
}

variable "create_task_execution_role" {
  type        = bool
  description = "Set to true to provision an ECS task execution role"
  default     = false
}

variable "task_execution_role_name_prefix" {
  type        = string
  description = "Name prefix applied to a generated ECS task execution role"
  default     = "kapten-task-execution-role"
}

variable "task_execution_role_managed_policies" {
  type        = list(string)
  description = "Managed policy ARNs attached to a generated task execution role"
  default     = [
    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
  ]
}

variable "ecs_task_execution_role_arn" {
  type        = string
  description = "Existing IAM role ARN that ECS tasks assume when invoked"
  default     = null
}

variable "assign_public_ip" {
  type        = bool
  description = "Assign a public IP when running ECS tasks"
  default     = false
}

variable "ecs_launch_type" {
  type        = string
  description = "Launch type used when invoking ECS tasks"
  default     = "FARGATE"
}

variable "logging_level" {
  type        = string
  description = "Logging level for Step Functions state machine executions"
  default     = "ALL"
}

variable "log_retention_in_days" {
  type        = number
  description = "Retention period for the Step Functions CloudWatch log group"
  default     = 30
}

variable "tags" {
  type        = map(string)
  description = "Common tags applied to created resources"
  default     = {}
}
"""

OUTPUTS_TF_TEMPLATE = """\
output "state_machine_arn" {
  description = "ARN of the generated Step Functions state machine"
  value       = aws_sfn_state_machine.this.arn
}

output "state_machine_role_arn" {
  description = "IAM role ARN assumed by the Step Functions state machine"
  value       = aws_iam_role.step_function.arn
}

output "log_group_name" {
  description = "CloudWatch log group capturing Step Functions execution logs"
  value       = aws_cloudwatch_log_group.step_function.name
}

output "ecs_cluster_arn_effective" {
  description = "ARN of the ECS cluster used by Kapten tasks (existing or provisioned)"
  value       = local.ecs_cluster_arn_effective
}

output "subnet_ids_effective" {
  description = "Subnet IDs available to ECS tasks"
  value       = local.subnet_ids_effective
}

output "security_group_ids_effective" {
  description = "Security group IDs attached to ECS tasks"
  value       = local.security_group_ids_effective
}

output "task_definition_arn_effective" {
  description = "ARN of the ECS task definition used by Kapten tasks"
  value       = local.ecs_task_definition_arn_effective
}

output "task_execution_role_arn_effective" {
  description = "ARN of the ECS task execution role used by Kapten tasks"
  value       = local.ecs_task_execution_role_arn_effective
}

output "ecr_repository_url_effective" {
  description = "URL of the ECR repository used for Kapten task images"
  value       = local.ecr_repository_url_effective
}
"""

README_TEMPLATE = """\
# Step Functions + ECS Infrastructure

This directory was generated by `kapten codegen-infra` for the `{pipeline_name}` pipeline.
It scaffolds the core AWS resources required to execute Kapten tasks via AWS Step Functions
and ECS.

## Contents

- `main.tf` provisions the Step Functions state machine, IAM role, and CloudWatch log group.
- `networking.tf` optionally creates a VPC, subnets, and security group when `create_networking` is true.
- `ecs.tf` optionally creates an ECS cluster when `create_ecs_cluster` is true.
- `task_definition.tf` optionally creates an ECS task definition when `create_task_definition` is true.
- `ecr.tf` optionally creates an ECR repository when `create_ecr_repository` is true.
- `task_execution_role.tf` optionally creates an ECS task execution role when `create_task_execution_role` is true.
- `locals.tf` centralizes computed references reused across the stack.
- `variables.tf` defines configuration inputs used by the Terraform stack.
- `outputs.tf` surfaces useful identifiers after `terraform apply` runs.

## Workflow

1. Ensure the Kapten Step Functions definition JSON is up to date by running `kapten codegen`.
2. Decide whether to reuse existing AWS resources or provision new ones:
   - Set `create_networking`, `create_security_group`, `create_ecr_repository`,
     `create_task_definition`, `create_task_execution_role`, and/or
     `create_ecs_cluster` to `true` to scaffold fresh infrastructure.
   - Leave them `false` and populate `vpc_id`, `subnet_ids`, `security_group_ids`,
     `ecr_repository_url`, `ecs_task_definition_arn`, `ecs_task_execution_role_arn`,
     and `ecs_cluster_arn` to reuse existing resources.
3. Update `terraform.tfvars` (generated via the interactive CLI, if used) with environment-specific values
   such as subnet IDs, security groups, ECS ARNs, task definitions, and networking CIDRs.
4. Initialize and apply the Terraform configuration:

   ```bash
   terraform init
   terraform plan -var="pipeline_name={pipeline_name}"
   terraform apply -var="pipeline_name={pipeline_name}"
   ```

   Include additional `-var` flags or reference a `.tfvars` file as needed.

Review the generated Terraform files to tailor permissions, logging, and network access policies to
match your organization's standards.
"""


@dataclass
class ScaffoldReport:
    created: list[Path]
    skipped: list[Path]
    output_dir: Path
    state_machine_file: Path
    state_machine_file_exists: bool
    terraform_tfvars_path: Path | None
    warnings: list[str]


def _ensure_trailing_newline(content: str) -> str:
    return content if content.endswith("\n") else f"{content}\n"


def _relative_definition_path(flows_dir: Path, pipeline_name: str, output_dir: Path) -> tuple[str, Path]:
    # Check for .json.tpl first (template file), then .json
    tpl_path = flows_dir / f"{pipeline_name}.json.tpl"
    json_path = flows_dir / f"{pipeline_name}.json"
    definition_path = tpl_path if tpl_path.exists() else json_path
    rel_path = os.path.relpath(definition_path, output_dir)
    return Path(rel_path).as_posix(), definition_path


def _to_hcl(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Path):
        return _to_hcl(str(value))
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(value, set):
        value = sorted(value)
    if isinstance(value, (list, tuple)):
        items = ", ".join(_to_hcl(item) for item in value)
        return f"[{items}]"
    if isinstance(value, dict):
        parts = [f"{key} = {_to_hcl(val)}" for key, val in value.items()]
        inner = ",\n  ".join(parts)
        return "{\n  " + inner + "\n}"
    raise TypeError(f"Unsupported value type for tfvars serialization: {type(value)!r}")


def _format_tfvars(values: dict[str, Any]) -> str:
    lines: list[str] = []
    for key in sorted(values):
        lines.append(f"{key} = {_to_hcl(values[key])}")
    return "\n".join(lines)


def scaffold_stepfunctions_infra(
    *,
    output_dir: Path,
    pipeline_name: str,
    flows_dir: Path,
    force: bool = False,
    tfvars_values: dict[str, Any] | None = None,
    warnings: Iterable[str] | None = None,
) -> ScaffoldReport:
    output_dir = output_dir.resolve()
    flows_dir = flows_dir.resolve()

    output_dir.mkdir(parents=True, exist_ok=True)

    rel_definition_path, definition_abs_path = _relative_definition_path(
        flows_dir, pipeline_name, output_dir
    )

    files: dict[str, str] = {
        "main.tf": MAIN_TF_TEMPLATE,
        "networking.tf": NETWORKING_TF_TEMPLATE,
        "ecs.tf": ECS_TF_TEMPLATE,
        "task_definition.tf": TASK_DEFINITION_TF_TEMPLATE,
        "ecr.tf": ECR_TF_TEMPLATE,
        "task_execution_role.tf": TASK_EXECUTION_ROLE_TF_TEMPLATE,
        "locals.tf": EFFECTIVE_LOCALS_TEMPLATE,
        "variables.tf": VARIABLES_TF_TEMPLATE.replace(
            "__STATE_MACHINE_DEFINITION_FILE__",
            rel_definition_path,
        ),
        "outputs.tf": OUTPUTS_TF_TEMPLATE,
        "README.md": README_TEMPLATE.format(pipeline_name=pipeline_name),
    }

    created: list[Path] = []
    skipped: list[Path] = []

    for filename, template in files.items():
        destination = output_dir / filename
        if destination.exists() and not force:
            skipped.append(destination)
            continue
        destination.write_text(
            _ensure_trailing_newline(textwrap.dedent(template).strip()),
            encoding="utf-8",
        )
        created.append(destination)

    tfvars_path: Path | None = None
    if tfvars_values:
        tfvars_path = output_dir / "terraform.tfvars"
        tfvars_content = _ensure_trailing_newline(_format_tfvars(tfvars_values))
        if tfvars_path.exists() and not force:
            skipped.append(tfvars_path)
        else:
            tfvars_path.write_text(tfvars_content, encoding="utf-8")
            created.append(tfvars_path)

    return ScaffoldReport(
        created=created,
        skipped=skipped,
        output_dir=output_dir,
        state_machine_file=definition_abs_path,
        state_machine_file_exists=definition_abs_path.exists(),
        terraform_tfvars_path=tfvars_path,
        warnings=list(warnings or []),
    )
