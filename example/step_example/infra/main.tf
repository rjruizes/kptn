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
