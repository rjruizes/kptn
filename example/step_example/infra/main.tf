terraform {
  required_version = ">= 1.4.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Get ECR authorization token for Docker provider
data "aws_ecr_authorization_token" "token" {
  count = var.build_and_push_image ? 1 : 0
}

# Configure Docker provider with ECR authentication
provider "docker" {
  dynamic "registry_auth" {
    for_each = var.build_and_push_image ? [1] : []
    content {
      address  = data.aws_ecr_authorization_token.token[0].proxy_endpoint
      username = data.aws_ecr_authorization_token.token[0].user_name
      password = data.aws_ecr_authorization_token.token[0].password
    }
  }
}

data "aws_region" "current" {}

data "aws_partition" "current" {}

data "aws_caller_identity" "current" {}

locals {
  pipeline_name = var.pipeline_name

  # Generate state machine definitions for each graph
  state_machine_definitions = {
    for graph_name, config in var.state_machines : graph_name => templatefile(
      "${path.module}/${config.definition_file}",
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
        dynamodb_table_name         = aws_dynamodb_table.kptn.name
        batch_job_queue_arn         = coalesce(local.batch_job_queue_arn_effective, "")
        batch_job_definition_arn    = coalesce(local.batch_job_definition_arn_effective, "")
        decider_lambda_arn          = coalesce(local.decider_lambda_arn_effective, "")
      }
    )
  }
}

resource "aws_cloudwatch_log_group" "step_function" {
  for_each = var.state_machines

  name              = "/aws/vendedlogs/states/${local.pipeline_name}-${each.key}"
  retention_in_days = var.log_retention_in_days
  tags              = var.tags
}

resource "aws_iam_role" "step_function" {
  for_each = var.state_machines

  name_prefix = "${local.pipeline_name}-${each.key}-sfn-"

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
  for_each = var.state_machines

  name = "${local.pipeline_name}-${each.key}-sfn-policy"
  role = aws_iam_role.step_function[each.key].id

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
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "ecs:TagResource"
        ]
        Resource = "arn:${data.aws_partition.current.partition}:ecs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:task/${local.ecs_cluster_name_effective}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          local.ecs_task_execution_role_arn_effective,
          local.task_role_arn_effective
        ]
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
        Resource = "${aws_cloudwatch_log_group.step_function[each.key].arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
          "events:PutPermission",
          "events:PutEvents",
          "events:TagResource"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = var.create_decider_lambda ? aws_lambda_function.decider[0].arn : coalesce(var.decider_lambda_arn, "*")
      },
      {
        Effect = "Allow"
        Action = [
          "batch:SubmitJob"
        ]
        Resource = length(local.batch_submit_job_resource_arns) > 0 ? local.batch_submit_job_resource_arns : ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "batch:TagResource"
        ]
        Resource = length(local.batch_submit_job_resource_arns) > 0 ? local.batch_submit_job_resource_arns : ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "batch:DescribeJobs"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_sfn_state_machine" "this" {
  for_each = var.state_machines

  name       = "${local.pipeline_name}-${each.key}"
  role_arn   = aws_iam_role.step_function[each.key].arn
  definition = local.state_machine_definitions[each.key]

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_function[each.key].arn}:*"
    include_execution_data = true
    level                  = var.logging_level
  }

  tags = var.tags
}
