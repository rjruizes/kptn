resource "aws_iam_role" "batch_service" {
  for_each = var.create_batch_resources && var.create_batch_service_role ? { main = true } : {}

  name_prefix = "${var.pipeline_name}-batch-service-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "batch.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "batch_service" {
  for_each = var.create_batch_resources && var.create_batch_service_role ? { main = true } : {}

  role       = aws_iam_role.batch_service["main"].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

resource "aws_batch_compute_environment" "kptn" {
  for_each = var.create_batch_resources ? { main = true } : {}

  name         = local.batch_compute_environment_name_effective
  type         = "MANAGED"
  service_role = local.batch_service_role_arn_effective

  compute_resources {
    type               = var.batch_compute_resources_type
    max_vcpus          = var.batch_max_vcpus
    subnets            = local.batch_subnet_ids_effective
    security_group_ids = local.batch_security_group_ids_effective
  }

  tags = var.tags

  lifecycle {
    precondition {
      condition     = length(local.batch_subnet_ids_effective) > 0
      error_message = "At least one subnet must be provided via batch_subnet_ids or subnet_ids when provisioning AWS Batch."
    }

    precondition {
      condition     = length(local.batch_security_group_ids_effective) > 0
      error_message = "At least one security group must be provided via batch_security_group_ids or security_group_ids when provisioning AWS Batch."
    }
  }
}

resource "aws_batch_job_queue" "kptn" {
  for_each = var.create_batch_resources ? { main = true } : {}

  name     = local.batch_job_queue_name_effective
  state    = "ENABLED"
  priority = var.batch_job_queue_priority

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.kptn["main"].arn
  }

  tags = var.tags
}

locals {
  batch_container_properties = merge(
    {
      image            = local.container_image_effective
      executionRoleArn = local.ecs_task_execution_role_arn_effective
      resourceRequirements = [
        {
          type  = "VCPU"
          value = local.batch_container_vcpu_effective
        },
        {
          type  = "MEMORY"
          value = local.batch_container_memory_effective
        }
      ]
      environment = [for k, v in local.batch_container_environment_effective : { name = k, value = v }]
      networkConfiguration = {
        assignPublicIp = var.assign_public_ip ? "ENABLED" : "DISABLED"
      }
    },
    length(local.batch_container_command_effective) > 0 ? { command = local.batch_container_command_effective } : {},
    local.task_role_arn_effective != null ? { jobRoleArn = local.task_role_arn_effective } : {}
  )
}

resource "aws_batch_job_definition" "kptn" {
  for_each = var.create_batch_resources ? { main = true } : {}

  name                  = local.batch_job_definition_name_effective
  type                  = "container"
  platform_capabilities = ["FARGATE"]
  container_properties  = jsonencode(local.batch_container_properties)
  propagate_tags        = true

  tags = var.tags

  lifecycle {
    precondition {
      condition     = local.ecs_task_execution_role_arn_effective != null && local.ecs_task_execution_role_arn_effective != ""
      error_message = "Provide ecs_task_execution_role_arn or enable create_task_execution_role before enabling Batch."
    }

    precondition {
      condition     = local.batch_container_vcpu_effective != null && local.batch_container_vcpu_effective != ""
      error_message = "Configure batch_container_vcpu or task_definition_cpu to supply vCPU capacity for Batch jobs."
    }

    precondition {
      condition     = local.batch_container_memory_effective != null && local.batch_container_memory_effective != ""
      error_message = "Configure batch_container_memory or task_definition_memory to supply memory capacity for Batch jobs."
    }
  }
}
