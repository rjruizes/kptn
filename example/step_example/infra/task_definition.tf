resource "aws_cloudwatch_log_group" "ecs_task" {
  for_each = var.create_task_definition && var.task_definition_enable_awslogs && var.task_definition_create_log_group ? { main = true } : {}

  name              = local.task_definition_log_group_name_effective
  retention_in_days = var.task_definition_log_retention_in_days
  tags              = var.tags
}

resource "aws_ecs_task_definition" "kptn" {
  for_each = var.create_task_definition ? { main = true } : {}

  family                   = var.task_definition_family
  cpu                      = var.task_definition_cpu
  memory                   = var.task_definition_memory
  network_mode             = var.task_definition_network_mode
  requires_compatibilities = var.task_definition_requires_compatibilities
  execution_role_arn       = local.ecs_task_execution_role_arn_effective
  task_role_arn            = local.task_role_arn_effective

  container_definitions = jsonencode([
    merge(
      {
        name        = var.task_definition_container_name
        image       = local.container_image_effective
        essential   = true
        command     = var.task_definition_container_command
        environment = [for k, v in local.task_definition_container_environment_effective : { name = k, value = v }]
        mountPoints = var.enable_efs ? [
          {
            sourceVolume  = "efs"
            containerPath = var.efs_container_mount_path
            readOnly      = false
          }
        ] : []
      },
      var.task_definition_enable_awslogs ? {
        logConfiguration = {
          logDriver = "awslogs"
          options = {
            "awslogs-group"         = local.task_definition_log_group_name_effective
            "awslogs-region"        = var.region
            "awslogs-stream-prefix" = local.task_definition_log_stream_prefix_effective
          }
        }
      } : {}
    )
  ])

  dynamic "volume" {
    for_each = var.enable_efs ? [1] : []
    content {
      name = "efs"

      efs_volume_configuration {
        file_system_id     = local.efs_file_system_id_effective
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = local.efs_access_point_id_effective
          iam             = "ENABLED"
        }
      }
    }
  }
}
