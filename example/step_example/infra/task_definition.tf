resource "aws_ecs_task_definition" "kapten" {
  for_each = var.create_task_definition ? { main = true } : {}

  family                   = var.task_definition_family
  cpu                      = var.task_definition_cpu
  memory                   = var.task_definition_memory
  network_mode             = var.task_definition_network_mode
  requires_compatibilities = var.task_definition_requires_compatibilities
  execution_role_arn       = local.ecs_task_execution_role_arn_effective
  task_role_arn            = local.task_role_arn_effective

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
