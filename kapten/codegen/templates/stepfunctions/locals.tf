locals {
  ecs_cluster_arn_effective = var.create_ecs_cluster ? aws_ecs_cluster.kapten["main"].arn : var.ecs_cluster_arn

  ecs_task_definition_arn_effective = var.create_task_definition ? aws_ecs_task_definition.kapten["main"].arn : var.ecs_task_definition_arn

  ecs_task_execution_role_arn_effective = var.create_task_execution_role ? aws_iam_role.kapten_execution["main"].arn : var.ecs_task_execution_role_arn

  task_role_arn_effective = var.create_task_role ? aws_iam_role.kapten_task["main"].arn : var.task_role_arn

  ecr_repository_url_effective = var.create_ecr_repository ? aws_ecr_repository.kapten["main"].repository_url : var.ecr_repository_url
}
