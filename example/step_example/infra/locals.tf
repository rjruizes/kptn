locals {
  ecs_cluster_arn_effective = var.create_ecs_cluster ? aws_ecs_cluster.kapten["main"].arn : var.ecs_cluster_arn

  # Extract cluster name from ARN for existing clusters or use the created cluster name
  ecs_cluster_name_effective = var.create_ecs_cluster ? aws_ecs_cluster.kapten["main"].name : split("/", var.ecs_cluster_arn)[1]

  ecs_task_definition_arn_effective = var.create_task_definition ? aws_ecs_task_definition.kapten["main"].arn : var.ecs_task_definition_arn

  ecs_task_execution_role_arn_effective = var.create_task_execution_role ? aws_iam_role.kapten_execution["main"].arn : var.ecs_task_execution_role_arn

  task_role_arn_effective = var.create_task_role ? aws_iam_role.kapten_task["main"].arn : var.task_role_arn

  ecr_repository_url_effective = var.create_ecr_repository ? aws_ecr_repository.kapten["main"].repository_url : var.ecr_repository_url

  # Use the built image if build_and_push_image is enabled, otherwise use the provided image
  container_image_effective = var.build_and_push_image ? docker_registry_image.kapten[0].name : var.task_definition_container_image

  task_definition_log_group_name_effective = var.task_definition_enable_awslogs ? coalesce(var.task_definition_log_group_name, "/aws/ecs/${var.pipeline_name}/${var.task_definition_container_name}") : null

  task_definition_log_stream_prefix_effective = var.task_definition_enable_awslogs ? coalesce(var.task_definition_log_stream_prefix, var.task_definition_container_name) : null
}
