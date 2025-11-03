locals {
  ecs_cluster_arn_effective = var.create_ecs_cluster ? aws_ecs_cluster.kptn["main"].arn : var.ecs_cluster_arn

  # Extract cluster name from ARN for existing clusters or use the created cluster name
  ecs_cluster_name_effective = var.create_ecs_cluster ? aws_ecs_cluster.kptn["main"].name : split("/", var.ecs_cluster_arn)[1]

  ecs_task_definition_arn_effective = var.create_task_definition ? aws_ecs_task_definition.kptn["main"].arn : var.ecs_task_definition_arn

  ecs_task_execution_role_arn_effective = var.create_task_execution_role ? aws_iam_role.kptn_execution["main"].arn : var.ecs_task_execution_role_arn

  task_role_arn_effective = var.create_task_role ? aws_iam_role.kptn_task["main"].arn : var.task_role_arn

  ecr_repository_url_effective = var.create_ecr_repository ? aws_ecr_repository.kptn["main"].repository_url : var.ecr_repository_url

  # Use the built image if build_and_push_image is enabled, otherwise use the provided image
  container_image_effective = var.build_and_push_image ? docker_registry_image.kptn[0].name : var.task_definition_container_image

  task_definition_log_group_name_effective = var.task_definition_enable_awslogs ? coalesce(var.task_definition_log_group_name, "/aws/ecs/${var.pipeline_name}/${var.task_definition_container_name}") : null

  task_definition_log_stream_prefix_effective = var.task_definition_enable_awslogs ? coalesce(var.task_definition_log_stream_prefix, var.task_definition_container_name) : null

  batch_service_role_arn_effective = var.create_batch_resources ? (var.create_batch_service_role ? aws_iam_role.batch_service["main"].arn : var.batch_service_role_arn) : null

  batch_subnet_ids_effective = length(var.batch_subnet_ids) > 0 ? var.batch_subnet_ids : local.subnet_ids_effective

  batch_security_group_ids_effective = length(var.batch_security_group_ids) > 0 ? var.batch_security_group_ids : local.security_group_ids_effective

  task_definition_container_environment_effective = merge(
    var.task_definition_container_environment,
    {
      ARTIFACT_STORE = var.artifact_store
      EXTERNAL_STORE = var.external_store
    }
  )

  batch_container_command_effective = length(var.batch_container_command) > 0 ? var.batch_container_command : var.task_definition_container_command

  batch_container_environment_effective = length(var.batch_container_environment) > 0 ? merge(
    var.batch_container_environment,
    {
      ARTIFACT_STORE = var.artifact_store
      EXTERNAL_STORE = var.external_store
    }
  ) : local.task_definition_container_environment_effective

  batch_container_vcpu_effective = var.batch_container_vcpu != "" ? var.batch_container_vcpu : (var.task_definition_cpu != "" ? tostring(tonumber(var.task_definition_cpu) / 1024) : null)

  batch_container_memory_effective = var.batch_container_memory != "" ? var.batch_container_memory : var.task_definition_memory

  batch_compute_environment_name_effective = "${var.batch_compute_environment_name_prefix}-${var.pipeline_name}"

  batch_job_queue_name_effective = var.batch_job_queue_name != "" ? var.batch_job_queue_name : "${var.pipeline_name}-batch-queue"

  batch_job_definition_name_effective = var.batch_job_definition_name != "" ? var.batch_job_definition_name : "${var.pipeline_name}-batch-job"

  batch_job_queue_arn_effective = try(aws_batch_job_queue.kptn["main"].arn, null)

  batch_job_definition_arn_effective = try(aws_batch_job_definition.kptn["main"].arn, null)

  batch_submit_job_resource_arns = [
    for arn in [
      local.batch_job_queue_arn_effective,
      local.batch_job_definition_arn_effective
    ] : arn if arn != null && arn != ""
  ]

  decider_lambda_arn_effective = var.create_decider_lambda ? aws_lambda_function.decider[0].arn : var.decider_lambda_arn
}
