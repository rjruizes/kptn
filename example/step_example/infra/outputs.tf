output "state_machine_arns" {
  description = "Map of state machine names to their ARNs"
  value       = { for k, v in aws_sfn_state_machine.this : k => v.arn }
}

output "state_machine_role_arns" {
  description = "Map of state machine names to their IAM role ARNs"
  value       = { for k, v in aws_iam_role.step_function : k => v.arn }
}

output "log_group_names" {
  description = "Map of state machine names to their CloudWatch log group names"
  value       = { for k, v in aws_cloudwatch_log_group.step_function : k => v.name }
}

output "ecs_cluster_arn_effective" {
  description = "ARN of the ECS cluster used by kptn tasks (existing or provisioned)"
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
  description = "ARN of the ECS task definition used by kptn tasks"
  value       = local.ecs_task_definition_arn_effective
}

output "task_execution_role_arn_effective" {
  description = "ARN of the ECS task execution role used by kptn tasks"
  value       = local.ecs_task_execution_role_arn_effective
}

output "ecr_repository_url_effective" {
  description = "URL of the ECR repository used for kptn task images"
  value       = local.ecr_repository_url_effective
}

output "task_role_arn_effective" {
  description = "ARN of the IAM task role used by kptn ECS tasks"
  value       = local.task_role_arn_effective
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table used by kptn tasks"
  value       = aws_dynamodb_table.kptn.name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table used by kptn tasks"
  value       = aws_dynamodb_table.kptn.arn
}

output "efs_file_system_id_effective" {
  description = "ID of the EFS file system used by kptn tasks (existing or provisioned)"
  value       = var.enable_efs ? local.efs_file_system_id_effective : null
}

output "efs_access_point_id_effective" {
  description = "ID of the EFS access point used by kptn tasks (existing or provisioned)"
  value       = var.enable_efs ? local.efs_access_point_id_effective : null
}

output "efs_file_system_arn" {
  description = "ARN of the EFS file system used by kptn tasks"
  value       = var.create_efs ? aws_efs_file_system.kptn["main"].arn : var.efs_file_system_arn
}

output "efs_access_point_arn" {
  description = "ARN of the EFS access point used by kptn tasks"
  value       = var.create_efs ? aws_efs_access_point.kptn["main"].arn : var.efs_access_point_arn
}

output "container_image_effective" {
  description = "Container image URI used by the ECS task definition (built or provided)"
  value       = local.container_image_effective
}

output "docker_image_name" {
  description = "Full name of the built and pushed Docker image (only when build_and_push_image is true)"
  value       = var.build_and_push_image ? docker_registry_image.kptn[0].name : null
}

output "batch_compute_environment_arn" {
  description = "ARN of the AWS Batch compute environment (when enabled)"
  value       = var.create_batch_resources ? aws_batch_compute_environment.kptn["main"].arn : null
}

output "batch_job_queue_arn" {
  description = "ARN of the AWS Batch job queue (when enabled)"
  value       = var.create_batch_resources ? aws_batch_job_queue.kptn["main"].arn : null
}

output "batch_job_definition_arn" {
  description = "ARN of the AWS Batch job definition (when enabled)"
  value       = var.create_batch_resources ? aws_batch_job_definition.kptn["main"].arn : null
}

output "batch_service_role_arn_effective" {
  description = "ARN of the IAM service role used by AWS Batch (created or supplied)"
  value       = local.batch_service_role_arn_effective
}

output "decider_lambda_arn" {
  description = "ARN of the kptn decider Lambda function invoked by Step Functions"
  value       = coalesce(local.decider_lambda_arn_effective, "")
}
