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

output "task_role_arn_effective" {
  description = "ARN of the IAM task role used by Kapten ECS tasks"
  value       = local.task_role_arn_effective
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table used by Kapten tasks"
  value       = aws_dynamodb_table.kapten.name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table used by Kapten tasks"
  value       = aws_dynamodb_table.kapten.arn
}
