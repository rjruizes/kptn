output "state_machine_arn" {
  description = "ARN of the generated Step Functions state machine"
  value       = aws_sfn_state_machine.this.arn
}

output "state_machine_role_arn" {
  description = "IAM role ARN assumed by the Step Functions state machine"
  value       = aws_iam_role.step_function.arn
}

output "log_group_name" {
  description = "CloudWatch log group capturing Step Functions execution logs"
  value       = aws_cloudwatch_log_group.step_function.name
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
