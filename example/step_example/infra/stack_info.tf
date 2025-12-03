locals {
  stack_info_ssm_parameter_name = coalesce(
    var.stack_info_ssm_parameter_name,
    "/kptn/stack/${var.pipeline_name}/info"
  )

  stack_info_state_machine_arns = { for k, v in aws_sfn_state_machine.this : k => v.arn }

  stack_info_primary_state_machine_arn = length(local.stack_info_state_machine_arns) > 0 ? local.stack_info_state_machine_arns[sort(keys(local.stack_info_state_machine_arns))[0]] : null

  stack_info_payload = {
    pipeline_name            = var.pipeline_name
    dynamodb_table_name      = aws_dynamodb_table.kptn.name
    dynamodb_table_arn       = aws_dynamodb_table.kptn.arn
    cluster_arn              = local.ecs_cluster_arn_effective
    subnet_ids               = local.subnet_ids_effective
    security_group_ids       = local.security_group_ids_effective
    assign_public_ip         = var.assign_public_ip
    ecs_launch_type          = var.ecs_launch_type
    task_definition_arn      = local.ecs_task_definition_arn_effective
    task_definition_container_name = var.task_definition_container_name
    task_execution_role_arn  = local.ecs_task_execution_role_arn_effective
    task_role_arn            = local.task_role_arn_effective
    state_machine_arns       = local.stack_info_state_machine_arns
    state_machine_arn        = local.stack_info_primary_state_machine_arn
    batch_job_queue_arn      = local.batch_job_queue_arn_effective
    batch_job_definition_arn = local.batch_job_definition_arn_effective
    batch_service_role_arn   = local.batch_service_role_arn_effective
    decider_lambda_arn       = local.decider_lambda_arn_effective
  }
}

resource "aws_ssm_parameter" "kptn_stack_info" {
  name  = local.stack_info_ssm_parameter_name
  type  = "String"
  value = jsonencode(local.stack_info_payload)

  tags = var.tags
}
