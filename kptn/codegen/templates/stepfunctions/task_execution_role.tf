resource "aws_iam_role" "kptn_execution" {
  for_each = var.create_task_execution_role ? { main = true } : {}

  name_prefix = var.task_execution_role_name_prefix

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "kptn_execution_managed" {
  for_each = var.create_task_execution_role ? { for idx, policy in var.task_execution_role_managed_policies : idx => policy } : {}

  role       = aws_iam_role.kptn_execution["main"].name
  policy_arn = each.value
}
