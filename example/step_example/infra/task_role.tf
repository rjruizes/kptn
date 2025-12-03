resource "aws_iam_role" "kptn_task" {
  for_each = var.create_task_role ? { main = true } : {}

  name_prefix = var.task_role_name_prefix

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

resource "aws_iam_role_policy" "kptn_task_dynamodb" {
  for_each = var.create_task_role ? { main = true } : {}

  name = "${var.task_role_name_prefix}-dynamodb"
  role = aws_iam_role.kptn_task["main"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = aws_dynamodb_table.kptn.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kptn_task_managed" {
  for_each = var.create_task_role ? { for idx, policy in var.task_role_managed_policies : idx => policy } : {}

  role       = aws_iam_role.kptn_task["main"].name
  policy_arn = each.value
}

resource "aws_iam_role_policy" "kptn_task_efs" {
  for_each = var.create_task_role && var.enable_efs ? { main = true } : {}

  name = "${var.task_role_name_prefix}-efs"
  role = aws_iam_role.kptn_task["main"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite",
          "elasticfilesystem:ClientRootAccess"
        ]
        Resource = var.create_efs ? aws_efs_file_system.kptn["main"].arn : var.efs_file_system_arn
        Condition = var.create_efs ? {
          StringEquals = {
            "elasticfilesystem:AccessPointArn" = aws_efs_access_point.kptn["main"].arn
          }
        } : (var.efs_access_point_arn != null ? {
          StringEquals = {
            "elasticfilesystem:AccessPointArn" = var.efs_access_point_arn
          }
        } : null)
      }
    ]
  })
}
