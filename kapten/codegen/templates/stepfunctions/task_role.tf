resource "aws_iam_role" "kapten_task" {
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

resource "aws_iam_role_policy" "kapten_task_dynamodb" {
  for_each = var.create_task_role ? { main = true } : {}

  name = "${var.task_role_name_prefix}-dynamodb"
  role = aws_iam_role.kapten_task["main"].id

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
        Resource = aws_dynamodb_table.kapten.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kapten_task_managed" {
  for_each = var.create_task_role ? { for idx, policy in var.task_role_managed_policies : idx => policy } : {}

  role       = aws_iam_role.kapten_task["main"].name
  policy_arn = each.value
}
