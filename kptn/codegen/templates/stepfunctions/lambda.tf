data "archive_file" "decider_lambda" {
  count       = var.create_decider_lambda ? 1 : 0
  type        = "zip"
  source_dir  = "${path.module}/lambda_decider"
  output_path = "${path.module}/lambda_decider.zip"
}

resource "aws_cloudwatch_log_group" "decider_lambda" {
  count = var.create_decider_lambda ? 1 : 0

  name              = "/aws/lambda/${var.pipeline_name}-decider"
  retention_in_days = var.log_retention_in_days
  tags              = var.tags
}

resource "aws_iam_role" "decider_lambda" {
  count = var.create_decider_lambda ? 1 : 0

  name_prefix = "${var.pipeline_name}-decider-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "decider_lambda" {
  count = var.create_decider_lambda ? 1 : 0

  name = "${var.pipeline_name}-decider-policy"
  role = aws_iam_role.decider_lambda[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.decider_lambda[0].arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:BatchGetItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:DescribeTable"
        ]
        Resource = aws_dynamodb_table.kptn.arn
      }
    ]
  })
}

resource "aws_lambda_function" "decider" {
  count = var.create_decider_lambda ? 1 : 0

  function_name = "${var.pipeline_name}-decider"
  role          = aws_iam_role.decider_lambda[0].arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  filename      = data.archive_file.decider_lambda[0].output_path
  source_code_hash = data.archive_file.decider_lambda[0].output_base64sha256
  timeout          = var.decider_lambda_timeout
  memory_size      = var.decider_lambda_memory_size

  environment {
    variables = {
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.kptn.name
      ARTIFACT_STORE      = var.artifact_store
      EXTERNAL_STORE      = var.external_store
    }
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [
    aws_iam_role_policy.decider_lambda,
    aws_cloudwatch_log_group.decider_lambda
  ]
}
