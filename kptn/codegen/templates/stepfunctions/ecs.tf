resource "aws_ecs_cluster" "kptn" {
  for_each = var.create_ecs_cluster ? { main = true } : {}

  name = "${var.pipeline_name}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = var.tags
}
