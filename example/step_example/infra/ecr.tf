resource "aws_ecr_repository" "kptn" {
  for_each = var.create_ecr_repository ? { main = true } : {}

  name                 = var.ecr_repository_name
  image_tag_mutability = var.ecr_repository_image_tag_mutability

  image_scanning_configuration {
    scan_on_push = var.ecr_repository_scan_on_push
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = var.tags
}
