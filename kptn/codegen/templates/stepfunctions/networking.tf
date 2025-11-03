data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "kptn" {
  for_each = var.create_networking ? { main = true } : {}

  cidr_block           = var.new_vpc_cidr_block
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = merge(var.tags, { Name = "${var.pipeline_name}-vpc" })
}

resource "aws_subnet" "kptn" {
  for_each = var.create_networking ? { for idx, cidr in var.new_subnet_cidr_blocks : idx => cidr } : {}

  vpc_id                  = aws_vpc.kptn["main"].id
  cidr_block              = each.value
  availability_zone       = length(var.new_subnet_availability_zones) > each.key ? var.new_subnet_availability_zones[each.key] : data.aws_availability_zones.available.names[each.key % length(data.aws_availability_zones.available.names)]
  map_public_ip_on_launch = true
  tags                    = merge(var.tags, { Name = format("%s-subnet-%02d", var.pipeline_name, each.key + 1) })
}

resource "aws_security_group" "kptn" {
  for_each = var.create_security_group ? { main = true } : {}

  name_prefix = "${var.pipeline_name}-tasks-"
  description = var.new_security_group_description
  vpc_id      = var.create_networking ? aws_vpc.kptn["main"].id : var.vpc_id
  tags        = merge(var.tags, { Name = "${var.pipeline_name}-tasks-sg" })
}

resource "aws_security_group_rule" "kptn_ingress" {
  for_each = var.create_security_group ? { for idx, cidr in var.new_security_group_ingress_cidr_blocks : idx => cidr } : {}

  security_group_id = aws_security_group.kptn["main"].id
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = [each.value]
}

resource "aws_security_group_rule" "kptn_egress" {
  for_each = var.create_security_group ? { for idx, cidr in var.new_security_group_egress_cidr_blocks : idx => cidr } : {}

  security_group_id = aws_security_group.kptn["main"].id
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = [each.value]
}

# Security group for VPC endpoints
resource "aws_security_group" "vpc_endpoints" {
  for_each = var.create_networking ? { main = true } : {}

  name_prefix = "${var.pipeline_name}-vpc-endpoints-"
  description = "Security group for VPC endpoints"
  vpc_id      = aws_vpc.kptn["main"].id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.new_vpc_cidr_block]
    description = "Allow HTTPS from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = merge(var.tags, { Name = "${var.pipeline_name}-vpc-endpoints-sg" })
}

# VPC Endpoint for ECR API
resource "aws_vpc_endpoint" "ecr_api" {
  for_each = var.create_networking ? { main = true } : {}

  vpc_id              = aws_vpc.kptn["main"].id
  service_name        = "com.amazonaws.${var.region}.ecr.api"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [for subnet in values(aws_subnet.kptn) : subnet.id]
  security_group_ids  = [aws_security_group.vpc_endpoints["main"].id]
  private_dns_enabled = true

  tags = merge(var.tags, { Name = "${var.pipeline_name}-ecr-api-endpoint" })
}

# VPC Endpoint for ECR Docker
resource "aws_vpc_endpoint" "ecr_dkr" {
  for_each = var.create_networking ? { main = true } : {}

  vpc_id              = aws_vpc.kptn["main"].id
  service_name        = "com.amazonaws.${var.region}.ecr.dkr"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [for subnet in values(aws_subnet.kptn) : subnet.id]
  security_group_ids  = [aws_security_group.vpc_endpoints["main"].id]
  private_dns_enabled = true

  tags = merge(var.tags, { Name = "${var.pipeline_name}-ecr-dkr-endpoint" })
}

# VPC Endpoint for S3 (Gateway endpoint for ECR image layers)
resource "aws_vpc_endpoint" "s3" {
  for_each = var.create_networking ? { main = true } : {}

  vpc_id            = aws_vpc.kptn["main"].id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_vpc.kptn["main"].default_route_table_id]

  tags = merge(var.tags, { Name = "${var.pipeline_name}-s3-endpoint" })
}

# VPC Endpoint for DynamoDB (Gateway endpoint for DynamoDB access)
resource "aws_vpc_endpoint" "dynamodb" {
  for_each = var.create_networking ? { main = true } : {}

  vpc_id            = aws_vpc.kptn["main"].id
  service_name      = "com.amazonaws.${var.region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_vpc.kptn["main"].default_route_table_id]

  tags = merge(var.tags, { Name = "${var.pipeline_name}-dynamodb-endpoint" })
}

# VPC Endpoint for CloudWatch Logs (optional but recommended for task logs)
resource "aws_vpc_endpoint" "logs" {
  for_each = var.create_networking ? { main = true } : {}

  vpc_id              = aws_vpc.kptn["main"].id
  service_name        = "com.amazonaws.${var.region}.logs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [for subnet in values(aws_subnet.kptn) : subnet.id]
  security_group_ids  = [aws_security_group.vpc_endpoints["main"].id]
  private_dns_enabled = true

  tags = merge(var.tags, { Name = "${var.pipeline_name}-logs-endpoint" })
}

locals {
  subnet_ids_effective = var.create_networking ? sort([for subnet in values(aws_subnet.kptn) : subnet.id]) : var.subnet_ids

  security_group_ids_effective = var.create_security_group ? concat([aws_security_group.kptn["main"].id], var.security_group_ids) : var.security_group_ids
}
