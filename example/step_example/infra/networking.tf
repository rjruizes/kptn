data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "kapten" {
  for_each = var.create_networking ? { main = true } : {}

  cidr_block           = var.new_vpc_cidr_block
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = merge(var.tags, { Name = "${var.pipeline_name}-vpc" })
}

resource "aws_subnet" "kapten" {
  for_each = var.create_networking ? { for idx, cidr in var.new_subnet_cidr_blocks : idx => cidr } : {}

  vpc_id            = aws_vpc.kapten["main"].id
  cidr_block        = each.value
  availability_zone = length(var.new_subnet_availability_zones) > each.key ? var.new_subnet_availability_zones[each.key] : data.aws_availability_zones.available.names[each.key % length(data.aws_availability_zones.available.names)]
  map_public_ip_on_launch = true
  tags = merge(var.tags, { Name = format("%s-subnet-%02d", var.pipeline_name, each.key + 1) })
}

resource "aws_security_group" "kapten" {
  for_each = var.create_security_group ? { main = true } : {}

  name_prefix = "${var.pipeline_name}-tasks-"
  description = var.new_security_group_description
  vpc_id      = var.create_networking ? aws_vpc.kapten["main"].id : var.vpc_id
  tags        = merge(var.tags, { Name = "${var.pipeline_name}-tasks-sg" })
}

resource "aws_security_group_rule" "kapten_ingress" {
  for_each = var.create_security_group ? { for idx, cidr in var.new_security_group_ingress_cidr_blocks : idx => cidr } : {}

  security_group_id = aws_security_group.kapten["main"].id
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = [each.value]
}

resource "aws_security_group_rule" "kapten_egress" {
  for_each = var.create_security_group ? { for idx, cidr in var.new_security_group_egress_cidr_blocks : idx => cidr } : {}

  security_group_id = aws_security_group.kapten["main"].id
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = [each.value]
}

locals {
  subnet_ids_effective = var.create_networking ? sort([for subnet in values(aws_subnet.kapten) : subnet.id]) : var.subnet_ids

  security_group_ids_effective = var.create_security_group ? concat([aws_security_group.kapten["main"].id], var.security_group_ids) : var.security_group_ids
}
