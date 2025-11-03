resource "aws_efs_file_system" "kptn" {
  for_each = var.create_efs ? { main = true } : {}

  encrypted = true
  tags      = merge(var.tags, { Name = "${var.pipeline_name}-efs" })

  lifecycle_policy {
    transition_to_ia = var.efs_transition_to_ia
  }
}

resource "aws_efs_mount_target" "kptn" {
  for_each = var.create_efs ? { for idx, subnet_id in local.subnet_ids_effective : idx => subnet_id } : {}

  file_system_id  = aws_efs_file_system.kptn["main"].id
  subnet_id       = each.value
  security_groups = local.efs_security_group_ids_effective
}

resource "aws_security_group" "kptn_efs" {
  for_each = var.create_efs && var.create_efs_security_group ? { main = true } : {}

  name_prefix = "${var.pipeline_name}-efs-"
  description = var.efs_security_group_description
  vpc_id      = var.create_networking ? aws_vpc.kptn["main"].id : var.vpc_id
  tags        = merge(var.tags, { Name = "${var.pipeline_name}-efs-sg" })
}

resource "aws_security_group_rule" "kptn_efs_ingress_from_tasks" {
  for_each = var.create_efs && var.create_efs_security_group ? { main = true } : {}

  security_group_id        = aws_security_group.kptn_efs["main"].id
  type                     = "ingress"
  from_port                = 2049
  to_port                  = 2049
  protocol                 = "tcp"
  source_security_group_id = var.create_security_group ? aws_security_group.kptn["main"].id : (length(var.security_group_ids) > 0 ? var.security_group_ids[0] : null)
}

resource "aws_efs_access_point" "kptn" {
  for_each = var.create_efs ? { main = true } : {}

  file_system_id = aws_efs_file_system.kptn["main"].id

  root_directory {
    path = var.efs_root_directory_path
    creation_info {
      owner_gid   = var.efs_owner_gid
      owner_uid   = var.efs_owner_uid
      permissions = var.efs_permissions
    }
  }

  posix_user {
    gid = var.efs_posix_gid
    uid = var.efs_posix_uid
  }

  tags = merge(var.tags, { Name = "${var.pipeline_name}-efs-ap" })
}

locals {
  efs_file_system_id_effective = var.create_efs ? aws_efs_file_system.kptn["main"].id : var.efs_file_system_id
  efs_access_point_id_effective = var.create_efs ? aws_efs_access_point.kptn["main"].id : var.efs_access_point_id
  efs_security_group_ids_effective = var.create_efs && var.create_efs_security_group ? [aws_security_group.kptn_efs["main"].id] : var.efs_security_group_ids
}
