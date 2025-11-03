locals {
  docker_context_watch_globs = [
    "Dockerfile",
    "pyproject.toml",
    "uv.lock",
    "kptn.yaml",
    "run.py",
    "kptn-*.whl",
    "src/**"
  ]

  docker_context_watch_files = distinct(flatten([
    for pattern in local.docker_context_watch_globs : fileset(var.docker_build_context, pattern)
  ]))

  docker_context_sha1 = sha1(join("", [
    for file in local.docker_context_watch_files : "${file}:${filesha1("${var.docker_build_context}/${file}")}"
  ]))
}

# Build Docker image and push to ECR
resource "docker_image" "kptn" {
  count = var.build_and_push_image ? 1 : 0

  name = "${local.ecr_repository_url_effective}:${var.docker_image_tag}"

  build {
    context    = var.docker_build_context
    dockerfile = var.docker_build_dockerfile
    platform   = var.docker_build_platform
  }

  # Trigger rebuild when relevant build context files change
  triggers = {
    context_sha1 = local.docker_context_sha1
  }
}

# Push image to ECR repository
resource "docker_registry_image" "kptn" {
  count = var.build_and_push_image ? 1 : 0

  name = docker_image.kptn[0].name

  # This ensures the image is kept in sync
  keep_remotely = var.docker_keep_remotely

  # Re-push image when the built image digest changes
  triggers = {
    source_digest = docker_image.kptn[0].repo_digest
  }
}
