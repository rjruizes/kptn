# Docker Image Building with Terraform

This infrastructure supports building and pushing Docker images to ECR directly through Terraform using the `kreuzwerker/docker` provider.

## How It Works

When you enable `build_and_push_image = true`, Terraform will:

1. Create an ECR repository (if `create_ecr_repository = true`)
2. Authenticate with ECR using temporary credentials
3. Build your Docker image from the specified Dockerfile
4. Tag the image with your ECR repository URL
5. Push the image to ECR
6. Configure your ECS task definition to use the pushed image

## Quick Start

### 1. Enable Docker Building

In your `terraform.tfvars`:

```hcl
build_and_push_image = true
create_ecr_repository = true
ecr_repository_name = "my-app"
docker_image_tag = "latest"
```

### 2. Configure Docker Build Settings

```hcl
# Build context (directory containing Dockerfile)
docker_build_context = ".."

# Dockerfile name
docker_build_dockerfile = "Dockerfile"

# Target platform
docker_build_platform = "linux/amd64"  # or "linux/arm64"
```

### 3. Deploy

```bash
terraform init
terraform plan
terraform apply
```

## Configuration Variables

### Required Variables

- **`build_and_push_image`** (bool): Set to `true` to enable Docker building
- **`create_ecr_repository`** (bool): Create a new ECR repository
- **`ecr_repository_name`** (string): Name for the ECR repository

### Optional Variables

- **`docker_image_tag`** (string, default: `"latest"`): Tag for your Docker image
- **`docker_build_context`** (string, default: `".."`): Path to build context
- **`docker_build_dockerfile`** (string, default: `"Dockerfile"`): Dockerfile name
- **`docker_build_platform`** (string, default: `"linux/amd64"`): Target platform
- **`docker_keep_remotely`** (bool, default: `true`): Keep image in ECR on destroy

## Advanced Usage

### Using an Existing ECR Repository

If you already have an ECR repository:

```hcl
build_and_push_image = true
create_ecr_repository = false
ecr_repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-existing-repo"
```

### Building for ARM64 (Graviton)

```hcl
docker_build_platform = "linux/arm64"
```

### Using Specific Image Tags

For production deployments, use semantic versioning:

```hcl
docker_image_tag = "v1.2.3"
```

Or use commit SHAs:

```hcl
docker_image_tag = "abc123def"
```

### Multi-Architecture Builds

Currently, the Docker provider builds for a single platform. For multi-arch images, consider using a separate build process with `docker buildx` and then reference the image:

```hcl
build_and_push_image = false
task_definition_container_image = "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-app:latest"
```

## Automatic Rebuilds

The Docker image will automatically rebuild when:

- Any file in the build context changes
- The Dockerfile changes
- Build arguments change

Terraform uses file hashing to detect changes and trigger rebuilds.

## Troubleshooting

### Docker Daemon Not Running

**Error**: `Cannot connect to the Docker daemon`

**Solution**: Ensure Docker is running on your machine:
```bash
docker ps
```

### ECR Authentication Errors

**Error**: `no basic auth credentials`

**Solution**: Ensure your AWS credentials are available to Terraform (via environment variables, AWS profile, or IAM role) so the provider can authenticate with ECR.

### Platform Mismatches

**Error**: Tasks fail with exec format errors

**Solution**: Ensure `docker_build_platform` matches your ECS launch type (Fargate uses amd64 or arm64).

## Best Practices

### 1. Use Specific Tags in Production

```hcl
docker_image_tag = var.app_version  # e.g., "v1.2.3"
```

### 2. Enable Image Scanning

The ECR repository is configured with image scanning by default:

```hcl
ecr_repository_scan_on_push = true
```

### 3. Keep Images in ECR

Set `docker_keep_remotely = true` to prevent accidental image deletion when destroying Terraform resources.

### 4. Use .dockerignore

Create a `.dockerignore` file to exclude unnecessary files from your build context:

```
.git
.terraform
*.md
.DS_Store
```

### 5. Build Caching

The Docker provider will use local build cache. For faster builds, consider:
- Using multi-stage builds
- Ordering Dockerfile commands from least to most frequently changing

## Outputs

After applying, you can access:

```hcl
# Full ECR repository URL
output.ecr_repository_url_effective

# Complete image URI with tag
output.container_image_effective

# Just the built image name
output.docker_image_name
```

## Example Workflow

```bash
# 1. Initialize Terraform
cd infra
terraform init

# 2. Configure variables
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars to enable Docker building

# 3. Plan and review
terraform plan

# 4. Apply
terraform apply

# 5. Get the image URI
terraform output container_image_effective
```

## Integration with CI/CD

For CI/CD pipelines, you might want to build images separately and reference them:

```hcl
# Disable Terraform building
build_and_push_image = false

# Reference pre-built image
task_definition_container_image = "${var.ecr_repository_url}:${var.image_tag}"
```

Then build and push in your CI pipeline:

```bash
# In GitHub Actions, GitLab CI, etc.
docker build -t $ECR_REPO:$TAG .
docker push $ECR_REPO:$TAG
terraform apply -var="image_tag=$TAG"
```

## Comparison with Manual Docker Build

### Using Terraform (This Approach)

**Pros**:
- Single command deployment (`terraform apply`)
- Automatic ECR authentication
- Declarative infrastructure
- Image versioning tied to infrastructure

**Cons**:
- Requires Docker running on deployment machine
- Longer Terraform apply times
- Build cache limited to local machine

### Manual Build + Terraform Reference

**Pros**:
- Separation of concerns
- Better for CI/CD pipelines
- Can use advanced build features (buildx, etc.)
- Faster Terraform applies

**Cons**:
- Two-step process
- Manual ECR authentication
- Image and infrastructure can get out of sync
