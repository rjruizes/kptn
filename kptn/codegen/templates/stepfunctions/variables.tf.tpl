variable "region" {
  type        = string
  description = "AWS region to deploy Step Functions resources into"
  default     = "us-east-1"
}

variable "pipeline_name" {
  type        = string
  description = "kptn pipeline name used as prefix for all resources"
}

variable "stack_info_ssm_parameter_name" {
  type        = string
  description = "SSM parameter name that stores kptn stack metadata such as ARNs for ECS, Step Functions, and Batch"
  default     = null
}

variable "state_machines" {
  type = map(object({
    definition_file = string
  }))
  description = "Map of state machine configurations where key is the graph name and value contains the definition file path"
  default     = STATE_MACHINES_PLACEHOLDER
}

variable "create_decider_lambda" {
  type        = bool
  description = "Set to true to provision the kptn decider Lambda function"
  default     = true
}

variable "decider_lambda_arn" {
  type        = string
  description = "Existing decider Lambda function ARN to reuse when not creating one"
  default     = null
}

variable "decider_lambda_timeout" {
  type        = number
  description = "Timeout, in seconds, for the kptn decider Lambda"
  default     = 30
}

variable "decider_lambda_memory_size" {
  type        = number
  description = "Memory size, in MB, for the kptn decider Lambda"
  default     = 512
}

variable "create_networking" {
  type        = bool
  description = "Set to true to provision a new VPC and subnets"
  default     = false
}

variable "create_security_group" {
  type        = bool
  description = "Set to true to provision a new security group"
  default     = false
}

variable "create_ecr_repository" {
  type        = bool
  description = "Set to true to provision an ECR repository"
  default     = false
}

variable "vpc_id" {
  type        = string
  description = "Existing VPC ID used when provisioning only a security group"
  default     = null
}

variable "ecr_repository_name" {
  type        = string
  description = "Name assigned to the generated ECR repository"
  default     = ""
}

variable "ecr_repository_image_tag_mutability" {
  type        = string
  description = "Image tag mutability for the generated ECR repository"
  default     = "MUTABLE"
}

variable "ecr_repository_scan_on_push" {
  type        = bool
  description = "Enable image scan on push for the generated ECR repository"
  default     = true
}

variable "ecr_repository_url" {
  type        = string
  description = "Existing ECR repository URL to reuse"
  default     = null
}

variable "new_vpc_cidr_block" {
  type        = string
  description = "CIDR block for a newly provisioned VPC"
  default     = "10.0.0.0/16"
}

variable "new_subnet_cidr_blocks" {
  type        = list(string)
  description = "CIDR blocks for subnets created when networking is provisioned"
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "new_subnet_availability_zones" {
  type        = list(string)
  description = "Optional AZ overrides for new subnets; leave empty to auto-select"
  default     = []
}

variable "new_security_group_ingress_cidr_blocks" {
  type        = list(string)
  description = "CIDR blocks allowed to reach ECS tasks when provisioning networking"
  default     = ["0.0.0.0/0"]
}

variable "new_security_group_egress_cidr_blocks" {
  type        = list(string)
  description = "CIDR blocks reachable from ECS tasks when provisioning networking"
  default     = ["0.0.0.0/0"]
}

variable "new_security_group_description" {
  type        = string
  description = "Description applied to the generated security group"
  default     = "kptn Step Functions tasks"
}

variable "subnet_ids" {
  type        = list(string)
  description = "Subnet IDs to reuse when not provisioning networking"
  default     = []
}

variable "security_group_ids" {
  type        = list(string)
  description = "Security group IDs to reuse when not provisioning networking"
  default     = []
}

variable "create_ecs_cluster" {
  type        = bool
  description = "Set to true to provision a dedicated ECS cluster"
  default     = false
}

variable "ecs_cluster_arn" {
  type        = string
  description = "Existing ECS cluster ARN to reuse when not creating one"
  default     = null
}

variable "ecs_task_definition_arn" {
  type        = string
  description = "Existing ECS task definition ARN to reuse when not creating one"
  default     = null
}

variable "create_task_definition" {
  type        = bool
  description = "Set to true to provision an ECS task definition"
  default     = false
}

variable "task_definition_family" {
  type        = string
  description = "Family name assigned to the generated ECS task definition"
  default     = "kptn-task"
}

variable "task_definition_cpu" {
  type        = string
  description = "CPU units assigned to the ECS task definition"
  default     = "512"
}

variable "task_definition_memory" {
  type        = string
  description = "Memory (MiB) assigned to the ECS task definition"
  default     = "1024"
}

variable "task_definition_network_mode" {
  type        = string
  description = "Network mode used by the ECS task definition"
  default     = "awsvpc"
}

variable "task_definition_requires_compatibilities" {
  type        = list(string)
  description = "Launch types supported by the ECS task definition"
  default     = ["FARGATE"]
}

variable "create_task_role" {
  type        = bool
  description = "Set to true to provision an IAM task role with DynamoDB permissions"
  default     = false
}

variable "task_role_name_prefix" {
  type        = string
  description = "Name prefix applied to a generated IAM task role"
  default     = "kptn-task-role"
}

variable "task_role_managed_policies" {
  type        = list(string)
  description = "Managed policy ARNs attached to a generated task role"
  default     = []
}

variable "task_role_arn" {
  type        = string
  description = "Existing IAM task role ARN to reuse when not creating one"
  default     = null
}

variable "task_definition_container_name" {
  type        = string
  description = "Name assigned to the container in the generated task definition"
  default     = "kptn"
}

variable "task_definition_container_image" {
  type        = string
  description = "Container image URI for the ECS task definition"
  default     = "public.ecr.aws/amazonlinux/amazonlinux:latest"
}

variable "task_definition_container_command" {
  type        = list(string)
  description = "Command override for the container (leave empty to use image defaults)"
  default     = []
}

variable "task_definition_container_environment" {
  type        = map(string)
  description = "Environment variables injected into the container"
  default     = {}
}

variable "task_definition_enable_awslogs" {
  type        = bool
  description = "Enable the awslogs log driver for the ECS task container"
  default     = true
}

variable "task_definition_create_log_group" {
  type        = bool
  description = "Create a CloudWatch log group for the ECS task when awslogs is enabled"
  default     = true
}

variable "task_definition_log_group_name" {
  type        = string
  description = "Override the CloudWatch log group name used by the ECS task when awslogs is enabled"
  default     = null
}

variable "task_definition_log_stream_prefix" {
  type        = string
  description = "Log stream prefix applied to awslogs streams for the ECS task container"
  default     = null
}

variable "task_definition_log_retention_in_days" {
  type        = number
  description = "Retention period (days) for the ECS task CloudWatch log group that Terraform manages"
  default     = 30
}

variable "create_task_execution_role" {
  type        = bool
  description = "Set to true to provision an ECS task execution role"
  default     = false
}

variable "task_execution_role_name_prefix" {
  type        = string
  description = "Name prefix applied to a generated ECS task execution role"
  default     = "kptn-task-execution-role"
}

variable "task_execution_role_managed_policies" {
  type        = list(string)
  description = "Managed policy ARNs attached to a generated task execution role"
  default     = [
    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
  ]
}

variable "ecs_task_execution_role_arn" {
  type        = string
  description = "Existing IAM role ARN that ECS tasks assume when invoked"
  default     = null
}

variable "assign_public_ip" {
  type        = bool
  description = "Assign a public IP when running ECS tasks"
  default     = false
}

variable "ecs_launch_type" {
  type        = string
  description = "Launch type used when invoking ECS tasks"
  default     = "FARGATE"
}

variable "logging_level" {
  type        = string
  description = "Logging level for Step Functions state machine executions"
  default     = "ALL"
}

variable "log_retention_in_days" {
  type        = number
  description = "Retention period for the Step Functions CloudWatch log group"
  default     = 30
}

variable "tags" {
  type        = map(string)
  description = "Common tags applied to created resources"
  default     = {}
}

variable "dynamodb_table_name" {
  type        = string
  description = "Name for the DynamoDB table used by kptn tasks"
}

variable "artifact_store" {
  type        = string
  description = "Artifact storage identifier exposed to runtime environments"
}

variable "external_store" {
  type        = string
  description = "External inputs storage identifier exposed to runtime environments"
}

variable "create_efs" {
  type        = bool
  description = "Set to true to provision an EFS file system"
  default     = false
}

variable "enable_efs" {
  type        = bool
  description = "Set to true to mount EFS in the task definition"
  default     = false
}

variable "efs_file_system_id" {
  type        = string
  description = "Existing EFS file system ID to reuse when not creating one"
  default     = null
}

variable "efs_access_point_id" {
  type        = string
  description = "Existing EFS access point ID to reuse when not creating one"
  default     = null
}

variable "efs_file_system_arn" {
  type        = string
  description = "ARN of the EFS file system (required for IAM policies when reusing existing EFS)"
  default     = null
}

variable "efs_access_point_arn" {
  type        = string
  description = "ARN of the EFS access point (required for IAM policies when reusing existing EFS)"
  default     = null
}

variable "efs_container_mount_path" {
  type        = string
  description = "Container path where EFS will be mounted"
  default     = "/mnt/efs"
}

variable "efs_root_directory_path" {
  type        = string
  description = "Root directory path for the EFS access point"
  default     = "/data"
}

variable "efs_owner_gid" {
  type        = number
  description = "Group ID for the EFS root directory owner"
  default     = 1000
}

variable "efs_owner_uid" {
  type        = number
  description = "User ID for the EFS root directory owner"
  default     = 1000
}

variable "efs_posix_gid" {
  type        = number
  description = "Group ID for POSIX user when accessing EFS"
  default     = 1000
}

variable "efs_posix_uid" {
  type        = number
  description = "User ID for POSIX user when accessing EFS"
  default     = 1000
}

variable "efs_permissions" {
  type        = string
  description = "Permissions for the EFS root directory"
  default     = "755"
}

variable "efs_transition_to_ia" {
  type        = string
  description = "Transition to Infrequent Access storage class"
  default     = "AFTER_30_DAYS"
}

variable "create_efs_security_group" {
  type        = bool
  description = "Set to true to provision a security group for EFS"
  default     = true
}

variable "efs_security_group_ids" {
  type        = list(string)
  description = "Security group IDs to attach to EFS mount targets when not creating one"
  default     = []
}

variable "efs_security_group_description" {
  type        = string
  description = "Description for the generated EFS security group"
  default     = "Security group for kptn EFS mount targets"
}

variable "build_and_push_image" {
  type        = bool
  description = "Set to true to build and push Docker image to ECR using Terraform"
  default     = true
}

variable "docker_image_tag" {
  type        = string
  description = "Tag to apply to the built Docker image"
  default     = "latest"
}

variable "docker_build_context" {
  type        = string
  description = "Path to the Docker build context (directory containing Dockerfile)"
  default     = ".."
}

variable "docker_build_dockerfile" {
  type        = string
  description = "Name of the Dockerfile (relative to build context)"
  default     = "Dockerfile"
}

variable "docker_build_platform" {
  type        = string
  description = "Platform to build the Docker image for (e.g., linux/amd64, linux/arm64)"
  default     = "linux/amd64"
}

variable "docker_keep_remotely" {
  type        = bool
  description = "Keep the Docker image in ECR when destroying the Terraform resource"
  default     = true
}

variable "create_batch_resources" {
  type        = bool
  description = "Set to true to provision AWS Batch compute environment, job queue, and job definition"
  default     = true
}

variable "create_batch_service_role" {
  type        = bool
  description = "Set to true to create an IAM service role for AWS Batch instead of using the AWS managed service-linked role"
  default     = false
}

variable "batch_service_role_arn" {
  type        = string
  description = "Existing IAM role ARN for AWS Batch to assume when not creating one"
  default     = null
}

variable "batch_compute_resources_type" {
  type        = string
  description = "Compute resource type for the AWS Batch compute environment"
  default     = "FARGATE"

  validation {
    condition     = contains(["FARGATE", "FARGATE_SPOT"], var.batch_compute_resources_type)
    error_message = "batch_compute_resources_type must be FARGATE or FARGATE_SPOT."
  }
}

variable "batch_compute_environment_name_prefix" {
  type        = string
  description = "Name prefix applied to the generated AWS Batch compute environment"
  default     = "kptn-batch-ce"
}

variable "batch_max_vcpus" {
  type        = number
  description = "Maximum number of vCPUs for the AWS Batch compute environment"
  default     = 32
}

variable "batch_subnet_ids" {
  type        = list(string)
  description = "Subnet IDs dedicated to AWS Batch; falls back to ECS subnets when empty"
  default     = []
}

variable "batch_security_group_ids" {
  type        = list(string)
  description = "Security group IDs dedicated to AWS Batch; falls back to ECS security groups when empty"
  default     = []
}

variable "batch_job_queue_name" {
  type        = string
  description = "Name override for the AWS Batch job queue"
  default     = ""
}

variable "batch_job_queue_priority" {
  type        = number
  description = "Priority assigned to the AWS Batch job queue"
  default     = 1
}

variable "batch_job_definition_name" {
  type        = string
  description = "Name override for the AWS Batch job definition"
  default     = ""
}

variable "batch_container_command" {
  type        = list(string)
  description = "Command override for AWS Batch jobs; defaults to the ECS task command"
  default     = []
}

variable "batch_container_environment" {
  type        = map(string)
  description = "Environment variables for AWS Batch jobs; defaults to the ECS task environment"
  default     = {}
}

variable "batch_container_vcpu" {
  type        = string
  description = "vCPU setting for AWS Batch jobs; defaults to the ECS task CPU converted to vCPUs"
  default     = ""
}

variable "batch_container_memory" {
  type        = string
  description = "Memory (MiB) setting for AWS Batch jobs; defaults to the ECS task memory"
  default     = ""
}
