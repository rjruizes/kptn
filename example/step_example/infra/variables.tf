variable "region" {
  type        = string
  description = "AWS region to deploy Step Functions resources into"
  default     = "us-east-1"
}

variable "pipeline_name" {
  type        = string
  description = "Kapten pipeline name represented by this state machine"
}

variable "state_machine_definition" {
  type        = string
  description = "Optional raw JSON definition override for the state machine"
  default     = ""
}

variable "state_machine_definition_file" {
  type        = string
  description = "Relative path to the generated Kapten Step Functions JSON definition"
  default     = "../basic.json.tpl"
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
  default     = "Kapten Step Functions tasks"
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
  default     = "kapten-task"
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

variable "task_definition_task_role_arn" {
  type        = string
  description = "IAM task role ARN used by containers in the ECS task definition"
  default     = null
}

variable "task_definition_container_name" {
  type        = string
  description = "Name assigned to the container in the generated task definition"
  default     = "kapten"
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

variable "create_task_execution_role" {
  type        = bool
  description = "Set to true to provision an ECS task execution role"
  default     = false
}

variable "task_execution_role_name_prefix" {
  type        = string
  description = "Name prefix applied to a generated ECS task execution role"
  default     = "kapten-task-execution-role"
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
