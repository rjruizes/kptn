"""Infrastructure scaffolding commands for kptn."""
import os
from dataclasses import dataclass
from pathlib import Path
import typer
from typing import Any, Dict, List, Optional

from kptn.cli.decider_bundle import BundleDeciderError, bundle_decider_lambda
from kptn.codegen.infra_scaffolder import scaffold_stepfunctions_infra
from kptn.read_config import read_config


@dataclass
class InfraInputs:
    create_networking: bool
    create_security_group: bool
    create_task_definition: bool
    create_ecr_repository: bool
    create_task_execution_role: bool
    create_task_role: bool
    create_ecs_cluster: bool
    create_efs: bool
    enable_efs: bool
    tfvars: Dict[str, Any]
    warnings: List[str]


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _prompt_list(prompt_text: str, default: List[str]) -> List[str]:
    default_text = ", ".join(default)
    response = typer.prompt(prompt_text, default=default_text)
    if isinstance(response, list):
        return [str(item).strip() for item in response if str(item).strip()]
    return _split_csv(str(response))


def _prompt_required(prompt_text: str, default: Optional[str] = None) -> str:
    while True:
        value = typer.prompt(prompt_text, default=default or "").strip()
        if value:
            return value
        typer.secho("A value is required.", fg=typer.colors.RED)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return str(path)


def _parse_env_pairs(pairs: List[str]) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Invalid environment entry '{pair}'. Expected KEY=VALUE format.")
        key, value = pair.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError("Environment variable key cannot be empty.")
        env[key] = value.strip()
    return env


def _collect_infra_inputs(
    *,
    pipeline_name: str,
    interactive: bool,
    auto_approve: bool,
    provision_networking: Optional[bool],
    provision_security_group: Optional[bool],
    provision_ecr_repository: Optional[bool],
    provision_task_definition: Optional[bool],
    provision_task_execution_role: Optional[bool],
    provision_task_role: Optional[bool],
    provision_ecs_cluster: Optional[bool],
    provision_efs: Optional[bool],
    enable_efs: Optional[bool],
    subnet_ids: List[str],
    security_group_ids: List[str],
    ecs_cluster_arn: Optional[str],
    ecs_task_definition_arn: Optional[str],
    ecs_task_execution_role_arn: Optional[str],
    task_role_arn: Optional[str],
    new_vpc_cidr_block: Optional[str],
    new_subnet_cidr: List[str],
    new_subnet_az: List[str],
    ingress_cidr: List[str],
    egress_cidr: List[str],
    new_security_group_description: Optional[str],
    vpc_id: Optional[str],
    ecr_repository_name: Optional[str],
    ecr_repository_image_tag_mutability: Optional[str],
    ecr_repository_scan_on_push: Optional[bool],
    ecr_repository_url: Optional[str],
    assign_public_ip: Optional[bool] = None,
    ecs_launch_type: Optional[str] = None,
    task_definition_family: Optional[str],
    task_definition_cpu: Optional[str],
    task_definition_memory: Optional[str],
    task_definition_network_mode: Optional[str],
    task_definition_container_name: Optional[str],
    task_definition_container_image: Optional[str],
    task_definition_command: List[str],
    task_definition_environment: Dict[str, str],
    task_definition_requires_compatibilities: List[str],
    task_definition_task_role_arn: Optional[str],
    task_execution_role_name: Optional[str],
    task_execution_role_managed_policies: List[str],
    task_role_name_prefix: Optional[str],
    task_role_managed_policies: List[str],
    dynamodb_table_name: Optional[str],
    efs_file_system_id: Optional[str],
    efs_access_point_id: Optional[str],
    efs_container_mount_path: Optional[str],
    efs_root_directory_path: Optional[str],
) -> InfraInputs:
    create_networking = provision_networking
    if create_networking is None:
        create_networking = True if auto_approve or not interactive else typer.confirm(
            "Provision new VPC networking (VPC and subnets)?",
            default=True,
        )

    create_security_group = provision_security_group
    if create_security_group is None:
        default_sg = bool(create_networking)
        create_security_group = default_sg if auto_approve or not interactive else typer.confirm(
            "Provision a new security group?",
            default=default_sg,
        )

    create_ecr_repository = provision_ecr_repository
    if create_ecr_repository is None:
        create_ecr_repository = True if auto_approve or not interactive else typer.confirm(
            "Provision a new ECR repository?",
            default=True,
        )

    create_task_definition = provision_task_definition
    if create_task_definition is None:
        create_task_definition = True if auto_approve or not interactive else typer.confirm(
            "Provision a new ECS task definition?",
            default=True,
        )

    create_task_execution_role = provision_task_execution_role
    if create_task_execution_role is None:
        create_task_execution_role = True if auto_approve or not interactive else typer.confirm(
            "Provision a new ECS task execution role?",
            default=True,
        )

    create_task_role = provision_task_role
    if create_task_role is None:
        create_task_role = True if auto_approve or not interactive else typer.confirm(
            "Provision a new IAM task role with DynamoDB permissions?",
            default=True,
        )

    create_ecs_cluster = provision_ecs_cluster
    if create_ecs_cluster is None:
        create_ecs_cluster = True if auto_approve or not interactive else typer.confirm(
            "Provision a new ECS cluster?",
            default=True,
        )

    tfvars: Dict[str, Any] = {
        "create_networking": create_networking,
        "create_security_group": create_security_group,
        "create_ecr_repository": create_ecr_repository,
        "create_task_definition": create_task_definition,
        "create_task_execution_role": create_task_execution_role,
        "create_task_role": create_task_role,
        "create_ecs_cluster": create_ecs_cluster,
        "create_decider_lambda": True,
    }
    warnings: List[str] = []

    if create_networking:
        vpc_cidr = new_vpc_cidr_block or "10.0.0.0/16"
        if interactive:
            vpc_cidr = _prompt_required(
                "CIDR block for the new VPC",
                default=vpc_cidr,
            )
        tfvars["new_vpc_cidr_block"] = vpc_cidr

        subnet_cidrs = new_subnet_cidr or ["10.0.1.0/24", "10.0.2.0/24"]
        if interactive:
            subnet_cidrs = _prompt_list(
                "CIDR blocks for new subnets (comma separated)",
                subnet_cidrs,
            ) or subnet_cidrs
        tfvars["new_subnet_cidr_blocks"] = subnet_cidrs

        if new_subnet_az:
            tfvars["new_subnet_availability_zones"] = new_subnet_az
        elif interactive:
            azs = _split_csv(
                typer.prompt(
                    "Availability zones for new subnets (optional, comma separated)",
                    default="",
                )
            )
            if azs:
                tfvars["new_subnet_availability_zones"] = azs

    if create_security_group:
        if not create_networking:
            vpc_value = vpc_id
            if not vpc_value and interactive:
                vpc_value = _prompt_required(
                    "Existing VPC ID to attach the new security group",
                )
            if vpc_value:
                tfvars["vpc_id"] = vpc_value
            else:
                warnings.append(
                    "No VPC ID provided for security group creation; update terraform.tfvars before applying."
                )

        ingress_blocks = ingress_cidr or ["0.0.0.0/0"]
        if interactive:
            ingress_blocks = _prompt_list(
                "Ingress CIDR blocks for the new security group",
                ingress_blocks,
            ) or ingress_blocks
        tfvars["new_security_group_ingress_cidr_blocks"] = ingress_blocks

        egress_blocks = egress_cidr or ["0.0.0.0/0"]
        if interactive:
            egress_blocks = _prompt_list(
                "Egress CIDR blocks for the new security group",
                egress_blocks,
            ) or egress_blocks
        tfvars["new_security_group_egress_cidr_blocks"] = egress_blocks

        description = new_security_group_description or "kptn Step Functions tasks"
        if interactive:
            description = typer.prompt(
                "Description for the new security group",
                default=description,
            )
        tfvars["new_security_group_description"] = description

    if create_ecr_repository:
        repository_name = ecr_repository_name or pipeline_name
        tfvars["ecr_repository_name"] = repository_name
        if ecr_repository_image_tag_mutability:
            tfvars["ecr_repository_image_tag_mutability"] = ecr_repository_image_tag_mutability
        if ecr_repository_scan_on_push is not None:
            tfvars["ecr_repository_scan_on_push"] = ecr_repository_scan_on_push
    elif ecr_repository_url:
        tfvars["ecr_repository_url"] = ecr_repository_url

    if create_task_definition:
        family = task_definition_family or f"{pipeline_name}-task"
        cpu_value = task_definition_cpu or "512"
        memory_value = task_definition_memory or "1024"
        network_mode_value = task_definition_network_mode or "awsvpc"
        container_name_value = task_definition_container_name or pipeline_name
        container_image_value = task_definition_container_image or "public.ecr.aws/amazonlinux/amazonlinux:latest"
        command_values = list(task_definition_command)
        env_map = dict(task_definition_environment)
        requires_compats = task_definition_requires_compatibilities or ["FARGATE"]

        tfvars.update(
            {
                "task_definition_family": family,
                "task_definition_cpu": cpu_value,
                "task_definition_memory": memory_value,
                "task_definition_network_mode": network_mode_value,
                "task_definition_container_name": container_name_value,
                "task_definition_container_image": container_image_value,
                "task_definition_requires_compatibilities": requires_compats,
            }
        )

        if command_values:
            tfvars["task_definition_container_command"] = command_values

        if env_map:
            tfvars["task_definition_container_environment"] = env_map

        if task_definition_task_role_arn:
            tfvars["task_definition_task_role_arn"] = task_definition_task_role_arn

    if not create_networking:
        if subnet_ids:
            tfvars["subnet_ids"] = subnet_ids
        elif interactive:
            collected = _split_csv(
                typer.prompt(
                    "Subnet IDs to reuse (comma separated)",
                    default="",
                )
            )
            if collected:
                tfvars["subnet_ids"] = collected
            else:
                warnings.append(
                    "No subnet IDs provided; update terraform.tfvars before applying."
                )
        else:
            warnings.append(
                "No subnet IDs provided; update terraform.tfvars before applying."
            )

    additional_sg_ids: List[str] = []
    if security_group_ids:
        additional_sg_ids = security_group_ids
    elif interactive:
        prompt = "Additional security group IDs to attach (comma separated)"
        if not create_security_group:
            prompt = "Security group IDs to reuse (comma separated)"
        collected = _split_csv(
            typer.prompt(
                prompt,
                default="",
            )
        )
        additional_sg_ids = collected

    if additional_sg_ids:
        tfvars["security_group_ids"] = additional_sg_ids
    elif not create_security_group:
        warnings.append(
            "No security group IDs provided; update terraform.tfvars before applying."
        )

    if not create_ecs_cluster:
        cluster_arn = ecs_cluster_arn
        if not cluster_arn and interactive:
            cluster_arn = _prompt_required(
                "Existing ECS cluster ARN to reuse",
            )
        if cluster_arn:
            tfvars["ecs_cluster_arn"] = cluster_arn
        else:
            warnings.append(
                "No ECS cluster ARN provided; set ecs_cluster_arn or enable provisioning."
            )

    if not create_task_definition:
        task_definition_arn = ecs_task_definition_arn
        if not task_definition_arn:
            if interactive:
                task_definition_arn = _prompt_required(
                    "ECS task definition ARN to execute"
                )
            else:
                raise typer.BadParameter(
                    "Missing required option",
                    param_hint="--ecs-task-definition-arn",
                )
        tfvars["ecs_task_definition_arn"] = task_definition_arn

    if create_task_execution_role:
        role_name_prefix = task_execution_role_name or f"{pipeline_name}-task-execution-role"
        tfvars["task_execution_role_name_prefix"] = role_name_prefix
        if task_execution_role_managed_policies:
            tfvars["task_execution_role_managed_policies"] = task_execution_role_managed_policies
    else:
        execution_role_arn = ecs_task_execution_role_arn
        if not execution_role_arn:
            if interactive:
                execution_role_arn = _prompt_required(
                    "IAM role ARN that ECS tasks assume"
                )
            else:
                raise typer.BadParameter(
                    "Missing required option",
                    param_hint="--ecs-task-execution-role-arn",
                )
        tfvars["ecs_task_execution_role_arn"] = execution_role_arn

    if create_task_role:
        role_prefix = task_role_name_prefix or f"{pipeline_name}-task-role"
        tfvars["task_role_name_prefix"] = role_prefix
        if task_role_managed_policies:
            tfvars["task_role_managed_policies"] = task_role_managed_policies
    else:
        if task_role_arn:
            tfvars["task_role_arn"] = task_role_arn
        elif interactive:
            task_role_value = typer.prompt(
                "IAM task role ARN (optional, press Enter to skip)",
                default="",
            ).strip()
            if task_role_value:
                tfvars["task_role_arn"] = task_role_value

    table_name = dynamodb_table_name or f"{pipeline_name}-table"
    if interactive and not dynamodb_table_name:
        table_name = typer.prompt(
            "DynamoDB table name",
            default=table_name,
        )
    tfvars["dynamodb_table_name"] = table_name

    tfvars["assign_public_ip"] = assign_public_ip if assign_public_ip is not None else False
    tfvars["ecs_launch_type"] = (ecs_launch_type or "FARGATE").upper()

    # EFS configuration
    create_efs = provision_efs
    if create_efs is None:
        create_efs = False if auto_approve or not interactive else typer.confirm(
            "Provision a new EFS file system?",
            default=False,
        )

    efs_enabled = enable_efs
    if efs_enabled is None:
        efs_enabled = create_efs if auto_approve or not interactive else typer.confirm(
            "Enable EFS mounting in task definition?",
            default=create_efs,
        )

    tfvars["create_efs"] = create_efs
    tfvars["enable_efs"] = efs_enabled

    if efs_enabled:
        if not create_efs:
            # Reusing existing EFS
            file_system_id = efs_file_system_id
            if not file_system_id and interactive:
                file_system_id = _prompt_required(
                    "Existing EFS file system ID to reuse"
                )
            if file_system_id:
                tfvars["efs_file_system_id"] = file_system_id
            else:
                warnings.append(
                    "No EFS file system ID provided; update terraform.tfvars before applying."
                )

            access_point_id = efs_access_point_id
            if not access_point_id and interactive:
                access_point_id = _prompt_required(
                    "Existing EFS access point ID to reuse"
                )
            if access_point_id:
                tfvars["efs_access_point_id"] = access_point_id
            else:
                warnings.append(
                    "No EFS access point ID provided; update terraform.tfvars before applying."
                )

        # EFS mount configuration
        mount_path = efs_container_mount_path or "/mnt/efs"
        if interactive and not efs_container_mount_path:
            mount_path = typer.prompt(
                "Container path where EFS will be mounted",
                default=mount_path,
            )
        tfvars["efs_container_mount_path"] = mount_path

        if create_efs:
            # EFS creation parameters
            root_dir = efs_root_directory_path or "/data"
            if interactive and not efs_root_directory_path:
                root_dir = typer.prompt(
                    "EFS root directory path",
                    default=root_dir,
                )
            tfvars["efs_root_directory_path"] = root_dir
            tfvars["efs_owner_gid"] = 1000
            tfvars["efs_owner_uid"] = 1000
            tfvars["efs_posix_gid"] = 1000
            tfvars["efs_posix_uid"] = 1000

    return InfraInputs(
        create_networking=create_networking,
        create_security_group=create_security_group,
        create_task_definition=create_task_definition,
        create_ecr_repository=create_ecr_repository,
        create_task_execution_role=create_task_execution_role,
        create_task_role=create_task_role,
        create_ecs_cluster=create_ecs_cluster,
        create_efs=create_efs,
        enable_efs=efs_enabled,
        tfvars=tfvars,
        warnings=warnings,
    )


def _run_codegen_infra(
    *,
    output_dir: Path,
    force: bool,
    interactive: bool,
    auto_approve: bool,
    provision_networking: Optional[bool],
    provision_security_group: Optional[bool],
    provision_ecr_repository: Optional[bool],
    provision_task_definition: Optional[bool],
    provision_task_execution_role: Optional[bool],
    provision_task_role: Optional[bool],
    provision_ecs_cluster: Optional[bool],
    provision_efs: Optional[bool],
    enable_efs: Optional[bool],
    subnet_ids: List[str],
    security_group_ids: List[str],
    ecs_cluster_arn: Optional[str],
    ecs_task_definition_arn: Optional[str],
    ecs_task_execution_role_arn: Optional[str],
    task_role_arn: Optional[str],
    new_vpc_cidr_block: Optional[str],
    new_subnet_cidr: List[str],
    new_subnet_az: List[str],
    ingress_cidr: List[str],
    egress_cidr: List[str],
    new_security_group_description: Optional[str],
    vpc_id: Optional[str],
    ecr_repository_name: Optional[str],
    ecr_repository_image_tag_mutability: Optional[str],
    ecr_repository_scan_on_push: Optional[bool],
    ecr_repository_url: Optional[str],
    task_definition_family: Optional[str],
    task_definition_cpu: Optional[str],
    task_definition_memory: Optional[str],
    task_definition_network_mode: Optional[str],
    task_definition_container_name: Optional[str],
    task_definition_container_image: Optional[str],
    task_definition_command: List[str],
    task_definition_environment: Dict[str, str],
    task_definition_requires_compatibilities: List[str],
    task_definition_task_role_arn: Optional[str],
    task_execution_role_name: Optional[str],
    task_execution_role_managed_policies: List[str],
    task_role_name_prefix: Optional[str],
    task_role_managed_policies: List[str],
    dynamodb_table_name: Optional[str],
    efs_file_system_id: Optional[str],
    efs_access_point_id: Optional[str],
    efs_container_mount_path: Optional[str],
    efs_root_directory_path: Optional[str],
) -> None:
    kap_conf = read_config()

    # Get all graph names from configuration
    graphs = kap_conf.get("graphs", {})
    if not graphs:
        raise ValueError("No graphs defined in kptn.yaml")

    # Scaffold infrastructure for all graphs; use the first graph name for resource defaults
    graph_names = sorted(graphs.keys())
    pipeline_name = graph_names[0]

    settings = kap_conf.get("settings", {})
    flows_dir_setting = settings.get("flows_dir", "flows")
    flows_dir = Path(flows_dir_setting)

    inputs = _collect_infra_inputs(
        pipeline_name=pipeline_name,
        interactive=interactive,
        auto_approve=auto_approve,
        provision_networking=provision_networking,
        provision_security_group=provision_security_group,
        provision_ecr_repository=provision_ecr_repository,
        provision_task_definition=provision_task_definition,
        provision_task_execution_role=provision_task_execution_role,
        provision_task_role=provision_task_role,
        provision_ecs_cluster=provision_ecs_cluster,
        provision_efs=provision_efs,
        enable_efs=enable_efs,
        subnet_ids=subnet_ids,
        security_group_ids=security_group_ids,
        ecs_cluster_arn=ecs_cluster_arn,
        ecs_task_definition_arn=ecs_task_definition_arn,
        ecs_task_execution_role_arn=ecs_task_execution_role_arn,
        task_role_arn=task_role_arn,
        new_vpc_cidr_block=new_vpc_cidr_block,
        new_subnet_cidr=new_subnet_cidr,
        new_subnet_az=new_subnet_az,
        ingress_cidr=ingress_cidr,
        egress_cidr=egress_cidr,
        new_security_group_description=new_security_group_description,
        vpc_id=vpc_id,
        ecr_repository_name=ecr_repository_name,
        ecr_repository_image_tag_mutability=ecr_repository_image_tag_mutability,
        ecr_repository_scan_on_push=ecr_repository_scan_on_push,
        ecr_repository_url=ecr_repository_url,
        task_definition_family=task_definition_family,
        task_definition_cpu=task_definition_cpu,
        task_definition_memory=task_definition_memory,
        task_definition_network_mode=task_definition_network_mode,
        task_definition_container_name=task_definition_container_name,
        task_definition_container_image=task_definition_container_image,
        task_definition_command=task_definition_command,
        task_definition_environment=task_definition_environment,
        task_definition_requires_compatibilities=task_definition_requires_compatibilities,
        task_definition_task_role_arn=task_definition_task_role_arn,
        task_execution_role_name=task_execution_role_name,
        task_execution_role_managed_policies=task_execution_role_managed_policies,
        task_role_name_prefix=task_role_name_prefix,
        task_role_managed_policies=task_role_managed_policies,
        dynamodb_table_name=dynamodb_table_name,
        efs_file_system_id=efs_file_system_id,
        efs_access_point_id=efs_access_point_id,
        efs_container_mount_path=efs_container_mount_path,
        efs_root_directory_path=efs_root_directory_path,
    )

    report = scaffold_stepfunctions_infra(
        output_dir=output_dir,
        pipeline_name=pipeline_name,
        graph_names=graph_names,
        flows_dir=flows_dir,
        force=force,
        tfvars_values=inputs.tfvars,
        warnings=inputs.warnings,
    )

    if report.created:
        typer.secho("Created:", fg=typer.colors.GREEN)
        for path in report.created:
            typer.echo(f"  {_display_path(path)}")
    if report.skipped:
        typer.secho("Skipped (already existed):", fg=typer.colors.YELLOW)
        for path in report.skipped:
            typer.echo(f"  {_display_path(path)}")
        typer.secho("Re-run with --force to overwrite.", fg=typer.colors.YELLOW)

    if report.terraform_tfvars_path:
        typer.echo(
            "Terraform variables written to "
            f"{_display_path(report.terraform_tfvars_path)}"
        )

    if report.state_machine_files_missing:
        typer.secho(
            "Warning: Step Functions definition files not found for: "
            f"{', '.join(report.state_machine_files_missing)}. "
            "Run 'kptn codegen' first.",
            fg=typer.colors.YELLOW,
        )

    if report.state_machine_files:
        typer.echo("State machine definitions referenced:")
        for graph_name, file_path in report.state_machine_files.items():
            status = "✓" if graph_name not in report.state_machine_files_missing else "✗"
            typer.echo(f"  {status} {graph_name}: {_display_path(file_path)}")

    repo_root = Path(__file__).resolve().parents[2]
    project_root = Path.cwd()
    try:
        bundle_result = bundle_decider_lambda(
            project_root=project_root,
            output_dir=report.output_dir / "lambda_decider",
            pipeline=pipeline_name,
            kptn_source=repo_root,
            project_source=project_root,
            install_project=False,
            prefer_local_kptn=True,
        )
    except BundleDeciderError as exc:
        typer.secho(f"Failed to build decider Lambda bundle: {exc}", fg=typer.colors.RED)
        raise typer.Exit(1) from exc

    suffix = ""
    if bundle_result.pipeline_name:
        suffix = f" (pipeline {bundle_result.pipeline_name})"
    typer.echo(
        "Decider Lambda bundle installed under "
        f"{_display_path(bundle_result.bundle_dir)}{suffix}"
    )

    combined_warnings = report.warnings
    if combined_warnings:
        typer.secho("Warnings:", fg=typer.colors.YELLOW)
        for warning in combined_warnings:
            typer.echo(f"  - {warning}")


def _resolve_project_call(project_dir: Optional[Path], func):
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            return func()
        finally:
            os.chdir(original_dir)
    return func()


def register_infra_commands(app: typer.Typer):
    """Register infrastructure-related commands to the Typer app."""

    @app.command(name="codegen-infra")
    def codegen_infra(
        project_dir: Optional[Path] = typer.Option(
            None,
            "--project-dir",
            "-p",
            help="Project directory containing kptn configuration",
        ),
        output_dir: Path = typer.Option(
            Path("infra"),
            "--output-dir",
            "-o",
            help="Directory where Terraform files will be written",
        ),
        force: bool = typer.Option(
            False,
            "--force",
            help="Overwrite existing files in the output directory",
        ),
        interactive: bool = typer.Option(
            True,
            "--interactive/--no-interactive",
            help="Prompt for infrastructure inputs instead of relying solely on flags",
        ),
        yes: bool = typer.Option(
            False,
            "-y",
            "--yes",
            help="Automatically accept provisioning defaults without prompting",
        ),
        provision_networking: Optional[bool] = typer.Option(
            None,
            "--provision-networking/--reuse-networking",
            help="Provision a new VPC and subnets (default: prompt)",
        ),
        provision_security_group: Optional[bool] = typer.Option(
            None,
            "--provision-security-group/--reuse-security-group",
            help="Provision a new security group (default: prompt)",
        ),
        provision_ecr_repository: Optional[bool] = typer.Option(
            None,
            "--provision-ecr/--reuse-ecr",
            help="Provision a new ECR repository (default: prompt)",
        ),
        provision_task_definition: Optional[bool] = typer.Option(
            None,
            "--provision-task-definition/--reuse-task-definition",
            help="Provision an ECS task definition (default: prompt)",
        ),
        provision_task_execution_role: Optional[bool] = typer.Option(
            None,
            "--provision-task-execution-role/--reuse-task-execution-role",
            help="Provision a new ECS task execution role (default: prompt)",
        ),
        provision_ecs_cluster: Optional[bool] = typer.Option(
            None,
            "--provision-ecs-cluster/--reuse-ecs-cluster",
            help="Provision a new ECS cluster (default: prompt)",
        ),
        ecs_cluster_arn: Optional[str] = typer.Option(
            None,
            "--ecs-cluster-arn",
            help="Existing ECS cluster ARN to reuse",
        ),
        ecs_task_definition_arn: Optional[str] = typer.Option(
            None,
            "--ecs-task-definition-arn",
            help="ECS task definition ARN executed by the state machine",
        ),
        ecs_task_execution_role_arn: Optional[str] = typer.Option(
            None,
            "--ecs-task-execution-role-arn",
            help="IAM role ARN assumed by ECS tasks",
        ),
        subnet_id: List[str] = typer.Option(
            [],
            "--subnet-id",
            help="Existing subnet ID to reuse (repeat flag to supply multiple)",
        ),
        security_group_id: List[str] = typer.Option(
            [],
            "--security-group-id",
            help="Existing security group ID to reuse (repeat flag to supply multiple)",
        ),
        new_vpc_cidr_block: Optional[str] = typer.Option(
            None,
            "--new-vpc-cidr",
            help="CIDR block for a newly provisioned VPC",
        ),
        new_subnet_cidr: List[str] = typer.Option(
            [],
            "--new-subnet-cidr",
            help="CIDR block for a new subnet (repeatable)",
        ),
        new_subnet_az: List[str] = typer.Option(
            [],
            "--new-subnet-az",
            help="Availability zone for a new subnet (repeatable, matches order of --new-subnet-cidr)",
        ),
        vpc_id: Optional[str] = typer.Option(
            None,
            "--vpc-id",
            help="Existing VPC ID to attach a generated security group when reusing networking",
        ),
        ingress_cidr: List[str] = typer.Option(
            [],
            "--ingress-cidr",
            help="Ingress CIDR block when creating a security group (repeatable)",
        ),
        egress_cidr: List[str] = typer.Option(
            [],
            "--egress-cidr",
            help="Egress CIDR block when creating a security group (repeatable)",
        ),
        new_security_group_description: Optional[str] = typer.Option(
            None,
            "--new-security-group-description",
            help="Description for the generated security group",
        ),
        ecr_repository_name: Optional[str] = typer.Option(
            None,
            "--ecr-repository-name",
            help="Name for a generated ECR repository",
        ),
        ecr_repository_image_tag_mutability: Optional[str] = typer.Option(
            None,
            "--ecr-image-tag-mutability",
            help="Image tag mutability for the generated ECR repository",
        ),
        ecr_repository_scan_on_push: Optional[bool] = typer.Option(
            None,
            "--ecr-scan-on-push/--no-ecr-scan-on-push",
            help="Enable ECR image scan on push (default: true)",
        ),
        ecr_repository_url: Optional[str] = typer.Option(
            None,
            "--ecr-repository-url",
            help="Existing ECR repository URL to reuse",
        ),
        task_definition_family: Optional[str] = typer.Option(
            None,
            "--task-definition-family",
            help="Family name for a generated ECS task definition",
        ),
        task_definition_cpu: Optional[str] = typer.Option(
            None,
            "--task-definition-cpu",
            help="CPU units for a generated ECS task definition",
        ),
        task_definition_memory: Optional[str] = typer.Option(
            None,
            "--task-definition-memory",
            help="Memory (MiB) for a generated ECS task definition",
        ),
        task_definition_network_mode: Optional[str] = typer.Option(
            None,
            "--task-definition-network-mode",
            help="Network mode for a generated ECS task definition",
        ),
        task_definition_container_name: Optional[str] = typer.Option(
            None,
            "--task-definition-container-name",
            help="Container name within the generated ECS task definition",
        ),
        task_definition_container_image: Optional[str] = typer.Option(
            None,
            "--task-definition-container-image",
            help="Container image URI for the generated ECS task definition",
        ),
        task_definition_command: List[str] = typer.Option(
            [],
            "--task-definition-command",
            help="Command element for the generated container (repeat to supply multiple arguments)",
        ),
        task_definition_env: List[str] = typer.Option(
            [],
            "--task-definition-env",
            help="Environment variable for the generated container (KEY=VALUE, repeatable)",
        ),
        task_definition_requires_compatibility: List[str] = typer.Option(
            [],
            "--task-definition-requires-compatibility",
            help="Launch type for the generated task definition (repeatable)",
        ),
        task_definition_task_role_arn: Optional[str] = typer.Option(
            None,
            "--task-definition-task-role-arn",
            help="Task role ARN for the generated ECS task definition",
        ),
        task_execution_role_name: Optional[str] = typer.Option(
            None,
            "--task-execution-role-name",
            help="Name prefix for a generated ECS task execution role",
        ),
        task_execution_role_managed_policy: List[str] = typer.Option(
            [],
            "--task-execution-role-managed-policy",
            help="Managed policy ARN to attach to a generated task execution role (repeatable)",
        ),
        provision_task_role: Optional[bool] = typer.Option(
            None,
            "--provision-task-role/--reuse-task-role",
            help="Provision a new IAM task role with DynamoDB permissions (default: prompt)",
        ),
        task_role_arn: Optional[str] = typer.Option(
            None,
            "--task-role-arn",
            help="Existing IAM task role ARN to reuse",
        ),
        task_role_name_prefix: Optional[str] = typer.Option(
            None,
            "--task-role-name-prefix",
            help="Name prefix for a generated IAM task role",
        ),
        task_role_managed_policy: List[str] = typer.Option(
            [],
            "--task-role-managed-policy",
            help="Managed policy ARN to attach to a generated task role (repeatable)",
        ),
        dynamodb_table_name: Optional[str] = typer.Option(
            None,
            "--dynamodb-table-name",
            help="Name for the DynamoDB table used by kptn tasks",
        ),
        provision_efs: Optional[bool] = typer.Option(
            None,
            "--provision-efs/--reuse-efs",
            help="Provision a new EFS file system (default: prompt)",
        ),
        enable_efs: Optional[bool] = typer.Option(
            None,
            "--enable-efs/--disable-efs",
            help="Enable EFS mounting in the task definition (default: prompt)",
        ),
        efs_file_system_id: Optional[str] = typer.Option(
            None,
            "--efs-file-system-id",
            help="Existing EFS file system ID to reuse",
        ),
        efs_access_point_id: Optional[str] = typer.Option(
            None,
            "--efs-access-point-id",
            help="Existing EFS access point ID to reuse",
        ),
        efs_container_mount_path: Optional[str] = typer.Option(
            None,
            "--efs-container-mount-path",
            help="Container path where EFS will be mounted (default: /mnt/efs)",
        ),
        efs_root_directory_path: Optional[str] = typer.Option(
            None,
            "--efs-root-directory-path",
            help="Root directory path for the EFS access point (default: /data)",
        ),
    ):
        """Scaffold Terraform IaC for running kptn Step Functions on ECS."""

        def _action() -> None:
            try:
                env_map = _parse_env_pairs(task_definition_env)
            except ValueError as exc:
                raise typer.BadParameter(str(exc), param_hint="--task-definition-env") from exc

            _run_codegen_infra(
                output_dir=output_dir,
                force=force,
                interactive=interactive and not yes,
                auto_approve=yes,
                provision_networking=provision_networking,
                provision_security_group=provision_security_group,
                provision_ecr_repository=provision_ecr_repository,
                provision_task_definition=provision_task_definition,
                provision_task_execution_role=provision_task_execution_role,
                provision_task_role=provision_task_role,
                provision_ecs_cluster=provision_ecs_cluster,
                provision_efs=provision_efs,
                enable_efs=enable_efs,
                subnet_ids=list(subnet_id),
                security_group_ids=list(security_group_id),
                ecs_cluster_arn=ecs_cluster_arn,
                ecs_task_definition_arn=ecs_task_definition_arn,
                ecs_task_execution_role_arn=ecs_task_execution_role_arn,
                task_role_arn=task_role_arn,
                new_vpc_cidr_block=new_vpc_cidr_block,
                new_subnet_cidr=list(new_subnet_cidr),
                new_subnet_az=list(new_subnet_az),
                vpc_id=vpc_id,
                ingress_cidr=list(ingress_cidr),
                egress_cidr=list(egress_cidr),
                new_security_group_description=new_security_group_description,
                ecr_repository_name=ecr_repository_name,
                ecr_repository_image_tag_mutability=ecr_repository_image_tag_mutability,
                ecr_repository_scan_on_push=ecr_repository_scan_on_push,
                ecr_repository_url=ecr_repository_url,
                task_definition_family=task_definition_family,
                task_definition_cpu=task_definition_cpu,
                task_definition_memory=task_definition_memory,
                task_definition_network_mode=task_definition_network_mode,
                task_definition_container_name=task_definition_container_name,
                task_definition_container_image=task_definition_container_image,
                task_definition_command=list(task_definition_command),
                task_definition_environment=env_map,
                task_definition_requires_compatibilities=list(task_definition_requires_compatibility),
                task_definition_task_role_arn=task_definition_task_role_arn,
                task_execution_role_name=task_execution_role_name,
                task_execution_role_managed_policies=list(task_execution_role_managed_policy),
                task_role_name_prefix=task_role_name_prefix,
                task_role_managed_policies=list(task_role_managed_policy),
                dynamodb_table_name=dynamodb_table_name,
                efs_file_system_id=efs_file_system_id,
                efs_access_point_id=efs_access_point_id,
                efs_container_mount_path=efs_container_mount_path,
                efs_root_directory_path=efs_root_directory_path,
            )

        _resolve_project_call(project_dir, _action)
