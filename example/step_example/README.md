Very alpha.

Run `uv run kptn codegen` to transform YAML to JSON (outputs basic.json.tpl and basic2.json.tpl), then `terraform apply` to build the docker image, tag, and push to ECR and update Step Functions with the updated JSON (if changed). The JSON are suffixed with .tpl because terraform will substitute in variables.

When the ECS Task runs, it uses the environment variables to determine which task's code to run.


