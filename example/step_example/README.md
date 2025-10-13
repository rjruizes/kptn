Very alpha.

Run `uv run kapten codegen` to transform YAML to JSON (outputs basic.json.tpl and basic2.json.tpl), then terraform apply to build the docker image, tag, and push to ECR and update Step Functions with the updated JSON (if changed).

When the ECS Task runs, it uses the environment variables to determine which task's code to run.
