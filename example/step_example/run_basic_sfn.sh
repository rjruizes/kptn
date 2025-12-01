#!/usr/bin/env bash
set -euo pipefail

# Deploy infra, start the basic Step Functions state machine, and monitor until it finishes.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INFRA_DIR="${ROOT_DIR}/example/step_example/infra"
REGION="${AWS_REGION:-us-east-1}"

tf_apply() {
  terraform -chdir="${INFRA_DIR}" apply -auto-approve \
    -var="pipeline_name=basic" \
    -var="build_and_push_image=true" \
    -var="docker_build_context=.." \
    -var="docker_build_dockerfile=Dockerfile"
}

get_basic_state_machine_arn() {
  terraform -chdir="${INFRA_DIR}" output -json state_machine_arns \
    | jq -r '.basic'
}

start_execution() {
  local sm_arn="$1"
  local exec_name="basic-$(date +%s)"
  aws stepfunctions start-execution \
    --region "${REGION}" \
    --state-machine-arn "${sm_arn}" \
    --name "${exec_name}" \
    --input '{}' \
    | jq -r '.executionArn'
}

poll_execution() {
  local exec_arn="$1"
  local last_event_id=""
  while true; do
    local desc status
    desc="$(aws stepfunctions describe-execution --region "${REGION}" --execution-arn "${exec_arn}")"
    status="$(jq -r '.status' <<<"${desc}")"
    printf '[%s] status=%s\n' "$(date +%H:%M:%S)" "${status}"

    # Fetch a few most recent events for a quick heartbeat
    aws stepfunctions get-execution-history \
      --region "${REGION}" \
      --execution-arn "${exec_arn}" \
      --max-items 5 \
      --reverse-order \
      --query 'events[].[id,timestamp,type]' \
      --output text \
      | sed 's/^/  /'

    case "${status}" in
      RUNNING|QUEUED) sleep 10 ;;
      SUCCEEDED) echo "Execution succeeded: ${exec_arn}"; return 0 ;;
      *) echo "Execution finished with status=${status}: ${exec_arn}"; return 1 ;;
    esac
  done
}

main() {
  command -v terraform >/dev/null || { echo "terraform not found" >&2; exit 1; }
  command -v aws >/dev/null || { echo "aws CLI not found" >&2; exit 1; }
  command -v jq >/dev/null || { echo "jq not found" >&2; exit 1; }

  echo "Applying infrastructure..."
  tf_apply

  echo "Fetching basic state machine ARN..."
  sm_arn="$(get_basic_state_machine_arn)"
  if [[ -z "${sm_arn}" || "${sm_arn}" == "null" ]]; then
    echo "Could not determine state machine ARN" >&2
    exit 1
  fi
  echo "State machine: ${sm_arn}"

  echo "Starting execution..."
  exec_arn="$(start_execution "${sm_arn}")"
  echo "Execution ARN: ${exec_arn}"

  echo "Polling execution progress..."
  poll_execution "${exec_arn}"
}

main "$@"
