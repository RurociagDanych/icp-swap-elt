#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  apps/ingestion/scripts/aws_build_push_and_run.sh [options]

Options:
  --mode <sync|canister|backfill|incremental|full>
                              Ingestion mode for the one-off ECS run. Default: sync.
                              'canister' (alias 'full') runs the one-time deep-history
                              archive; 'backfill'/'incremental' use the REST API;
                              'sync' runs backfill-if-needed then REST incremental.
  --tag <tag>                 Image tag to build and push. Default: latest
                              ('latest' is mutable; prefer a unique tag for anything
                              beyond a quick test and point Terraform at it.)
  --platform <os/arch>        Target platform of the image build. Default: linux/amd64
                              (also settable via DOCKER_PLATFORM). Must match the Fargate
                              task's cpu_architecture, or the task fails to start.
  --terraform-dir <path>      Terraform AWS compute root. Default: infra/ingestion/compute
  --page-size <n>             Override page size for the one-off ECS run. Default: 1000
  --concurrency <n>           Override concurrency for the one-off ECS run. Default: 5
  --overlap <n>               DEPRECATED, validated but ignored (the app no longer uses it).
  --backfill-floor <ms>       Epoch-ms floor for backfill/sync when state has no
                              canisterMaxTxTime (e.g. first backfill on a pre-existing
                              bucket). Passed through to backfill and sync modes.
  --backfill-pages-per-file <n>
                              REST pages per committed backfill file. Default: 50.
                              Passed through to backfill and sync modes.
  --build-only                Build and push the image, but do not run ECS task
  --push-only                 Push the already-built local image tag, but do not build or run
  --run-only                  Skip build/push and only run the ECS task
  --help                      Show this help

Environment:
  AWS_PROFILE / AWS_REGION / normal AWS credential chain are used by the AWS CLI.

Notes:
  - The script reads cluster, task definition, security group, subnet IDs, and region
    from Terraform outputs in the AWS compute stack.
  - The safest/default path is to use tag 'latest', because the Terraform task definition
    points at ':latest' unless you override container_image in Terraform.
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${APP_DIR}/../.." && pwd)"

MODE="sync"
IMAGE_TAG="latest"
TF_DIR="${REPO_ROOT}/infra/ingestion/compute"
PAGE_SIZE="1000"
CONCURRENCY="5"
OVERLAP=""
PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"
BACKFILL_FLOOR=""
BACKFILL_PAGES_PER_FILE=""
DO_BUILD=1
DO_PUSH=1
DO_RUN=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:?missing value for --mode}"
      shift 2
      ;;
    --tag)
      IMAGE_TAG="${2:?missing value for --tag}"
      shift 2
      ;;
    --terraform-dir)
      TF_DIR_INPUT="${2:?missing value for --terraform-dir}"
      if [[ "${TF_DIR_INPUT}" = /* ]]; then
        TF_DIR="${TF_DIR_INPUT}"
      else
        TF_DIR="${REPO_ROOT}/${TF_DIR_INPUT}"
      fi
      shift 2
      ;;
    --platform)
      PLATFORM="${2:?missing value for --platform}"
      shift 2
      ;;
    --page-size)
      PAGE_SIZE="${2:?missing value for --page-size}"
      shift 2
      ;;
    --concurrency)
      CONCURRENCY="${2:?missing value for --concurrency}"
      shift 2
      ;;
    --overlap)
      OVERLAP="${2:?missing value for --overlap}"
      shift 2
      ;;
    --backfill-floor)
      BACKFILL_FLOOR="${2:?missing value for --backfill-floor}"
      shift 2
      ;;
    --backfill-pages-per-file)
      BACKFILL_PAGES_PER_FILE="${2:?missing value for --backfill-pages-per-file}"
      shift 2
      ;;
    --build-only)
      DO_RUN=0
      shift
      ;;
    --push-only)
      DO_BUILD=0
      DO_RUN=0
      shift
      ;;
    --run-only)
      DO_BUILD=0
      DO_PUSH=0
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

# Numeric options: integers only, with the same bounds the app enforces.
require_int() { # name value min [max]
  local name="$1" value="$2" min="$3" max="${4:-}"
  if [[ ! "${value}" =~ ^[0-9]+$ ]] || (( value < min )) || { [[ -n "${max}" ]] && (( value > max )); }; then
    echo "Invalid ${name} '${value}': expected an integer >= ${min}${max:+ and <= ${max}}." >&2
    exit 1
  fi
}

require_int --page-size "${PAGE_SIZE}" 1 1000
require_int --concurrency "${CONCURRENCY}" 1 20
[[ -z "${OVERLAP}" ]] || require_int --overlap "${OVERLAP}" 0
[[ -z "${BACKFILL_FLOOR}" ]] || require_int --backfill-floor "${BACKFILL_FLOOR}" 0
[[ -z "${BACKFILL_PAGES_PER_FILE}" ]] || require_int --backfill-pages-per-file "${BACKFILL_PAGES_PER_FILE}" 1
if [[ ! "${IMAGE_TAG}" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
  echo "Invalid --tag '${IMAGE_TAG}': expected a Docker tag ([A-Za-z0-9_.-], max 128 chars)." >&2
  exit 1
fi
if [[ -n "${OVERLAP}" ]]; then
  echo "Warning: --overlap is deprecated and ignored; it is not passed to the app." >&2
fi

case "${MODE}" in
  sync|canister|backfill|incremental|full) ;;
  *)
    echo "Invalid --mode '${MODE}'. Expected one of: sync, canister, backfill, incremental, full." >&2
    exit 1
    ;;
esac

require_cmd aws
require_cmd docker
require_cmd terraform
require_cmd node

if [[ ! -d "${TF_DIR}" ]]; then
  echo "Terraform directory not found: ${TF_DIR}" >&2
  exit 1
fi

tf_output_raw() {
  terraform -chdir="${TF_DIR}" output -raw "$1"
}

tf_output_json() {
  terraform -chdir="${TF_DIR}" output -json "$1"
}

AWS_REGION="$(tf_output_raw aws_region)"
ECR_REPO_URL="$(tf_output_raw ecr_repository_url)"
ECS_CLUSTER_NAME="$(tf_output_raw ecs_cluster_name)"
TASK_DEFINITION_ARN="$(tf_output_raw task_definition_arn)"
TASK_SECURITY_GROUP_ID="$(tf_output_raw task_security_group_id)"
ASSIGN_PUBLIC_IP_RAW="$(tf_output_raw assign_public_ip)"
SUBNET_IDS_JSON="$(tf_output_json effective_subnet_ids)"

if [[ "${ASSIGN_PUBLIC_IP_RAW}" == "true" ]]; then
  ASSIGN_PUBLIC_IP="ENABLED"
else
  ASSIGN_PUBLIC_IP="DISABLED"
fi

NETWORK_CONFIGURATION="$(
  SUBNET_IDS_JSON="${SUBNET_IDS_JSON}" \
  TASK_SECURITY_GROUP_ID="${TASK_SECURITY_GROUP_ID}" \
  ASSIGN_PUBLIC_IP="${ASSIGN_PUBLIC_IP}" \
  node --input-type=module -e '
    const subnets = JSON.parse(process.env.SUBNET_IDS_JSON);
    const sg = process.env.TASK_SECURITY_GROUP_ID;
    const assign = process.env.ASSIGN_PUBLIC_IP;
    const quoted = (values) => values.map((value) => `"${value}"`).join(",");
    process.stdout.write(`awsvpcConfiguration={subnets=[${quoted(subnets)}],securityGroups=["${sg}"],assignPublicIp=${assign}}`);
  '
)"

IMAGE_URI="${ECR_REPO_URL}:${IMAGE_TAG}"
LOCAL_IMAGE_TAG="icp-swap-elt-ingestion:${IMAGE_TAG}"

echo "AWS region: ${AWS_REGION}"
echo "ECR image: ${IMAGE_URI}"
echo "ECS cluster: ${ECS_CLUSTER_NAME}"
echo "Task definition: ${TASK_DEFINITION_ARN}"

if (( DO_BUILD || DO_PUSH )); then
  aws ecr get-login-password --region "${AWS_REGION}" \
    | docker login --username AWS --password-stdin "${ECR_REPO_URL}"
fi

if (( DO_BUILD )); then
  docker build --platform "${PLATFORM}" -f "${APP_DIR}/Dockerfile" -t "${LOCAL_IMAGE_TAG}" "${APP_DIR}"
fi

if (( DO_PUSH )); then
  docker tag "${LOCAL_IMAGE_TAG}" "${IMAGE_URI}"
  docker push "${IMAGE_URI}"
fi

if (( ! DO_RUN )); then
  exit 0
fi

OVERRIDES_JSON="$(
  MODE="${MODE}" \
  PAGE_SIZE="${PAGE_SIZE}" \
  CONCURRENCY="${CONCURRENCY}" \
  BACKFILL_FLOOR="${BACKFILL_FLOOR}" \
  BACKFILL_PAGES_PER_FILE="${BACKFILL_PAGES_PER_FILE}" \
  node --input-type=module -e '
    const mode = process.env.MODE;
    const command = ["node", "dist/index.js", "--mode", mode];
    // Canister archive (one-time) uses page-size/concurrency; REST modes
    // (sync/backfill/incremental) rely on their own configured defaults.
    if (mode === "canister" || mode === "full") {
      command.push("--page-size", process.env.PAGE_SIZE, "--concurrency", process.env.CONCURRENCY);
    }
    // These are only meaningful where backfill runs (backfill, or sync before backfill completes).
    if (process.env.BACKFILL_FLOOR && (mode === "backfill" || mode === "sync")) {
      command.push("--backfill-floor", process.env.BACKFILL_FLOOR);
    }
    if (process.env.BACKFILL_PAGES_PER_FILE && (mode === "backfill" || mode === "sync")) {
      command.push("--backfill-pages-per-file", process.env.BACKFILL_PAGES_PER_FILE);
    }
    process.stdout.write(JSON.stringify({
      containerOverrides: [{ name: "ingestion", command }]
    }));
  '
)"

if [[ "${IMAGE_TAG}" != "latest" ]]; then
  echo "Warning: task definition still references ':latest' unless Terraform was updated separately." >&2
else
  echo "Warning: image tag 'latest' is mutable; the pushed image replaces whatever 'latest' pointed to. Use a unique --tag for traceable deploys." >&2
fi

RUN_TASK_OUTPUT="$(aws ecs run-task \
  --region "${AWS_REGION}" \
  --cluster "${ECS_CLUSTER_NAME}" \
  --launch-type FARGATE \
  --task-definition "${TASK_DEFINITION_ARN}" \
  --network-configuration "${NETWORK_CONFIGURATION}" \
  --overrides "${OVERRIDES_JSON}")"
echo "${RUN_TASK_OUTPUT}"

# run-task exits 0 even when ECS could not place the task; the reason is in `failures`.
RUN_TASK_OUTPUT="${RUN_TASK_OUTPUT}" node --input-type=module -e '
  const out = JSON.parse(process.env.RUN_TASK_OUTPUT || "{}");
  const failures = out.failures ?? [];
  const started = out.tasks ?? [];
  if (failures.length > 0 || started.length === 0) {
    console.error("ECS run-task did not start a task: " + JSON.stringify(failures.length ? failures : "no tasks returned"));
    process.exit(1);
  }
'
