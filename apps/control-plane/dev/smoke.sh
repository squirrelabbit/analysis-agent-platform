#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
API_BASE="${API_BASE:-http://127.0.0.1:18080}"
DEFAULT_DATASET_PATH="${REPO_ROOT}/data/issues.csv"
if [[ -f /workspace/data/issues.csv ]]; then
  DEFAULT_DATASET_PATH="/workspace/data/issues.csv"
fi
DATASET_NAME="${DATASET_NAME:-${DEFAULT_DATASET_PATH}}"
DATASET_VERSION_ID="${DATASET_VERSION_ID:-issues-v1}"
GOAL="${GOAL:-VOC 이슈를 요약해줘}"

post_json() {
  local method="$1"
  local path="$2"
  local payload="${3:-}"
  if [[ -n "$payload" ]]; then
    curl -sS -X "$method" "${API_BASE}${path}" -H 'Content-Type: application/json' -d "$payload"
  else
    curl -sS -X "$method" "${API_BASE}${path}"
  fi
}

project_json="$(post_json POST /projects '{"name":"dev-stack-smoke","description":"compose smoke"}')"
project_id="$(printf '%s' "$project_json" | python -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"

dataset_json="$(post_json POST "/projects/${project_id}/datasets" '{"name":"issues","data_type":"unstructured"}')"
dataset_id="$(printf '%s' "$dataset_json" | python -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"

upload_metadata="$(python - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
}, ensure_ascii=False))
PY
)"
version_json="$(curl -sS -X POST "${API_BASE}/projects/${project_id}/datasets/${dataset_id}/uploads" \
  -F "file=@${DATASET_NAME}" \
  -F 'data_type=unstructured' \
  -F "metadata=${upload_metadata}")"
version_id="$(printf '%s' "$version_json" | python -c 'import json,sys; print(json.load(sys.stdin)["dataset_version_id"])')"

prepare_payload="$(python - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
}, ensure_ascii=False))
PY
)"
prepare_json="$(post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/prepare" "$prepare_payload")"
printf '%s\n' "$prepare_json"

analysis_payload="$(DATASET_VERSION_ID="$version_id" GOAL="$GOAL" python - <<'PY'
import json
import os
print(json.dumps({
    "dataset_version_id": os.environ["DATASET_VERSION_ID"],
    "data_type": "unstructured",
    "goal": os.environ["GOAL"],
}, ensure_ascii=False))
PY
)"

plan_json="$(DATASET_VERSION_ID="$version_id" GOAL="$GOAL" post_json POST "/projects/${project_id}/analysis_requests" "$analysis_payload")"
printf '%s\n' "$plan_json"
printf '%s' "$plan_json" | python3 -c 'import json,sys; steps=[step["skill_name"] for step in json.load(sys.stdin)["plan"]["plan"]["steps"]]; expected=["document_filter","term_frequency","document_sample","unstructured_issue_summary","issue_evidence_summary"]; assert steps == expected, steps; print(json.dumps({"plan_steps": steps}, ensure_ascii=False))'
printf '\n'
plan_id="$(printf '%s' "$plan_json" | python -c 'import json,sys; print(json.load(sys.stdin)["plan"]["plan_id"])')"

execution_json="$(post_json POST "/projects/${project_id}/plans/${plan_id}/execute")"
execution_id="$(printf '%s' "$execution_json" | python -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])')"

for _ in $(seq 1 30); do
  current="$(post_json GET "/projects/${project_id}/executions/${execution_id}")"
  status="$(printf '%s' "$current" | python -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [[ "$status" == "completed" || "$status" == "failed" ]]; then
    printf '%s\n' "$current"
    break
  fi
  sleep 1
done

result_json="$(post_json GET "/projects/${project_id}/executions/${execution_id}/result")"
printf '%s\n' "$result_json"
printf '%s' "$result_json" | python -c 'import json,sys; data=json.load(sys.stdin); artifacts=data["artifacts"]; assert any(key.endswith(":unstructured_issue_summary") for key in artifacts), "missing unstructured_issue_summary artifact"; assert any(key.endswith(":issue_evidence_summary") for key in artifacts), "missing issue_evidence_summary artifact"; print(json.dumps({"artifact_keys": sorted(artifacts)}, ensure_ascii=False))'
printf '\n'
