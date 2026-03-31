#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
API_BASE="${API_BASE:-http://127.0.0.1:18080}"
DEFAULT_DATASET_PATH="${REPO_ROOT}/apps/control-plane/dev/testdata/issues_taxonomy.csv"
if [[ -f /workspace/apps/control-plane/dev/testdata/issues_taxonomy.csv ]]; then
  DEFAULT_DATASET_PATH="/workspace/apps/control-plane/dev/testdata/issues_taxonomy.csv"
fi
DATASET_NAME="${DATASET_NAME:-${DEFAULT_DATASET_PATH}}"
GOAL="${GOAL:-결제 로그인 배송 문의를 분류체계로 정리해줘}"

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

project_json="$(post_json POST /projects '{"name":"taxonomy-smoke","description":"issue taxonomy smoke"}')"
project_id="$(printf '%s' "$project_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"

dataset_json="$(post_json POST "/projects/${project_id}/datasets" '{"name":"issues-taxonomy","data_type":"unstructured"}')"
dataset_id="$(printf '%s' "$dataset_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"

upload_metadata="$(python3 - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
    "sample_n": 4,
}, ensure_ascii=False))
PY
)"
version_json="$(curl -sS -X POST "${API_BASE}/projects/${project_id}/datasets/${dataset_id}/uploads" \
  -F "file=@${DATASET_NAME}" \
  -F 'data_type=unstructured' \
  -F "metadata=${upload_metadata}")"
version_id="$(printf '%s' "$version_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_version_id"])')"

prepare_payload="$(python3 - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
}, ensure_ascii=False))
PY
)"
prepare_json="$(post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/prepare" "$prepare_payload")"
printf '%s\n' "$prepare_json"

analysis_payload="$(DATASET_VERSION_ID="$version_id" GOAL="$GOAL" python3 - <<'PY'
import json
import os

plan = {
    "steps": [
        {
            "skill_name": "document_filter",
            "inputs": {
                "text_column": "text",
                "query": os.environ["GOAL"],
                "sample_n": 10,
            },
        },
        {
            "skill_name": "dictionary_tagging",
            "inputs": {
                "text_column": "text",
                "sample_n": 2,
                "top_n": 3,
                "max_tags_per_document": 3,
            },
        },
        {
            "skill_name": "issue_taxonomy_summary",
            "inputs": {
                "text_column": "text",
                "sample_n": 2,
                "top_n": 3,
                "max_tags_per_document": 3,
            },
        },
        {
            "skill_name": "issue_evidence_summary",
            "inputs": {
                "text_column": "text",
                "query": os.environ["GOAL"],
                "sample_n": 2,
            },
        },
    ],
    "notes": "taxonomy smoke requested plan",
}

print(json.dumps({
    "dataset_version_id": os.environ["DATASET_VERSION_ID"],
    "data_type": "unstructured",
    "goal": os.environ["GOAL"],
    "requested_plan": plan,
}, ensure_ascii=False))
PY
)"
analysis_json="$(post_json POST "/projects/${project_id}/analysis_requests" "$analysis_payload")"
printf '%s\n' "$analysis_json"
plan_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["plan"]["plan_id"])')"

execution_json="$(post_json POST "/projects/${project_id}/plans/${plan_id}/execute")"
execution_id="$(printf '%s' "$execution_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])')"

for _ in $(seq 1 30); do
  current="$(post_json GET "/projects/${project_id}/executions/${execution_id}")"
  status="$(printf '%s' "$current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [[ "$status" == "completed" || "$status" == "failed" ]]; then
    printf '%s\n' "$current"
    break
  fi
  sleep 1
done

result_json="$(post_json GET "/projects/${project_id}/executions/${execution_id}/result")"
printf '%s\n' "$result_json"
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); artifacts=data["artifacts"]; tagging=[json.loads(value) for key,value in artifacts.items() if key.endswith(":dictionary_tagging")]; taxonomy=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_taxonomy_summary")]; evidence=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_evidence_summary")]; assert tagging, "missing dictionary_tagging artifact"; assert taxonomy, "missing issue_taxonomy_summary artifact"; assert evidence, "missing issue_evidence_summary artifact"; assert tagging[0]["summary"]["tagged_row_count"] == 4, tagging[0]["summary"]; assert taxonomy[0]["summary"]["dominant_taxonomy"] == "payment_billing", taxonomy[0]["summary"]; assert taxonomy[0]["summary"]["dominant_taxonomy_count"] == 2, taxonomy[0]["summary"]; assert taxonomy[0]["taxonomy_breakdown"][0]["taxonomy_id"] == "payment_billing", taxonomy[0]["taxonomy_breakdown"]; print(json.dumps({"artifact_keys": sorted(artifacts), "dominant_taxonomy": taxonomy[0]["summary"]["dominant_taxonomy"], "dominant_taxonomy_count": taxonomy[0]["summary"]["dominant_taxonomy_count"], "taxonomy_count": taxonomy[0]["summary"]["taxonomy_count"]}, ensure_ascii=False))'
printf '\n'
