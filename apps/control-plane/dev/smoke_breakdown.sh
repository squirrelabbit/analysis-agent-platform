#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
API_BASE="${API_BASE:-http://127.0.0.1:18080}"
DEFAULT_DATASET_PATH="${REPO_ROOT}/data/issues_compare.csv"
if [[ -f /workspace/data/issues_compare.csv ]]; then
  DEFAULT_DATASET_PATH="/workspace/data/issues_compare.csv"
fi
DATASET_NAME="${DATASET_NAME:-${DEFAULT_DATASET_PATH}}"
GOAL="${GOAL:-채널별 이슈를 분해해서 보여줘}"

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

project_json="$(post_json POST /projects '{"name":"breakdown-smoke","description":"issue breakdown smoke"}')"
project_id="$(printf '%s' "$project_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"

dataset_json="$(post_json POST "/projects/${project_id}/datasets" '{"name":"issues-breakdown","data_type":"unstructured"}')"
dataset_id="$(printf '%s' "$dataset_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"

version_payload="$(DATASET_NAME="$DATASET_NAME" python3 - <<'PY'
import json
import os
print(json.dumps({
    "storage_uri": os.environ["DATASET_NAME"],
    "data_type": "unstructured",
    "metadata": {
        "text_column": "text",
        "breakdown_column": "channel",
        "sample_n": 3,
    },
}, ensure_ascii=False))
PY
)"
version_json="$(post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions" "$version_payload")"
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

analysis_payload="$(DATASET_VERSION_ID="$version_id" GOAL="$GOAL" DATASET_NAME="$DATASET_NAME" python3 - <<'PY'
import json
import os

plan = {
    "steps": [
        {
            "skill_name": "issue_breakdown_summary",
            "dataset_name": os.environ["DATASET_NAME"],
            "inputs": {
                "text_column": "text",
                "dimension_column": "channel",
                "top_n": 3,
                "sample_n": 2,
            },
        },
        {
            "skill_name": "issue_evidence_summary",
            "dataset_name": os.environ["DATASET_NAME"],
            "inputs": {
                "text_column": "text",
                "query": os.environ["GOAL"],
                "sample_n": 2,
            },
        },
    ],
    "notes": "breakdown smoke requested plan",
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
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); artifacts=data["artifacts"]; breakdown=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_breakdown_summary")]; evidence=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_evidence_summary")]; assert breakdown, "missing issue_breakdown_summary artifact"; assert evidence, "missing issue_evidence_summary artifact"; summary=breakdown[0]["summary"]; assert summary["top_group"] == "app", summary; assert breakdown[0]["breakdown"][0]["count"] == 2, breakdown[0]["breakdown"]; print(json.dumps({"artifact_keys": sorted(artifacts), "top_group": summary["top_group"], "breakdown": breakdown[0]["breakdown"]}, ensure_ascii=False))'
printf '\n'
