#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
API_BASE="${API_BASE:-http://127.0.0.1:18080}"
DEFAULT_DATASET_PATH="${REPO_ROOT}/data/issues_sentiment.csv"
if [[ -f /workspace/data/issues_sentiment.csv ]]; then
  DEFAULT_DATASET_PATH="/workspace/data/issues_sentiment.csv"
fi
DATASET_NAME="${DATASET_NAME:-${DEFAULT_DATASET_PATH}}"
GOAL="${GOAL:-결제 오류와 관련된 반응은 어때?}"
BUILD_WAIT_SECONDS="${BUILD_WAIT_SECONDS:-600}"
EXECUTION_WAIT_SECONDS="${EXECUTION_WAIT_SECONDS:-600}"

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

project_json="$(post_json POST /projects '{"name":"auto-resume-sentiment-smoke","description":"auto resume sentiment smoke"}')"
project_id="$(printf '%s' "$project_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"

dataset_json="$(post_json POST "/projects/${project_id}/datasets" '{"name":"issues-sentiment","data_type":"unstructured"}')"
dataset_id="$(printf '%s' "$dataset_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"

upload_metadata="$(python3 - <<'PY'
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
version_id="$(printf '%s' "$version_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_version_id"])')"

prepare_jobs_json=""
for _ in $(seq 1 "$BUILD_WAIT_SECONDS"); do
  current="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/build_jobs")"
  prepare_status="$(printf '%s' "$current" | python3 -c 'import json,sys; items=json.load(sys.stdin)["items"]; prepare=[item for item in items if item["build_type"]=="prepare"]; print(prepare[0]["status"] if prepare else "")')"
  if [[ "$prepare_status" == "completed" ]]; then
    prepare_jobs_json="$current"
    break
  fi
  sleep 1
done

if [[ -z "$prepare_jobs_json" ]]; then
  current="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/build_jobs")"
  printf '%s\n' "$current"
  echo "auto resume sentiment smoke timed out while waiting for prepare completion" >&2
  exit 1
fi

analysis_payload="$(DATASET_VERSION_ID="$version_id" GOAL="$GOAL" python3 - <<'PY'
import json
import os

plan = {
    "steps": [
        {
            "skill_name": "garbage_filter",
            "inputs": {
                "text_column": "text",
                "sample_n": 5,
            },
        },
        {
            "skill_name": "issue_sentiment_summary",
            "inputs": {
                "text_column": "text",
                "sample_n": 3,
            },
        },
        {
            "skill_name": "issue_evidence_summary",
            "inputs": {
                "text_column": "text",
                "query": os.environ["GOAL"],
                "sample_n": 3,
            },
        },
    ],
    "notes": "auto resume sentiment smoke requested plan",
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
plan_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["plan"]["plan_id"])')"

execution_json="$(post_json POST "/projects/${project_id}/plans/${plan_id}/execute")"
execution_id="$(printf '%s' "$execution_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])')"

sentiment_jobs_json=""
for _ in $(seq 1 "$BUILD_WAIT_SECONDS"); do
  current="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/build_jobs")"
  sentiment_status="$(printf '%s' "$current" | python3 -c 'import json,sys; items=json.load(sys.stdin)["items"]; jobs=[item for item in items if item["build_type"]=="sentiment"]; print(jobs[0]["status"] if jobs else "")')"
  if [[ "$sentiment_status" == "completed" ]]; then
    sentiment_jobs_json="$current"
    break
  fi
  sleep 1
done

if [[ -z "$sentiment_jobs_json" ]]; then
  current="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/build_jobs")"
  printf '%s\n' "$current"
  echo "auto resume sentiment smoke timed out while waiting for sentiment completion" >&2
  exit 1
fi

final_execution_json=""
for _ in $(seq 1 "$EXECUTION_WAIT_SECONDS"); do
  current="$(post_json GET "/projects/${project_id}/executions/${execution_id}")"
  status="$(printf '%s' "$current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [[ "$status" == "completed" || "$status" == "failed" ]]; then
    final_execution_json="$current"
    break
  fi
  sleep 1
done

if [[ -z "$final_execution_json" ]]; then
  current="$(post_json GET "/projects/${project_id}/executions/${execution_id}")"
  printf '%s\n' "$current"
  echo "auto resume sentiment smoke timed out while waiting for execution completion" >&2
  exit 1
fi

result_json="$(post_json GET "/projects/${project_id}/executions/${execution_id}/result")"
printf '%s\n' "$result_json"
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data["result_v1"]["status"] == "completed", data["result_v1"]; artifacts=data["artifacts"]; sentiment=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_sentiment_summary")]; evidence=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_evidence_summary")]; assert sentiment, "missing issue_sentiment_summary artifact"; assert evidence, "missing issue_evidence_summary artifact"; print(json.dumps({"artifact_keys": sorted(artifacts), "result_status": data["result_v1"]["status"], "primary_skill_name": data["result_v1"].get("primary_skill_name")}, ensure_ascii=False))'
printf '\n'

FINAL_EXECUTION_JSON="$final_execution_json" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["FINAL_EXECUTION_JSON"])
events = data["events"]
waiting = [event for event in events if event["event_type"] == "WORKFLOW_WAITING"]
resumed = [event for event in events if event["event_type"] == "RESUME_ENQUEUED"]
payload = {"execution_status": data["status"]}

if waiting and resumed:
    assert waiting[-1]["payload"]["waiting_for"] == "sentiment_labels", waiting[-1]
    assert resumed[-1]["payload"]["triggered_by"] == "dataset_build_job", resumed[-1]
    payload["waiting_for"] = waiting[-1]["payload"]["waiting_for"]
    payload["resume_triggered_by"] = resumed[-1]["payload"]["triggered_by"]
    payload["resume_mode"] = "auto_resume"
else:
    payload["resume_mode"] = "build_completed_before_wait_check"

print(json.dumps(payload, ensure_ascii=False))
PY
printf '\n'

printf '%s' "$sentiment_jobs_json" | python3 -c 'import json,sys; items=json.load(sys.stdin)["items"]; prepare=[item for item in items if item["build_type"]=="prepare"]; sentiment=[item for item in items if item["build_type"]=="sentiment"]; assert prepare and sentiment, items; assert prepare[0]["status"] == "completed", prepare[0]; assert sentiment[0]["status"] == "completed", sentiment[0]; print(json.dumps({"prepare_job_status": prepare[0]["status"], "sentiment_job_status": sentiment[0]["status"], "sentiment_resumed_execution_count": sentiment[0].get("resumed_execution_count")}, ensure_ascii=False))'
printf '\n'
