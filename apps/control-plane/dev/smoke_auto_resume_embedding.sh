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
GOAL="${GOAL:-결제 오류 관련 근거를 찾아줘}"
EMBEDDING_MODEL="${EMBEDDING_MODEL:-intfloat/multilingual-e5-small}"
BUILD_WAIT_SECONDS="${BUILD_WAIT_SECONDS:-900}"
EXECUTION_WAIT_SECONDS="${EXECUTION_WAIT_SECONDS:-900}"

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

project_json="$(post_json POST /projects '{"name":"auto-resume-embedding-smoke","description":"auto resume embedding smoke"}')"
project_id="$(printf '%s' "$project_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"

dataset_json="$(post_json POST "/projects/${project_id}/datasets" '{"name":"issues","data_type":"unstructured"}')"
dataset_id="$(printf '%s' "$dataset_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"

upload_metadata="$(python3 - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
    "embedding_required": True,
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
  echo "auto resume embedding smoke timed out while waiting for prepare completion" >&2
  exit 1
fi

analysis_payload="$(DATASET_VERSION_ID="$version_id" GOAL="$GOAL" EMBEDDING_MODEL="$EMBEDDING_MODEL" python3 - <<'PY'
import json
import os

plan = {
    "steps": [
        {
            "skill_name": "semantic_search",
            "inputs": {
                "text_column": "text",
                "query": os.environ["GOAL"],
                "top_k": 5,
                "embedding_model": os.environ["EMBEDDING_MODEL"],
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
    "notes": "auto resume embedding smoke requested plan",
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

embedding_jobs_json=""
for _ in $(seq 1 "$BUILD_WAIT_SECONDS"); do
  current="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/build_jobs")"
  embedding_status="$(printf '%s' "$current" | python3 -c 'import json,sys; items=json.load(sys.stdin)["items"]; jobs=[item for item in items if item["build_type"]=="embedding"]; print(jobs[0]["status"] if jobs else "")')"
  if [[ "$embedding_status" == "completed" ]]; then
    embedding_jobs_json="$current"
    break
  fi
  sleep 1
done

if [[ -z "$embedding_jobs_json" ]]; then
  current="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/build_jobs")"
  printf '%s\n' "$current"
  echo "auto resume embedding smoke timed out while waiting for embedding completion" >&2
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
  echo "auto resume embedding smoke timed out while waiting for execution completion" >&2
  exit 1
fi

result_json="$(post_json GET "/projects/${project_id}/executions/${execution_id}/result")"
printf '%s\n' "$result_json"
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data["result_v1"]["status"] == "completed", data["result_v1"]; artifacts=data["artifacts"]; semantic=[json.loads(value) for key,value in artifacts.items() if key.endswith(":semantic_search")]; evidence=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_evidence_summary")]; assert semantic, "missing semantic_search artifact"; assert evidence, "missing issue_evidence_summary artifact"; assert semantic[0]["retrieval_backend"] == "pgvector", semantic[0]; assert evidence[0]["selection_source"] == "semantic_search", evidence[0]; print(json.dumps({"artifact_keys": sorted(artifacts), "retrieval_backend": semantic[0]["retrieval_backend"], "selection_source": evidence[0]["selection_source"]}, ensure_ascii=False))'
printf '\n'

printf '%s' "$final_execution_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); events=data["events"]; waiting=[event for event in events if event["event_type"]=="WORKFLOW_WAITING"]; resumed=[event for event in events if event["event_type"]=="RESUME_ENQUEUED"]; assert waiting, events; assert resumed, events; assert waiting[-1]["payload"]["waiting_for"] == "embeddings", waiting[-1]; assert resumed[-1]["payload"]["triggered_by"] == "dataset_build_job", resumed[-1]; print(json.dumps({"waiting_for": waiting[-1]["payload"]["waiting_for"], "resume_triggered_by": resumed[-1]["payload"]["triggered_by"], "execution_status": data["status"]}, ensure_ascii=False))'
printf '\n'

printf '%s' "$embedding_jobs_json" | python3 -c 'import json,sys; items=json.load(sys.stdin)["items"]; prepare=[item for item in items if item["build_type"]=="prepare"]; embedding=[item for item in items if item["build_type"]=="embedding"]; assert prepare and embedding, items; assert prepare[0]["status"] == "completed", prepare[0]; assert embedding[0]["status"] == "completed", embedding[0]; print(json.dumps({"prepare_job_status": prepare[0]["status"], "embedding_job_status": embedding[0]["status"], "embedding_resumed_execution_count": embedding[0].get("resumed_execution_count")}, ensure_ascii=False))'
printf '\n'
