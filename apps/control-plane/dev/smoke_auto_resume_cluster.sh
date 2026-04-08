#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
API_BASE="${API_BASE:-http://127.0.0.1:18080}"
DEFAULT_DATASET_PATH="${REPO_ROOT}/apps/control-plane/dev/testdata/issues_cluster.csv"
if [[ -f /workspace/apps/control-plane/dev/testdata/issues_cluster.csv ]]; then
  DEFAULT_DATASET_PATH="/workspace/apps/control-plane/dev/testdata/issues_cluster.csv"
fi
DATASET_NAME="${DATASET_NAME:-${DEFAULT_DATASET_PATH}}"
GOAL="${GOAL:-결제 로그인 배송 관련 VOC를 군집으로 묶어줘}"
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

project_json="$(post_json POST /projects '{"name":"auto-resume-cluster-smoke","description":"auto resume cluster smoke"}')"
project_id="$(printf '%s' "$project_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"

dataset_json="$(post_json POST "/projects/${project_id}/datasets" '{"name":"issues-cluster","data_type":"unstructured"}')"
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
  echo "auto resume cluster smoke timed out while waiting for prepare completion" >&2
  exit 1
fi

analysis_payload="$(DATASET_VERSION_ID="$version_id" GOAL="$GOAL" python3 - <<'PY'
import json
import os

plan = {
    "steps": [
        {
            "skill_name": "embedding_cluster",
            "inputs": {
                "cluster_similarity_threshold": 0.2,
                "sample_n": 2,
                "top_n": 3,
            },
        },
        {
            "skill_name": "cluster_label_candidates",
            "inputs": {
                "sample_n": 2,
                "top_n": 3,
            },
        },
        {
            "skill_name": "issue_cluster_summary",
            "inputs": {
                "text_column": "text",
                "sample_n": 2,
                "top_n": 3,
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
    "notes": "auto resume cluster smoke requested plan",
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

cluster_jobs_json=""
for _ in $(seq 1 "$BUILD_WAIT_SECONDS"); do
  current="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/build_jobs")"
  cluster_status="$(printf '%s' "$current" | python3 -c 'import json,sys; items=json.load(sys.stdin)["items"]; jobs=[item for item in items if item["build_type"]=="cluster"]; print(jobs[0]["status"] if jobs else "")')"
  if [[ "$cluster_status" == "completed" ]]; then
    cluster_jobs_json="$current"
    break
  fi
  sleep 1
done

if [[ -z "$cluster_jobs_json" ]]; then
  current="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/build_jobs")"
  printf '%s\n' "$current"
  echo "auto resume cluster smoke timed out while waiting for cluster completion" >&2
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
  echo "auto resume cluster smoke timed out while waiting for execution completion" >&2
  exit 1
fi

result_json="$(post_json GET "/projects/${project_id}/executions/${execution_id}/result")"
printf '%s\n' "$result_json"
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data["result_v1"]["status"] == "completed", data["result_v1"]; artifacts=data["artifacts"]; clusters=[json.loads(value) for key,value in artifacts.items() if key.endswith(":embedding_cluster")]; summaries=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_cluster_summary")]; assert clusters, "missing embedding_cluster artifact"; assert summaries, "missing issue_cluster_summary artifact"; assert clusters[0]["summary"]["cluster_similarity_threshold"] == 0.2, clusters[0]["summary"]; assert clusters[0]["summary"]["top_n"] == 3, clusters[0]["summary"]; assert clusters[0]["summary"]["sample_n"] == 2, clusters[0]["summary"]; assert clusters[0].get("cluster_membership_ref"), clusters[0]; print(json.dumps({"artifact_keys": sorted(artifacts), "cluster_count": clusters[0]["summary"]["cluster_count"], "cluster_similarity_threshold": clusters[0]["summary"]["cluster_similarity_threshold"], "cluster_membership_ref": clusters[0].get("cluster_membership_ref"), "dominant_cluster_label": summaries[0]["summary"].get("dominant_cluster_label")}, ensure_ascii=False))'
printf '\n'

printf '%s' "$final_execution_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); events=data["events"]; waiting=[event for event in events if event["event_type"]=="WORKFLOW_WAITING"]; resumed=[event for event in events if event["event_type"]=="RESUME_ENQUEUED"]; assert waiting, events; assert resumed, events; assert waiting[-1]["payload"]["waiting_for"] == "embeddings", waiting[-1]; assert resumed[-1]["payload"]["triggered_by"] == "dataset_build_job", resumed[-1]; print(json.dumps({"waiting_for": waiting[-1]["payload"]["waiting_for"], "resume_triggered_by": resumed[-1]["payload"]["triggered_by"], "execution_status": data["status"]}, ensure_ascii=False))'
printf '\n'

printf '%s' "$cluster_jobs_json" | python3 -c 'import json,sys; items=json.load(sys.stdin)["items"]; prepare=[item for item in items if item["build_type"]=="prepare"]; embedding=[item for item in items if item["build_type"]=="embedding"]; cluster=[item for item in items if item["build_type"]=="cluster"]; assert prepare and embedding and cluster, items; assert prepare[0]["status"] == "completed", prepare[0]; assert embedding[0]["status"] == "completed", embedding[0]; assert cluster[0]["status"] == "completed", cluster[0]; assert cluster[0].get("resumed_execution_count") == 1, cluster[0]; print(json.dumps({"prepare_job_status": prepare[0]["status"], "embedding_job_status": embedding[0]["status"], "cluster_job_status": cluster[0]["status"], "cluster_resumed_execution_count": cluster[0].get("resumed_execution_count")}, ensure_ascii=False))'
printf '\n'
