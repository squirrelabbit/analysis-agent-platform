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

project_json="$(post_json POST /projects '{"name":"cluster-smoke","description":"issue cluster smoke"}')"
project_id="$(printf '%s' "$project_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"

dataset_json="$(post_json POST "/projects/${project_id}/datasets" '{"name":"issues-cluster","data_type":"unstructured"}')"
dataset_id="$(printf '%s' "$dataset_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"

upload_metadata="$(python3 - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
    "sample_n": 6,
    "embedding_required": True,
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

embedding_payload="$(python3 - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
    "embedding_model": "token-overlap-v1",
}, ensure_ascii=False))
PY
)"
embedding_json="$(post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/embeddings" "$embedding_payload")"
printf '%s\n' "$embedding_json"

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
            "skill_name": "deduplicate_documents",
            "inputs": {
                "text_column": "text",
                "duplicate_threshold": 0.8,
                "sample_n": 2,
            },
        },
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
    "notes": "cluster smoke requested plan",
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
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); artifacts=data["artifacts"]; dedup=[json.loads(value) for key,value in artifacts.items() if key.endswith(":deduplicate_documents")]; clusters=[json.loads(value) for key,value in artifacts.items() if key.endswith(":embedding_cluster")]; labels=[json.loads(value) for key,value in artifacts.items() if key.endswith(":cluster_label_candidates")]; summaries=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_cluster_summary")]; evidence=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_evidence_summary")]; assert dedup, "missing deduplicate_documents artifact"; assert clusters, "missing embedding_cluster artifact"; assert labels, "missing cluster_label_candidates artifact"; assert summaries, "missing issue_cluster_summary artifact"; assert evidence, "missing issue_evidence_summary artifact"; assert dedup[0]["summary"]["canonical_row_count"] == 5, dedup[0]["summary"]; assert clusters[0]["summary"]["clustered_document_count"] == 5, clusters[0]["summary"]; assert clusters[0]["summary"]["cluster_count"] == 3, clusters[0]["summary"]; assert labels[0]["summary"]["cluster_count"] == 3, labels[0]["summary"]; assert summaries[0]["summary"]["dominant_cluster_count"] == 2, summaries[0]["summary"]; assert any("결제" in cluster["label"] for cluster in summaries[0]["clusters"]), summaries[0]["clusters"]; print(json.dumps({"artifact_keys": sorted(artifacts), "canonical_row_count": dedup[0]["summary"]["canonical_row_count"], "cluster_count": summaries[0]["summary"]["cluster_count"], "dominant_cluster_label": summaries[0]["summary"].get("dominant_cluster_label")}, ensure_ascii=False))'
printf '\n'
