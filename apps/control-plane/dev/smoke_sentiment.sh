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
GOAL="${GOAL:-긍정 부정 감성 분포를 보여줘}"

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

project_json="$(post_json POST /projects '{"name":"sentiment-smoke","description":"sentiment summary smoke"}')"
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

prepare_payload="$(python3 - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
}, ensure_ascii=False))
PY
)"
prepare_json="$(post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/prepare" "$prepare_payload")"
printf '%s\n' "$prepare_json"

sentiment_payload="$(python3 - <<'PY'
import json
print(json.dumps({
    "text_column": "normalized_text",
}, ensure_ascii=False))
PY
)"
sentiment_json="$(post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/sentiment" "$sentiment_payload")"
printf '%s\n' "$sentiment_json"

analysis_payload="$(DATASET_VERSION_ID="$version_id" GOAL="$GOAL" python3 - <<'PY'
import json
import os
print(json.dumps({
    "dataset_version_id": os.environ["DATASET_VERSION_ID"],
    "data_type": "unstructured",
    "goal": os.environ["GOAL"],
}, ensure_ascii=False))
PY
)"
analysis_json="$(post_json POST "/projects/${project_id}/analysis_requests" "$analysis_payload")"
printf '%s\n' "$analysis_json"
plan_id="$(printf '%s' "$analysis_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["plan"]["plan_id"])')"
printf '%s' "$analysis_json" | python3 -c 'import json,sys; steps=[step["skill_name"] for step in json.load(sys.stdin)["plan"]["plan"]["steps"]]; expected=["document_filter","document_sample","issue_sentiment_summary","issue_evidence_summary"]; assert steps == expected, steps; print(json.dumps({"plan_steps": steps}, ensure_ascii=False))'
printf '\n'

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
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); artifacts=data["artifacts"]; sentiment=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_sentiment_summary")]; evidence=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_evidence_summary")]; assert sentiment, "missing issue_sentiment_summary artifact"; assert evidence, "missing issue_evidence_summary artifact"; assert sentiment[0]["summary"]["document_count"] >= 1, sentiment[0]; print(json.dumps({"artifact_keys": sorted(artifacts), "dominant_label": sentiment[0]["summary"].get("dominant_label"), "negative_count": sentiment[0]["summary"].get("negative_count"), "positive_count": sentiment[0]["summary"].get("positive_count")}, ensure_ascii=False))'
printf '\n'
