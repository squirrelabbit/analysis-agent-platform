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
EXECUTION_WAIT_SECONDS="${EXECUTION_WAIT_SECONDS:-90}"

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

project_json="$(post_json POST /projects '{"name":"semantic-search-smoke","description":"semantic evidence smoke"}')"
project_id="$(printf '%s' "$project_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"

dataset_json="$(post_json POST "/projects/${project_id}/datasets" '{"name":"issues","data_type":"unstructured"}')"
dataset_id="$(printf '%s' "$dataset_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"

upload_metadata="$(python3 - <<'PY'
import json
print(json.dumps({
    "text_column": "text",
    "sample_n": 3,
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

embedding_payload="$(EMBEDDING_MODEL="$EMBEDDING_MODEL" python3 - <<'PY'
import json
import os
print(json.dumps({
    "text_column": "text",
    "embedding_model": os.environ["EMBEDDING_MODEL"],
}, ensure_ascii=False))
PY
)"
embedding_json="$(post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/embeddings" "$embedding_payload")"
printf '%s\n' "$embedding_json"
printf '%s' "$embedding_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); assert data.get("embedding_model") == "intfloat/multilingual-e5-small", data; metadata=data.get("metadata") or {}; assert str(metadata.get("embedding_vector_dim")) == "384", metadata; assert metadata.get("embedding_index_backend") == "pgvector", metadata; print(json.dumps({"embedding_model": data.get("embedding_model"), "embedding_vector_dim": metadata.get("embedding_vector_dim"), "embedding_index_backend": metadata.get("embedding_index_backend")}, ensure_ascii=False))'
printf '\n'

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
printf '%s' "$analysis_json" | python3 -c 'import json,sys; steps=[step["skill_name"] for step in json.load(sys.stdin)["plan"]["plan"]["steps"]]; assert steps == ["semantic_search", "issue_evidence_summary"], steps; print(json.dumps({"plan_steps": steps}, ensure_ascii=False))'
printf '\n'

execution_json="$(post_json POST "/projects/${project_id}/plans/${plan_id}/execute")"
execution_id="$(printf '%s' "$execution_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])')"

final_execution_json=""
for _ in $(seq 1 "$EXECUTION_WAIT_SECONDS"); do
  current="$(post_json GET "/projects/${project_id}/executions/${execution_id}")"
  status="$(printf '%s' "$current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [[ "$status" == "completed" || "$status" == "failed" ]]; then
    final_execution_json="$current"
    printf '%s\n' "$current"
    break
  fi
  sleep 1
done

if [[ -z "$final_execution_json" ]]; then
  current="$(post_json GET "/projects/${project_id}/executions/${execution_id}")"
  printf '%s\n' "$current"
  echo "semantic smoke timed out while waiting for execution completion" >&2
  exit 1
fi

result_json="$(post_json GET "/projects/${project_id}/executions/${execution_id}/result")"
printf '%s\n' "$result_json"
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); artifacts=data["artifacts"]; semantic=[json.loads(value) for key,value in artifacts.items() if key.endswith(":semantic_search")]; evidence=[json.loads(value) for key,value in artifacts.items() if key.endswith(":issue_evidence_summary")]; assert semantic, "missing semantic_search artifact"; assert evidence, "missing issue_evidence_summary artifact"; assert semantic[0]["matches"], "semantic_search returned no matches"; assert semantic[0]["retrieval_backend"] == "pgvector", semantic[0].get("retrieval_backend"); assert evidence[0]["selection_source"] == "semantic_search", evidence[0].get("selection_source"); print(json.dumps({"artifact_keys": sorted(artifacts), "semantic_top_text": semantic[0]["matches"][0]["text"], "retrieval_backend": semantic[0]["retrieval_backend"], "evidence_selection_source": evidence[0]["selection_source"]}, ensure_ascii=False))'
printf '\n'
