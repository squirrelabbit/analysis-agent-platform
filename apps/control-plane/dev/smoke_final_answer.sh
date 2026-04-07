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
GOAL="${GOAL:-결제 오류 관련 근거를 찾아서 요약해줘}"
EMBEDDING_MODEL="${EMBEDDING_MODEL:-intfloat/multilingual-e5-small}"
EXECUTION_WAIT_SECONDS="${EXECUTION_WAIT_SECONDS:-120}"

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

project_json="$(post_json POST /projects '{"name":"final-answer-smoke","description":"final answer smoke"}')"
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

prepare_json="$(post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/prepare" '{"text_column":"text"}')"
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
  echo "final answer smoke timed out while waiting for execution completion" >&2
  exit 1
fi

result_json="$(post_json GET "/projects/${project_id}/executions/${execution_id}/result")"
printf '%s\n' "$result_json"
printf '%s' "$result_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); result_v1=data["result_v1"]; final_answer=data.get("final_answer") or {}; diagnostics=data.get("diagnostics") or {}; assert result_v1["status"] == "completed", result_v1; assert final_answer, data; assert final_answer["schema_version"] == "execution-final-answer-v1", final_answer; assert final_answer["status"] == "ready", final_answer; assert final_answer["generation_mode"] in ("llm", "fallback"), final_answer; assert str(final_answer.get("answer_text") or "").strip(), final_answer; assert diagnostics.get("final_answer_status") == "ready", diagnostics; evidence=final_answer.get("evidence") or []; assert evidence, final_answer; print(json.dumps({"result_status": result_v1["status"], "final_answer_status": final_answer["status"], "generation_mode": final_answer["generation_mode"], "headline": final_answer.get("headline"), "evidence_count": len(evidence), "prompt_version": final_answer.get("prompt_version")}, ensure_ascii=False))'
printf '\n'

FINAL_EXECUTION_JSON="$final_execution_json" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["FINAL_EXECUTION_JSON"])
events = data["events"]
generated = [event for event in events if event["event_type"] == "FINAL_ANSWER_GENERATED"]
failed = [event for event in events if event["event_type"] == "FINAL_ANSWER_FAILED"]
assert generated, events
assert not failed, failed
payload = generated[-1].get("payload") or {}
print(json.dumps({
    "final_answer_event_type": generated[-1]["event_type"],
    "generation_mode": payload.get("generation_mode"),
    "prompt_version": payload.get("prompt_version"),
}, ensure_ascii=False))
PY
printf '\n'
