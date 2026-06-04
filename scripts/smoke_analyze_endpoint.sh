#!/usr/bin/env bash
# silverone 6단계 endpoint smoke — compose/dev 환경에서 `/tasks/plan_v2`,
# `/tasks/analyze_v2` (direct plan / user_question / ambiguous fail) 4 case를
# 차례로 호출해 라우팅/배포/환경 + LLM planner 흐름을 검증한다.
#
# 전제:
# - compose dev 환경이 떠 있고 python-ai-worker가 http://127.0.0.1:18090에서 listen
# - host의 ./data/ 가 컨테이너 /workspace/data/ 로 마운트 (compose.dev.yml 기본)
# - .env (또는 환경)에 ANTHROPIC_API_KEY 설정
#
# usage:
#   ./scripts/smoke_analyze_endpoint.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKER_URL="${PYTHON_AI_WORKER_URL:-http://127.0.0.1:18090}"
HOST_FIXTURE_SRC="${REPO_ROOT}/workers/python-ai/tests/fixtures/plan_v2_smoke"
HOST_FIXTURE_DST="${REPO_ROOT}/data/plan_v2_smoke"
CONTAINER_PATH="/workspace/data/plan_v2_smoke"
USER_QUESTION="작년과 올해의 aspect 증감수치 계산해줘"

green() { printf "\033[32m%s\033[0m\n" "$*"; }
red()   { printf "\033[31m%s\033[0m\n" "$*"; }

require_jq() {
  if ! command -v jq >/dev/null 2>&1; then
    red "jq required (brew install jq)"; exit 1
  fi
}

mirror_fixture() {
  mkdir -p "${HOST_FIXTURE_DST}"
  cp -f \
    "${HOST_FIXTURE_SRC}/cleaned.parquet" \
    "${HOST_FIXTURE_SRC}/clause_label.jsonl" \
    "${HOST_FIXTURE_SRC}/doc_genuineness.jsonl" \
    "${HOST_FIXTURE_SRC}/aspect_delta_plan.json" \
    "${HOST_FIXTURE_DST}/"
}

post_json() {
  # $1 = path, $2 = body json
  curl -sS --fail-with-body -X POST "${WORKER_URL}$1" \
    -H 'Content-Type: application/json' \
    -d "$2"
}


require_jq
mirror_fixture

echo "==> 0. /health"
curl -sS "${WORKER_URL}/health" | jq -c '{status, anthropic_model}'

echo
echo "==> 1. /tasks/plan_v2 (user_question -> plan only)"
PLAN_PAYLOAD=$(jq -n --arg q "${USER_QUESTION}" '{dataset_version_id:"smoke-endpoint", user_question:$q}')
PLAN_RESP="$(post_json /tasks/plan_v2 "${PLAN_PAYLOAD}")"
echo "${PLAN_RESP}" | jq '{plan_version, step_count: (.plan.steps|length), planner_attempts: (.planner.attempts|length), usage: .planner.usage}'

# taxonomy-driven config Phase 4 (silverone 2026-05-27) — sidecar metadata를
# 읽어 worker payload에 clause_label_metadata로 inject. fixture builder가
# clause_label_summary.json을 생성해 taxonomy_id/hash를 sidecar로 노출한다.
# sidecar가 없으면 legacy_missing 분기로 떨어진다 (옛 호환).
CLAUSE_LABEL_META_FILE="${HOST_FIXTURE_SRC}/clause_label_summary.json"
if [[ -f "${CLAUSE_LABEL_META_FILE}" ]]; then
  CLAUSE_LABEL_META=$(jq -c '{taxonomy_id, taxonomy_hash}' "${CLAUSE_LABEL_META_FILE}")
else
  CLAUSE_LABEL_META="null"
fi

echo
echo "==> 2. /tasks/analyze_v2 (direct plan mode — committed aspect_delta_plan.json)"
DIRECT_PAYLOAD=$(jq -n \
  --arg vid "smoke-endpoint" \
  --slurpfile plan "${HOST_FIXTURE_SRC}/aspect_delta_plan.json" \
  --arg docs   "${CONTAINER_PATH}/cleaned.parquet" \
  --arg cls    "${CONTAINER_PATH}/clause_label.jsonl" \
  --arg gen    "${CONTAINER_PATH}/doc_genuineness.jsonl" \
  --argjson meta "${CLAUSE_LABEL_META}" \
  '{dataset_version_id:$vid, plan:$plan[0], artifact_paths:{docs:$docs, clauses:$cls, genuineness:$gen}} + (if $meta == null then {} else {clause_label_metadata:$meta} end)')
DIRECT_RESP="$(post_json /tasks/analyze_v2 "${DIRECT_PAYLOAD}")"
echo "${DIRECT_RESP}" | jq '{plan_version, step_count: (.steps|length), present_row_count: .present.row_count, rows: .present.rows, taxonomy_check_status: .taxonomy_check.status}'

echo
echo "==> 3. /tasks/analyze_v2 (user_question mode — planner LLM + executor)"
UQ_PAYLOAD=$(jq -n \
  --arg vid "smoke-endpoint" \
  --arg q   "${USER_QUESTION}" \
  --arg docs "${CONTAINER_PATH}/cleaned.parquet" \
  --arg cls  "${CONTAINER_PATH}/clause_label.jsonl" \
  --arg gen  "${CONTAINER_PATH}/doc_genuineness.jsonl" \
  --argjson meta "${CLAUSE_LABEL_META}" \
  '{dataset_version_id:$vid, user_question:$q, artifact_paths:{docs:$docs, clauses:$cls, genuineness:$gen}} + (if $meta == null then {} else {clause_label_metadata:$meta} end)')
UQ_RESP="$(post_json /tasks/analyze_v2 "${UQ_PAYLOAD}")"
echo "${UQ_RESP}" | jq '{plan_version, step_count: (.steps|length), present_row_count: .present.row_count, planner_attempts: (.planner.attempts|length), rows: .present.rows, taxonomy_check_status: .taxonomy_check.status}'

echo
echo "==> 4. /tasks/analyze_v2 (ambiguous: plan + user_question — must fail)"
AMBIG_PAYLOAD=$(jq -n \
  --arg vid "smoke-endpoint" \
  --arg q "${USER_QUESTION}" \
  --slurpfile plan "${HOST_FIXTURE_SRC}/aspect_delta_plan.json" \
  '{dataset_version_id:$vid, plan:$plan[0], user_question:$q}')
AMBIG_TMP="$(mktemp)"
AMBIG_STATUS="$(curl -sS -o "${AMBIG_TMP}" -w '%{http_code}' -X POST "${WORKER_URL}/tasks/analyze_v2" \
  -H 'Content-Type: application/json' -d "${AMBIG_PAYLOAD}")"
AMBIG_BODY="$(cat "${AMBIG_TMP}")"
rm -f "${AMBIG_TMP}"
if [[ "${AMBIG_STATUS}" == "400" || "${AMBIG_STATUS}" == "500" ]]; then
  green "ambiguous request rejected (HTTP ${AMBIG_STATUS})"
  echo "${AMBIG_BODY}" | jq -c '.' 2>/dev/null || echo "${AMBIG_BODY}"
else
  red "ambiguous request unexpectedly accepted (HTTP ${AMBIG_STATUS})"
  echo "${AMBIG_BODY}"
  exit 1
fi

echo
green "endpoint smoke PASS — 4 cases verified"
