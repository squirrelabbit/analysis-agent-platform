#!/usr/bin/env bash
# silverone 2026-05-26 — cost-2 A/B 측정 harness.
#
# planner system prompt의 ko 버전(`planner-v2-anthropic-v1`)과 en 버전
# (`planner-v2-anthropic-en-v1`)을 동일 5질의로 비교한다. cache 상태를 같은
# 출발점에서 비교하기 위해 두 variant를 *별도 dataset*에 대해 측정 — 같은
# dataset에 두 variant를 섞으면 cache key가 분리되지만 비교 순서에 따라 1차/2차
# 호출 분포가 어긋난다. 각 variant 5질의 모두 *같은 dataset*에서 연속 호출하므로
# 1차 호출은 cache_creation, 2~5 호출은 cache_read로 측정된다.
#
# 출력: JSONL (one line per query) — usage / retry / executor success / row_count
# / estimated cost.
#
# usage:
#   ./scripts/measure_planner_prompt_variant.sh                    # ko + en
#   ./scripts/measure_planner_prompt_variant.sh --variant ko
#   ./scripts/measure_planner_prompt_variant.sh --variant en
#   ./scripts/measure_planner_prompt_variant.sh --out result.jsonl

set -euo pipefail

VARIANT_FILTER="all"
OUT_PATH=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --variant) VARIANT_FILTER="$2"; shift 2 ;;
    --variant=*) VARIANT_FILTER="${1#*=}"; shift ;;
    --out) OUT_PATH="$2"; shift 2 ;;
    --out=*) OUT_PATH="${1#*=}"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
case "${VARIANT_FILTER}" in
  all|ko|en) ;;
  *) echo "--variant must be all|ko|en" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKER="${PYTHON_AI_WORKER_URL:-http://127.0.0.1:18090}"
PG_CONTAINER="analysis-support-platform-postgres-1"
HOST_FIXTURE_SRC="${REPO_ROOT}/workers/python-ai/tests/fixtures/plan_v2_smoke"
HOST_FIXTURE_DST="${REPO_ROOT}/data/plan_v2_smoke"
CONTAINER_FIXTURE="/workspace/data/plan_v2_smoke"
DOCS="${CONTAINER_FIXTURE}/cleaned.parquet"
CLAUSES="${CONTAINER_FIXTURE}/clause_label.jsonl"
GEN="${CONTAINER_FIXTURE}/doc_genuineness.jsonl"

# claude-sonnet 4.6 단가 (per 1M tokens) — estimated_cost 계산용.
# 운영 실제 단가는 변경될 수 있으므로 절대값이 아닌 *상대 비교* 용도.
PRICE_INPUT=3.0
PRICE_OUTPUT=15.0
PRICE_CACHE_CREATE=3.75   # input × 1.25
PRICE_CACHE_READ=0.30     # input × 0.10

# 5질의 — festival typical.
QUERIES=(
  "작년과 올해의 aspect 증감수치 계산해줘"
  "긍정 절을 보여줘"
  "음식 aspect 중 부정 평가 상위는?"
  "올해 분위기 관련 후기 중 부정 비율은?"
  "전체 aspect별 카운트 보여줘"
)

mkdir -p "${HOST_FIXTURE_DST}"
cp -f "${HOST_FIXTURE_SRC}/cleaned.parquet" "${HOST_FIXTURE_SRC}/clause_label.jsonl" "${HOST_FIXTURE_SRC}/doc_genuineness.jsonl" "${HOST_FIXTURE_DST}/"

emit() {
  local line="$1"
  if [[ -n "${OUT_PATH}" ]]; then
    printf '%s\n' "${line}" >>"${OUT_PATH}"
  fi
  printf '%s\n' "${line}"
}

run_variant() {
  local variant="$1"
  local prompt_version
  case "${variant}" in
    ko) prompt_version="planner-v2-anthropic-v1" ;;
    en) prompt_version="planner-v2-anthropic-en-v1" ;;
    *) echo "unknown variant: ${variant}" >&2; return 1 ;;
  esac

  local version_id
  version_id="measure-${variant}-$(date +%s)"

  local idx=0
  for q in "${QUERIES[@]}"; do
    idx=$((idx + 1))
    local body
    body=$(jq -n \
      --arg vid "${version_id}" \
      --arg q "${q}" \
      --arg pv "${prompt_version}" \
      --arg docs "${DOCS}" \
      --arg clauses "${CLAUSES}" \
      --arg gen "${GEN}" \
      '{
        dataset_version_id: $vid,
        user_question: $q,
        prompt_version: $pv,
        artifact_paths: {docs: $docs, clauses: $clauses, genuineness: $gen}
      }')

    local tmp_resp
    tmp_resp=$(mktemp)
    local http_status
    http_status=$(curl -sS -o "${tmp_resp}" -w '%{http_code}' \
      -X POST "${WORKER}/tasks/analyze_v2" \
      -H 'Content-Type: application/json' \
      -d "${body}")

    if [[ "${http_status}" != "200" ]]; then
      local err_line
      err_line=$(jq -n \
        --arg variant "${variant}" \
        --arg pv "${prompt_version}" \
        --arg q "${q}" \
        --argjson idx ${idx} \
        --arg status "${http_status}" \
        --arg body "$(cat "${tmp_resp}")" \
        '{variant: $variant, prompt_version: $pv, query_index: $idx, query: $q, error: "http_\($status)", body: $body}')
      emit "${err_line}"
      rm -f "${tmp_resp}"
      continue
    fi

    local resp
    resp=$(cat "${tmp_resp}")
    rm -f "${tmp_resp}"

    local metrics
    metrics=$(echo "${resp}" | jq \
      --arg variant "${variant}" \
      --arg pv "${prompt_version}" \
      --arg q "${q}" \
      --argjson idx ${idx} \
      --argjson price_input ${PRICE_INPUT} \
      --argjson price_output ${PRICE_OUTPUT} \
      --argjson price_cache_create ${PRICE_CACHE_CREATE} \
      --argjson price_cache_read ${PRICE_CACHE_READ} '
      {
        variant: $variant,
        prompt_version: $pv,
        query_index: $idx,
        query: $q,
        planner_prompt_version: .planner.prompt_version,
        attempts_count: (.planner.attempts | length),
        attempt_phases: [.planner.attempts[].phase],
        attempt_versions: [.planner.attempts[].prompt_version],
        validator_fail_initial: (
          (.planner.attempts[0].validation_issues // [])
          | length > 0
        ),
        retry_count: ((.planner.attempts | length) - 1),
        input_tokens: (.planner.usage.input_tokens // 0),
        output_tokens: (.planner.usage.output_tokens // 0),
        cache_creation_input_tokens: (.planner.usage.cache_creation_input_tokens // 0),
        cache_read_input_tokens: (.planner.usage.cache_read_input_tokens // 0),
        present_row_count: (.present.row_count // 0),
        has_present: (.present != null),
        steps_count: (.steps | length),
        estimated_cost_usd: (
          ((.planner.usage.input_tokens // 0) * $price_input
           + (.planner.usage.output_tokens // 0) * $price_output
           + (.planner.usage.cache_creation_input_tokens // 0) * $price_cache_create
           + (.planner.usage.cache_read_input_tokens // 0) * $price_cache_read
          ) / 1000000
        )
      }
    ')
    emit "${metrics}"
  done
}

if [[ -n "${OUT_PATH}" ]]; then
  : >"${OUT_PATH}"   # truncate
fi

case "${VARIANT_FILTER}" in
  all) run_variant ko; run_variant en ;;
  ko)  run_variant ko ;;
  en)  run_variant en ;;
esac
