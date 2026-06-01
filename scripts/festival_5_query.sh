#!/usr/bin/env bash
# scripts/festival_5_query.sh
#
# 사용자 가치 비교 검증 — 본 플랫폼 측 5 query 자동 실행.
# vault `사용자_가치_비교_festival_2026-04.md` §4용 결과를 stdout으로 출력.

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:18080}"
CSV_PATH="${CSV_PATH:-docs/eval/quality_v1/datasets/festival_sample_50.csv}"
TIMESTAMP="$(date +%s)"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

post_json() {
  local method="$1" path="$2" payload="${3:-}"
  if [[ -n "$payload" ]]; then
    curl -sS -X "$method" "${API_BASE}${path}" -H 'Content-Type: application/json' -d "$payload"
  else
    curl -sS -X "$method" "${API_BASE}${path}"
  fi
}

# ---------- Stage 1: project + dataset + upload ----------
log "Stage 1: project + dataset 생성"
project_id="$(post_json POST /projects "{\"name\":\"festival-5q-${TIMESTAMP}\",\"description\":\"5 query 검증\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"
log "project_id=${project_id}"

dataset_id="$(post_json POST "/projects/${project_id}/datasets" \
  '{"name":"festival_sample","data_type":"unstructured"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"
log "dataset_id=${dataset_id}"

upload_metadata='{"text_columns":["제목","본문"]}'
log "Stage 1: CSV 업로드 (${CSV_PATH})"
version_id="$(curl -sS -X POST "${API_BASE}/projects/${project_id}/datasets/${dataset_id}/uploads" \
  -F "file=@${CSV_PATH}" \
  -F 'data_type=unstructured' \
  -F "metadata=${upload_metadata}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_version_id"])')"
log "version_id=${version_id}"

# ---------- Stage 2: clean → prepare → sentiment → embedding 빌드 대기 ----------
wait_for_status() {
  # 5/7 결정 5-step pipeline 신규 status (segment_status / clause_label_status
  # / embedding_cluster_status / keyword_index_status)는 DatasetVersion struct
  # 컬럼이 아니라 metadata jsonb 안에만 저장. 기존 4 status(clean_status 등)와
  # 둘 다 호환되도록 top-level → metadata fallback 순서로 검사.
  local field="$1" expected="$2" timeout="${3:-300}"
  for _ in $(seq 1 $timeout); do
    local data status
    data="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}")"
    status="$(printf '%s' "$data" | python3 -c "
import json, sys
data = json.load(sys.stdin)
direct = str(data.get('${field}') or '').strip()
if direct:
    print(direct)
else:
    meta = data.get('metadata') or {}
    print(str(meta.get('${field}') or '').strip())
")"
    if [[ "$status" == "$expected" ]]; then
      return 0
    fi
    if [[ "$status" == "failed" ]]; then
      echo "${field} failed:" >&2
      printf '%s\n' "$data" >&2
      return 1
    fi
    sleep 1
  done
  echo "${field} timeout (last status=${status})" >&2
  printf '%s\n' "$data" >&2
  return 1
}

log "Stage 2-a: clean 대기"
wait_for_status "clean_status" "ready" 60

# 5/7 결정 5-step pipeline — 신규 endpoint 흐름 (segment → clause_label →
# embedding_cluster + keyword_index 병렬). 기존 prepare/sentiment/embeddings/
# cluster는 deprecated. 410 처리는 후속 plan.

log "Stage 2-b: segment 시작 (kss sentence split)"
post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/segment" '{}' >/dev/null
wait_for_status "segment_status" "ready" 300

log "Stage 2-c: clause_label 시작 (LLM 단일 호출 — clause + is_relevant + sentiment + aspect)"
post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/clause_label" '{}' >/dev/null || log "clause_label 시작 실패"
# 5/8 fix: festival 50 rows ≈ 119 batch × ~9~14초 = 18~28분. timeout 2400 (40분)으로
# 늘림. d1b3c343 답습한 string fallback에서도 LLM 호출 자체 시간이 큼.
wait_for_status "clause_label_status" "ready" 2400 || log "clause_label 미준비 — 분석 본문 영향"

log "Stage 2-d: embedding_cluster 시작"
post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/embedding_cluster" '{}' >/dev/null || log "embedding_cluster 시작 실패"
wait_for_status "embedding_cluster_status" "ready" 600 || log "embedding_cluster 미준비"

log "Stage 2-e: keyword_index 시작"
post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/keyword_index" '{}' >/dev/null 2>&1 || log "keyword_index 시작 실패 (skip)"
wait_for_status "keyword_index_status" "ready" 300 || log "keyword_index 미준비 (skip)"

log "Stage 2 완료 — 분석 요청 단계로"

# ---------- Stage 3: 5 query ----------
queries=(
  "Q1|강릉 문화재 야행 SNS 게시글에서 가장 많이 언급된 긍정적 평가 또는 칭찬 5개를 빈도 순으로 나열해줘. 각 항목마다 대표 SNS 인용문 1개와 출처(수집ID 8글자)를 함께 보여줘."
  "Q2|같은 SNS 게시글에서 부정적 반응 또는 불만을 빈도 순으로 5개 추출. 각 항목별로 (a) 심각도(상중하 + 판단 근거 한 줄) (b) 대표 SNS 인용문 1개 (c) 출처를 보여줘. 부정 반응이 5개 미만이면 그렇게 답해."
  "Q3|데이터 안에서 외지 관광객으로 추정되는 작성자와 강릉 지역민으로 추정되는 작성자의 반응 차이를 분석해줘. 각 그룹별로 주요 관심사 만족 포인트 불만을 정리하고 데이터에 명시 라벨이 없으면 어떻게 그룹을 추정했는지 명시해."
  "Q4|강릉 문화재 야행 운영팀에 보고할 1 페이지 발표 슬라이드의 헤드라인 한 문장을 작성해줘. 50 rows 전체에서 가장 임팩트 있는 인사이트 1개를 골라 누구나 한 번에 이해할 수 있는 한국어 문장으로."
  "Q5|다음 3개 팀에 각각 줄 수 있는 실행 액션 3개씩을 데이터 근거 기반으로 작성해줘. (a) 홍보팀 (b) 현장운영팀 (c) 상권협력팀. 각 액션마다 무엇을 왜(데이터 근거+출처 ID) 효과 가설을 한 줄씩."
)

run_query() {
  local label="$1" goal="$2"
  local started_at finished_at duration
  started_at=$(date +%s)
  log "${label}: 분석 요청 제출"

  local analysis_payload
  analysis_payload="$(VERSION_ID="$version_id" GOAL="$goal" python3 -c "
import json, os
print(json.dumps({
    'dataset_version_id': os.environ['VERSION_ID'],
    'data_type': 'unstructured',
    'goal': os.environ['GOAL'],
}, ensure_ascii=False))")"

  local plan_response plan_id
  plan_response="$(post_json POST "/projects/${project_id}/analysis_requests" "$analysis_payload")"
  plan_id="$(printf '%s' "$plan_response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["plan"]["plan_id"])')"
  log "${label}: plan_id=${plan_id}"

  local execution_response execution_id
  execution_response="$(post_json POST "/projects/${project_id}/plans/${plan_id}/execute")"
  execution_id="$(printf '%s' "$execution_response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])')"
  log "${label}: execution_id=${execution_id}"

  status="unknown"
  for _ in $(seq 1 600); do
    local cur
    cur="$(post_json GET "/projects/${project_id}/executions/${execution_id}")"
    status="$(printf '%s' "$cur" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
    if [[ "$status" == "completed" || "$status" == "failed" ]]; then
      break
    fi
    sleep 1
  done

  finished_at=$(date +%s)
  duration=$((finished_at - started_at))
  log "${label}: status=${status} (duration=${duration}s)"

  local result
  result="$(post_json GET "/projects/${project_id}/executions/${execution_id}/result")"

  echo "<<<${label}_BEGIN>>>"
  printf '%s\n' "$result"
  echo "<<<${label}_END duration=${duration}s status=${status}>>>"
  echo

  # 5/6 fix: query가 완료되지 않으면 fail count 누적. 5 query 다 돈 후
  # exit 0이지만 1건이라도 completed가 아니면 exit 1로 끝낸다. 이전엔
  # status=failed/timeout이어도 stdout으로 결과처럼 흘려서 deploy 직전
  # smoke가 "성공한 척" 보였음 (4/30 baseline 신뢰성 깎인 직접 원인).
  if [[ "$status" != "completed" ]]; then
    failed_queries+=("${label}:${status}")
  fi
}

failed_queries=()
for q in "${queries[@]}"; do
  label="${q%%|*}"
  goal="${q#*|}"
  run_query "$label" "$goal"
done

log "5 query 완료. project_id=${project_id} dataset_id=${dataset_id} version_id=${version_id}"

if (( ${#failed_queries[@]} > 0 )); then
  log "⚠️  실패/미완 query ${#failed_queries[@]}건: ${failed_queries[*]}"
  log "smoke 실패 — deploy 신뢰성 신호로 사용하지 말 것"
  exit 1
fi
log "✅ 5 query 모두 completed"
