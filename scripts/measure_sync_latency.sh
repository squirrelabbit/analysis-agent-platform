#!/usr/bin/env bash
# silverone 2026-05-27 — sync analyze latency 측정 harness.
#
# 목적: 현재 sync HTTP 분석이 실제로 얼마나 걸리는지 측정하고 async 전환
# 필요성을 판단한다 (ADR-019 §2 트리거).
#
# 측정 모드:
#   1. worker — Python /tasks/analyze_v2 직접 호출 (control-plane overhead 제거,
#      fixture 사용. plan_v2_smoke). 가장 빠른 lower bound.
#   2. control-plane — /datasets/{did}/analyze + /analysis_threads/{tid}/messages
#      (실 dataset 필요, setup 후). 사용자 체감 시간 측정.
#
# 측정 항목:
#   - total_elapsed_ms (curl time_total)
#   - server_elapsed_ms (run.completed_at - run.created_at, control-plane 모드만)
#   - run.status
#   - result.present.total_rows / returned_rows / truncated
#   - result.composer.metadata.{mode, template}
#   - result.planner.usage.{input_tokens, output_tokens, cache_read_input_tokens}
#
# 5 query (festival 도메인 기반):
#   Q1: 전체 aspect별 카운트 보여줘
#   Q2: 작년과 올해의 aspect 증감수치 계산해줘
#   Q3: 음식 aspect 중 부정 평가 상위는?
#   Q4: 올해 분위기 관련 후기 중 부정 비율은?
#   Q5: 긍정 절을 보여줘
#
# usage:
#   ./scripts/measure_sync_latency.sh --mode worker
#   ./scripts/measure_sync_latency.sh --mode worker --runs 3
#   ./scripts/measure_sync_latency.sh --mode control-plane \
#       --project PID --dataset DID
#   ./scripts/measure_sync_latency.sh --mode control-plane \
#       --project PID --dataset DID --version VID  # version-specific debug

set -euo pipefail

MODE="worker"
RUNS=1
OUT_PATH=""
PROJECT_ID=""
DATASET_ID=""
VERSION_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --out) OUT_PATH="$2"; shift 2 ;;
    --project) PROJECT_ID="$2"; shift 2 ;;
    --dataset) DATASET_ID="$2"; shift 2 ;;
    --version) VERSION_ID="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ "$MODE" != "worker" && "$MODE" != "control-plane" ]]; then
  echo "mode must be worker or control-plane" >&2
  exit 2
fi

if [[ "$MODE" == "control-plane" && ( -z "$PROJECT_ID" || -z "$DATASET_ID" ) ]]; then
  echo "control-plane mode requires --project and --dataset" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKER_URL="${WORKER_URL:-http://127.0.0.1:18090}"
CONTROL_PLANE_URL="${CONTROL_PLANE_URL:-http://127.0.0.1:18080}"
HOST_FIX="$REPO_ROOT/workers/python-ai/tests/fixtures/plan_v2_smoke"
CONTAINER_FIX="/workspace/data/plan_v2_smoke"

QUERIES=(
  "전체 aspect별 카운트 보여줘"
  "작년과 올해의 aspect 증감수치 계산해줘"
  "음식 aspect 중 부정 평가 상위는?"
  "올해 분위기 관련 후기 중 부정 비율은?"
  "긍정 절을 보여줘"
)

mkdir -p "$REPO_ROOT/data/measurements"
if [[ -z "$OUT_PATH" ]]; then
  OUT_PATH="$REPO_ROOT/data/measurements/sync_latency_$(date +%Y%m%d-%H%M%S)_${MODE}.jsonl"
fi
: > "$OUT_PATH"

mirror_fixture_to_compose() {
  # plan_v2_smoke 디렉터리를 compose data 볼륨에 복사 (worker가 같은 경로로 본다).
  local target="$REPO_ROOT/data/plan_v2_smoke"
  mkdir -p "$target"
  cp -f "$HOST_FIX"/{cleaned.parquet,clause_label.jsonl,doc_genuineness.jsonl,aspect_delta_plan.json} "$target/" 2>/dev/null || true
}

call_worker() {
  local query="$1"
  local payload
  payload=$(jq -n \
    --arg vid "measure-sync" \
    --arg q "$query" \
    --arg docs "$CONTAINER_FIX/cleaned.parquet" \
    --arg cls  "$CONTAINER_FIX/clause_label.jsonl" \
    --arg gen  "$CONTAINER_FIX/doc_genuineness.jsonl" \
    '{dataset_version_id:$vid, user_question:$q, artifact_paths:{docs:$docs, clauses:$cls, genuineness:$gen}}')
  local tmp; tmp=$(mktemp)
  local start; start=$(date +%s%N)
  local http_status
  http_status=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -H 'Content-Type: application/json' \
    -X POST -d "$payload" \
    "$WORKER_URL/tasks/analyze_v2")
  local end; end=$(date +%s%N)
  local elapsed_ms=$(( (end - start) / 1000000 ))
  jq -c --arg query "$query" --argjson elapsed "$elapsed_ms" --argjson status "$http_status" \
    '{
      mode: "worker",
      query: $query,
      total_elapsed_ms: $elapsed,
      http_status: $status,
      run_status: (if $status == 200 then "completed" else "failed" end),
      total_rows: .present.total_rows,
      returned_rows: .present.returned_rows,
      truncated: .present.truncated,
      composer_template: .composer.metadata.template,
      composer_mode: .composer.metadata.mode,
      planner_input_tokens: .planner.usage.input_tokens,
      planner_output_tokens: .planner.usage.output_tokens,
      planner_cache_read_tokens: .planner.usage.cache_read_input_tokens
    }' "$tmp"
  rm -f "$tmp"
}

call_control_plane_analyze() {
  # 첫 turn: POST /datasets/{did}/analyze
  local query="$1"
  local tmp; tmp=$(mktemp)
  local start; start=$(date +%s%N)
  local http_status
  http_status=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -H 'Content-Type: application/json' \
    -X POST -d "$(jq -nc --arg q "$query" '{user_question:$q}')" \
    "$CONTROL_PLANE_URL/projects/$PROJECT_ID/datasets/$DATASET_ID/analyze")
  local end; end=$(date +%s%N)
  local elapsed_ms=$(( (end - start) / 1000000 ))
  jq -c --arg query "$query" --argjson elapsed "$elapsed_ms" --argjson status "$http_status" \
    '{
      mode: "control_plane_analyze",
      query: $query,
      total_elapsed_ms: $elapsed,
      http_status: $status,
      thread_id: .thread_id,
      run_id: .run.run_id,
      run_status: .run.status,
      run_created_at: .run.created_at,
      run_completed_at: .run.completed_at,
      total_rows: .result.present.total_rows,
      returned_rows: .result.present.returned_rows,
      truncated: .result.present.truncated,
      composer_template: .result.composer.metadata.template,
      composer_mode: .result.composer.metadata.mode,
      reuse_applied: .result.reuse.applied,
      planner_input_tokens: .result.planner.usage.input_tokens,
      planner_output_tokens: .result.planner.usage.output_tokens,
      planner_cache_read_tokens: .result.planner.usage.cache_read_input_tokens
    }' "$tmp"
  rm -f "$tmp"
}

call_control_plane_version() {
  # version-specific debug: POST /versions/{vid}/analyze (plan만 받음)
  local plan_path="$HOST_FIX/aspect_delta_plan.json"
  local payload; payload=$(jq -nc --slurpfile plan "$plan_path" '{plan: $plan[0]}')
  local tmp; tmp=$(mktemp)
  local start; start=$(date +%s%N)
  local http_status
  http_status=$(curl -sS -o "$tmp" -w '%{http_code}' \
    -H 'Content-Type: application/json' \
    -X POST -d "$payload" \
    "$CONTROL_PLANE_URL/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/analyze")
  local end; end=$(date +%s%N)
  local elapsed_ms=$(( (end - start) / 1000000 ))
  jq -c --argjson elapsed "$elapsed_ms" --argjson status "$http_status" \
    '{
      mode: "control_plane_version",
      query: "(direct plan replay)",
      total_elapsed_ms: $elapsed,
      http_status: $status,
      total_rows: .result.present.total_rows,
      returned_rows: .result.present.returned_rows,
      truncated: .result.present.truncated,
      composer_template: .result.composer.metadata.template,
      composer_mode: .result.composer.metadata.mode
    }' "$tmp"
  rm -f "$tmp"
}

echo "==> mode=$MODE runs=$RUNS out=$OUT_PATH"

if [[ "$MODE" == "worker" ]]; then
  mirror_fixture_to_compose
  for run_idx in $(seq 1 "$RUNS"); do
    echo "--- run $run_idx ---"
    for q in "${QUERIES[@]}"; do
      result=$(call_worker "$q")
      echo "$result" | tee -a "$OUT_PATH"
    done
  done
else
  for run_idx in $(seq 1 "$RUNS"); do
    echo "--- run $run_idx ---"
    for q in "${QUERIES[@]}"; do
      result=$(call_control_plane_analyze "$q")
      echo "$result" | tee -a "$OUT_PATH"
    done
    if [[ -n "$VERSION_ID" ]]; then
      result=$(call_control_plane_version)
      echo "$result" | tee -a "$OUT_PATH"
    fi
  done
fi

echo
echo "==> 통계 요약 (total_elapsed_ms)"
jq -s 'group_by(.query) | map({
    query: .[0].query,
    n: length,
    avg_ms: ((map(.total_elapsed_ms) | add) / length | floor),
    min_ms: (map(.total_elapsed_ms) | min),
    max_ms: (map(.total_elapsed_ms) | max),
    statuses: (map(.run_status) | unique)
  })' "$OUT_PATH"

echo
echo "==> 전체 평균"
jq -s 'map(.total_elapsed_ms) | {n: length, avg_ms: (add/length | floor), min_ms: min, max_ms: max}' "$OUT_PATH"
