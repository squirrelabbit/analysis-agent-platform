#!/usr/bin/env bash
# scripts/smoke_doc_genuineness.sh
#
# ADR-017 / 5/19 결정 — dataset_doc_genuineness skill end-to-end smoke.
# festival_sample_50.csv (50 rows, stratified, gitignored 원본에서 추출)을
# 사용해 clean → doc_genuineness 호출 → tier 분포 출력.
#
# 환경 필수:
#   - LLOA_API_KEY (또는 WISENUT_LLOA_API_KEY / WISENUT_LLOA_MAX_V1_2_1_API_KEY)
#   - control plane + python-ai worker 실행 중 (docker compose -f compose.dev.yml up)
#   - 사내망에서 LLOA endpoint 접근 가능

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
project_id="$(post_json POST /projects "{\"name\":\"doc-genuineness-smoke-${TIMESTAMP}\",\"description\":\"ADR-017 doc_genuineness smoke\"}" \
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

# ---------- Stage 2: clean 대기 → doc_genuineness ----------
wait_for_status() {
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

log "Stage 2-a: clean 대기 (upload 직후 자동 trigger)"
wait_for_status "clean_status" "ready" 60

log "Stage 2-b: doc_genuineness 시작 (LLOA 호출, 50 docs)"
post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/doc_genuineness" '{}' >/dev/null
# 50 docs × LLOA 응답 5~15초 = 4~12분. timeout 1800 (30분).
wait_for_status "doc_genuineness_status" "ready" 1800

# ---------- Stage 3: 결과 확인 ----------
log "Stage 3: tier 분포 + 샘플 출력"
# GET 응답을 임시 파일에 저장 후 python3가 파일에서 직접 읽는다.
# 변수 캡처 후 printf로 pipe하면 shell quoting/encoding 이슈로
# json.load가 빈 stdin을 보는 경우가 발생했다 (b7urxwye9 등).
final_file="$(mktemp -t doc_genuineness_smoke_final.XXXXXX.json)"
trap 'rm -f "$final_file"' EXIT
for attempt in 1 2 3; do
  post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}" > "$final_file" 2>/dev/null || true
  if [[ -s "$final_file" ]]; then
    break
  fi
  log "Stage 3: GET 빈 응답 (attempt ${attempt}). 재시도."
  sleep 1
done
if [[ ! -s "$final_file" ]]; then
  echo "Stage 3 GET failed after 3 attempts" >&2
  exit 1
fi

python3 - "$final_file" <<'PY'
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)
meta = data.get("metadata") or {}
summary = meta.get("doc_genuineness_summary") or {}
tiers = summary.get("tier_counts") or {}
print()
print("=== doc_genuineness summary ===")
print(f"input_row_count     = {summary.get('input_row_count')}")
print(f"processed_row_count = {summary.get('processed_row_count')}")
print(f"parse_failures      = {summary.get('parse_failures')}")
print(f"prompt_version      = {summary.get('prompt_version')}")
print(f"model               = {summary.get('model')}")
print(f"prompt_tokens       = {summary.get('total_prompt_tokens')}")
print(f"completion_tokens   = {summary.get('total_completion_tokens')}")
print()
print("=== tier distribution ===")
for tier in ("genuine_review", "mixed", "non_review"):
    print(f"  {tier:>14s}: {tiers.get(tier, 0)}")
PY

# artifact ref 위치 + 샘플 라인 3개 출력. ref는 worker container 내부 경로라
# host -f로 검증 안 되므로, host에 마운트된 ./data 와 매핑한 후 head 한다.
ref="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1], "r", encoding="utf-8")); print((d.get("metadata") or {}).get("doc_genuineness_ref") or "")' "$final_file")"
if [[ -n "$ref" ]]; then
  echo
  echo "=== artifact ref ==="
  echo "${ref}"
  host_ref="${ref/#\/workspace\/data\//$(pwd)/data/}"
  if [[ -f "$host_ref" ]]; then
    echo
    echo "=== sample records (first 3 lines) ==="
    head -n 3 "$host_ref"
  else
    echo "(host path ${host_ref} not accessible — use 'docker compose exec python-ai-worker head -n 3 ${ref}')"
  fi
fi

log "Smoke 완료. project_id=${project_id} dataset_id=${dataset_id} version_id=${version_id}"
