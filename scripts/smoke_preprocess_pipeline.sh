#!/usr/bin/env bash
# scripts/smoke_preprocess_pipeline.sh
#
# ADR-017 / 5/19 결정 — 전처리 파이프라인 end-to-end smoke.
# upload → clean → doc_genuineness → clause_label까지 전 단계 LLOA로 처리.
#
# 환경 필수:
#   - LLOA_API_KEY (or WISENUT_LLOA_API_KEY / WISENUT_LLOA_MAX_V1_2_1_API_KEY)
#   - control plane + python-ai worker 실행 (docker compose -f compose.dev.yml up)
#   - 사내망에서 LLOA endpoint 접근 가능

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:18080}"
CSV_PATH="${CSV_PATH:-docs/eval/quality_v1/datasets/festival_sample_50.csv}"
TIMESTAMP="$(date +%s)"
INCLUDE_GENUINENESS="${INCLUDE_GENUINENESS:-}"  # 예: "genuine_review,mixed" — 비우면 모든 doc 처리

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
project_id="$(post_json POST /projects "{\"name\":\"preprocess-pipeline-${TIMESTAMP}\",\"description\":\"ADR-017 전처리 파이프라인 smoke\"}" \
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

# ---------- Stage 2: status wait helper ----------
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
wait_for_status "clean_status" "ready" 120

log "Stage 2-b: doc_genuineness 시작 (LLOA 호출)"
post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/doc_genuineness" '{}' >/dev/null
wait_for_status "doc_genuineness_status" "ready" 1800

log "Stage 2-c: clause_label 시작 (LLOA, clean source)"
if [[ -n "$INCLUDE_GENUINENESS" ]]; then
  # 콤마 분리 string → JSON array
  filter_json="$(python3 -c "
import json, sys
tiers = [t.strip() for t in '${INCLUDE_GENUINENESS}'.split(',') if t.strip()]
print(json.dumps({'include_genuineness': tiers}))
")"
  log "Stage 2-c: include_genuineness filter=${filter_json}"
  post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/clause_label" "$filter_json" >/dev/null
else
  post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/clause_label" '{}' >/dev/null
fi
wait_for_status "clause_label_status" "ready" 3600

# ---------- Stage 3: 결과 요약 ----------
log "Stage 3: tier 분포 + clause 분포 + 샘플"
final_file="$(mktemp -t preprocess_smoke_final.XXXXXX.json)"
trap 'rm -f "$final_file"' EXIT
for attempt in 1 2 3; do
  post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}" > "$final_file" 2>/dev/null || true
  if [[ -s "$final_file" ]]; then
    break
  fi
  sleep 1
done
if [[ ! -s "$final_file" ]]; then
  echo "Stage 3 GET failed" >&2
  exit 1
fi

python3 - "$final_file" <<'PY'
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)
meta = data.get("metadata") or {}
gen = meta.get("doc_genuineness_summary") or {}
cls = meta.get("clause_label_summary") or {}
print()
print("=== doc_genuineness ===")
print(f"  processed       = {gen.get('processed_row_count')} / {gen.get('input_row_count')}")
print(f"  parse_failures  = {gen.get('parse_failures')}")
for tier, count in (gen.get('tier_counts') or {}).items():
    print(f"  tier {tier:>14s}: {count}")
print()
print("=== clause_label ===")
print(f"  input_rows           = {cls.get('input_row_count')}")
print(f"  processed_docs       = {cls.get('processed_doc_count')}")
print(f"  skipped_by_filter    = {cls.get('skipped_by_filter')}")
print(f"  skipped_empty        = {cls.get('skipped_empty')}")
print(f"  parse_failures       = {cls.get('parse_failures')}")
print(f"  clause_count         = {cls.get('clause_count')}")
print(f"  include_genuineness  = {cls.get('include_genuineness')}")
for sentiment, count in (cls.get('sentiment_counts') or {}).items():
    print(f"  sentiment {sentiment:>10s}: {count}")
PY

# clause_label artifact sample
clause_ref="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print((d.get("metadata") or {}).get("clause_label_ref") or "")' "$final_file")"
if [[ -n "$clause_ref" ]]; then
  echo
  echo "=== clause_label artifact ==="
  echo "$clause_ref"
  host_ref="${clause_ref/#\/workspace\/data\//$(pwd)/data/}"
  if [[ -f "$host_ref" ]]; then
    echo
    echo "=== sample clause records (first 5 lines) ==="
    head -n 5 "$host_ref"
  else
    echo "(host path ${host_ref} not accessible — use 'docker compose exec python-ai-worker head -n 5 ${clause_ref}')"
  fi
fi

log "Smoke 완료. project_id=${project_id} dataset_id=${dataset_id} version_id=${version_id}"
