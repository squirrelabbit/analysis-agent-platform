#!/usr/bin/env bash
# scripts/smoke_doc_genuineness_verify.sh
#
# ADR-026 — doc_genuineness verify(교차모델 검증) end-to-end smoke.
# festival_sample_50.csv 50 rows로 clean → doc_genuineness(verify) 호출 →
# resolution 분포 출력. 특히 classify_error / partial_classify (이번 fix 대상)
# 발생 건을 doc_id와 함께 명시 출력해서 "더 이상 빈칸/실패로 안 떨어지는지" 확인.
#
# 환경 필수:
#   - control plane이 AUTH_ENABLED=false 로 떠 있어야 함 (smoke는 인증 안 함)
#   - worker에 LLOA_API_KEY + classify_models 2개 allowlist
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:18080}"
CSV_PATH="${CSV_PATH:-docs/eval/quality_v1/datasets/festival_sample_50.csv}"
MODEL_A="${MODEL_A:-wisenut/wise-lloa-max-v1.2.1}"
MODEL_B="${MODEL_B:-wisenut/wise-lloa-ultra-v1.1.0}"
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
project_id="$(post_json POST /projects "{\"name\":\"verify-smoke-${TIMESTAMP}\",\"description\":\"ADR-026 verify smoke\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])')"
log "project_id=${project_id}"

# doc_genuineness build는 metadata.doc_genuineness.subject_name 필수 (festival fallback 제거됨)
dataset_payload='{
  "name":"festival_sample","data_type":"unstructured",
  "metadata":{"doc_genuineness":{
    "subject_type":"festival","subject_name":"강릉 국가유산야행",
    "subject_aliases":["문화유산야행","문화재야행","강릉야행"],
    "recruitment_keywords":["서포터즈","푸드트럭"]
  }}
}'
dataset_id="$(post_json POST "/projects/${project_id}/datasets" "$dataset_payload" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])')"
log "dataset_id=${dataset_id}"

log "Stage 1: CSV 업로드 (${CSV_PATH})"
version_id="$(curl -sS -X POST "${API_BASE}/projects/${project_id}/datasets/${dataset_id}/uploads" \
  -F "file=@${CSV_PATH}" \
  -F 'data_type=unstructured' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_version_id"])')"
log "version_id=${version_id}"

# ---------- Stage 2: clean → doc_genuineness(verify) ----------
# version 응답은 clean/doc_genuineness/clause_label을 {status} 중첩 객체로 준다.
wait_for_status() {
  local block="$1" expected="$2" timeout="${3:-300}"
  for _ in $(seq 1 $timeout); do
    local data status
    data="$(post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}")"
    status="$(printf '%s' "$data" | python3 -c "
import json, sys
data = json.load(sys.stdin)
block = data.get('${block}') or {}
print(str(block.get('status') or '').strip())
")"
    if [[ "$status" == "$expected" ]]; then return 0; fi
    if [[ "$status" == "failed" ]]; then
      echo "${block} failed:" >&2; printf '%s\n' "$data" >&2; return 1
    fi
    sleep 1
  done
  echo "${block} timeout (last status=${status})" >&2; printf '%s\n' "$data" >&2; return 1
}

log "Stage 2-a: clean 트리거 + 대기 (text_columns=제목,본문)"
post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/clean" \
  '{"text_columns":["제목","본문"]}' >/dev/null
wait_for_status "clean" "ready" 120

verify_payload="$(python3 -c "
import json
print(json.dumps({'verify': True, 'classify_models': ['${MODEL_A}', '${MODEL_B}']}))
")"
log "Stage 2-b: doc_genuineness verify 시작 (classify: ${MODEL_A} + ${MODEL_B})"
post_json POST "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}/doc_genuineness" "$verify_payload" >/dev/null
wait_for_status "doc_genuineness" "ready" 1800

# ---------- Stage 3: resolution 분포 + 실패건 명시 ----------
log "Stage 3: resolution 분포 + classify_error/partial 명시"
final_file="$(mktemp -t verify_smoke_final.XXXXXX.json)"
trap 'rm -f "$final_file"' EXIT
post_json GET "/projects/${project_id}/datasets/${dataset_id}/versions/${version_id}" > "$final_file"

ref="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print((d.get("metadata") or {}).get("doc_genuineness_ref") or "")' "$final_file")"
echo "artifact ref = ${ref}" >&2
host_ref="${ref/#\/workspace\/data\//$(pwd)/data/}"

if [[ ! -f "$host_ref" ]]; then
  log "host에서 artifact 직접 접근 불가 → container에서 cat"
  docker compose -f compose.dev.yml exec -T python-ai-worker cat "$ref" > "${final_file}.jsonl"
  host_ref="${final_file}.jsonl"
fi

python3 - "$host_ref" <<'PY'
import json, sys, collections
res = collections.Counter()
blanks, errors, partials = [], [], []
total = 0
with open(sys.argv[1], encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line: continue
        rec = json.loads(line)
        total += 1
        r = rec.get("resolution", "?")
        res[r] += 1
        fl = rec.get("final_label")
        did = rec.get("doc_id")
        if fl in (None, ""):
            blanks.append((did, r))
        if r in ("classify_error", "judge_error"):
            errors.append((did, r, rec.get("error")))
        if r == "partial_classify":
            partials.append((did, rec.get("final_label")))
print()
print(f"=== verify resolution 분포 (total={total}) ===")
for k, v in res.most_common():
    print(f"  {k:>22s}: {v}")
print()
print(f"빈칸 final_label 건수      = {len(blanks)}  {blanks if blanks else ''}")
print(f"classify_error/judge_error = {len(errors)}  {[(d,r) for d,r,_ in errors] if errors else ''}")
print(f"partial_classify (한쪽실패)= {len(partials)}  {partials if partials else ''}")
print()
if blanks:
    print(">>> 여전히 빈칸으로 떨어진 doc 있음 — fix 미흡")
else:
    print(">>> 빈칸 0건 — 모든 doc이 final_label 보유 (fix 의도대로)")
PY

log "Smoke 완료. project_id=${project_id} dataset_id=${dataset_id} version_id=${version_id}"
