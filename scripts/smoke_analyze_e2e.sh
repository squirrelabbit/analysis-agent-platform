#!/usr/bin/env bash
# silverone 2026-05-21 α compose dev e2e smoke — committed fixture를 dataset
# version artifact로 등록한 뒤 일반 분석 entrypoint
# (POST /projects/{pid}/datasets/{did}/analyze)가 active version을
# resolve해서 끝까지 동작하는지 검증.
#
# 흐름:
#   1. fixture를 host data 폴더에 mirror (compose가 ./data:/workspace/data 마운트)
#   2. project + dataset 생성
#   3. 더미 파일 업로드해서 dataset_version 생성 (storage_uri 확보용)
#   4. dataset_versions.metadata에 fixture artifact path 직접 inject + 상태 ready 설정
#   5. dataset.active_dataset_version_id 설정
#   6. POST /datasets/{did}/analyze 호출 (--mode direct-plan 또는 user-question)
#   7. 결과 검증 (aspect별 last/this count + delta_count)
#
# usage:
#   ./scripts/smoke_analyze_e2e.sh                          # direct-plan (기본)
#   ./scripts/smoke_analyze_e2e.sh --mode direct-plan
#   ./scripts/smoke_analyze_e2e.sh --mode user-question     # ANTHROPIC_API_KEY 필요
#
# user-question mode는 LLM 호출 실패해도 direct-plan smoke와 분리 판단.

set -euo pipefail

MODE="direct-plan"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"; shift 2 ;;
    --mode=*)
      MODE="${1#*=}"; shift ;;
    *)
      echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

case "${MODE}" in
  direct-plan|user-question) ;;
  *) echo "--mode must be direct-plan or user-question" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
API="${CONTROL_PLANE_URL:-http://127.0.0.1:18080}"
HOST_FIXTURE_SRC="${REPO_ROOT}/workers/python-ai/tests/fixtures/plan_v2_smoke"
HOST_FIXTURE_DST="${REPO_ROOT}/data/plan_v2_smoke"
CONTAINER_PATH="/workspace/data/plan_v2_smoke"
USER_QUESTION="작년과 올해의 aspect 증감수치 계산해줘"
PG_CONTAINER="analysis-support-platform-postgres-1"

green() { printf "\033[32m%s\033[0m\n" "$*"; }
red()   { printf "\033[31m%s\033[0m\n" "$*"; }

require() {
  command -v "$1" >/dev/null 2>&1 || { red "$1 required"; exit 1; }
}
require jq
require curl

# ===== step 1: mirror fixture =====
mkdir -p "${HOST_FIXTURE_DST}"
cp -f \
  "${HOST_FIXTURE_SRC}/cleaned.parquet" \
  "${HOST_FIXTURE_SRC}/clause_label.jsonl" \
  "${HOST_FIXTURE_SRC}/doc_genuineness.jsonl" \
  "${HOST_FIXTURE_SRC}/aspect_delta_plan.json" \
  "${HOST_FIXTURE_DST}/"

# ===== step 2: project + dataset =====
PROJECT_ID="$(curl -sS -X POST "${API}/projects" \
  -H 'Content-Type: application/json' \
  -d '{"name":"plan-v2-e2e"}' | jq -r '.project_id')"
echo "project_id: ${PROJECT_ID}"

DATASET_ID="$(curl -sS -X POST "${API}/projects/${PROJECT_ID}/datasets" \
  -H 'Content-Type: application/json' \
  -d '{"name":"plan-v2-smoke","data_type":"unstructured"}' | jq -r '.dataset_id')"
echo "dataset_id: ${DATASET_ID}"

# ===== step 3: upload 더미 파일 → dataset_version 생성 =====
DUMMY="$(mktemp -t plan-v2-dummy-XXXXXX.csv)"
printf 'text\nsmoke fixture marker\n' > "${DUMMY}"
VERSION_ID="$(curl -sS -X POST "${API}/projects/${PROJECT_ID}/datasets/${DATASET_ID}/uploads" \
  -F "file=@${DUMMY}" \
  -F "data_type=unstructured" \
  -F "activate_on_create=true" \
  | jq -r '.dataset_version_id')"
rm -f "${DUMMY}"
echo "version_id: ${VERSION_ID}"
if [[ -z "${VERSION_ID}" || "${VERSION_ID}" == "null" ]]; then
  red "version 생성 실패"; exit 1
fi

# ===== step 4: metadata + active 설정 (Go service가 보는 키만 inject) =====
DOCS_PATH="${CONTAINER_PATH}/cleaned.parquet"
CLAUSES_PATH="${CONTAINER_PATH}/clause_label.jsonl"
GEN_PATH="${CONTAINER_PATH}/doc_genuineness.jsonl"

SQL_UPDATE="UPDATE dataset_versions SET metadata = metadata || jsonb_build_object(
  'clean_uri', '${DOCS_PATH}',
  'cleaned_ref', '${DOCS_PATH}',
  'clean_status', 'ready',
  'doc_genuineness_uri', '${GEN_PATH}',
  'doc_genuineness_ref', '${GEN_PATH}',
  'doc_genuineness_status', 'ready',
  'clause_label_uri', '${CLAUSES_PATH}',
  'clause_label_ref', '${CLAUSES_PATH}',
  'clause_label_status', 'ready'
) WHERE dataset_version_id = '${VERSION_ID}';"

docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -c "${SQL_UPDATE}" >/dev/null

# active_version은 upload 시 activate_on_create=true로 이미 설정되지만 확정 차원에서 다시.
docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -c \
  "UPDATE datasets SET active_dataset_version_id = '${VERSION_ID}' WHERE dataset_id = '${DATASET_ID}'::uuid;" >/dev/null

# 확인용 — dataset.active_dataset_version_id가 보이는지
ACTIVE="$(curl -sS "${API}/projects/${PROJECT_ID}/datasets/${DATASET_ID}" | jq -r '.active_dataset_version_id')"
echo "active_dataset_version_id (from API): ${ACTIVE}"

# ===== step 5: analyze 호출 =====
# K-안 (2026-05-22): 두 경로가 모드별로 분리됨.
# - direct-plan → /versions/{vid}/analyze (plan 디버그 전용)
# - user-question → /datasets/{did}/analyze (active version, user_question만)
echo
if [[ "${MODE}" == "direct-plan" ]]; then
  echo "=== mode: direct-plan (POST /versions/{vid}/analyze) ==="
  BODY="$(jq -n --slurpfile plan "${HOST_FIXTURE_SRC}/aspect_delta_plan.json" '{plan: $plan[0]}')"
  ANALYZE_PATH="${API}/projects/${PROJECT_ID}/datasets/${DATASET_ID}/versions/${ACTIVE}/analyze"
else
  echo "=== mode: user-question (POST /datasets/{did}/analyze, active version) ==="
  BODY="$(jq -n --arg q "${USER_QUESTION}" '{user_question: $q}')"
  ANALYZE_PATH="${API}/projects/${PROJECT_ID}/datasets/${DATASET_ID}/analyze"
fi

TMP_RESP="$(mktemp)"
trap 'rm -f "${TMP_RESP}"' EXIT

# silverone 2026-05-26 (저장형 analysis thread 도입 후) — user-question mode는
# 호출 후 analysis_threads/messages/runs row가 +1/+2/+1 증가해야 한다. 호출
# 직전에 baseline을 떠 두고 호출 후 delta를 검증한다. direct-plan mode는 thread
# 저장 흐름과 무관하므로 baseline 측정 자체를 skip.
THREAD_COUNT_BEFORE=0
MESSAGE_COUNT_BEFORE=0
RUN_COUNT_BEFORE=0
if [[ "${MODE}" == "user-question" ]]; then
  THREAD_COUNT_BEFORE="$(docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -tA -c \
    "SELECT COUNT(*) FROM analysis_threads WHERE dataset_id = '${DATASET_ID}'::uuid;")"
  MESSAGE_COUNT_BEFORE="$(docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -tA -c \
    "SELECT COUNT(*) FROM analysis_messages WHERE dataset_id = '${DATASET_ID}'::uuid;")"
  RUN_COUNT_BEFORE="$(docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -tA -c \
    "SELECT COUNT(*) FROM analysis_runs WHERE dataset_id = '${DATASET_ID}'::uuid;")"
fi

HTTP_STATUS="$(curl -sS -o "${TMP_RESP}" -w '%{http_code}' \
  -X POST "${ANALYZE_PATH}" \
  -H 'Content-Type: application/json' \
  -d "${BODY}")"

if [[ "${HTTP_STATUS}" != "200" ]]; then
  red "analyze failed: HTTP ${HTTP_STATUS}"
  cat "${TMP_RESP}"
  exit 1
fi

RESULT_JSON="$(cat "${TMP_RESP}")"

# ===== step 6: 결과 요약 + 검증 =====
echo "${RESULT_JSON}" | jq '{project_id, dataset_id, version_id, mode, plan_version: .result.plan_version, step_count: (.result.steps|length), present_row_count: .result.present.row_count}'
echo
echo "=== present rows ==="
echo "${RESULT_JSON}" | jq '.result.present.rows'

# fixture 기대값 잠금 — direct-plan mode에서만 expected값 검증 (LLM은 컬럼 이름 다를 수 있음)
if [[ "${MODE}" == "direct-plan" ]]; then
  ASSERT="$(echo "${RESULT_JSON}" | jq '
    .result.present.rows as $rows
    | {
        ambiance_scenery: ($rows | map(select(.aspect=="ambiance_scenery"))[0]),
        food:             ($rows | map(select(.aspect=="food"))[0]),
        show_program:     ($rows | map(select(.aspect=="show_program"))[0])
      }
    | [.ambiance_scenery.delta_count == 1, .food.delta_count == -1, .show_program.delta_count == 1,
       .ambiance_scenery.delta_rate == 100.0, .food.delta_rate == -100.0, .show_program.delta_rate == null]
    | all
  ')"
  if [[ "${ASSERT}" == "true" ]]; then
    green "direct-plan e2e PASS — aspect_delta 기대값 정확히 일치"
  else
    red "direct-plan e2e FAIL — expected values mismatch"
    exit 1
  fi
else
  # silverone 2026-05-26 — codex 작업으로 /datasets/{did}/analyze 응답이 저장형
  # analysis thread shape으로 확장. 옛 `.result.present.row_count`는 그대로
  # 살아있고 thread/run/message invariant가 top-level에 추가됨.
  ROW_COUNT="$(echo "${RESULT_JSON}" | jq -r '.result.present.row_count // empty')"
  if [[ -z "${ROW_COUNT}" || "${ROW_COUNT}" -lt 3 ]]; then
    red "user-question e2e FAIL — present row_count=${ROW_COUNT:-<missing>} (expected >= 3)"
    exit 1
  fi

  # === response shape invariants ===
  INVARIANT="$(echo "${RESULT_JSON}" | jq -r '
    [
      (.mode == "user_question"),
      ((.thread_id // "") | length > 0),
      ((.run.run_id // "") | length > 0),
      (.run.status == "completed"),
      (.run.error_message == null),
      (.assistant_message.run_id == .run.run_id),
      (.user_message.role == "user"),
      (.assistant_message.role == "assistant"),
      (.assistant_message.context_summary.row_count == .result.present.row_count)
    ] | all
  ')"
  if [[ "${INVARIANT}" != "true" ]]; then
    red "user-question e2e FAIL — response invariant mismatch"
    echo "${RESULT_JSON}" | jq '{
      mode, thread_id,
      run: {run_id: .run.run_id, status: .run.status, error_message: .run.error_message},
      user_role: .user_message.role,
      assistant_role: .assistant_message.role,
      assistant_run_id: .assistant_message.run_id,
      context_row_count: .assistant_message.context_summary.row_count,
      present_row_count: .result.present.row_count
    }'
    exit 1
  fi

  # === DB row count delta + 링크 정합성 ===
  THREAD_ID="$(echo "${RESULT_JSON}" | jq -r '.thread_id')"
  RUN_ID="$(echo "${RESULT_JSON}" | jq -r '.run.run_id')"
  USER_MSG_ID="$(echo "${RESULT_JSON}" | jq -r '.user_message.message_id')"
  ASSISTANT_MSG_ID="$(echo "${RESULT_JSON}" | jq -r '.assistant_message.message_id')"

  THREAD_COUNT_AFTER="$(docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -tA -c \
    "SELECT COUNT(*) FROM analysis_threads WHERE dataset_id = '${DATASET_ID}'::uuid;")"
  MESSAGE_COUNT_AFTER="$(docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -tA -c \
    "SELECT COUNT(*) FROM analysis_messages WHERE dataset_id = '${DATASET_ID}'::uuid;")"
  RUN_COUNT_AFTER="$(docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -tA -c \
    "SELECT COUNT(*) FROM analysis_runs WHERE dataset_id = '${DATASET_ID}'::uuid;")"

  THREAD_DELTA=$(( THREAD_COUNT_AFTER - THREAD_COUNT_BEFORE ))
  MESSAGE_DELTA=$(( MESSAGE_COUNT_AFTER - MESSAGE_COUNT_BEFORE ))
  RUN_DELTA=$(( RUN_COUNT_AFTER - RUN_COUNT_BEFORE ))
  if [[ "${THREAD_DELTA}" != "1" || "${MESSAGE_DELTA}" != "2" || "${RUN_DELTA}" != "1" ]]; then
    red "user-question e2e FAIL — DB row delta mismatch: threads+${THREAD_DELTA} (want +1), messages+${MESSAGE_DELTA} (want +2), runs+${RUN_DELTA} (want +1)"
    exit 1
  fi

  # 신규 row 정합성: thread_id / run_id / user_message_id / assistant.run_id 매핑.
  DB_LINKS="$(docker exec -i "${PG_CONTAINER}" psql -U platform -d analysis_support -tA -c "
    SELECT
      (SELECT thread_id FROM analysis_runs WHERE run_id = '${RUN_ID}'),
      (SELECT user_message_id FROM analysis_runs WHERE run_id = '${RUN_ID}'),
      (SELECT run_id FROM analysis_messages WHERE message_id = '${ASSISTANT_MSG_ID}'),
      (SELECT role FROM analysis_messages WHERE message_id = '${USER_MSG_ID}'),
      (SELECT role FROM analysis_messages WHERE message_id = '${ASSISTANT_MSG_ID}'),
      (SELECT thread_id FROM analysis_messages WHERE message_id = '${USER_MSG_ID}');
  ")"
  IFS='|' read -r DB_RUN_THREAD DB_RUN_USERMSG DB_ASSIST_RUN DB_USER_ROLE DB_ASSIST_ROLE DB_USERMSG_THREAD <<<"${DB_LINKS}"
  if [[ "${DB_RUN_THREAD}"   != "${THREAD_ID}"        || \
        "${DB_RUN_USERMSG}"  != "${USER_MSG_ID}"      || \
        "${DB_ASSIST_RUN}"   != "${RUN_ID}"           || \
        "${DB_USER_ROLE}"    != "user"                || \
        "${DB_ASSIST_ROLE}"  != "assistant"           || \
        "${DB_USERMSG_THREAD}" != "${THREAD_ID}" ]]; then
    red "user-question e2e FAIL — DB link mismatch:
      analysis_runs.thread_id=${DB_RUN_THREAD} (want ${THREAD_ID})
      analysis_runs.user_message_id=${DB_RUN_USERMSG} (want ${USER_MSG_ID})
      assistant.run_id=${DB_ASSIST_RUN} (want ${RUN_ID})
      user.role=${DB_USER_ROLE} (want user)
      assistant.role=${DB_ASSIST_ROLE} (want assistant)
      user.thread_id=${DB_USERMSG_THREAD} (want ${THREAD_ID})"
    exit 1
  fi

  green "user-question e2e PASS — row_count=${ROW_COUNT}, threads+${THREAD_DELTA}/messages+${MESSAGE_DELTA}/runs+${RUN_DELTA}, run_status=completed, DB links 정합"
fi
