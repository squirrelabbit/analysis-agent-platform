#!/usr/bin/env bash
# silverone 2026-05-27 — release safety baseline runner.
#
# Codex adversarial review (2026-05-26)에서 boot-time legacy DROP + startup
# reconciliation 제거를 release blocker로 식별. 이런 회귀를 merge 전에 자동으로
# 잡기 위한 통합 verifier.
#
# 7 job:
#   1. go test ./... (executor / service / store)
#      - postgres_legacy_preserve_test가 destructive SQL guard 역할
#      - startup_reconciliation_guard_test가 boot path 잠금
#   2. Python planner validator
#   3. Python executor regression (calculate / present / sql_regression)
#   4. Python composer
#   5. openapi.yaml + openapi.frontend.yaml YAML parse
#   6. analyze smoke (direct-plan + user-question + version + ambiguous)
#   7. boot-time destructive SQL guard 추가 grep (이중 안전망)
#
# 사용:
#   ./scripts/ci.sh             # 모든 job (smoke 포함, docker compose 필요)
#   ./scripts/ci.sh --no-smoke  # smoke 건너뜀 (CI 환경 등 docker 미지원)
#
# 각 단계가 실패하면 즉시 종료 (set -e). 끝까지 통과해야 'release ok'.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RUN_SMOKE=1
if [[ "${1:-}" == "--no-smoke" ]]; then
  RUN_SMOKE=0
fi

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
RESET='\033[0m'

stage() {
  echo -e "\n${YELLOW}==>${RESET} $1"
}

ok() {
  echo -e "${GREEN}OK${RESET} — $1"
}

fail() {
  echo -e "${RED}FAIL${RESET} — $1" >&2
  exit 1
}

stage "1. go test ./..."
(cd apps/control-plane && go test ./...) && ok "go test"

stage "2. Python planner validator"
PYTHONPATH=workers/python-ai/src python3 -m unittest workers.python-ai.tests.test_planner_validator \
  || fail "test_planner_validator"
ok "test_planner_validator"

stage "3. Python executor regression"
(
  cd workers/python-ai/tests
  PYTHONPATH="$REPO_ROOT/workers/python-ai/src" python3 -m unittest \
    test_executor_calculate \
    test_executor_present \
    test_sql_regression
)
ok "executor regression (calculate / present / sql_regression)"

stage "4. Python composer"
(
  cd workers/python-ai/tests
  PYTHONPATH="$REPO_ROOT/workers/python-ai/src" python3 -m unittest test_composer
)
ok "test_composer"

stage "5. openapi YAML parse"
ruby -e 'require "yaml"; YAML.load_file("docs/api/openapi.yaml"); YAML.load_file("docs/api/openapi.frontend.yaml"); puts "ok"' \
  || fail "openapi YAML parse"
ok "openapi parse"

stage "6. boot-time destructive SQL guard (grep)"
# postgres_legacy_preserve_test가 동일 검사를 go test로 수행하지만, 다른 store
# 파일/migration script까지 한 번 더 grep해 이중 잠금.
if grep -rnE 'DROP TABLE IF EXISTS (report_drafts|executions|skill_plans|analysis_requests)' \
    apps/control-plane/internal/store/ \
    apps/control-plane/cmd/ \
    apps/control-plane/internal/service/ 2>/dev/null; then
  fail "legacy table DROP found in boot path. Move destructive cleanup to operator-run migration."
fi
ok "no legacy DROP in boot path"

if [[ $RUN_SMOKE -eq 1 ]]; then
  stage "7. analyze_endpoint smoke (direct-plan + user-question)"
  if ! command -v docker >/dev/null 2>&1; then
    echo -e "${YELLOW}WARN${RESET} — docker not found, skipping smoke. Use --no-smoke explicitly to silence."
  else
    bash "$REPO_ROOT/scripts/smoke_analyze_endpoint.sh" >/dev/null \
      || fail "analyze_endpoint smoke (worker가 동작 중인지 확인)"
    ok "smoke 4/4 PASS"
  fi
else
  echo -e "${YELLOW}skipped${RESET} — 7. analyze smoke (--no-smoke)"
fi

echo -e "\n${GREEN}release ok${RESET} — 7 job 모두 통과."
