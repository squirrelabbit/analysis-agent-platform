# Release Checklist

silverone 2026-05-27 작성. Codex adversarial review (2026-05-26)가 release blocker 2건을 식별한 이후, 같은 종류 회귀가 merge 전에 자동으로 잡히도록 정리한 baseline. 이 문서는 *코드 계약*이라 `docs/` 영역에 둔다 (vault는 검토/계획 위주).

자동 검증은 `scripts/ci.sh` + `.github/workflows/ci.yml`이 담당. 본 문서는 그 의미와 사람이 의식적으로 확인할 항목을 명시.

## 자동 잠금 (CI가 차단)

| 항목 | 잠금 위치 |
|---|---|
| `go test ./...` 전체 pass | `.github/workflows/ci.yml::go-tests`, `scripts/ci.sh` |
| Python planner validator + composer + executor regression | `.github/workflows/ci.yml::python-tests`, `scripts/ci.sh` |
| openapi.yaml + openapi.frontend.yaml YAML parse | `.github/workflows/ci.yml::release-guard`, `scripts/ci.sh` |
| **boot-time destructive SQL guard** — `report_drafts` / `executions` / `skill_plans` / `analysis_requests`에 대한 `DROP TABLE IF EXISTS`가 `apps/control-plane/internal/store/` / `cmd/` / `internal/service/`에 존재하면 fail | `.github/workflows/ci.yml::release-guard`, `scripts/ci.sh`, `internal/store/postgres_legacy_preserve_test.go` |
| **startup reconciliation 호출 잠금** — `main.go`에 `server.ReconcileStartup`이 빠지면 fail. listening 전에 호출되어야 함 | `cmd/server/main_recovery_guard_test.go` (`go test ./cmd/server/`) |
| analyze smoke 4 case (direct-plan / user-question / version-debug / ambiguous reject) | `scripts/ci.sh` (docker compose 필요), 로컬에서만 |

## merge 전 사람이 의식적으로 확인할 항목

### 1. Destructive schema cleanup 금지

- `ensureSchema` / `bootstrap` / `init` 등 자동 startup에서 `DROP TABLE` / `TRUNCATE` / `DELETE` 추가 금지.
- 운영/감사 이력이 들어갈 가능성이 있는 테이블은 row 단위로도 자동 삭제 금지.
- 추가 cleanup 후보가 생기면 본 잠금 list에 테이블명을 추가한다.

### 2. DROP TABLE은 operator-run migration만 허용

- `scripts/migrate_*.py` 또는 `apps/control-plane/dev/sql/migrate_*.sql` 같은 *명시 실행* 경로에만 위치.
- migration에는 다음 4 항목 필수:
  1. **backup** — `pg_dump` 또는 `dataset_version_artifacts` 같은 jsonb 저장으로 사전 archive.
  2. **archive** — 삭제 대상 row를 별도 테이블 또는 파일에 복사 (논리적 trash).
  3. **rollback guide** — migration 실패 시 복구 절차 문서 (commit message + vault 노트).
  4. **production gate** — 운영자가 실행을 명시 확인하는 단계 (인자 `--confirm-destroy` 또는 환경변수).

### 3. Startup reconciliation 제거 금지

- `main.go::server.ReconcileStartup` 호출을 제거하려면 *동등하거나 더 강한* 대체 recovery path가 같은 PR에 포함되어야 한다.
- 대체 후보:
  - Temporal `workflow_id`로 build job 실제 status 재조회 (장기, ADR-019)
  - 별도 cron / sidecar job
- recovery path 없이 reconcile만 제거 → CI fail (`main_recovery_guard_test`).

### 4. analyze smoke 필수

- merge 전 로컬에서 `./scripts/ci.sh` 또는 `./scripts/smoke_analyze_endpoint.sh` 1회 통과 확인.
- 4 case 모두 PASS:
  1. `/tasks/analyze` direct-plan
  2. `/tasks/analyze` user-question (planner LLM)
  3. `/tasks/analyze` ambiguous (plan + user_question 둘 다) → 400 reject
  4. (script가 묶음으로 처리)

  옛 `/tasks/analyze_v2`는 backward-compatible alias로 worker가 계속 받아준다.
  새 호출은 canonical `/tasks/analyze`를 쓴다.

### 5. composer 응답 shape 확인

- `result.composer`에 `assistant_content` / `display` / `context_summary` / `metadata` 4 필드가 모두 있는지.
- `composer.metadata.mode = "deterministic"` (PR-A 정상 흐름), `template`은 enum (table_normal / table_truncated / empty / reuse_applied / failed / fallback).
- PR-B LLM composer 도입 후에는 mode가 `llm_backed` / `deterministic_fallback`도 허용.
- `assistant_message.content`가 `composer.assistant_content`와 동일한지.

### 6. execution regression R1~R9 통과

- `workers/python-ai/tests/test_sql_regression.py` (R1/R3/R4/R5/R7/R8/R9) — validator-only
- `workers/python-ai/tests/test_sql_regression_exec.py` (R2/R6) — executor-level
- 새 SQL contract 변경 PR이면 위 두 파일에 잠금 case 1건 이상 추가.

### 7. OpenAPI 계약 갱신 동반

- `openapi.yaml` / `openapi.frontend.yaml`에 신규 path / schema / 필드가 들어가면 CI parse가 통과해도 다음을 사람이 확인:
  - description이 한국어로 작성되어 있는가 (저장소 운영 규칙).
  - 응답 예시가 `docs/api/analysis_response_examples.md`에 반영되어 있는가 (필요 시).
  - frontend.yaml에 노출 여부 결정 (default는 노출 안 함, 명시 선택).

## 실행 방법

### 로컬

```bash
# 전체 검증 (smoke 포함, docker compose 필요)
./scripts/ci.sh

# smoke 없이 (CI 환경 또는 docker 미지원)
./scripts/ci.sh --no-smoke
```

### 자동 (GitHub Actions)

- push / PR마다 `python-tests` / `go-tests` / `release-guard` 3 job 자동 실행.
- smoke는 docker 의존이라 CI에서 제외. 로컬에서 수행.

## 관련 인시던트

- 2026-05-26 — boot-time legacy DROP incident ([[legacy_schema_drop_incident_2026-05-26]] in vault). 본 checklist 도입의 직접적 계기.

## 갱신 정책

- 새로운 release blocker 가 발견될 때마다 본 문서에 잠금 항목 추가.
- 자동 잠금은 항상 CI에 반영 (`scripts/ci.sh` + `.github/workflows/ci.yml`).
- 사람이 의식적으로 확인할 항목은 PR 템플릿에 체크박스로 둘 수도 있음 (후속).
