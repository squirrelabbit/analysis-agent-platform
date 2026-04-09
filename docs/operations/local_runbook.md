# 로컬 운영 Runbook

## 1. stack 실행과 정지

```bash
docker compose -f compose.dev.yml up -d --build
docker compose -f compose.dev.yml ps
docker compose -f compose.dev.yml down
```

기본 서비스:
- `control-plane`
- `python-ai-worker`
- `temporal`
- `temporal-worker`
- `postgres`

## 2. health 확인

```bash
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18080/runtime_status | jq
curl -fsS http://127.0.0.1:18090/health
curl -fsS http://127.0.0.1:18080/openapi.yaml | head
```

Swagger:
- `http://127.0.0.1:18080/swagger`

프로젝트 단위 운영 요약:

```bash
curl -fsS "$API/projects/$PROJECT_ID/operations/summary" | jq
```

## 3. web console scaffold

```bash
cd apps/web
npm install
npm run dev
```

개발 중 프론트는 Docker가 아니라 로컬 dev server로 띄우는 현재 구성을 기준으로 한다.

## 4. 자주 보는 로그

```bash
docker compose -f compose.dev.yml logs -f control-plane
docker compose -f compose.dev.yml logs -f temporal-worker
docker compose -f compose.dev.yml logs -f python-ai-worker
docker compose -f compose.dev.yml logs -f postgres
```

운영 이슈를 볼 때 우선순위:
1. `control-plane`
2. `temporal-worker`
3. `python-ai-worker`
4. `postgres`

## 5. 결과와 artifact 위치

- upload 원본: `data/uploads/`
- execution artifact: `data/artifacts/`
- dataset build artifact:
  - `prepared.parquet`
  - `sentiment.parquet`
  - `chunks.parquet`
  - `embeddings.index.parquet`
  - `clusters.json`
  - `clusters.memberships.parquet`

실행 상태와 snapshot은 Postgres metadata와 artifact 파일을 함께 본다.
execution row에는 raw artifact 전체가 아니라 compact payload가 저장되므로, 큰 본문이 필요하면 `artifact_ref`나 dataset build sidecar를 직접 본다.

## 6. 자주 확인하는 API

- `GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}`
- `GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/build_jobs`
- `GET /projects/{project_id}/dataset_build_jobs/{job_id}`
- `GET /projects/{project_id}/executions/{execution_id}`
- `GET /projects/{project_id}/executions/{execution_id}/events`
- `GET /projects/{project_id}/executions/{execution_id}/progress`
- `GET /projects/{project_id}/executions/{execution_id}/steps/{step_id}`
- `GET /projects/{project_id}/executions/{execution_id}/result`
- `GET /projects/{project_id}/operations/summary`
- `GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/clusters/{cluster_id}/members`
- `GET /dataset_profiles/validate`
- `GET /dataset_profiles`
- `GET /prompt_catalog`
- `GET /skill_policy_catalog`
- `GET /skill_policies/validate`
- `GET /rule_catalog`
- `GET /runtime_status`

cluster 경로 확인 팁:
- `step preview`에서 `cluster_execution_mode`가 `materialized_full_dataset`이면 precomputed cluster를 읽은 경로다.
- `cluster_execution_mode`가 `on_demand_subset_fallback`이면 prior artifact 때문에 subset 재계산한 경로다.
- `cluster_fallback_reason`으로 fallback 원인을 본다.

skill policy 확인 팁:
- `skill_policy_catalog`에서 현재 worker가 읽는 policy version과 hash를 본다.
- `skill_policies/validate`에서 default policy 누락이나 malformed file을 먼저 확인한다.
- `embedding_cluster`, `cluster_label_candidates`, `issue_evidence_summary` artifact에는 `policy_version`, `policy_hash`가 함께 남는다.

## 7. 관련 문서

- smoke와 테스트: [../testing/smoke_and_checks.md](../testing/smoke_and_checks.md)
- 수동 API 예시: [../testing/manual_api_walkthrough.md](../testing/manual_api_walkthrough.md)
- 복구 절차: [../recovery_guide.md](../recovery_guide.md)
- Postgres reset: [../architecture/dev_postgres_reset.md](../architecture/dev_postgres_reset.md)
