# 운영 가이드

이 문서는 로컬 개발과 운영 검증의 입구만 제공한다. 상세 절차는 성격별 상세 문서로 분리했다.

## 1. 로컬 stack 실행

```bash
docker compose -f compose.dev.yml up -d --build
```

기본 주소:
- control plane: `http://127.0.0.1:18080`
- python-ai worker: `http://127.0.0.1:18090`
- Swagger UI: `http://127.0.0.1:18080/swagger`

선택:
- web console scaffold: `cd apps/web && npm install && npm run dev`

## 2. 기본 상태 확인

```bash
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18090/health
```

일반적인 기본 흐름:
1. dataset upload 또는 version 생성
2. `prepare` async job 확인
3. 필요 시 `sentiment / embedding / cluster` build job 확인
4. execution 결과와 `final_answer` 확인
5. 실패나 `waiting`이 남으면 복구 가이드 확인

## 3. 상세 문서

| 문서 | 역할 |
| --- | --- |
| [docs/operations/local_runbook.md](docs/operations/local_runbook.md) | stack 실행, health, 로그, artifact 경로, 로컬 운영 메모 |
| [docs/operations/frontend_handoff.md](docs/operations/frontend_handoff.md) | 프론트 화면 기준 API 호출 순서와 polling 규칙 |
| [docs/testing/smoke_and_checks.md](docs/testing/smoke_and_checks.md) | 코드 테스트, smoke script, 추천 검증 순서 |
| [docs/testing/manual_api_walkthrough.md](docs/testing/manual_api_walkthrough.md) | 프로젝트 생성부터 execution/result 조회까지 수동 API 예시 |
| [docs/recovery_guide.md](docs/recovery_guide.md) | build failed, execution waiting/failed 대응 절차 |
| [docs/operations/dev_postgres_reset.md](docs/operations/dev_postgres_reset.md) | 개발용 Postgres volume 재초기화 절차 |

## 4. 지금 기억할 운영 규칙

- 비정형 dataset version은 기본적으로 `prepare` async job을 먼저 enqueue한다.
- `sentiment`, `embedding`, `cluster`는 plan step이 요구할 때 자동 build 후 execution을 다시 이어간다.
- `result_v1`는 실행 결과의 기준 스냅샷이고, `final_answer`는 그 위에 얹는 사용자용 후처리 레이어다.
- 기동 후에는 startup reconciliation이 남아 있던 build job과 execution을 다시 평가한다.
- `확인 필요:` Temporal workflow history 장기 보존은 아직 dev server 기본값을 따른다.
