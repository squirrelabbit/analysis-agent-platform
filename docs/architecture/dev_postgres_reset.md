# 개발용 Postgres 재초기화 가이드

## 목적

- `compose.dev.yml`의 Postgres 이미지를 `pgvector/pgvector:pg16`으로 바꾼 뒤 남을 수 있는 `collation version mismatch` 경고를 정리하기 위한 개발용 절차다.
- 대상은 로컬 개발용 named volume `postgres_dev_data`뿐이다.
- 운영 DB나 공유 환경에는 적용하지 않는다.

## 언제 필요한가

- `docker compose -f compose.dev.yml up -d` 이후 Postgres 로그에 `collation version mismatch` 경고가 반복될 때
- 기존 Postgres volume을 유지한 채 이미지나 호스트 libc/locale 환경이 바뀌었을 때

## 영향 범위

- 삭제 대상은 Postgres named volume뿐이다.
- repo의 [`data/`](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data) 디렉터리와 uploaded/artifact 파일은 그대로 남는다.
- Temporal 상태와 Postgres 메타데이터는 다시 초기화된다.

## 절차

가장 빠른 방법:

```bash
./apps/control-plane/dev/reset_postgres_dev.sh
```

수동 절차:

1. 현재 dev stack을 내린다.

```bash
docker compose -f compose.dev.yml down
```

2. 삭제할 volume 이름을 확인한다.

```bash
docker volume ls | rg 'postgres_dev_data'
```

3. 확인한 Postgres volume만 삭제한다.

```bash
docker volume rm analysis-support-platform_postgres_dev_data
```

4. Postgres를 다시 올린다.

```bash
docker compose -f compose.dev.yml up -d postgres
```

5. health가 붙으면 나머지 서비스를 다시 올린다.

```bash
docker compose -f compose.dev.yml up -d python-ai-worker control-plane temporal temporal-worker
```

6. 경고가 사라졌는지 확인한다.

```bash
docker compose -f compose.dev.yml logs postgres | rg 'collation|mismatch'
```

## 주의점

- `analysis-support-platform_postgres_dev_data`는 기본 compose project name 기준 예시다.
- 확인 필요: 실행 환경에서 compose project name을 바꿨다면 실제 volume 이름은 달라질 수 있다.
- helper script [`reset_postgres_dev.sh`](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/reset_postgres_dev.sh)는 compose label 기준 volume을 먼저 찾고, 못 찾으면 기본 volume 이름으로 fallback한다.
- 초기화 뒤에는 project, dataset version, execution metadata를 다시 만들어야 한다.
- smoke 재검증은 [`smoke_semantic.sh`](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_semantic.sh), [`smoke_cluster.sh`](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_cluster.sh)부터 다시 돌리는 편이 안전하다.
