# 수동 API Walkthrough

이 문서는 로컬에서 API를 직접 호출해 기본 흐름을 확인할 때만 본다. 일상적인 검증은 smoke script를 우선 사용한다.

## 1. 공통 변수

```bash
export API=http://127.0.0.1:18080
```

기존 리소스를 재사용할 때만 직접 지정한다.

```bash
export PROJECT_ID=
export DATASET_ID=
export VERSION_ID=
export EXECUTION_ID=
```

## 2. 프로젝트와 dataset 준비

프로젝트 생성:

```bash
PROJECT_ID=$(
  curl -sS -X POST "$API/projects" \
    -H 'Content-Type: application/json' \
    -d '{"name":"manual-check"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])'
)
```

dataset 생성:

```bash
DATASET_ID=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/datasets" \
    -H 'Content-Type: application/json' \
    -d '{"name":"manual-dataset"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])'
)
```

upload:

```bash
VERSION_ID=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/uploads" \
  -F "file=@data/issues.csv" \
  -F 'data_type=unstructured' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_version_id"])'
)
```

업로드 응답 전체를 보고 싶으면 같은 요청을 한 번 더 실행하거나 `versions/{version_id}` 조회를 사용한다.

```bash
curl -sS "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID" | python3 -m json.tool
```

## 3. build job 상태 확인

```bash
JOB_ID=$(
  curl -sS "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/build_jobs" \
  | python3 -c 'import json,sys; items=json.load(sys.stdin); print(items[0]["job_id"] if items else "")'
)

curl -sS "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/build_jobs" | python3 -m json.tool
[ -n "$JOB_ID" ] && curl -sS "$API/projects/$PROJECT_ID/dataset_build_jobs/$JOB_ID" | python3 -m json.tool
```

기본 정책:
- `clean`: eager
- `prepare`: sample-first optional
- `sentiment / embedding / cluster`: lazy

## 4. 일반 질문 실행

request 생성:

```bash
REQUEST_ID=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/analysis_requests" \
    -H 'Content-Type: application/json' \
    -d "{
      \"dataset_version_id\":\"$VERSION_ID\",
      \"goal\":\"이 데이터의 주요 이슈를 요약해줘\"
    }" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["request_id"])'
)
```

plan 조회 후 실행:

```bash
PLAN_ID=$(curl -sS "$API/projects/$PROJECT_ID/analysis_requests/$REQUEST_ID" | python3 -c 'import json,sys; print(json.load(sys.stdin)["requested_plan"]["plan_id"])')

EXECUTION_ID=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/plans/$PLAN_ID/execute" \
    -H 'Content-Type: application/json' \
    -d '{}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])'
)
```

## 5. 선택: 시나리오 기반 실행

```bash
EXECUTION_ID=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/scenarios/S1/execute" \
  -H 'Content-Type: application/json' \
  -d "{\"dataset_version_id\":\"$VERSION_ID\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])'
)
```

`strict` 시나리오는 저장된 step을 그대로 plan으로 바꿔 execution까지 enqueue한다. 시나리오가 준비되지 않았거나 데이터와 안 맞으면 이 단계는 건너뛰고 일반 질문 경로만 확인하면 된다.

## 6. execution / result 조회

```bash
curl -sS "$API/projects/$PROJECT_ID/executions/$EXECUTION_ID" | python3 -m json.tool
curl -sS "$API/projects/$PROJECT_ID/executions/$EXECUTION_ID/result" | python3 -m json.tool
```

확인 포인트:
- `status`
- `diagnostics`
- `result_v1`
- `final_answer`
- `warnings`
- `waiting`

## 7. waiting / failed 대응

- `waiting`이면 build job이 자동 생성됐는지 먼저 본다.
- `failed`면 `diagnostics.failure_reason`, build job `last_error_type`, service 로그를 함께 본다.
- 상세 절차는 [../recovery_guide.md](../recovery_guide.md)를 따른다.
