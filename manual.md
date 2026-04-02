# Manual

이 문서는 현재 개발 스택에서 직접 기능을 확인하는 방법을 정리한 수동 테스트 가이드다.

기준 workspace:

```bash
/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform
```

기준 API:

- control plane: `http://127.0.0.1:18080`
- python-ai-worker: `http://127.0.0.1:18090`


## 1. 개발 스택 실행

```bash
cd /Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform
docker compose -f compose.dev.yml up -d --build
docker compose -f compose.dev.yml ps
```

결과 확인:

- `postgres`
- `temporal`
- `control-plane`
- `python-ai-worker`
- `temporal-worker`

위 5개가 `Up`이면 된다.

참고:

- Swagger UI: [http://127.0.0.1:18080/swagger](http://127.0.0.1:18080/swagger)
- OpenAPI YAML: [http://127.0.0.1:18080/openapi.yaml](http://127.0.0.1:18080/openapi.yaml)


## 2. health 확인

```bash
curl -s http://127.0.0.1:18080/health | python3 -m json.tool
curl -s http://127.0.0.1:18090/health | python3 -m json.tool
curl -s http://127.0.0.1:18090/capabilities | python3 -m json.tool
```

결과 확인:

- 두 `/health`가 모두 HTTP 200이어야 한다.
- `/capabilities`에는 `skill_bundle_version`과 task 목록이 보여야 한다.
- worker `/health`에는 `rule_config.rule_config_path`, `rule_config.rule_config_inline`가 있으면 layered rule config가 현재 어떤 소스에서 켜졌는지 같이 확인할 수 있다.


## 3. 개발용 Postgres warning 확인

```bash
./apps/control-plane/dev/reset_postgres_dev.sh --check-only
```

결과 확인:

- `경고 없음: reset이 필요하지 않습니다.` 이면 정상
- `경고 감지: reset이 필요할 수 있습니다.` 이면 아래 실행

```bash
./apps/control-plane/dev/reset_postgres_dev.sh
```

관련 스크립트:

- [reset_postgres_dev.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/reset_postgres_dev.sh)


## 4. 코드 레벨 테스트

```bash
PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'
cd apps/control-plane && go test ./...
cd /Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform
```

결과 확인:

- Python: 마지막에 `OK`
- Go: 패키지별 `ok`


## 5. 로컬 임베딩 모델 평가

로컬에서 직접 실행:

```bash
PYTHONPATH=workers/python-ai/src python3 -m python_ai_worker.devtools.evaluate_embedding_model --model intfloat/multilingual-e5-small --format markdown
```

컨테이너 안에서 실행:

```bash
docker compose -f compose.dev.yml exec -T python-ai-worker \
  python -m python_ai_worker.devtools.evaluate_embedding_model \
  --model intfloat/multilingual-e5-small \
  --format markdown
```

현재 기대값:

- `search top1 pass: 2/2`
- `search topk pass: 2/2`
- `cluster dense-hybrid pass: 2/2`
- `cluster dense-only pass: 0/2`

관련 파일:

- [evaluate_embedding_model.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/devtools/evaluate_embedding_model.py)
- [embedding_eval_cases.py](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/workers/python-ai/src/python_ai_worker/devtools/embedding_eval_cases.py)


## 6. smoke 테스트

```bash
./apps/control-plane/dev/smoke.sh
./apps/control-plane/dev/smoke_semantic.sh
./apps/control-plane/dev/smoke_cluster.sh
./apps/control-plane/dev/smoke_sentiment.sh
./apps/control-plane/dev/smoke_trend.sh
./apps/control-plane/dev/smoke_compare.sh
./apps/control-plane/dev/smoke_breakdown.sh
./apps/control-plane/dev/smoke_taxonomy.sh
```

요약만 보고 싶으면:

```bash
./apps/control-plane/dev/smoke_semantic.sh | tail -n 1 | python3 -m json.tool
```

기능별로 마지막 요약 JSON에서 볼 핵심 키:

| 기능 | 스크립트 | 핵심 키 |
| --- | --- | --- |
| 기본 비정형 요약 | [smoke.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke.sh) | `artifact_keys` |
| 의미 검색 | [smoke_semantic.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_semantic.sh) | `semantic_top_text`, `retrieval_backend`, `evidence_selection_source` |
| 군집화 | [smoke_cluster.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_cluster.sh) | `canonical_row_count`, `cluster_count`, `cluster_similarity_backend`, `embedding_source_backend`, `dominant_cluster_label` |
| 감성 | [smoke_sentiment.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_sentiment.sh) | `dominant_label`, `negative_count`, `positive_count` |
| 추세 | [smoke_trend.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_trend.sh) | `peak_bucket`, `series` |
| 기간 비교 | [smoke_compare.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_compare.sh) | `current_count`, `previous_count`, `count_delta` |
| breakdown | [smoke_breakdown.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_breakdown.sh) | `top_group`, `breakdown` |
| taxonomy | [smoke_taxonomy.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_taxonomy.sh) | `dominant_taxonomy`, `dominant_taxonomy_count`, `taxonomy_count` |

특히 확인할 기대값:

- `smoke_semantic.sh`: `retrieval_backend = pgvector`
- `smoke_cluster.sh`: `cluster_count = 3`
- `smoke_cluster.sh`: `cluster_similarity_backend = dense-hybrid`
- `smoke_cluster.sh`: `dominant_cluster_label = 결제 / 오류`


## 7. 수동 API 테스트

아래 예시는 `festival.csv`를 직접 업로드하고 `prepare -> sentiment -> embedding -> analysis -> execution result`까지 따라가는 절차다.

### 7-1. 프로젝트 생성, dataset 생성, 업로드

```bash
cd /Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform

API=http://127.0.0.1:18080
FILE=/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/festival.csv

PROJECT_ID=$(
  curl -sS -X POST "$API/projects" \
    -H 'Content-Type: application/json' \
    -d '{"name":"festival-manual","description":"manual test"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["project_id"])'
)

DATASET_ID=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/datasets" \
    -H 'Content-Type: application/json' \
    -d '{"name":"festival","data_type":"unstructured"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_id"])'
)

VERSION_JSON=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/uploads" \
    -F "file=@$FILE" \
    -F 'data_type=unstructured' \
    -F 'metadata={"text_column":"text"}'
)

echo "$VERSION_JSON" | python3 -m json.tool

VERSION_ID=$(
  printf '%s' "$VERSION_JSON" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["dataset_version_id"])'
)
```

결과 확인:

- `dataset_version_id`
- `storage_uri`
- `prepare_status`
- `sentiment_status`
- `embedding_status`


### 7-2. prepare 실행

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/prepare" \
  -H 'Content-Type: application/json' \
  -d '{"text_column":"text"}' \
| python3 -m json.tool
```

결과 확인:

- `prepare_status = "ready"`
- `prepare_uri`
- `metadata.prepared_ref`
- `metadata.prepared_format`
- `metadata.prepare_regex_rule_names`
- `metadata.prepare_usage`

기본 regex 정제 규칙:

- `media_placeholder`
- `html_artifact`
- `url_cleanup`
- `zero_width_cleanup`


### 7-2-b. garbage_filter 직접 호출 예시

`garbage_filter`는 dataset build가 아니라 support skill이므로 control plane dataset API가 아니라 worker task 또는 analysis plan에서 사용한다.

prepare 결과 경로를 먼저 꺼낸다.

```bash
PREPARED_URI=$(
  curl -sS "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["prepare_uri"])'
)
```

그 다음 worker task를 직접 호출한다.

```bash
curl -sS -X POST "http://127.0.0.1:18090/tasks/garbage_filter" \
  -H 'Content-Type: application/json' \
  -d "{
    \"dataset_name\":\"$PREPARED_URI\",
    \"text_column\":\"normalized_text\",
    \"sample_n\":5,
    \"artifact_output_path\":\"/tmp/garbage_filter.rows.parquet\"
  }" \
| python3 -m json.tool
```

결과 확인:

- `artifact.summary.input_row_count`
- `artifact.summary.retained_row_count`
- `artifact.summary.removed_row_count`
- `artifact.summary.garbage_rule_hits`
- `artifact.removed_samples`
- `artifact.artifact_ref`
- `artifact.artifact_format = "parquet"`


### 7-3. sentiment 실행

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/sentiment" \
  -H 'Content-Type: application/json' \
  -d '{"text_column":"normalized_text"}' \
| python3 -m json.tool
```

결과 확인:

- `sentiment_status = "ready"`
- `sentiment_uri`
- `metadata.sentiment_ref`
- `metadata.sentiment_format`
- `metadata.sentiment_usage`


### 7-4. embedding 실행

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/embeddings" \
  -H 'Content-Type: application/json' \
  -d '{"embedding_model":"intfloat/multilingual-e5-small"}' \
| python3 -m json.tool
```

결과 확인:

- `embedding_status = "ready"`
- `embedding_model = "intfloat/multilingual-e5-small"`
- `embedding_uri`
- `metadata.embedding_index_backend = "pgvector"`
- `metadata.embedding_vector_dim = 384`
- `metadata.embedding_index_source_format = "parquet"`
- `metadata.embedding_usage`


### 7-5. dataset version 전체 상태 조회

```bash
curl -sS "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID" \
| python3 -m json.tool
```

결과 확인:

- `prepare_status`
- `sentiment_status`
- `embedding_status`
- `prepare_uri`
- `sentiment_uri`
- `embedding_uri`
- `metadata.prepare_usage`
- `metadata.sentiment_usage`
- `metadata.embedding_usage`


### 7-6. 질문 제출 -> plan 생성

```bash
ANALYSIS_JSON=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/analysis_requests" \
    -H 'Content-Type: application/json' \
    -d "{
      \"dataset_version_id\":\"$VERSION_ID\",
      \"data_type\":\"unstructured\",
      \"goal\":\"축제 관련 주요 이슈와 근거를 정리해줘\"
    }"
)

echo "$ANALYSIS_JSON" | python3 -m json.tool

PLAN_ID=$(
  printf '%s' "$ANALYSIS_JSON" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["plan"]["plan_id"])'
)
```

결과 확인:

- `plan.plan_id`
- `plan.plan.steps`
- `plan.planner_type`
- `plan.planner_model`
- `plan.planner_prompt_version`

plan만 다시 조회:

```bash
curl -sS "$API/projects/$PROJECT_ID/plans/$PLAN_ID" | python3 -m json.tool
```


### 7-7. plan 실행

```bash
EXEC_JSON=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/plans/$PLAN_ID/execute"
)

echo "$EXEC_JSON" | python3 -m json.tool

EXEC_ID=$(
  printf '%s' "$EXEC_JSON" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])'
)
```

결과 확인:

- `execution.execution_id`
- `execution.status`
- `execution.events`


### 7-8. execution 상태 조회

```bash
curl -sS "$API/projects/$PROJECT_ID/executions/$EXEC_ID" | python3 -m json.tool
```

결과 확인:

- `status`
- `events`
- `artifacts`
- `artifacts["step:<step_id>:issue_evidence_summary"]` 또는 `artifacts["step:<step_id>:evidence_pack"]` 안의 `prompt_compaction`
- 완료 이벤트 payload 안의 `step_hooks`


### 7-9. 최종 결과 조회

```bash
curl -sS "$API/projects/$PROJECT_ID/executions/$EXEC_ID/result" | python3 -m json.tool
```

결과 확인:

- `artifacts`
- `contract.artifact_keys`
- `contract.skill_names`
- `contract.evidence_artifact_keys`
- `contract.usage_summary`
- `contract.step_hooks`
- `result_v1.schema_version`
- `result_v1.primary_skill_name`
- `result_v1.answer.summary`
- `result_v1.answer.key_findings`
- `result_v1.answer.evidence`
- `result_v1.step_results`
- `result_v1.usage_summary`
- `result_v1.warnings`
- `result_v1.waiting`
- evidence artifact 안에 `prompt_compaction.analysis_context`, `prompt_compaction.selected_documents`가 있으면 evidence LLM 입력이 compaction된 것이다.

`contract.usage_summary`는 현재 usage가 남은 artifact를 기준으로 집계한다. 가격 env를 넣지 않았으면 `estimated_cost_usd`는 비어 있거나 0으로 남을 수 있다.
`contract.step_hooks`는 현재 기본 runtime hook가 남긴 step 전/후 record다. 각 record에는 `phase`, `step_id`, `skill_name`, `payload.input_keys` 또는 `payload.artifact_bytes`, `payload.usage` 같은 값이 들어갈 수 있다.
`result_v1`는 현재 사용자용 응답 레이어다. `answer`는 대표 artifact를 기준으로 사람이 읽기 좋은 요약/근거/후속질문을 정리하고, `step_results`는 각 skill step의 상태/요약/ref를 묶어준다. 실행이 `waiting`이면 `result_v1.waiting.waiting_for`, `result_v1.waiting.reason`을 먼저 보면 된다.
execution이 `completed` 상태가 되면 control plane은 현재 `result_v1 snapshot`을 execution metadata에 저장한다. 이후 `/executions/{id}/result`는 이 저장된 snapshot을 우선 사용하므로, 나중에 리스트 선택이나 보고서 초안 생성에서 같은 answer를 재사용하기 쉽다.


### 7-10. waiting 상태면 resume

`status = "waiting"`이면 필요한 dependency를 먼저 준비한 뒤 다시 resume하면 된다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/executions/$EXEC_ID/resume" \
  -H 'Content-Type: application/json' \
  -d '{"reason":"dependency is ready","triggered_by":"manual"}' \
| python3 -m json.tool
```


## 8. 결과 파일 위치

호스트 기준:

- upload 원본: [data/uploads](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/uploads)
- prepare / sentiment / embedding artifact: [data/artifacts](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/artifacts)
- execution 안에서 sidecar로 저장되는 support skill artifact: [data/artifacts](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/artifacts)`/projects/<project_id>/executions/<execution_id>/steps/`
- 현재는 `garbage_filter`, `document_filter`, `deduplicate_documents`가 이 경로를 사용한다.
- `issue_evidence_summary`, `evidence_pack`의 compaction 여부는 별도 파일이 아니라 execution artifact JSON 안의 `prompt_compaction` metadata로 확인한다.
- 파일명 예시:
  - `step-1.garbage_filter.rows.parquet`
  - `step-2.document_filter.matches.parquet`
  - `step-3.deduplicate_documents.rows.parquet`

특정 `dataset_version_id` 기준으로 찾는 방법:

```bash
find /Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/artifacts -path "*$VERSION_ID*" | sort
find /Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/uploads -path "*$VERSION_ID*" | sort
```


## 9. 로그 확인

```bash
docker compose -f compose.dev.yml logs control-plane
docker compose -f compose.dev.yml logs python-ai-worker
docker compose -f compose.dev.yml logs temporal-worker
docker compose -f compose.dev.yml logs postgres
```


## 10. 구현 상태

| 항목 | 상태 | 설명 |
| --- | --- | --- |
| 프로젝트 생성 | 완료 | `POST /projects` 가능 |
| dataset 생성 | 완료 | `POST /projects/{project_id}/datasets` 가능 |
| 파일 업로드 | 완료 | upload와 dataset version 생성 가능 |
| dataset version 저장 | 완료 | `dataset_versions`에 상태, 모델, URI 저장 |
| prepare 단독 실행 | 완료 | `POST .../prepare` |
| sentiment 단독 실행 | 완료 | `POST .../sentiment` |
| embedding 단독 실행 | 완료 | `POST .../embeddings` |
| noun_frequency 단독 실행 | 완료 | worker `POST /tasks/noun_frequency` |
| sentence_split 단독 실행 | 완료 | worker `POST /tasks/sentence_split` |
| prepare 결과 Parquet 저장 | 완료 | `prepared.parquet` 경로 |
| sentiment 결과 Parquet 저장 | 완료 | `sentiment.parquet` 경로 |
| chunk Parquet 생성 | 완료 | embedding 전에 chunk dataset 생성 |
| pgvector 적재 | 완료 | embedding 후 index 적재 |
| upload 후 자동 prepare 실행 | 미구현 | 상태만 `queued` 가능, 실제 auto trigger는 없음 |
| upload 후 prepare -> sentiment -> embedding 자동 연쇄 | 미구현 | 지금은 전부 수동 호출 |
| 질문 제출 | 완료 | `POST /analysis_requests` |
| LLM 플래너 경로 | 완료 | dev compose에서는 `PLANNER_BACKEND=python-ai` |
| plan 저장 / 조회 | 완료 | `skill_plans`와 조회 API 구현 |
| Temporal 실행 | 완료 | execute / resume / rerun / diff 포함 |
| 스킬 실행 결과 저장 | 완료 | `executions.artifacts`, `events` 저장 |
| waiting 판정 | 완료 | prepare / sentiment / embedding readiness 확인 |
| waiting 후 resume | 완료 | 수동 resume 가능 |
| 최종 evidence 해석 artifact | 완료 | `issue_evidence_summary`, `evidence_pack` |
| 최종 사용자 답변 전용 API | 부분완료 | artifact는 있지만 최종 natural language answer 전용 레이어는 약함 |
| 업로드부터 분석까지 one-shot API | 미구현 | 단일 orchestration endpoint 없음 |
| 운영 auth / approval | 미구현 | 별도 백로그 |
| observability | 미구현 | 별도 백로그 |


## 11. 빠른 추천 테스트 순서

1. `docker compose -f compose.dev.yml up -d --build`
2. `curl -s http://127.0.0.1:18080/health | python3 -m json.tool`
3. `./apps/control-plane/dev/reset_postgres_dev.sh --check-only`
4. `PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'`
5. `cd apps/control-plane && go test ./...`
6. `PYTHONPATH=workers/python-ai/src python3 -m python_ai_worker.devtools.evaluate_embedding_model --model intfloat/multilingual-e5-small --format markdown`
7. `./apps/control-plane/dev/smoke_semantic.sh`
8. `./apps/control-plane/dev/smoke_cluster.sh`
