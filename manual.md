# Manual

이 문서는 현재 개발 스택에서 직접 기능을 확인하는 방법을 정리한 수동 테스트 가이드다.

기준 workspace:

```bash
/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform
```

기준 API:

- control plane: `http://127.0.0.1:18080`
- python-ai-worker: `http://127.0.0.1:18090`
- web console dev server: `http://127.0.0.1:4173`


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

재기동 복구:

- control plane은 현재 기동 시 startup reconciliation을 수행한다.
- 남아 있던 `queued/running` dataset build job은 다시 dispatch된다.
- 남아 있던 `queued/running` execution은 다시 enqueue된다.
- `waiting` execution은 dataset dependency를 다시 계산해 resume 가능 여부를 재평가한다.

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


## 2-1. web console 실행

```bash
cd /Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/web
npm install
npm run dev
```

결과 확인:

- 기본 주소는 `http://127.0.0.1:4173`
- `apps/web/.env.example`의 `VITE_API_BASE_URL`을 바꾸면 다른 control plane 주소를 붙일 수 있다.
- 개발 서버에서는 `/api/*`가 control plane으로 proxy된다.


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
./apps/control-plane/dev/smoke_auto_resume_sentiment.sh
./apps/control-plane/dev/smoke_auto_resume_embedding.sh
./apps/control-plane/dev/smoke_final_answer.sh
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
| auto resume 감성 | [smoke_auto_resume_sentiment.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_auto_resume_sentiment.sh) | `result_status`, `waiting_for`, `resume_triggered_by`, `sentiment_job_status` |
| auto resume 임베딩 | [smoke_auto_resume_embedding.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_auto_resume_embedding.sh) | `retrieval_backend`, `selection_source`, `waiting_for`, `embedding_job_status` |
| final answer | [smoke_final_answer.sh](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/smoke_final_answer.sh) | `final_answer_status`, `generation_mode`, `evidence_count`, `final_answer_event_type` |

특히 확인할 기대값:

- `smoke_semantic.sh`: `retrieval_backend = pgvector`
- `smoke_cluster.sh`: `cluster_count = 3`
- `smoke_cluster.sh`: `cluster_similarity_backend = dense-hybrid`
- `smoke_cluster.sh`: `dominant_cluster_label = 결제 / 오류`
- `smoke_auto_resume_sentiment.sh`: `waiting_for = sentiment_labels`, `resume_triggered_by = dataset_build_job`
- `smoke_auto_resume_embedding.sh`: `waiting_for = embeddings`, `resume_triggered_by = dataset_build_job`, `selection_source = semantic_search`
- `smoke_final_answer.sh`: `final_answer_status = ready`, `generation_mode = llm|fallback`, `final_answer_event_type = FINAL_ANSWER_GENERATED`

auto resume smoke fixture:

- `smoke_auto_resume_sentiment.sh`: [data/issues_sentiment.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues_sentiment.csv)
- `smoke_auto_resume_embedding.sh`: [data/issues.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues.csv)
- `smoke_final_answer.sh`: [data/issues.csv](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/issues.csv)


## 7. 수동 API 테스트

아래 예시는 `festival.csv`를 직접 업로드하고 `prepare -> sentiment -> embedding -> analysis -> execution result`까지 따라가는 절차다.

시나리오 등록과 시나리오 기반 `plan / execute`는 별도 endpoint로 수동 확인할 수 있다.
아래 시나리오 등록 명령은 `PROJECT_ID`가 준비된 상태를 전제로 한다. `scenario -> plan` 생성과 `scenario -> execute`는 `VERSION_ID`가 필요하므로 `7-1` 이후에 실행한다.
저장소에는 현재 축제 질문 기준 strict 시나리오 fixture가 [festival_scenarios.import.json](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/testdata/festival_scenarios.import.json) 로 들어 있다.

### 7-0. 공통 변수 준비

기존 프로젝트를 재사용해 일부 단계만 다시 실행할 때는 아래 변수부터 맞춰 두면 편하다.

```bash
cd /Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform

API=http://127.0.0.1:18080
FILE=/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/data/festival.csv
PROFILE_JSON='{"profile_id":"festival-default","prepare_prompt_version":"dataset-prepare-anthropic-batch-v1","sentiment_prompt_version":"sentiment-anthropic-v1","regex_rule_names":["media_placeholder","html_artifact","url_cleanup","zero_width_cleanup"],"garbage_rule_names":["ad_marker","promotion_link","platform_placeholder","empty_or_noise"],"embedding_model":"intfloat/multilingual-e5-small"}'

# 기존 리소스를 재사용할 때만 직접 넣는다.
# 새로 만들면 7-1, 7-1-c, 7-7 단계에서 자동으로 채워진다.
PROJECT_ID=
DATASET_ID=
VERSION_ID=
PLAN_ID=
EXEC_ID=
DRAFT_ID=
```

기본 profile registry는 현재 [dataset_profiles.json](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/dataset_profiles.json) 이다.
`profile`을 아예 보내지 않으면 data type 기준 기본 profile이 자동으로 resolve되어 dataset version에 저장된다.
즉 `PROFILE_JSON`은 기본값을 덮고 싶을 때만 넣으면 된다.
`prepare_prompt_version`, `sentiment_prompt_version` 값은 현재 [config/prompts](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/prompts) 아래 Markdown template 파일명을 그대로 쓴다. 예를 들어 `dataset-prepare-anthropic-v2`는 [dataset-prepare-anthropic-v2.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/prompts/dataset-prepare-anthropic-v2.md)를 뜻한다.
row prompt version 이름을 profile에 넣어도 batch build 시 대응되는 `*-batch-*` prompt가 있으면 자동으로 그 버전을 사용한다.

profile / prompt / rule 검증:

```bash
curl -sS "$API/dataset_profiles/validate" | python3 -m json.tool
```

여기서 확인할 필드:

- `registry.prompt_catalog`
- `registry.rule_catalog`
- `issues[].code`
- `issues[].scope`
- `issues[].resource_ref`

### 7-0-a. 시나리오 등록 / 일괄 등록 / 목록 / 상세 조회

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/scenarios" \
  -H 'Content-Type: application/json' \
  -d '{
    "scenario_id":"S1",
    "planning_mode":"strict",
    "user_query":"이번 벚꽃 축제 반응 어때?",
    "query_type":"여론 요약",
    "interpretation":"전체 여론 및 분위기 파악",
    "analysis_scope":"축제 기간",
    "steps":[
      {
        "step":1,
        "function_name":"가비지 필터링",
        "runtime_skill_name":"garbage_filter",
        "result_description":"분석 대상 정제"
      },
      {
        "step":2,
        "function_name":"빈도 기반 키워드 추출",
        "parameter_text":"top_n=10",
        "parameters":{"top_n":10},
        "result_description":"주요 키워드"
      }
    ]
  }' \
| python3 -m json.tool

curl -sS "$API/projects/$PROJECT_ID/scenarios" | python3 -m json.tool
curl -sS "$API/projects/$PROJECT_ID/scenarios/S1" | python3 -m json.tool
```

row 형태 표가 있으면 아래처럼 한 번에 넣을 수 있다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/scenarios/import" \
  -H 'Content-Type: application/json' \
  -d '{
    "rows":[
      {
        "scenario_id":"S1",
        "planning_mode":"strict",
        "user_query":"이번 벚꽃 축제 반응 어때?",
        "query_type":"여론 요약",
        "interpretation":"전체 여론 및 분위기 파악",
        "analysis_scope":"축제 기간",
        "step":1,
        "function_name":"가비지 필터링",
        "runtime_skill_name":"garbage_filter",
        "result_description":"분석 대상 정제"
      },
      {
        "scenario_id":"S1",
        "planning_mode":"strict",
        "user_query":"이번 벚꽃 축제 반응 어때?",
        "query_type":"여론 요약",
        "interpretation":"전체 여론 및 분위기 파악",
        "analysis_scope":"축제 기간",
        "step":2,
        "function_name":"빈도 기반 키워드 추출",
        "runtime_skill_name":"keyword_frequency",
        "parameter_text":"top_n=10",
        "parameters":{"top_n":10},
        "result_description":"주요 키워드"
      },
      {
        "scenario_id":"S2",
        "planning_mode":"strict",
        "user_query":"이번 축제 문제 뭐였어?",
        "query_type":"이슈 분석",
        "interpretation":"부정 의견 중심 문제 파악",
        "analysis_scope":"축제 기간",
        "step":1,
        "function_name":"문서 단위 감성 분류",
        "runtime_skill_name":"issue_sentiment_summary",
        "result_description":"감성 분류"
      }
    ]
  }' \
| python3 -m json.tool
```

fixture 파일을 그대로 쓰려면 아래처럼 등록한다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/scenarios/import" \
  -H 'Content-Type: application/json' \
  --data-binary @/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/apps/control-plane/dev/testdata/festival_scenarios.import.json \
| python3 -m json.tool
```

결과 확인:

- `scenario_id`
- `user_query`
- `planning_mode`
- `query_type`
- `interpretation`
- `analysis_scope`
- `steps[].step`
- `steps[].function_name`
- `steps[].runtime_skill_name`
- `steps[].parameter_text`
- `steps[].parameters`
- `steps[].result_description`
- `POST /scenarios/import` 결과의 `scenario_count`
- `POST /scenarios/import` 결과의 `row_count`
- `POST /scenarios/import` 결과의 `items[].scenario_id`

주의:

- 자동 plan 생성은 `runtime_skill_name`이 있거나 현재 control plane이 지원하는 `function_name -> runtime skill` 매핑이 있는 step만 처리한다.
- 현재 `planning_mode`는 `strict`만 지원한다.
- 직접 매핑되지 않는 step은 시나리오 등록 시 `runtime_skill_name`을 명시해야 한다.
- `guided`나 guardrail 기반 planner는 아직 backlog다.
- 일괄 등록에서는 같은 `scenario_id`의 `user_query`, `query_type`, `interpretation`, `analysis_scope`, `planning_mode`가 서로 다르면 에러를 돌린다.
- [scenario_templates.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/skill/scenario_templates.md)에 축제 시나리오 `S1~S5`의 현재 strict 매핑 기준과 원본 대비 차이를 정리해 두었다.

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
    -F 'metadata={"text_column":"text"}' \
    -F "profile=$PROFILE_JSON"
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
- `profile.profile_id`
- `profile.prepare_prompt_version`
- `profile.sentiment_prompt_version`
- `profile.embedding_model`

`dataset version`을 JSON으로 직접 생성할 때도 같은 `profile` object를 넣을 수 있다.
`profile`을 생략하면 기본 registry profile이 자동으로 들어간다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions" \
  -H 'Content-Type: application/json' \
  -d "{
    \"storage_uri\":\"$FILE\",
    \"data_type\":\"unstructured\",
    \"metadata\":{\"text_column\":\"text\"},
    \"profile\":$PROFILE_JSON
  }" \
| python3 -m json.tool
```

### 7-1-b. 시나리오 기반 plan 생성

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/scenarios/S1/plans" \
  -H 'Content-Type: application/json' \
  -d "{
    \"dataset_version_id\":\"$VERSION_ID\"
  }" \
| python3 -m json.tool
```

결과 확인:

- `request.goal`
- `request.context.scenario`
- `plan.plan.steps[].skill_name`
- `plan.plan.steps[].inputs`

### 7-1-c. 시나리오 기반 one-shot 실행

```bash
EXEC_JSON=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/scenarios/S1/execute" \
    -H 'Content-Type: application/json' \
    -d "{
      \"dataset_version_id\":\"$VERSION_ID\"
    }"
)

echo "$EXEC_JSON" | python3 -m json.tool

EXEC_ID=$(
  printf '%s' "$EXEC_JSON" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution"]["execution_id"])'
)
```

결과 확인:

- `request.goal`
- `plan.plan_id`
- `execution.execution_id`
- `execution.status`
- `job_id`
- `EXEC_ID`

시나리오 실행 직후 현재 상태를 바로 보려면:

```bash
curl -sS "$API/projects/$PROJECT_ID/executions/$EXEC_ID" | python3 -m json.tool
curl -sS "$API/projects/$PROJECT_ID/executions/$EXEC_ID/result" | python3 -m json.tool
```

현재 기본 정책은 아래다.

- `prepare`: upload/version 생성 직후 async job 자동 enqueue
- `sentiment`: 실행 step이 필요할 때만 자동 build
- `embedding`: 실행 step이 필요할 때만 자동 build

즉 `S1`처럼 `garbage_filter`, `issue_sentiment_summary`가 들어간 시나리오는 보통 별도 수동 build 없이 바로 진행된다.
그래도 아래처럼 `waiting`이 보이면 예외 상황으로 보면 된다.

- `waiting_for = dataset_prepare`
- `waiting_for = sentiment_labels`
- `waiting_for = embeddings`

이 경우에만 `7-2`, `7-3`, `7-4`로 필요한 build를 직접 확인하거나 예외 복구를 하고, 자동 resume이 실패했을 때만 아래 수동 resume를 실행하면 된다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/executions/$EXEC_ID/resume" \
  -H 'Content-Type: application/json' \
  -d '{"reason":"scenario dependencies are ready","triggered_by":"manual"}' \
| python3 -m json.tool
```

resume 후 다시 상태와 결과를 본다.

```bash
curl -sS "$API/projects/$PROJECT_ID/executions/$EXEC_ID" | python3 -m json.tool
curl -sS "$API/projects/$PROJECT_ID/executions/$EXEC_ID/result" | python3 -m json.tool
```

결과 확인:

- `execution.profile_snapshot.profile_id`
- `result_v1.profile.profile_id`
- `result_v1.profile.prepare_prompt_version`
- `result_v1.profile.sentiment_prompt_version`
- `result_v1.profile.embedding_model`

### 7-1-d. dataset build job 상태 조회

`prepare`는 현재 eager 정책이지만 sync 응답이 아니라 async job으로 시작된다. 직접 상태를 확인하려면 version 단위 build job 목록이나 개별 job 조회를 사용한다.

```bash
curl -sS "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/build_jobs" \
| python3 -m json.tool
```

결과 확인:

- `items[].job_id`
- `items[].build_type`
- `items[].status`
- `items[].workflow_id`
- `items[].workflow_run_id`
- `items[].attempt`
- `items[].last_error_type`
- `items[].resumed_execution_count`
- `items[].diagnostics.retry_count`
- `items[].diagnostics.last_error_message`
- `items[].diagnostics.workflow_id`
- `items[].diagnostics.workflow_run_id`
- `items[].diagnostics.resumed_execution_count`
- `items[].created_at`
- `items[].started_at`
- `items[].completed_at`
- `items[].error_message`

개별 job을 보려면:

```bash
BUILD_JOB_ID=

curl -sS "$API/projects/$PROJECT_ID/dataset_build_jobs/$BUILD_JOB_ID" \
| python3 -m json.tool
```

운영 중 장애 대응 절차는 [recovery_guide.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/docs/recovery_guide.md)를 본다.

비동기 build를 직접 시작할 수도 있다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/prepare_jobs" \
  -H 'Content-Type: application/json' \
  -d '{"text_column":"text"}' \
| python3 -m json.tool

curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/sentiment_jobs" \
  -H 'Content-Type: application/json' \
  -d '{"text_column":"normalized_text"}' \
| python3 -m json.tool

curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/embedding_jobs" \
  -H 'Content-Type: application/json' \
  -d '{"embedding_model":"intfloat/multilingual-e5-small"}' \
| python3 -m json.tool

curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/cluster_jobs" \
  -H 'Content-Type: application/json' \
  -d '{}' \
| python3 -m json.tool
```

주의:

- 현재 async build job은 Temporal workflow로 실행된다.
- 기본 build queue는 `TEMPORAL_BUILD_TASK_QUEUE`이고, 비우면 `<TEMPORAL_TASK_QUEUE>-build`를 사용한다.
- 현재 기본 activity 정책은 `prepare=20분/최대 4회`, `sentiment=45분/최대 4회`, `embedding=60분/최대 3회`, `cluster=60분/최대 3회`, `backoff=10초 x2 최대 5분`이다.
- worker HTTP timeout은 현재 `prepare=10분`, `sentiment=30분`, `embedding=45분`, `cluster=45분`으로 분리돼 있다.
- worker 동시성 기본값은 현재 `analysis activity=8`, `build activity=4`, `prepare slot=3`, `sentiment slot=2`, `embedding slot=1`, `cluster slot=1`이다.
- build 완료 후 같은 dataset version을 기다리던 execution은 dependency를 다시 계산한 뒤 자동 resume을 시도한다.
- 확인 필요: build workflow history의 장기 보존 기간은 아직 Temporal 서버 기본값을 따르고 있고, 운영 환경별 실제 동시성 상한은 머신 자원 기준으로 추가 튜닝이 필요하다.


### 7-2. prepare 실행

보통은 upload/version 생성 시 async job으로 자동 시작되므로, 이 단계는 재실행이나 예외 복구가 필요할 때만 직접 사용하면 된다.

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

기본 정책은 `lazy`라서 감성 step이 필요한 실행이 자동으로 먼저 build를 시도한다. 이 단계는 수동 재실행이나 예외 복구가 필요할 때 사용한다. 비동기 추적이 필요하면 `sentiment_jobs`를 먼저 쓰고, 즉시 완료가 필요하면 아래 sync endpoint를 쓴다.

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

기본 정책은 `lazy`라서 embedding step이 필요한 실행이 자동으로 먼저 build를 시도한다. 이 단계는 수동 재실행이나 예외 복구가 필요할 때 사용한다. 비동기 추적이 필요하면 `embedding_jobs`를 먼저 쓰고, 즉시 완료가 필요하면 아래 sync endpoint를 쓴다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/embeddings" \
  -H 'Content-Type: application/json' \
  -d '{"embedding_model":"intfloat/multilingual-e5-small"}' \
| python3 -m json.tool
```

결과 확인:

- `embedding_status = "ready"`
- `embedding_model = "intfloat/multilingual-e5-small"`
- `metadata.embedding_index_source_ref`
- `metadata.embedding_index_backend = "pgvector"`
- `metadata.embedding_vector_dim = 384`
- `metadata.embedding_index_source_format = "parquet"`

JSONL debug export가 필요하면 아래처럼 명시합니다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/embeddings" \
  -H 'Content-Type: application/json' \
  -d '{"embedding_model":"intfloat/multilingual-e5-small","debug_export_jsonl":true}' \
| python3 -m json.tool
```
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

시나리오 one-shot 실행인 `7-1-c`를 이미 사용했다면 이 단계는 건너뛰고 `7-8`부터 보면 된다.

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
- `final_answer.status`
- `final_answer.answer_text`
- `final_answer.key_points`
- `final_answer.caveats`
- evidence artifact 안에 `prompt_compaction.analysis_context`, `prompt_compaction.selected_documents`가 있으면 evidence LLM 입력이 compaction된 것이다.

`contract.usage_summary`는 현재 usage가 남은 artifact를 기준으로 집계한다. 가격 env를 넣지 않았으면 `estimated_cost_usd`는 비어 있거나 0으로 남을 수 있다.
`contract.step_hooks`는 현재 기본 runtime hook가 남긴 step 전/후 record다. 각 record에는 `phase`, `step_id`, `skill_name`, `payload.input_keys` 또는 `payload.artifact_bytes`, `payload.usage` 같은 값이 들어갈 수 있다.
`result_v1`는 현재 사용자용 응답 레이어다. `answer`는 대표 artifact를 기준으로 사람이 읽기 좋은 요약/근거/후속질문을 정리하고, `step_results`는 각 skill step의 상태/요약/ref를 묶어준다. 실행이 `waiting`이면 `result_v1.waiting.waiting_for`, `result_v1.waiting.reason`을 먼저 보면 된다.
`final_answer`는 `result_v1`와 evidence를 바탕으로 생성되는 grounded LLM 후처리 레이어다. 생성 실패 시 `result_v1` 기반 fallback answer를 응답에서 계속 내려준다.
execution이 `completed` 상태가 되면 control plane은 현재 `result_v1 snapshot`과 `final_answer snapshot`을 execution metadata에 저장한다. 이후 `/executions/{id}/result`는 이 저장된 snapshot을 우선 사용하므로, 나중에 리스트 선택이나 보고서 초안 생성에서 같은 answer를 재사용하기 쉽다.


### 7-10. execution 목록 preview 조회

```bash
curl -sS "$API/projects/$PROJECT_ID/executions" | python3 -m json.tool
```

결과 확인:

- `items[].execution_id`
- `items[].status`
- `items[].created_at`
- `items[].primary_skill_name`
- `items[].answer_preview`
- `items[].warning_count`
- `items[].waiting`

이 endpoint는 현재 `result_v1 snapshot`이 있으면 그 snapshot 기준으로 preview를 만들고, snapshot이 없으면 최소 상태 정보만 보여준다.


### 7-11. report draft 생성

```bash
DRAFT_JSON=$(
  curl -sS -X POST "$API/projects/$PROJECT_ID/report_drafts" \
    -H 'Content-Type: application/json' \
    -d "{
      \"title\":\"축제 VOC 보고서 초안\",
      \"execution_ids\":[\"$EXEC_ID\"]
    }"
)

echo "$DRAFT_JSON" | python3 -m json.tool

DRAFT_ID=$(
  printf '%s' "$DRAFT_JSON" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["draft_id"])'
)
```

결과 확인:

- `draft_id`
- `content.schema_version = "report-draft-v1"`
- `content.title`
- `content.overview`
- `content.execution_count`
- `content.sections`
- `content.key_findings`
- `content.evidence`
- `content.follow_up_questions`
- `content.usage_summary`
- `content.warnings`

현재 report draft는 선택한 execution의 `result_v1 snapshot`을 묶어 만든 저장형 초안이다.


### 7-12. report draft 조회

```bash
curl -sS "$API/projects/$PROJECT_ID/report_drafts/$DRAFT_ID" | python3 -m json.tool
```

결과 확인:

- `draft_id`
- `title`
- `execution_ids`
- `content`
- `created_at`


### 7-13. waiting 상태면 resume

`status = "waiting"`이면 자동 orchestration으로 해결하지 못한 예외 상황이다. 필요한 dependency를 먼저 준비한 뒤 자동 resume이 되지 않았을 때만 수동 resume하면 된다.

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/executions/$EXEC_ID/resume" \
  -H 'Content-Type: application/json' \
  -d '{"reason":"dependency is ready","triggered_by":"manual"}' \
| python3 -m json.tool
```

시나리오 one-shot 실행인 `7-1-c`에서 만든 `EXEC_ID`도 같은 endpoint로 resume한다.


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
| 시나리오 등록 | 완료 | `POST /projects/{project_id}/scenarios` |
| 시나리오 일괄 등록 | 완료 | `POST /projects/{project_id}/scenarios/import` |
| 시나리오 목록 / 상세 조회 | 완료 | `GET /projects/{project_id}/scenarios`, `GET /projects/{project_id}/scenarios/{scenario_id}` |
| 시나리오 기반 plan 생성 | 완료 | `POST /projects/{project_id}/scenarios/{scenario_id}/plans` |
| 시나리오 기반 one-shot 실행 | 완료 | `POST /projects/{project_id}/scenarios/{scenario_id}/execute` |
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
| dataset build async job 생성 / 조회 | 완료 | `POST .../prepare_jobs`, `POST .../sentiment_jobs`, `POST .../embedding_jobs`, `POST .../cluster_jobs`, `GET .../build_jobs`, `GET /dataset_build_jobs/{job_id}` |
| upload 후 자동 prepare 실행 | 완료 | eager 정책으로 async prepare job 자동 enqueue |
| execution 전 sentiment / embedding 자동 build | 부분완료 | dependency가 필요하면 sync build를 먼저 시도 |
| upload 후 prepare -> sentiment -> embedding 자동 연쇄 | 미구현 | sentiment, embedding warm-up 연쇄는 아직 없음 |
| 질문 제출 | 완료 | `POST /analysis_requests` |
| LLM 플래너 경로 | 완료 | dev compose에서는 `PLANNER_BACKEND=python-ai` |
| plan 저장 / 조회 | 완료 | `skill_plans`와 조회 API 구현 |
| Temporal 실행 | 완료 | execute / resume / rerun / diff 포함 |
| execution 목록 preview | 완료 | `GET /projects/{project_id}/executions` |
| 스킬 실행 결과 저장 | 완료 | `executions.artifacts`, `events` 저장 |
| waiting 판정 | 완료 | prepare / sentiment / embedding readiness 확인 |
| waiting 후 resume | 완료 | 수동 resume 가능 |
| 최종 evidence 해석 artifact | 완료 | `issue_evidence_summary`, `evidence_pack` |
| execution 결과 스냅샷 저장 | 완료 | `result_v1_snapshot` 저장 후 `/executions/{id}/result`에서 재사용 |
| report draft 생성 / 조회 | 완료 | execution snapshot 기반 `report-draft-v1` 저장 |
| 최종 사용자 답변 전용 API | 부분완료 | `result_v1`는 있으나 별도 conversation answer orchestration은 아직 약함 |
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
