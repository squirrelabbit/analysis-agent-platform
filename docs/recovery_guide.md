# 운영 복구 가이드

이 문서는 control plane / Temporal / python-ai worker 운영 중 `build failed`, `execution waiting`, `execution failed`가 발생했을 때 확인 순서와 수동 복구 절차를 정리한다.

## 1. 먼저 볼 것

1. dataset version 상태
2. dataset build job 상태
3. execution 상태와 최근 event
4. profile / prompt / rule validator 결과

기본 확인 명령:

```bash
curl -sS "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID" | python3 -m json.tool
curl -sS "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/build_jobs" | python3 -m json.tool
curl -sS "$API/projects/$PROJECT_ID/executions/$EXEC_ID" | python3 -m json.tool
curl -sS "$API/projects/$PROJECT_ID/executions/$EXEC_ID/result" | python3 -m json.tool
curl -sS "$API/dataset_profiles/validate" | python3 -m json.tool
```

## 2. build failed

확인 포인트:

- `items[].status == "failed"`
- `items[].error_message`
- `items[].last_error_type`
- `items[].diagnostics.retry_count`
- `items[].diagnostics.workflow_id`
- `items[].diagnostics.workflow_run_id`

주요 분기:

- `last_error_type = workflow_start_failed`
  - Temporal enqueue 자체가 실패한 경우다.
  - control plane / temporal / temporal-worker 기동 상태를 먼저 확인한다.
- `error_message`에 `unsupported ... prompt version`, `... rule ... not found`, `text_column` 관련 오류가 있으면
  - profile / prompt / rule 설정 오류일 가능성이 높다.
  - 먼저 `/dataset_profiles/validate`로 registry 문제를 확인한다.
- worker HTTP timeout 또는 일시 장애면
  - build job 재시도를 기다리거나, 필요 시 같은 build endpoint로 새 job을 다시 만든다.

수동 재실행 예:

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/datasets/$DATASET_ID/versions/$VERSION_ID/prepare_jobs" \
  -H 'Content-Type: application/json' \
  -d '{}' \
| python3 -m json.tool
```

## 3. execution waiting

`result_v1.waiting`과 `diagnostics.waiting`을 같이 본다.

대표 예:

- `waiting_for = dataset_prepare`
- `waiting_for = sentiment_labels`
- `waiting_for = embeddings`

확인 순서:

1. version의 `prepare_status`, `sentiment_status`, `embedding_status`
2. 해당 build job이 `queued/running/failed/completed` 중 어디인지
3. build job이 완료됐는데도 waiting이면 execution event 최신 상태 확인

정상 경로에서는 build 완료 후 auto resume이 붙는다. 그래서 waiting은 기본적으로 예외 상태다.

수동 resume는 아래 조건에서만 쓴다.

- 필요한 build가 모두 `completed`
- 자동 resume이 붙지 않음
- execution 상태가 여전히 `waiting`

```bash
curl -sS -X POST "$API/projects/$PROJECT_ID/executions/$EXEC_ID/resume" \
  -H 'Content-Type: application/json' \
  -d '{"reason":"manual recovery after dependency build","triggered_by":"operator"}' \
| python3 -m json.tool
```

## 4. execution failed

확인 포인트:

- `diagnostics.failure_reason`
- `diagnostics.latest_event_type`
- `diagnostics.latest_event_message`
- `result_v1.warnings`

주요 분기:

- prompt/rule/profile mismatch
  - `/dataset_profiles/validate` 먼저 확인
- artifact/path mismatch
  - dataset version metadata와 plan input의 ref를 같이 확인
- worker task failure
  - 관련 step artifact가 비어 있는지
  - build job이나 execution event에 error가 남았는지 확인

복구 방법:

1. 설정 문제 수정
2. 필요한 build 재실행
3. 기존 execution은 `rerun` 또는 새 plan execute

## 5. control plane 재시작 이후

현재 control plane은 startup reconciliation을 수행한다.

- `queued/running` build job 재-dispatch
- `queued/running` execution 재-enqueue
- `waiting` execution dependency 재평가

즉 재시작 직후에는 바로 수동 조작하지 말고, 먼저 build job / execution 상태가 다시 움직이는지 확인한다.

확인 필요:

- 현재 dev compose는 `TEMPORAL_PERSISTENCE_MODE=dev_ephemeral`, `TEMPORAL_RETENTION_MODE=temporal_dev_default`, `TEMPORAL_RECOVERY_MODE=startup_reconciliation` 기준이다.
- `GET /runtime_status`로 현재 런타임 보장 범위를 확인할 수 있다.
- 앱 상태는 Postgres / artifact storage 기준으로 복구되지만, Temporal 자체 history 장기 보존 정책은 별도 운영 환경에서 다시 고정해야 한다.
