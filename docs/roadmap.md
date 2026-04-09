# 앞으로 해야 할 작업

이 문서는 현재 구현 상태를 기준으로, **프론트를 제외한 백엔드 우선순위**만 다시 정리한 실행용 로드맵이다.
이미 끝난 작업 목록보다, 지금 남아 있는 리스크와 다음 순서를 짧게 유지한다.
코드나 문서로 확정되지 않은 항목은 `확인 필요:`로 표기한다.

## 현재 기준

- strict 시나리오 등록/import/plan/execute는 동작한다.
- `prepare`는 eager, `sentiment / embedding / cluster`는 lazy build를 기본 정책으로 사용한다.
- dataset build는 Temporal workflow와 async build job으로 실행된다.
- execution은 `events`, `progress`, `step preview`, `result`, `final_answer` 조회 API를 제공한다.
- full-dataset `embedding_cluster`는 precomputed cluster를 우선 사용하고, cluster 산출물은 `summary JSON + membership parquet`로 분리 저장한다.
- cluster 결과에는 `cluster_execution_mode`와 `cluster_fallback_reason`가 포함돼 materialized/full-dataset 경로와 subset fallback 경로를 구분할 수 있다.

## 지금 가장 먼저 볼 것

1. `final_answer` 품질 보정
2. cluster subset fallback / membership 저장 전략 안정화
3. progress / partial artifact 저장 부담 점검
4. profile / prompt catalog API
5. Temporal retention / observability

## Step 1. `final_answer` 품질 보정

목표:
- `result_v1`와 `final_answer`가 어긋나지 않게 하고, 사용자에게 보이는 최종 답변 품질을 먼저 잠근다.

할 일:
1. representative dataset 기준 `final_answer` 회귀 케이스를 늘린다.
2. 과장된 문장, 근거 밖 주장, evidence 누락을 점검한다.
3. 필요하면 `execution-final-answer-v1.md` prompt를 보정한다.
4. 확인 필요: dataset/profile별 `answer_prompt_version` override가 필요한지 판단한다.

결과물:
- 안정된 `final_answer` 품질 기준
- prompt 보정안
- smoke / regression 강화

## Step 2. cluster 후속 안정화

목표:
- 현재 닫아둔 cluster materialization 경로를 실제 운영 부하 기준으로 더 안정화한다.

할 일:
1. subset/filter가 붙는 cluster 질문의 on-demand fallback 성능을 다시 본다.
2. `cluster_membership.parquet`를 계속 sidecar parquet로 둘지, DB 승격이 필요한지 판단한다.
3. `top_terms` 기반 cluster label 품질을 실제 데이터 기준으로 재검토한다.
4. cluster detail/evidence 응답 레이어가 더 필요한지 판단한다.

결과물:
- cluster fallback 운영 기준
- membership 저장 전략 결정
- label 품질 보정 메모

주의:
- 확인 필요: cluster 조회가 프론트 drill-down에서 자주 일어나면 sidecar parquet만으로 충분하지 않을 수 있다.

## Step 3. progress / partial artifact 저장 부담 점검

목표:
- step 완료 시 artifact를 execution row에 즉시 저장하는 구조가 큰 플랜에서 부담이 되는지 확인한다.

할 일:
1. artifact 크기와 execution row 저장량을 샘플 dataset 기준으로 측정한다.
2. partial artifact를 execution metadata에 계속 저장할지, 요약본만 남길지 기준을 정한다.
3. 확인 필요: artifact가 큰 skill은 `artifact_ref + preview`만 execution에 남기는 방식이 더 나은지 검토한다.

결과물:
- execution progress 저장 정책
- 큰 artifact 처리 기준

## Step 4. profile / prompt catalog API

목표:
- 현재 `validate`만 있는 profile/prompt 운영 surface를 목록형 API까지 확장한다.

할 일:
1. `dataset profile` 목록 조회 API
2. prompt catalog 조회 API
3. rule catalog 조회 API
4. profile 변경 절차와 prompt 변경 절차를 운영 문서에 맞춘다.

결과물:
- 운영/프론트에서 재사용 가능한 catalog API
- profile/prompt 관리 절차

## Step 5. Temporal retention / observability

목표:
- dev server 기본값에 기대고 있는 workflow history와 운영 관측성을 정리한다.

할 일:
1. Temporal history persistence / retention 정책을 결정한다.
2. build queue concurrency와 worker 자원 상한을 운영 환경 기준으로 다시 튜닝한다.
3. metrics / tracing / alert 기준을 정리한다.
4. 장애 대응 문서와 실제 운영값을 맞춘다.

결과물:
- retention / persistence 기준
- 운영 관측성 기준
- resource tuning 메모

## 보류 가능

- planning async job/status
- guided scenario planner
- Rust hot path 이관
- human-in-the-loop / review queue

위 항목은 현재 백엔드 핵심 흐름을 막는 일은 아니다. 정말 planner latency나 운영 부하가 문제가 될 때 다시 본다.
