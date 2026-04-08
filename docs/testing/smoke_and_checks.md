# 테스트와 Smoke 가이드

## 1. 코드 레벨 검증

```bash
cd apps/control-plane && go test ./...
PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'
PYTHONPATH=workers/python-ai/src python3 -m python_ai_worker.devtools.run_skill_case --validate
```

필요하면 OpenAPI 파싱까지 확인한다.

```bash
ruby -e 'require "yaml"; YAML.load_file("docs/api/openapi.yaml"); puts "ok"'
```

## 2. 주요 smoke script

| 스크립트 | 목적 |
| --- | --- |
| `apps/control-plane/dev/smoke.sh` | 기본 request -> plan -> execute 흐름 확인 |
| `apps/control-plane/dev/smoke_auto_resume_sentiment.sh` | `prepare -> sentiment -> auto resume` 확인 |
| `apps/control-plane/dev/smoke_auto_resume_embedding.sh` | `prepare -> embedding -> auto resume` 확인 |
| `apps/control-plane/dev/smoke_final_answer.sh` | execution 완료 뒤 `final_answer` 생성 확인 |
| `apps/control-plane/dev/smoke_semantic.sh` | `semantic_search`와 `pgvector` retrieval 확인 |
| `apps/control-plane/dev/smoke_cluster.sh` | cluster build와 `embedding_cluster` 결과 확인 |
| `apps/control-plane/dev/smoke_sentiment.sh` | 감성 요약 경로 기본 확인 |
| `apps/control-plane/dev/smoke_trend.sh` | 추세 분석 경로 기본 확인 |
| `apps/control-plane/dev/smoke_compare.sh` | period compare 경로 기본 확인 |
| `apps/control-plane/dev/smoke_breakdown.sh` | meta group / breakdown 경로 확인 |
| `apps/control-plane/dev/smoke_taxonomy.sh` | rule-based taxonomy 경로 확인 |

## 3. 추천 검증 순서

1. 코드 테스트 3종
2. `smoke.sh`
3. `smoke_auto_resume_sentiment.sh`
4. `smoke_auto_resume_embedding.sh`
5. `smoke_final_answer.sh`
6. 필요 시 `smoke_semantic.sh`, `smoke_cluster.sh`

도메인별 skill smoke는 변경 범위가 닿았을 때만 추가로 돌린다.

## 4. 로컬 임베딩 평가

```bash
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.evaluate_embedding_model --model intfloat/multilingual-e5-small --format markdown
```

이 평가는 search top-k와 cluster 품질 회귀를 볼 때 사용한다.

## 5. 확인 메모

- smoke script는 가능하면 `/uploads` API를 사용해 host/container 경로 차이를 줄인다.
- `smoke_cluster.sh`는 full-dataset cluster materialization 경로를 우선 본다.
- `확인 필요:` 대용량 fixture 기준 메모리 회귀 smoke는 아직 고정되지 않았다.
