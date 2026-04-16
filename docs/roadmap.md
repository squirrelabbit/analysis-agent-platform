# 후속 메모

이 문서는 현재 상태의 authoritative source가 아니다.
현재 구현 상태는 [project_summary.md](project_summary.md), 운영 절차는 [operations/local_runbook.md](operations/local_runbook.md), API 계약은 [api/openapi.yaml](api/openapi.yaml)을 우선 본다.

지금 남은 큰 후속 후보:

1. `final_answer` 품질 보정
2. subset cluster fallback 성능과 저장 전략 재검토
3. 외부 metrics / tracing / alert 연동
4. 운영 환경의 Temporal persistence / retention 고정
5. `확인 필요:` Rust hot path 도입 여부 판단

즉, 이 문서는 backlog 메모 정도로만 유지한다.
