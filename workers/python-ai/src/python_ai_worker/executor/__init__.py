"""plan_v2 executor — DuckDB 기반 deterministic skill executor.

silverone 2026-05-21 3단계 결정:
- (artifact_paths, plan) input contract
- in-memory DuckDB session에 docs/clauses/genuineness view 등록
- step output은 step_id 이름의 temp view로 누적
- clauses.clause_id 자동 생성 (doc_id + ROW_NUMBER OVER PARTITION BY doc_id)
- docs.created_at은 표준 timestamp 필수 — 없거나 cast 실패 시 명확한 error
- 1차 범위: join / filter / aggregate / compare / calculate / sort / present
  (summarize는 LLM 호출 필요 — 후속)
"""

from .context import ArtifactPaths, ExecutorContext, ExecutorContextError
from .runner import ExecutionStepResult, ExecutorError, execute_plan
from .service import (
    ArtifactPathResolutionError,
    coerce_artifact_paths_payload,
    execute_analyze_plan,
    plan_and_execute_analyze,
    plan_from_question,
)

__all__ = [
    "ArtifactPaths",
    "ArtifactPathResolutionError",
    "ExecutionStepResult",
    "ExecutorContext",
    "ExecutorContextError",
    "ExecutorError",
    "coerce_artifact_paths_payload",
    "execute_analyze_plan",
    "execute_plan",
    "plan_and_execute_analyze",
    "plan_from_question",
]
