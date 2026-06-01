"""plan_v2 — LLM main planner + deterministic skill executor 구조의 entrypoint.

2026-05-21 silverone 결정으로 도입. 기존 ``planner/``는 4단계 우회 endpoint
도입 시점까지 보존하고, plan_v2는 별도 모듈로 살림.
"""

from .llm import (
    PlannerCallError,
    PlannerParseError,
    PlannerResult,
    PlannerValidationError,
    generate_plan,
)
from .prompt import (
    DEFAULT_PLANNER_V2_PROMPT_VERSION,
    DatasetSpecificColumn,
    render_conversation_context,
    render_dataset_specific_columns,
    render_planner_prompt,
    render_skill_catalog,
    render_table_schemas,
)
from .schema import (
    PLAN_VERSION,
    RESERVED_INPUT_NAMES,
    SKILL_CATALOG,
    TABLE_SCHEMAS,
    ColumnSpec,
    SkillSpec,
    TableSchema,
)
from .validator import (
    PlanValidationError,
    ValidationIssue,
    collect_plan_issues,
    validate_plan,
)

__all__ = [
    "PLAN_VERSION",
    "RESERVED_INPUT_NAMES",
    "SKILL_CATALOG",
    "TABLE_SCHEMAS",
    "ColumnSpec",
    "SkillSpec",
    "TableSchema",
    "PlanValidationError",
    "ValidationIssue",
    "collect_plan_issues",
    "validate_plan",
    "DEFAULT_PLANNER_V2_PROMPT_VERSION",
    "DatasetSpecificColumn",
    "render_conversation_context",
    "render_dataset_specific_columns",
    "render_planner_prompt",
    "render_skill_catalog",
    "render_table_schemas",
    "PlannerCallError",
    "PlannerParseError",
    "PlannerResult",
    "PlannerValidationError",
    "generate_plan",
]
