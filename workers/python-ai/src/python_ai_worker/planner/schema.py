from __future__ import annotations

"""plan_v2 schema — planner LLM이 만드는 plan의 형태 + standard table·skill 카탈로그.

silverone 2026-05-21 결정:
- multi-table input: docs / clauses / genuineness (RESERVED)
- skill 8개: join / filter / aggregate / compare / calculate / sort / present / summarize
- executor backend: DuckDB
- summarize는 plan step skill, final_answer는 별도 wrapper
- plan storage: 기존 skill_plans 테이블 + plan_version 분기
- ``input`` 필드는 standard table name 또는 이전 step id (둘 다 허용). table name은
  RESERVED라 step id로 사용 금지.
"""

from dataclasses import dataclass, field

from ..taxonomies import load_taxonomy
# Skill Contract v2 Step 2 (silverone 2026-06-04) — calculate/present의 prompt
# params_schema는 skill_specs.py의 spec에서 생성한다 (단일 source). 생성값이 기존
# 하드코딩과 byte 동일함은 test_skill_specs가 잠근다 → prompt 출력 불변.
from .skill_specs import CALCULATE_SPEC, PRESENT_SPEC, render_params_schema


PLAN_VERSION = "v2"

# taxonomy-driven config Phase 3-A (2026-05-27) — clauses.aspect description을
# config/taxonomies/festival-v2.json에서 derive. Phase 3-B에서 dataset_version
# metadata 기반 동적 lookup으로 전환 예정 — 현재는 single taxonomy 고정.
_FESTIVAL_TAXONOMY = load_taxonomy("festival-v2")

# step id로 사용 금지. multi-table input ``input`` 필드와의 충돌을 방지하기 위해
# 예약한다. clause_keywords는 optional artifact(키워드 build이 돈 dataset에만 존재)지만
# 이름 충돌 방지를 위해 항상 예약어로 둔다. 실제 prompt 노출은 artifact가 있을 때만
# (render_reserved_extra_tables) — 없는 dataset에서 planner가 쓰지 않게.
RESERVED_INPUT_NAMES: frozenset[str] = frozenset(
    {"docs", "clauses", "genuineness", "clause_keywords"}
)


@dataclass(frozen=True)
class ColumnSpec:
    """table column 정의. planner prompt에 그대로 노출된다."""

    name: str
    type: str
    description: str = ""


@dataclass(frozen=True)
class TableSchema:
    """standard input table 정의. plan step의 ``input``에서 이름으로 직접 참조."""

    name: str
    description: str
    columns: tuple[ColumnSpec, ...]
    dynamic_columns: bool = False  # True면 dataset마다 추가 컬럼이 있음 (runtime 주입)


@dataclass(frozen=True)
class SkillSpec:
    """plan_v2의 skill 정의. ``params_schema``는 planner prompt용 informal docs.
    실제 검증은 planner.validator에서 skill별 hardcoded rule로 한다."""

    name: str
    description: str
    input_type: str  # table | table_pair
    output_type: str  # table | presentation | text
    params_schema: dict[str, str] = field(default_factory=dict)


# dataset_build 파이프라인 결과를 plan_v2가 보는 형태로 표준화한 3 standard table.
TABLE_SCHEMAS: dict[str, TableSchema] = {
    "docs": TableSchema(
        name="docs",
        description="clean 단계 결과. doc 단위. dataset별 원본 컬럼이 추가로 보존된다.",
        columns=(
            ColumnSpec("doc_id", "string", "doc 고유 식별자"),
            ColumnSpec("row_id", "string", "source row id (dataset_version_id 포함)"),
            ColumnSpec("raw_text", "string", "원본 텍스트 결합 결과"),
            ColumnSpec("cleaned_text", "string", "noise scrub + regex rule 적용 결과"),
            # clean 단계가 dataset별 원본 날짜 컬럼(pub_year/pub_month/pub_day,
            # published_at, posted_at 등)을 ``created_at`` ISO 8601 timestamp로
            # 표준화하는 책임을 진다. 현재 clean 단계는 아직 이 표준화를 하지
            # 않으므로 plan_v2 도입 시점에 정렬 작업이 필요하다.
            ColumnSpec("created_at", "timestamp", "원본 게시 시각. clean 단계에서 dataset별 날짜 컬럼을 표준화."),
        ),
        dynamic_columns=True,
    ),
    # 후속 ``clause_label_v2``에서 확장 가능한 optional 컬럼:
    # - ``scope`` (string): festival_direct | festival_adjacent | unrelated.
    #   연관지(adjacent) 분석 도입 시 LLOA prompt에서 함께 라벨링한다.
    # 추가 시점에는 lock test도 같이 갱신.
    "clauses": TableSchema(
        name="clauses",
        description="clause_label 단계 결과. doc 단위 LLOA 호출로 절 분리 + sentiment + aspect 라벨링.",
        columns=(
            ColumnSpec("doc_id", "string", "docs.doc_id와 join 가능"),
            # clause_label artifact에는 ``clause_id``가 없다. executor가 적재 시
            # (doc_id, row_number) 또는 hash 기반으로 생성한 row 식별자를 노출하는
            # 전제. clause 단위 count / evidence trace / UI drill-down에 사용.
            ColumnSpec("clause_id", "string", "clause row 식별자 — executor가 적재 시 생성 (doc_id + row_number / hash)"),
            ColumnSpec("clause", "string", "분리된 절 본문"),
            ColumnSpec("sentiment", "string", "positive | neutral | negative"),
            ColumnSpec("aspect", "string", " | ".join(_FESTIVAL_TAXONOMY.aspect_keys)),
            ColumnSpec("prompt_version", "string", "라벨링에 사용된 prompt 버전"),
            ColumnSpec("source", "string", "라벨링 호출 식별자 (lloa / fallback 등)"),
        ),
        dynamic_columns=False,
    ),
    "genuineness": TableSchema(
        name="genuineness",
        description="doc_genuineness 단계 결과. doc-level 3-tier 진성 분류.",
        columns=(
            ColumnSpec("doc_id", "string", "docs.doc_id와 join 가능"),
            ColumnSpec("genuineness", "string", "genuine_review | mixed | non_review"),
            ColumnSpec("reason", "string", "분류 사유 (LLM 출력)"),
            ColumnSpec("prompt_version", "string", "분류에 사용된 prompt 버전"),
            ColumnSpec("source", "string", "분류 호출 식별자"),
        ),
        dynamic_columns=False,
    ),
    # optional — dataset_clause_keywords build이 돈 dataset/버전에만 존재. long-format
    # (절-키워드 1행). system(cache) 영역 standard table에는 넣지 않고, artifact가 있을
    # 때만 user 영역에서 노출(render_reserved_extra_tables). silverone 2026-06-10.
    "clause_keywords": TableSchema(
        name="clause_keywords",
        description="clause_keywords 단계 결과. 절-키워드 long-format(한 행 = 한 절의 한 키워드). 키워드 집계/순위용.",
        columns=(
            ColumnSpec("doc_id", "string", "docs.doc_id와 join 가능"),
            ColumnSpec("clause_id", "string", "clauses.clause_id와 동일 규칙 — 절 단위 식별자"),
            ColumnSpec("clause", "string", "키워드가 추출된 절 본문"),
            ColumnSpec("sentiment", "string", "positive | neutral | negative (절 sentiment)"),
            ColumnSpec("aspect", "string", " | ".join(_FESTIVAL_TAXONOMY.aspect_keys)),
            ColumnSpec("keyword", "string", "절에서 추출된 핵심 명사 키워드 (집계 대상)"),
            ColumnSpec("extractor_version", "string", "키워드 추출기 버전"),
        ),
        dynamic_columns=False,
    ),
}


# 초기 skill set 8개. docs/clauses 분리로 인해 join이 핵심에 포함.
SKILL_CATALOG: dict[str, SkillSpec] = {
    "join": SkillSpec(
        name="join",
        description="두 table을 키 컬럼 기준으로 결합한다. docs/clauses 분리 흐름에서 흔히 다대일 join이 발생.",
        input_type="table_pair",
        output_type="table",
        params_schema={
            "left": "table_or_step_id",
            "right": "table_or_step_id",
            "on": "string[]",
            "how": "inner|left|right|outer",
        },
    ),
    "filter": SkillSpec(
        name="filter",
        description="조건에 맞는 row를 추출한다.",
        input_type="table",
        output_type="table",
        params_schema={
            "input": "table_or_step_id",
            "column": "string",
            "operator": "eq|neq|in|not_in|gt|gte|lt|lte|between|contains|is_null|not_null",
            "value": "any",
        },
    ),
    "aggregate": SkillSpec(
        name="aggregate",
        description="group_by 기준으로 count/sum/avg/min/max 같은 집계를 수행한다.",
        input_type="table",
        output_type="table",
        params_schema={
            "input": "table_or_step_id",
            "group_by": "string[]",
            "metrics": "metric[] — {name, function: count|sum|avg|min|max, column}",
        },
    ),
    "compare": SkillSpec(
        name="compare",
        description="두 집계 결과를 join_key 기준으로 합쳐 left/right 라벨로 컬럼을 prefix한다.",
        input_type="table_pair",
        output_type="table",
        params_schema={
            "left": "table_or_step_id",
            "right": "table_or_step_id",
            "join_key": "string[]",
            "left_label": "string",
            "right_label": "string",
        },
    ),
    "calculate": SkillSpec(
        name="calculate",
        description="파생 컬럼을 추가한다. 사칙연산 + percent_change + ratio + share_of_total.",
        input_type="table",
        output_type="table",
        params_schema=render_params_schema(CALCULATE_SPEC),
    ),
    "sort": SkillSpec(
        name="sort",
        description="컬럼 기준으로 정렬하고 옵션으로 상위 N개를 추출한다.",
        input_type="table",
        output_type="table",
        params_schema={
            "input": "table_or_step_id",
            "by": "string[]",
            "order": "asc|desc (기본 desc)",
            "limit": "int|null",
        },
    ),
    "present": SkillSpec(
        name="present",
        description="결과를 사용자에게 보여줄 형식 (표/차트/json)으로 변환한다. plan의 최종 결과 step에서 사용.",
        input_type="table",
        output_type="presentation",
        params_schema=render_params_schema(PRESENT_SPEC),
    ),
    "summarize": SkillSpec(
        name="summarize",
        description="수치 결과를 부분적으로 자연어로 설명한다. 최종 답변 wrapper(final_answer)와 별개의 plan step 단위 요약.",
        input_type="table",
        output_type="text",
        params_schema={
            "input": "table_or_step_id",
            "focus": "string — 요약 관점",
            "prompt_version": "string|null",
        },
    ),
}


# validator R4-A (2026-05-27) — skill param enum 단일 source.
#
# 옛 위치는 validator.py module-level frozenset 6종 (`_FILTER_OPERATORS` /
# `_JOIN_HOWS` / `_AGGREGATE_FUNCTIONS` / `_CALCULATE_OPERATIONS` /
# `_SORT_ORDERS` / `_PRESENT_FORMATS`). plan_v2 wire contract의 enum이므로
# schema.py가 source. validator는 import해서 set membership 검증에 사용한다.
#
# SKILL_CATALOG.params_schema의 string ("eq|neq|...")은 prompt에 그대로 노출
# 되므로 hand-written 그대로 보존 — string ordering이 cache hit에 영향. enum
# 상수와 params_schema string의 정합성은 test_planner_schema가 잠근다.
#
# 본 PR(R4-A)에서는 *위치만 통합*. 값 변경 없음 — 기존 허용 enum 그대로.
FILTER_OPERATORS: frozenset[str] = frozenset(
    {"eq", "neq", "in", "not_in", "gt", "gte", "lt", "lte", "between", "contains", "is_null", "not_null"}
)
JOIN_HOWS: frozenset[str] = frozenset({"inner", "left", "right", "outer"})
AGGREGATE_FUNCTIONS: frozenset[str] = frozenset({"count", "sum", "avg", "min", "max"})
CALCULATE_OPERATIONS: frozenset[str] = frozenset(
    {"add", "subtract", "multiply", "divide", "percent_change", "ratio", "share_of_total"}
)
SORT_ORDERS: frozenset[str] = frozenset({"asc", "desc"})
PRESENT_FORMATS: frozenset[str] = frozenset({"table", "chart", "json"})

# reject reason taxonomy (silverone 2026-06-01, PR1) — planner가 answerable=false
# 로 거절할 때의 사유 분류. v1은 3종:
#   out_of_dataset_scope    — 선택 데이터셋과 무관한 외부/일반 질문 (날씨/시각/맛집)
#   unsupported_skill       — 데이터셋 관련은 있으나 현 skill set으로 불가
#                             (클러스터링/원인 설명 등). capability_gap 동반 → skill
#                             backlog 저장 후보(PR2).
#   missing_data_or_artifact — 지원 분석 유형이지만 필요한 컬럼/아티팩트/build 부재
# 후속 후보: ambiguous_question / unsafe_or_disallowed / planner_failed.
REJECT_REASONS: frozenset[str] = frozenset(
    {"out_of_dataset_scope", "unsupported_skill", "missing_data_or_artifact"}
)

# silverone 2026-06-09 — system이 생성하는 거절 사유(planner가 emit하지 않음).
#   planner_validation_error — planner가 repair 후에도 유효 plan 실패 (raw 500 대신 거절)
#   execution_error          — executor(DuckDB) 실행 실패를 graceful 거절로 렌더
# validator는 planner-emitted(REJECT_REASONS)만 검증하고, composer 거절 렌더는
# ALL_REJECT_REASONS(둘의 합집합)를 단일 source로 쓴다.
SYSTEM_REJECT_REASONS: frozenset[str] = frozenset(
    {"planner_validation_error", "execution_error"}
)
ALL_REJECT_REASONS: frozenset[str] = REJECT_REASONS | SYSTEM_REJECT_REASONS


# validator R3 (2026-05-27) — column type 분류 단일 source.
#
# 옛 위치는 validator.py module-level constants (`_NUMERIC_TYPES` /
# `_TIMESTAMP_TYPES` / `_TEXT_TYPES` / `_RESERVED_COLUMN_TYPES` /
# `_RESERVED_STRING_COLUMNS`). schema 정의와 type 분류는 한 모듈에 있어야 자연
# 스러우므로 schema.py로 모은다. validator는 import해서 사용.
#
# 수치 비교/연산에 안전한 type set.
NUMERIC_COLUMN_TYPES: frozenset[str] = frozenset(
    {"integer", "long", "float", "double", "number", "numeric"}
)
# 시간 비교에 안전한 type set. 문자열 값은 executor가 CAST.
TIMESTAMP_COLUMN_TYPES: frozenset[str] = frozenset({"timestamp", "date", "datetime"})
# text 비교(contains)에 안전한 type set.
TEXT_COLUMN_TYPES: frozenset[str] = frozenset({"string", "text", "varchar"})


# RESERVED 테이블별 컬럼 type 메타. TABLE_SCHEMAS에서 파생 — schema 정의가
# 바뀌면 자동으로 따라간다. 정적 검증(calculate 수치 expression, aggregate
# sum/avg, filter type, sort by 등)에서 false-positive 회피용으로
# input=RESERVED root case에만 사용한다. chain을 거친 step 출력
# (aggregate/compare/join/sort)은 type 추적이 복잡해 1차 제외.
RESERVED_COLUMN_TYPES: dict[str, dict[str, str]] = {
    table_name: {col.name: col.type for col in schema.columns}
    for table_name, schema in TABLE_SCHEMAS.items()
}
# 편의: RESERVED 테이블별 string 컬럼 set. calculate expression이 RESERVED
# string column을 참조하는 케이스 reject(SQL-2.3)에 사용.
RESERVED_STRING_COLUMNS: dict[str, frozenset[str]] = {
    table_name: frozenset(name for name, type_ in cols.items() if type_ == "string")
    for table_name, cols in RESERVED_COLUMN_TYPES.items()
}


__all__ = [
    "PLAN_VERSION",
    "RESERVED_INPUT_NAMES",
    "TABLE_SCHEMAS",
    "SKILL_CATALOG",
    "ColumnSpec",
    "TableSchema",
    "SkillSpec",
    "NUMERIC_COLUMN_TYPES",
    "TIMESTAMP_COLUMN_TYPES",
    "TEXT_COLUMN_TYPES",
    "RESERVED_COLUMN_TYPES",
    "RESERVED_STRING_COLUMNS",
    "FILTER_OPERATORS",
    "JOIN_HOWS",
    "AGGREGATE_FUNCTIONS",
    "CALCULATE_OPERATIONS",
    "SORT_ORDERS",
    "PRESENT_FORMATS",
    "REJECT_REASONS",
    "SYSTEM_REJECT_REASONS",
    "ALL_REJECT_REASONS",
]
