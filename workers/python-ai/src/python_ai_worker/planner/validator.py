from __future__ import annotations

"""plan_v2 validator — LLM planner 결과를 executor에 넘기기 전에 invariant 검증.

silverone 2026-05-21 결정 5 일반 규칙:
1. ``input``은 standard table name 또는 이전 step id만 허용
2. 존재하지 않는 input 참조는 validation error
3. step id 중복 금지
4. step id는 RESERVED_INPUT_NAMES (docs/clauses/genuineness) 사용 금지
5. 현재 step보다 뒤에 있는 step id 참조 금지

+ skill별 hardcoded rule (operator/function enum, params required key 셋 등).

Column 존재 검증은 input이 standard table을 직접 가리키는 경우만 한다.
step output schema 추적은 v2 후속 작업 (executor 통합 시).
"""

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable

from ..sql_identifiers import SAFE_SQL_IDENTIFIER_RE
from .schema import (
    AGGREGATE_FUNCTIONS,
    CALCULATE_OPERATIONS,
    FILTER_OPERATORS,
    JOIN_HOWS,
    NUMERIC_COLUMN_TYPES,
    PLAN_VERSION,
    REJECT_REASONS,
    RESERVED_COLUMN_TYPES,
    RESERVED_INPUT_NAMES,
    RESERVED_STRING_COLUMNS,
    SKILL_CATALOG,
    SORT_ORDERS,
    TABLE_SCHEMAS,
    TEXT_COLUMN_TYPES,
    TIMESTAMP_COLUMN_TYPES,
)
from .skill_specs import CALCULATE_SPEC
from .recipes import RUNTIME_ENABLED_RECIPES


# step id는 executor에서 DuckDB temp view 이름으로 직접 사용된다.
# SQL identifier로 안전한 형태만 허용 (validator R2, 단일 source).


# silverone 2026-05-27 (validator R3) — column type 분류 상수는 planner/schema.py로
# 이전했다. 이 module에서는 import 후 그대로 사용.


# ===== validator error types =====


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    step_id: str = ""
    step_index: int = -1


class PlanValidationError(ValueError):
    """plan invariant 위반. ``issues`` 필드에 발견한 모든 issue를 담는다."""

    def __init__(self, issues: list[ValidationIssue]):
        self.issues = list(issues)
        summary = "; ".join(f"[{issue.code}] {issue.message}" for issue in issues)
        super().__init__(summary or "plan validation failed")


# ===== skill param enum =====
#
# validator R4-A (2026-05-27) — enum 상수는 planner/schema.py로 이전 (단일
# source). validator는 import해서 set membership 검증에 사용한다.
#
# Skill Contract v2 Step 1 (silverone 2026-06-04) — calculate operation의 required
# param / 컬럼-ref 사실은 이제 planner/skill_specs.CALCULATE_SPEC가 단일 source다.
# 아래 lookup들은 spec에서 *파생*한다 (행동 변화 0 — 기존 값과 동일):
#   - _CALCULATE_OP_REQUIRED_KEYS: required 키가 있는 operation만 (ratio 제외 — ratio는
#     alt_required로 표현돼 여기 안 들어감, 기존과 동일)
#   - _CALCULATE_RATIO_ALT_REQUIRED: ratio의 '둘 중 한 묶음' 그룹
#   - _CALCULATE_NUMERIC_REF_KEYS: 컬럼 존재/타입 검증 대상 scalar 컬럼 param(첫 등장
#     순서 = (left,right,base,current,numerator,denominator,value))
_CALCULATE_OP_REQUIRED_KEYS: dict[str, tuple[str, ...]] = {
    op.name: op.required for op in CALCULATE_SPEC.operations if op.required
}
_CALCULATE_RATIO_ALT_REQUIRED: tuple[tuple[str, ...], ...] = next(
    (op.alt_required for op in CALCULATE_SPEC.operations if op.name == "ratio"), ()
)

_TOP_N_FILTER_OPS: frozenset[str] = frozenset(
    {
        "=",
        "==",
        "eq",
        "!=",
        "neq",
        ">",
        "gt",
        ">=",
        "gte",
        "<",
        "lt",
        "<=",
        "lte",
        "in",
        "not_in",
        "contains",
    }
)


def _ordered_calculate_numeric_ref_keys() -> tuple[str, ...]:
    seen: list[str] = []
    for op in CALCULATE_SPEC.operations:
        for key in op.numeric_refs:
            if key not in seen:
                seen.append(key)
    return tuple(seen)


_CALCULATE_NUMERIC_REF_KEYS: tuple[str, ...] = _ordered_calculate_numeric_ref_keys()

# silverone 2026-05-26 (prefix contract B안) — aggregate metric name에 비교
# label prefix를 넣으면 compare가 left_label/right_label로 prefix를 다시 붙여
# `last_last_count` 같은 중복 prefix가 생긴다. metric name은 generic name
# (count, sum_value 등)으로 강제하고, 비교 label은 compare에서만 부여한다.
_LABEL_PREFIX_PATTERN = re.compile(
    r"^(last|this|prev|previous|curr|current|year|month|baseline)_",
    re.IGNORECASE,
)


# silverone 2026-05-26 (SQL-1.4, audit C6) — compare가 `{label}_{column}`
# alias로 SQL identifier를 만들기 때문에 label 자체가 identifier-safe해야
# 한다 (executor/skills/base.py:safe_identifier). 한국어·공백·하이픈·숫자
# 시작은 모두 ExecutorError로 빌드 실패하므로 validator가 미리 reject.
# validator R2 (2026-05-27) — step id와 같은 SAFE_SQL_IDENTIFIER_RE를 공유한다.


# ===== public entrypoints =====


def validate_plan(plan: dict[str, Any]) -> None:
    """plan을 검증한다. issue가 하나라도 발견되면 ``PlanValidationError``를 raise."""

    issues = collect_plan_issues(plan)
    if issues:
        raise PlanValidationError(issues)


def collect_plan_issues(plan: dict[str, Any]) -> list[ValidationIssue]:
    """plan을 검증하고 발견한 모든 issue를 리스트로 돌려준다. raise하지 않음."""

    issues: list[ValidationIssue] = []
    if not isinstance(plan, dict):
        return [ValidationIssue(code="plan.not_object", message="plan must be a JSON object")]

    version = str(plan.get("plan_version") or "").strip()
    if version != PLAN_VERSION:
        issues.append(
            ValidationIssue(
                code="plan.version_mismatch",
                message=f"plan_version must be '{PLAN_VERSION}', got '{version or '<missing>'}'",
            )
        )

    # silverone 2026-06-01 (PR1) — reject plan: answerable=false면 step skill 검증
    # 대신 reason/message/steps-empty만 검증하고 끝낸다. answerable 미지정/true는
    # 기존 step 검증 흐름 그대로(하위 호환).
    if plan.get("answerable") is False:
        return _collect_reject_plan_issues(plan, issues)

    raw_steps = plan.get("steps")
    if not isinstance(raw_steps, list):
        issues.append(
            ValidationIssue(code="plan.steps_not_list", message="plan.steps must be a JSON array")
        )
        return issues

    if not raw_steps:
        issues.append(ValidationIssue(code="plan.steps_empty", message="plan.steps must not be empty"))
        return issues

    # 1차 패스: step shape + id rule
    seen_ids: dict[str, int] = {}
    # silverone 2026-05-26 (prefix-2) — calculate column 정적 검증에서 input
    # step의 output schema를 추론하려면 step body 전체 lookup이 필요하다.
    step_lookup: dict[str, dict[str, Any]] = {}
    for index, step in enumerate(raw_steps):
        if not isinstance(step, dict):
            issues.append(
                ValidationIssue(
                    code="step.not_object",
                    message=f"step #{index} must be a JSON object",
                    step_index=index,
                )
            )
            continue

        step_id = str(step.get("id") or "").strip()
        if not step_id:
            issues.append(
                ValidationIssue(
                    code="step.id_missing", message="step id is required", step_index=index
                )
            )
            continue
        if not SAFE_SQL_IDENTIFIER_RE.match(step_id):
            issues.append(
                ValidationIssue(
                    code="step.id_invalid",
                    message=(
                        f"step id '{step_id}' must match {SAFE_SQL_IDENTIFIER_RE.pattern} "
                        "(SQL identifier rule — DuckDB temp view에 사용됨)"
                    ),
                    step_id=step_id,
                    step_index=index,
                )
            )
            continue
        if step_id in RESERVED_INPUT_NAMES:
            issues.append(
                ValidationIssue(
                    code="step.id_reserved",
                    message=f"step id '{step_id}' collides with a reserved table name",
                    step_id=step_id,
                    step_index=index,
                )
            )
        if step_id in seen_ids:
            issues.append(
                ValidationIssue(
                    code="step.id_duplicated",
                    message=f"step id '{step_id}' duplicated (first at #{seen_ids[step_id]})",
                    step_id=step_id,
                    step_index=index,
                )
            )
            continue
        seen_ids[step_id] = index
        step_lookup[step_id] = step

    # 2차 패스: skill + params 검증 (id 셋 확정 후 input 참조 검사 가능)
    for index, step in enumerate(raw_steps):
        if not isinstance(step, dict):
            continue
        step_id = str(step.get("id") or "").strip()
        if not step_id or step_id in RESERVED_INPUT_NAMES:
            continue

        skill_name = str(step.get("skill") or "").strip()
        if not skill_name:
            issues.append(
                ValidationIssue(
                    code="step.skill_missing",
                    message="step skill is required",
                    step_id=step_id,
                    step_index=index,
                )
            )
            continue

        # Recipe step. validator 활성 recipe는 unknown으로 거절하지 않고 recipe
        # param을 검증한다. recipe step은 planner output에 남고, 실행 시
        # expand_recipes가 atomic으로 lower한다. 미활성 recipe는 아래 catalog 분기로
        # 떨어져 skill_unknown으로 거절된다.
        if skill_name in RUNTIME_ENABLED_RECIPES:
            params = step.get("params")
            if not isinstance(params, dict):
                issues.append(
                    ValidationIssue(
                        code="step.params_not_object",
                        message="step params must be a JSON object",
                        step_id=step_id,
                        step_index=index,
                    )
                )
                continue
            recipe_ctx = _StepContext(
                step_id=step_id,
                step_index=index,
                seen_ids=seen_ids,
                step_lookup=step_lookup,
                issues=issues,
            )
            _validate_recipe_params(skill_name, params, recipe_ctx)
            continue

        spec = SKILL_CATALOG.get(skill_name)
        if spec is None:
            issues.append(
                ValidationIssue(
                    code="step.skill_unknown",
                    message=f"skill '{skill_name}' is not in the catalog",
                    step_id=step_id,
                    step_index=index,
                )
            )
            continue

        params = step.get("params")
        if not isinstance(params, dict):
            issues.append(
                ValidationIssue(
                    code="step.params_not_object",
                    message="step params must be a JSON object",
                    step_id=step_id,
                    step_index=index,
                )
            )
            continue

        _validate_skill_params(
            skill_name=skill_name,
            params=params,
            step_id=step_id,
            step_index=index,
            seen_ids=seen_ids,
            step_lookup=step_lookup,
            issues=issues,
        )

    return issues


def _collect_reject_plan_issues(
    plan: dict[str, Any], issues: list[ValidationIssue]
) -> list[ValidationIssue]:
    """answerable=false plan 검증 (silverone 2026-06-01, PR1).

    - reason은 REJECT_REASONS 중 하나.
    - message는 비어 있으면 안 됨 (사용자 노출 문구).
    - steps는 비어 있어야 한다 (거절 plan은 실행 step 없음).
    - reason=unsupported_skill이면 capability_gap이 있으면 dict여야 한다 (v1은
      존재 시 shape만 가볍게 확인; 없어도 통과 — composer/PR2가 optional 처리).
    """
    reason = str(plan.get("reason") or "").strip()
    if reason not in REJECT_REASONS:
        issues.append(
            ValidationIssue(
                code="plan.reason_invalid",
                message=(
                    f"answerable=false plan.reason must be one of {sorted(REJECT_REASONS)}, "
                    f"got '{reason or '<missing>'}'"
                ),
            )
        )

    if not str(plan.get("message") or "").strip():
        issues.append(
            ValidationIssue(
                code="plan.reject_message_required",
                message="answerable=false plan must include a non-empty 'message'",
            )
        )

    steps = plan.get("steps")
    if steps not in (None, []) and not (isinstance(steps, list) and len(steps) == 0):
        issues.append(
            ValidationIssue(
                code="plan.reject_steps_not_empty",
                message="answerable=false plan.steps must be empty",
            )
        )

    capability_gap = plan.get("capability_gap")
    if capability_gap is not None and not isinstance(capability_gap, dict):
        issues.append(
            ValidationIssue(
                code="plan.capability_gap_not_object",
                message="plan.capability_gap must be a JSON object when present",
            )
        )

    return issues


# ===== skill 별 검증 =====


def _validate_skill_params(
    *,
    skill_name: str,
    params: dict[str, Any],
    step_id: str,
    step_index: int,
    seen_ids: dict[str, int],
    step_lookup: dict[str, dict[str, Any]],
    issues: list[ValidationIssue],
) -> None:
    ctx = _StepContext(
        step_id=step_id,
        step_index=step_index,
        seen_ids=seen_ids,
        step_lookup=step_lookup,
        issues=issues,
    )

    if skill_name == "join":
        _validate_join(params, ctx)
    elif skill_name == "filter":
        _validate_filter(params, ctx)
    elif skill_name == "aggregate":
        _validate_aggregate(params, ctx)
    elif skill_name == "compare":
        _validate_compare(params, ctx)
    elif skill_name == "calculate":
        _validate_calculate(params, ctx)
    elif skill_name == "sort":
        _validate_sort(params, ctx)
    elif skill_name == "present":
        _validate_present(params, ctx)
    elif skill_name == "summarize":
        _validate_summarize(params, ctx)


@dataclass
class _StepContext:
    step_id: str
    step_index: int
    seen_ids: dict[str, int]
    # silverone 2026-05-26 (prefix-2) — calculate column 정적 검증에서 input
    # step의 output schema를 추론하기 위해 모든 step body lookup을 보존한다.
    step_lookup: dict[str, dict[str, Any]] = field(default_factory=dict)
    issues: list[ValidationIssue] = field(default_factory=list)

    def issue(self, code: str, message: str) -> None:
        self.issues.append(
            ValidationIssue(code=code, message=message, step_id=self.step_id, step_index=self.step_index)
        )


def _check_required_keys(params: dict[str, Any], required: tuple[str, ...], ctx: _StepContext) -> bool:
    missing = [key for key in required if key not in params]
    if missing:
        ctx.issue(
            code="params.missing_keys",
            message=f"missing required params: {', '.join(missing)}",
        )
        return False
    return True


def _check_input_ref(
    value: Any,
    param_name: str,
    ctx: _StepContext,
    *,
    require_column: str | list[str] | None = None,
) -> None:
    """``input``/``left``/``right`` 같은 table 참조 필드를 검증한다.

    - standard table name이면 OK (옵션으로 column 존재 검사).
    - step id면 ctx.step_index보다 앞 step이어야 함.
    - 그 외 (존재하지 않는 이름)는 issue.
    """

    name = str(value or "").strip()
    if not name:
        ctx.issue(
            code="params.input_missing",
            message=f"'{param_name}' must reference a table or a prior step id",
        )
        return

    if name in TABLE_SCHEMAS:
        if require_column is not None:
            _check_columns_on_table(name, require_column, param_name, ctx)
        return

    source_index = ctx.seen_ids.get(name)
    if source_index is None:
        ctx.issue(
            code="params.input_unknown",
            message=f"'{param_name}' references unknown table or step id '{name}'",
        )
        return

    if source_index >= ctx.step_index:
        ctx.issue(
            code="params.input_forward_ref",
            message=f"'{param_name}' references step '{name}' that is not before the current step",
        )


def _check_columns_on_table(
    table_name: str,
    columns: str | list[str],
    param_name: str,
    ctx: _StepContext,
) -> None:
    schema = TABLE_SCHEMAS.get(table_name)
    if schema is None:
        return
    if schema.dynamic_columns:
        return  # dynamic 컬럼이 있는 table은 column 검증 보류 (runtime hydration)
    available = {column.name for column in schema.columns}
    requested = [columns] if isinstance(columns, str) else list(columns)
    for col in requested:
        col_name = str(col or "").strip()
        if not col_name:
            continue
        if col_name == "*":
            continue  # aggregate.metrics에서 count(*) 같은 wildcard 허용
        if col_name not in available:
            ctx.issue(
                code="params.column_unknown",
                message=f"'{param_name}' references unknown column '{col_name}' on table '{table_name}'",
            )


def _validate_join(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("left", "right", "on", "how"), ctx):
        return
    on_keys = params.get("on")
    if not isinstance(on_keys, list) or not on_keys:
        ctx.issue(code="params.on_not_list", message="join.on must be a non-empty list of column names")
        on_columns: list[str] = []
    else:
        # silverone 2026-05-26 (SQL-1.2, audit C2) — LLM이 가끔 `on`을
        # ``[{"left": "doc_id", "right": "doc_id"}]`` 같은 object-array로 만든다
        # (en variant Q4 회귀 사례). executor는 string[]만 지원하므로 SQL
        # 생성 시점에 실패한다 → validator가 사전에 reject.
        non_string = [
            idx for idx, key in enumerate(on_keys)
            if not isinstance(key, str) or not str(key).strip()
        ]
        if non_string:
            ctx.issue(
                code="params.on_not_string_list",
                message=(
                    "join.on must be a list of column-name strings "
                    f"(e.g. ['doc_id']); got non-string elements at indices {non_string}. "
                    "If both tables share the same key column, pass it as a plain string in the list."
                ),
            )
            on_columns = []
        else:
            on_columns = [str(key).strip() for key in on_keys]
    _check_input_ref(params.get("left"), "left", ctx, require_column=on_columns)
    _check_input_ref(params.get("right"), "right", ctx, require_column=on_columns)
    # silverone 2026-05-26 (SQL-3.3, audit M4) — step input의 경우도 inferred
    # output에 on_keys가 있는지 검증 (RESERVED 테이블은 _check_input_ref가 처리).
    for side in ("left", "right"):
        _check_input_columns_exist(
            input_ref=str(params.get(side) or "").strip(),
            required_columns=on_columns,
            ctx=ctx,
            issue_code="params.join_key_unknown",
            message_builder=lambda key, ref, available, _side=side: (
                f"join.{_side} step '{ref}'의 output에 key '{key}'가 없다. "
                f"available: {available}."
            ),
        )
    how = str(params.get("how") or "").strip()
    if how not in JOIN_HOWS:
        ctx.issue(
            code="params.how_invalid",
            message=f"join.how must be one of {sorted(JOIN_HOWS)}; got '{how}'",
        )


def _check_input_columns_exist(
    *,
    input_ref: str,
    required_columns: list[str],
    ctx: _StepContext,
    issue_code: str,
    message_builder: Callable[[str, str, list[str]], str],
) -> None:
    """step input의 inferred output에 required_columns가 모두 있는지 검증한다.

    R1 통합 helper (validator refactor 2026-05-27) — 옛
    ``_check_join_step_columns`` / ``_check_compare_step_columns`` /
    ``_check_sort_step_columns`` 3종을 본문이 동일해서 하나로 묶었다. issue
    code와 message는 callsite가 결정 (skill별 한국어 문구·side label 보존).

    SQL-3.3(M4, join) / SQL-3.4(M6, sort) / SQL-6.1(Q4 audit, compare)가 이
    helper의 호출자다. RESERVED 테이블 input / inference 불가 / 빈 required는
    false positive 방지로 skip — ``_check_input_ref``가 RESERVED 컬럼 검증을
    이미 담당한다.

    Args:
        input_ref: step id 또는 RESERVED 테이블 이름.
        required_columns: 검증할 컬럼 이름 리스트. 빈 항목은 건너뜀.
        ctx: ``_StepContext``.
        issue_code: ``ctx.issue``에 넘길 code (skill별 호환을 위해 callsite가 결정).
        message_builder: ``(column, input_ref, available_sorted) -> str``.
    """

    if not input_ref or input_ref in TABLE_SCHEMAS or not required_columns:
        return
    inferred = _infer_step_output_columns(input_ref, ctx.step_lookup, visiting=set())
    if inferred is None:
        return
    available = sorted(inferred)
    for col in required_columns:
        if col and col not in inferred:
            ctx.issue(
                code=issue_code,
                message=message_builder(col, input_ref, available),
            )


def _validate_filter(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("input", "column", "operator", "value"), ctx):
        return
    column = str(params.get("column") or "").strip()
    _check_input_ref(params.get("input"), "input", ctx, require_column=column or None)
    operator = str(params.get("operator") or "").strip()
    if operator not in FILTER_OPERATORS:
        ctx.issue(
            code="params.operator_invalid",
            message=f"filter.operator must be one of {sorted(FILTER_OPERATORS)}; got '{operator}'",
        )
        return
    value = params.get("value")
    if operator in {"is_null", "not_null"}:
        # silverone 2026-05-26 (SQL-1.5, audit M2) — is_null/not_null는 column
        # 값과 비교하지 않으므로 value가 들어오면 의미가 없다. executor는 무시
        # 하지만 plan 의도가 잘못된 신호 (예: operator=is_null + value=0은 사실
        # operator=eq 의도) → validation issue로 표면화. None/""/[] 또는 key
        # 자체 누락은 의도된 sentinel로 통과.
        is_empty_sentinel = (
            value is None
            or (isinstance(value, str) and value == "")
            or (isinstance(value, list) and len(value) == 0)
        )
        if not is_empty_sentinel:
            ctx.issue(
                code="params.value_unexpected",
                message=(
                    f"filter.value must be omitted or null when operator is '{operator}'. "
                    "value가 의미를 갖는다면 operator는 eq/neq/in/not_in 등을 사용한다."
                ),
            )
    elif operator in {"in", "not_in", "between"}:
        if not isinstance(value, list) or not value:
            ctx.issue(
                code="params.value_not_list",
                message=f"filter.value must be a non-empty list when operator is '{operator}'",
            )
        elif operator == "between" and len(value) != 2:
            ctx.issue(
                code="params.value_between_arity",
                message="filter.value must contain exactly two elements when operator is 'between'",
            )

    # silverone 2026-05-26 (SQL-3.1, audit C4) — RESERVED 테이블 직접 input일 때
    # column type과 value type을 정적으로 검증. timestamp/date column + string
    # value는 executor가 CAST하므로 통과시킨다. 명백히 호환 불가한 조합만 reject.
    input_ref = str(params.get("input") or "").strip()
    column_type = RESERVED_COLUMN_TYPES.get(input_ref, {}).get(column)
    if column_type:
        _check_filter_value_type(column, column_type, operator, value, ctx)


def _check_filter_value_type(
    column: str,
    column_type: str,
    operator: str,
    value: Any,
    ctx: _StepContext,
) -> None:
    """RESERVED 테이블 column type 기반 filter value 호환성 검증.
    SQL-3.1 (audit C4) — false positive 피하기 위해 명백한 mismatch만 reject."""

    # contains는 text-like column 한정. 다른 type에 적용하면 DuckDB가 silent
    # cast하거나 부정확한 결과.
    if operator == "contains" and column_type not in TEXT_COLUMN_TYPES:
        ctx.issue(
            code="params.value_type_mismatch",
            message=(
                f"filter.contains는 string/text column에만 적용 가능. "
                f"column '{column}' (type={column_type})에는 eq/neq/in/not_in을 사용한다."
            ),
        )
        return

    # is_null/not_null은 value가 없으니 type check 의미 없음.
    if operator in {"is_null", "not_null"}:
        return

    # 비교 operator(eq/neq/gt/gte/lt/lte/between/in/not_in) — value type 검증.
    def _values() -> list[Any]:
        if isinstance(value, list):
            return list(value)
        return [value]

    raw_values = _values()
    for raw in raw_values:
        if raw is None:
            continue
        if column_type in NUMERIC_COLUMN_TYPES:
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                continue
            # string이라도 숫자로 파싱되면 executor가 cast 가능. 명백한 mismatch만 reject.
            if isinstance(raw, str):
                try:
                    float(raw.strip())
                    continue
                except ValueError:
                    pass
            ctx.issue(
                code="params.value_type_mismatch",
                message=(
                    f"filter.value '{raw!r}' is not numeric, but column '{column}' "
                    f"is type={column_type}."
                ),
            )
            return
        if column_type in TIMESTAMP_COLUMN_TYPES:
            # timestamp/date column에 string ISO 날짜는 executor가 CAST해서 처리한다.
            # number/bool은 의미가 없으므로 reject.
            if isinstance(raw, str):
                continue
            ctx.issue(
                code="params.value_type_mismatch",
                message=(
                    f"filter.value '{raw!r}' must be an ISO date/timestamp string for "
                    f"column '{column}' (type={column_type})."
                ),
            )
            return
        # text column은 사실상 모든 비교 OK (DuckDB가 string cast).


def _validate_aggregate(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("input", "group_by", "metrics"), ctx):
        return
    group_by = params.get("group_by")
    if not isinstance(group_by, list) or not group_by:
        ctx.issue(code="params.group_by_not_list", message="aggregate.group_by must be a non-empty list")
        group_columns: list[str] = []
    else:
        group_columns = [str(col or "").strip() for col in group_by]

    metrics = params.get("metrics")
    metric_columns: list[str] = []
    if not isinstance(metrics, list) or not metrics:
        ctx.issue(code="params.metrics_not_list", message="aggregate.metrics must be a non-empty list")
    else:
        seen_metric_names: set[str] = set()
        for idx, metric in enumerate(metrics):
            if not isinstance(metric, dict):
                ctx.issue(
                    code="params.metric_not_object",
                    message=f"aggregate.metrics[{idx}] must be an object",
                )
                continue
            name = str(metric.get("name") or "").strip()
            if not name:
                ctx.issue(
                    code="params.metric_name_missing",
                    message=f"aggregate.metrics[{idx}].name is required",
                )
            elif name in seen_metric_names:
                ctx.issue(
                    code="params.metric_name_duplicated",
                    message=f"aggregate.metrics[{idx}].name '{name}' duplicated",
                )
            else:
                seen_metric_names.add(name)
                # silverone 2026-05-26 (prefix contract B안) — metric name에 비교
                # label prefix(last_/this_/prev_/curr_/year_/month_/baseline_)를
                # 넣으면 compare가 다시 prefix를 붙여 중복 prefix(last_last_count)가
                # 생긴다. metric name은 generic (count, sum_value)으로만.
                if _LABEL_PREFIX_PATTERN.match(name):
                    ctx.issue(
                        code="params.metric_name_label_prefix",
                        message=(
                            f"aggregate.metrics[{idx}].name '{name}'에 비교 label prefix가 포함됨. "
                            "aggregate metric name에는 비교 label prefix를 넣지 않는다. "
                            "compare skill이 left_label/right_label로 prefix를 자동 부여하므로 "
                            "metric name은 'count', 'sum_value' 같은 generic name을 사용한다."
                        ),
                    )
                # silverone 2026-05-26 (SQL-1.3, audit C5) — metric name이 group_by
                # column과 같으면 SELECT 절에 같은 alias가 두 번 생긴다
                # (`SELECT aspect, COUNT(*) AS aspect`). DuckDB는 column overlap을
                # 받아주지만 downstream skill이 metric 컬럼 대신 group key를 읽게
                # 되어 silent regression이 된다.
                if name in group_columns:
                    ctx.issue(
                        code="params.metric_name_collides_group_by",
                        message=(
                            f"aggregate.metrics[{idx}].name '{name}'이 group_by 컬럼과 충돌. "
                            "SELECT 절에서 group key alias와 metric alias가 겹쳐 downstream skill이 "
                            "잘못된 컬럼을 읽는다. metric name은 group_by 컬럼명을 사용하지 않는다 "
                            "(예: group_by=['aspect']면 metric name은 'count'·'aspect_count' 등)."
                        ),
                    )
            function = str(metric.get("function") or "").strip()
            if function not in AGGREGATE_FUNCTIONS:
                ctx.issue(
                    code="params.metric_function_invalid",
                    message=f"aggregate.metrics[{idx}].function must be one of {sorted(AGGREGATE_FUNCTIONS)}; got '{function}'",
                )
            column = str(metric.get("column") or "").strip()
            if function == "count" and column in {"", "*"}:
                pass  # count(*) wildcard 허용
            elif not column:
                ctx.issue(
                    code="params.metric_column_missing",
                    message=f"aggregate.metrics[{idx}].column is required for function '{function}'",
                )
            else:
                metric_columns.append(column)
                # silverone 2026-05-26 (SQL-3.2, audit M3) — sum/avg/min/max는
                # RESERVED 테이블의 string column에 적용 불가. count는 모든 type OK.
                _check_aggregate_numeric_column(
                    str(params.get("input") or "").strip(),
                    function,
                    column,
                    idx,
                    ctx,
                )

    required_columns = [col for col in group_columns + metric_columns if col]
    _check_input_ref(params.get("input"), "input", ctx, require_column=required_columns or None)


def _check_aggregate_numeric_column(
    input_ref: str, function: str, column: str, idx: int, ctx: _StepContext
) -> None:
    """RESERVED root input일 때 sum/avg/min/max metric column이 수치형/시간형인지
    검증. min/max는 timestamp도 의미 있으므로 numeric/timestamp 모두 허용,
    sum/avg는 numeric만 허용. RESERVED chain을 거친 step output은 type 추적
    복잡해 1차 제외."""

    if function not in {"sum", "avg", "min", "max"}:
        return
    column_type = RESERVED_COLUMN_TYPES.get(input_ref, {}).get(column)
    if not column_type:
        return
    if function in {"sum", "avg"} and column_type not in NUMERIC_COLUMN_TYPES:
        ctx.issue(
            code="params.metric_column_not_numeric",
            message=(
                f"aggregate.metrics[{idx}] function='{function}' requires a numeric column, "
                f"but '{column}' from RESERVED table '{input_ref}' is type={column_type}. "
                "count(*) 또는 group_by + count로 빈도를 구하거나, 수치 column이 있는 step에 적용한다."
            ),
        )
    elif function in {"min", "max"} and column_type not in NUMERIC_COLUMN_TYPES | TIMESTAMP_COLUMN_TYPES:
        ctx.issue(
            code="params.metric_column_not_orderable",
            message=(
                f"aggregate.metrics[{idx}] function='{function}' requires a numeric or "
                f"timestamp column, but '{column}' from RESERVED table '{input_ref}' is type={column_type}."
            ),
        )


def _validate_compare(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("left", "right", "join_key", "left_label", "right_label"), ctx):
        return
    join_key = params.get("join_key")
    if not isinstance(join_key, list) or not join_key:
        ctx.issue(code="params.join_key_not_list", message="compare.join_key must be a non-empty list")
        join_columns: list[str] = []
    else:
        join_columns = [str(key or "").strip() for key in join_key]
    _check_input_ref(params.get("left"), "left", ctx, require_column=join_columns)
    _check_input_ref(params.get("right"), "right", ctx, require_column=join_columns)
    # silverone 2026-05-27 (SQL-6.1, Q4 audit 2026-05-27) — step input의
    # inferred output에 join_key가 모두 있는지 검증. RESERVED 테이블 input은
    # 위 _check_input_ref가 처리. SQL-3.3 (join M4) 패턴을 compare에도 적용.
    for side in ("left", "right"):
        _check_input_columns_exist(
            input_ref=str(params.get(side) or "").strip(),
            required_columns=join_columns,
            ctx=ctx,
            issue_code="params.compare_join_key_unknown",
            message_builder=lambda key, ref, available, _side=side: (
                f"compare.join_key \"{key}\" not found in {_side} input output columns. "
                f"step '{ref}' output: {available}. "
                "양쪽 aggregate의 group_by에 같은 key를 두거나, compare 대신 "
                "calculate.ratio 등을 사용한다."
            ),
        )
    left_label = str(params.get("left_label") or "").strip()
    right_label = str(params.get("right_label") or "").strip()
    if left_label == "":
        ctx.issue(code="params.left_label_missing", message="compare.left_label must not be empty")
    elif not SAFE_SQL_IDENTIFIER_RE.match(left_label):
        # SQL-1.4 (C6) — `{label}_{column}` alias가 SQL identifier가 되어야 함.
        ctx.issue(
            code="params.left_label_unsafe",
            message=(
                f"compare.left_label '{left_label}'은 SQL identifier로 사용 불가. "
                "letter/underscore로 시작하고 [a-zA-Z0-9_]만 허용 "
                "(예: 'last', 'this_year', 'baseline'). 한국어·공백·하이픈·숫자 시작은 사용 불가."
            ),
        )
    if right_label == "":
        ctx.issue(code="params.right_label_missing", message="compare.right_label must not be empty")
    elif not SAFE_SQL_IDENTIFIER_RE.match(right_label):
        ctx.issue(
            code="params.right_label_unsafe",
            message=(
                f"compare.right_label '{right_label}'은 SQL identifier로 사용 불가. "
                "letter/underscore로 시작하고 [a-zA-Z0-9_]만 허용 "
                "(예: 'last', 'this_year', 'baseline'). 한국어·공백·하이픈·숫자 시작은 사용 불가."
            ),
        )
    if left_label and right_label and left_label == right_label:
        # 둘 다 같으면 `{label}_{col}` alias가 겹쳐 left/right column이 묶이거나
        # SELECT alias가 충돌. SQL build 시 ExecutorError로 떨어진다.
        ctx.issue(
            code="params.compare_labels_identical",
            message=(
                f"compare.left_label와 right_label이 동일('{left_label}'). "
                "alias collision이 발생하므로 두 label은 서로 달라야 한다 "
                "(예: 'last' / 'this')."
            ),
        )


def _validate_calculate(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("input", "expressions"), ctx):
        return
    _check_input_ref(params.get("input"), "input", ctx)
    expressions = params.get("expressions")
    if not isinstance(expressions, list) or not expressions:
        ctx.issue(code="params.expressions_not_list", message="calculate.expressions must be a non-empty list")
        return

    # silverone 2026-05-26 (prefix-2) — input step의 output column set을 추론할
    # 수 있으면 expression이 참조하는 column이 그 안에 있는지 정적으로 검증한다.
    # 추론 불가(체인이 너무 복잡하거나 추론 미지원 skill 거치는 경우) → skip
    # (false positive 방지).
    input_ref = str(params.get("input") or "").strip()
    input_output: set[str] | None = _infer_step_output_columns(input_ref, ctx.step_lookup, visiting=set())
    # silverone 2026-06-04 — numeric operation이 string 컬럼을 참조하면 executor SQL이
    # Binder Error(500)이 되므로, input output에서 string으로 확신되는 컬럼을 미리 추론한다.
    # RESERVED root뿐 아니라 prior step output(aggregate group 컬럼 등)도 포함. None/빈
    # 집합이면 차단 안 함(기존 동작 유지).
    input_string_cols: set[str] | None = _infer_step_output_string_columns(
        input_ref, ctx.step_lookup, visiting=set()
    )

    seen_names: set[str] = set()
    for idx, expression in enumerate(expressions):
        if not isinstance(expression, dict):
            ctx.issue(
                code="params.expression_not_object",
                message=f"calculate.expressions[{idx}] must be an object",
            )
            continue
        name = str(expression.get("name") or "").strip()
        if not name:
            ctx.issue(
                code="params.expression_name_missing",
                message=f"calculate.expressions[{idx}].name is required",
            )
        elif name in seen_names:
            ctx.issue(
                code="params.expression_name_duplicated",
                message=f"calculate.expressions[{idx}].name '{name}' duplicated",
            )
        else:
            seen_names.add(name)
        operation = str(expression.get("operation") or "").strip()
        if operation not in CALCULATE_OPERATIONS:
            ctx.issue(
                code="params.expression_operation_invalid",
                message=f"calculate.expressions[{idx}].operation must be one of {sorted(CALCULATE_OPERATIONS)}; got '{operation}'",
            )
            continue
        required_keys = _CALCULATE_OP_REQUIRED_KEYS.get(operation)
        if required_keys is not None:
            missing = [key for key in required_keys if not str(expression.get(key) or "").strip()]
            if missing:
                ctx.issue(
                    code="params.expression_keys_missing",
                    message=(
                        f"calculate.expressions[{idx}] (operation='{operation}') "
                        f"must include keys: {', '.join(required_keys)}; missing {missing}"
                    ),
                )
        elif operation == "ratio":
            # spec(CALCULATE_SPEC ratio.alt_required) 기반 — '둘 중 한 묶음' 충족 여부.
            satisfied = any(
                group and all(str(expression.get(key) or "").strip() for key in group)
                for group in _CALCULATE_RATIO_ALT_REQUIRED
            )
            if not satisfied:
                ctx.issue(
                    code="params.expression_keys_missing",
                    message=(
                        f"calculate.expressions[{idx}] (operation='ratio') must include "
                        "either {numerator, denominator} or {left, right}"
                    ),
                )

        # silverone 2026-05-26 (prefix-2) — input output schema 추론이 가능한
        # 경우, expression이 참조하는 column이 input output에 있는지 정적으로
        # 검증한다. input_output이 None이면 추론 불가 → skip (false positive 방지).
        if input_output is not None:
            for key in _CALCULATE_NUMERIC_REF_KEYS:
                col_name = str(expression.get(key) or "").strip()
                if not col_name:
                    continue
                if col_name not in input_output:
                    ctx.issue(
                        code="params.expression_column_unknown",
                        message=(
                            f"calculate.expressions[{idx}] references column '{col_name}' "
                            f"(via key '{key}'), but input step '{input_ref}' output schema "
                            f"does not include it. Available columns: {sorted(input_output)}. "
                            "compare 결과 컬럼은 `{left_label}_{metric_name}` / "
                            "`{right_label}_{metric_name}` 형태이다."
                        ),
                    )
            # silverone 2026-06-02 — share_of_total.partition_by(선택)도 input
            # output에 있어야 한다. list 형식 검증 + 각 컬럼 존재 검증.
            partition_by = expression.get("partition_by")
            if partition_by is not None:
                if not isinstance(partition_by, list):
                    ctx.issue(
                        code="params.expression_partition_by_not_list",
                        message=(
                            f"calculate.expressions[{idx}].partition_by must be a "
                            "string array of column names or null"
                        ),
                    )
                else:
                    for col in partition_by:
                        col_name = str(col or "").strip()
                        if col_name and col_name not in input_output:
                            ctx.issue(
                                code="params.expression_column_unknown",
                                message=(
                                    f"calculate.expressions[{idx}] partition_by references "
                                    f"column '{col_name}', but input step '{input_ref}' output "
                                    f"schema does not include it. Available columns: "
                                    f"{sorted(input_output)}."
                                ),
                            )

        # silverone 2026-05-26 (SQL-2.3, audit M8) / 2026-06-04 확장 — 수치
        # expression이 string column을 참조하지 않는지 검증한다. 옛 버전은 input이
        # RESERVED root인 case만 봤지만, 이제 prior step output(aggregate group
        # 컬럼 등)의 string 타입도 `_infer_step_output_string_columns`로 추론해 본다.
        # 추론 불가(None)/빈 집합/numeric/timestamp 컬럼은 통과(false positive 방지).
        # timestamp(`docs.created_at`)는 string 집합에 없으므로 reject되지 않는다.
        if input_string_cols:
            for key in _CALCULATE_NUMERIC_REF_KEYS:
                col_name = str(expression.get(key) or "").strip()
                if col_name and col_name in input_string_cols:
                    ctx.issue(
                        code="params.expression_column_not_numeric",
                        message=(
                            f"calculate.expressions[{idx}] (operation='{operation}') "
                            f"references string column '{col_name}' (via key '{key}') "
                            f"from input '{input_ref}'. "
                            "수치 연산(add/subtract/multiply/divide/percent_change/ratio/"
                            "share_of_total)은 string column에 적용할 수 없다. 먼저 aggregate로 "
                            "count/sum/avg를 구한 뒤 그 metric에 calculate를 적용한다."
                        ),
                    )


def _validate_sort(params: dict[str, Any], ctx: _StepContext) -> None:
    """validator R5-sort (2026-05-27) — body는 ``SortSkillContract.validate``에
    위임. dispatch 측 명명·signature는 유지 (다른 skill _validate_X와 대칭).
    contract 본문은 ``planner/skill_contracts/sort.py``."""

    # cycle 회피 — registry import는 함수 호출 시점에 lazy로.
    from .skill_contracts.registry import CONTRACTS

    CONTRACTS["sort"].validate(params, ctx)


def _validate_present(params: dict[str, Any], ctx: _StepContext) -> None:
    """validator R5 pilot (2026-05-27) — body는 ``PresentSkillContract.validate``
    에 위임. dispatch 측 명명·signature는 유지 (다른 skill _validate_X와 대칭).
    contract 본문은 ``planner/skill_contracts/present.py``."""

    # cycle 회피 — registry import는 함수 호출 시점에 lazy로.
    from .skill_contracts.registry import CONTRACTS

    CONTRACTS["present"].validate(params, ctx)


def _validate_summarize(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("input", "focus"), ctx):
        return
    _check_input_ref(params.get("input"), "input", ctx)
    focus = str(params.get("focus") or "").strip()
    if not focus:
        ctx.issue(code="params.focus_empty", message="summarize.focus must not be empty")


# ===== recipe param validation (R2a, silverone 2026-06-04) =====
#
# recipe step은 planner output에 남고 실행 시 expand_recipes가 atomic으로 lower한다
# (안 ii). validator는 recipe param만 검증한다 — atomic 규칙은 lower 후 atomic step에
# 적용된다(lowering 테스트가 lowered plan의 validator 통과를 잠금).


def _validate_recipe_params(skill_name: str, params: dict[str, Any], ctx: _StepContext) -> None:
    if skill_name == "distribution":
        _validate_distribution_recipe(params, ctx)
    elif skill_name == "event_window_count":
        _validate_event_window_count_recipe(params, ctx)
    elif skill_name == "top_n":
        _validate_top_n_recipe(params, ctx)
    else:
        ctx.issue(
            code="step.skill_unknown",
            message=f"recipe '{skill_name}' is runtime-enabled but has no validator",
        )


def _validate_distribution_recipe(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("input",), ctx):
        return
    _check_input_ref(params.get("input"), "input", ctx)

    group_by = params.get("group_by")
    if (
        not isinstance(group_by, list)
        or not group_by
        or not all(isinstance(c, str) and c.strip() for c in group_by)
    ):
        ctx.issue(
            code="params.recipe_group_by_invalid",
            message="distribution.group_by must be a non-empty list of column names",
        )

    metric = params.get("metric")
    if metric is not None and str(metric).strip() != "count":
        ctx.issue(
            code="params.recipe_metric_unsupported",
            message="distribution.metric only supports 'count'",
        )

    include_share = params.get("include_share")
    if include_share is not None and not isinstance(include_share, bool):
        ctx.issue(
            code="params.recipe_include_share_invalid",
            message="distribution.include_share must be a boolean",
        )

    for key in ("count_column", "share_column"):
        value = params.get(key)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            ctx.issue(
                code="params.recipe_column_name_invalid",
                message=f"distribution.{key} must be a non-empty string",
            )

    title = params.get("title")
    if title is not None and not isinstance(title, str):
        ctx.issue(
            code="params.recipe_title_invalid",
            message="distribution.title must be a string or null",
        )


def _validate_event_window_count_recipe(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("input", "event_date"), ctx):
        return

    input_ref = params.get("input")
    date_column = params.get("date_column")
    if date_column is None:
        date_column_name = "created_at"
    elif isinstance(date_column, str) and date_column.strip():
        date_column_name = date_column.strip()
    else:
        date_column_name = "created_at"
        ctx.issue(
            code="params.recipe_date_column_invalid",
            message="event_window_count.date_column must be a non-empty string",
        )

    _check_input_ref(input_ref, "input", ctx, require_column=date_column_name)
    input_name = str(input_ref or "").strip()
    column_type = RESERVED_COLUMN_TYPES.get(input_name, {}).get(date_column_name)
    if column_type and column_type not in TIMESTAMP_COLUMN_TYPES:
        ctx.issue(
            code="params.recipe_date_column_invalid",
            message=(
                "event_window_count.date_column must reference a timestamp/date column "
                f"(got {date_column_name}: {column_type})"
            ),
        )

    event_date = params.get("event_date")
    if not isinstance(event_date, str) or not event_date.strip():
        ctx.issue(
            code="params.recipe_event_date_invalid",
            message="event_window_count.event_date must be YYYY-MM-DD",
        )
    else:
        try:
            date.fromisoformat(event_date.strip())
        except ValueError:
            ctx.issue(
                code="params.recipe_event_date_invalid",
                message="event_window_count.event_date must be YYYY-MM-DD",
            )

    for key in ("before_days", "after_days"):
        value = params.get(key)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            ctx.issue(
                code="params.recipe_window_invalid",
                message=f"event_window_count.{key} must be a non-negative integer",
            )

    grain = params.get("grain")
    if grain is not None and str(grain).strip() != "day":
        ctx.issue(
            code="params.recipe_grain_unsupported",
            message="event_window_count.grain only supports 'day'",
        )

    count_column = params.get("count_column")
    if count_column is not None and (not isinstance(count_column, str) or not count_column.strip()):
        ctx.issue(
            code="params.recipe_column_name_invalid",
            message="event_window_count.count_column must be a non-empty string",
        )
    elif isinstance(count_column, str) and count_column.strip() == date_column_name:
        ctx.issue(
            code="params.recipe_column_name_invalid",
            message="event_window_count.count_column must not collide with date_column",
        )

    title = params.get("title")
    if title is not None and not isinstance(title, str):
        ctx.issue(
            code="params.recipe_title_invalid",
            message="event_window_count.title must be a string or null",
        )


def _validate_top_n_recipe(params: dict[str, Any], ctx: _StepContext) -> None:
    if not _check_required_keys(params, ("input",), ctx):
        return

    input_ref = params.get("input")
    group_by = params.get("group_by")
    group_columns: list[str] = []
    if (
        not isinstance(group_by, list)
        or not group_by
        or not all(isinstance(c, str) and c.strip() for c in group_by)
    ):
        ctx.issue(
            code="params.recipe_group_by_invalid",
            message="top_n.group_by must be a non-empty list of column names",
        )
    else:
        group_columns = [c.strip() for c in group_by]

    count_column = params.get("count_column")
    if count_column is None:
        count_column_name = "count"
    elif isinstance(count_column, str) and count_column.strip():
        count_column_name = count_column.strip()
    else:
        count_column_name = "count"
        ctx.issue(
            code="params.recipe_column_name_invalid",
            message="top_n.count_column must be a non-empty string",
        )

    _check_input_ref(input_ref, "input", ctx, require_column=group_columns or None)

    metric = params.get("metric")
    if metric is not None and str(metric).strip() != "count":
        ctx.issue(
            code="params.recipe_metric_unsupported",
            message="top_n.metric only supports 'count'",
        )

    filters = params.get("filters")
    if filters is not None:
        if not isinstance(filters, list):
            ctx.issue(
                code="params.recipe_filter_invalid",
                message="top_n.filters must be a list",
            )
        else:
            _validate_top_n_filters(input_ref, filters, ctx)

    sort = params.get("sort")
    if sort is not None:
        if not isinstance(sort, dict):
            ctx.issue(
                code="params.recipe_sort_invalid",
                message="top_n.sort must be an object",
            )
        else:
            sort_column = sort.get("column")
            if sort_column is not None:
                if not isinstance(sort_column, str) or not sort_column.strip():
                    ctx.issue(
                        code="params.recipe_column_name_invalid",
                        message="top_n.sort.column must be a non-empty string",
                    )
                elif group_columns and sort_column.strip() not in {*group_columns, count_column_name}:
                    ctx.issue(
                        code="params.recipe_sort_invalid",
                        message=(
                            "top_n.sort.column must be one of group_by columns or "
                            f"count_column ({sorted([*group_columns, count_column_name])})"
                        ),
                    )
            direction = sort.get("direction")
            if direction is not None and str(direction).strip() not in SORT_ORDERS:
                ctx.issue(
                    code="params.recipe_sort_invalid",
                    message="top_n.sort.direction must be asc or desc",
                )

    limit = params.get("limit")
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0):
        ctx.issue(
            code="params.recipe_limit_invalid",
            message="top_n.limit must be a positive integer",
        )

    title = params.get("title")
    if title is not None and not isinstance(title, str):
        ctx.issue(
            code="params.recipe_title_invalid",
            message="top_n.title must be a string or null",
        )


def _validate_top_n_filters(
    input_ref: Any,
    filters: list[Any],
    ctx: _StepContext,
) -> None:
    input_name = str(input_ref or "").strip()
    for index, item in enumerate(filters):
        if not isinstance(item, dict):
            ctx.issue(
                code="params.recipe_filter_invalid",
                message=f"top_n.filters[{index}] must be an object",
            )
            continue

        column = item.get("column")
        if not isinstance(column, str) or not column.strip():
            ctx.issue(
                code="params.recipe_filter_invalid",
                message=f"top_n.filters[{index}].column must be a non-empty string",
            )
        elif input_name in TABLE_SCHEMAS:
            _check_input_ref(input_ref, "input", ctx, require_column=column.strip())

        op = item.get("op")
        op_name = str(op or "").strip()
        if op_name not in _TOP_N_FILTER_OPS:
            ctx.issue(
                code="params.recipe_filter_invalid",
                message=(
                    f"top_n.filters[{index}].op must be one of "
                    f"{sorted(_TOP_N_FILTER_OPS)}"
                ),
            )
            continue

        if op_name in {"in", "not_in"}:
            value = item.get("value")
            if not isinstance(value, list) or not value:
                ctx.issue(
                    code="params.recipe_filter_value_invalid",
                    message=f"top_n.filters[{index}].value must be a non-empty list for op '{op_name}'",
                )


# ===== schema inference (prefix-2) =====


def _infer_step_output_columns(
    ref: str,
    step_lookup: dict[str, dict[str, Any]],
    *,
    visiting: set[str],
) -> set[str] | None:
    """step ref의 output column set을 추론한다.

    silverone 2026-05-26 (prefix-2) — calculate column 정적 검증용. 보수적으로
    aggregate / compare / filter / calculate / **join / sort** 6종 지원.
    그 외 skill을 거치면 None 반환 → calculate validator에서 검증 skip
    (false positive 방지).

    silverone 2026-05-26 (SQL-1.1) — join/sort 추가. join 거친 chain에서
    calculate column 검증이 동작하도록 한다 (audit C1).

    Args:
        ref: 참조할 step id 또는 RESERVED_INPUT_NAMES (docs/clauses/genuineness).
        step_lookup: 1차 패스에서 만든 step_id → step body 매핑.
        visiting: 순환 참조 가드.

    Returns:
        output column set, 또는 추론 불가 시 None.
    """
    ref = ref.strip()
    if not ref:
        return None
    # RESERVED tables (docs/clauses/genuineness) — column 집합을 외부에서 알기 어렵고
    # dataset-specific 컬럼도 inject되므로 추론 안 함. None 반환.
    if ref in RESERVED_INPUT_NAMES:
        return None
    if ref in visiting:
        return None
    if ref not in step_lookup:
        return None

    step = step_lookup[ref]
    if not isinstance(step, dict):
        return None
    skill_name = str(step.get("skill") or "").strip()
    params = step.get("params")
    if not isinstance(params, dict):
        return None

    visiting = visiting | {ref}

    if skill_name == "aggregate":
        group_by = params.get("group_by")
        metrics = params.get("metrics")
        cols: set[str] = set()
        if isinstance(group_by, list):
            for col in group_by:
                name = str(col or "").strip()
                if name:
                    cols.add(name)
        if isinstance(metrics, list):
            for metric in metrics:
                if isinstance(metric, dict):
                    name = str(metric.get("name") or "").strip()
                    if name:
                        cols.add(name)
        return cols or None

    if skill_name == "compare":
        join_key = params.get("join_key")
        left_label = str(params.get("left_label") or "").strip()
        right_label = str(params.get("right_label") or "").strip()
        if not left_label or not right_label:
            return None
        left_ref = str(params.get("left") or "").strip()
        right_ref = str(params.get("right") or "").strip()
        left_cols = _infer_step_output_columns(left_ref, step_lookup, visiting=visiting)
        right_cols = _infer_step_output_columns(right_ref, step_lookup, visiting=visiting)
        if left_cols is None or right_cols is None:
            return None
        join_set: set[str] = set()
        if isinstance(join_key, list):
            for col in join_key:
                name = str(col or "").strip()
                if name:
                    join_set.add(name)
        cols = set(join_set)
        for col in left_cols - join_set:
            cols.add(f"{left_label}_{col}")
        for col in right_cols - join_set:
            cols.add(f"{right_label}_{col}")
        return cols or None

    if skill_name == "filter":
        return _infer_step_output_columns(
            str(params.get("input") or "").strip(),
            step_lookup,
            visiting=visiting,
        )

    if skill_name == "calculate":
        upstream = _infer_step_output_columns(
            str(params.get("input") or "").strip(),
            step_lookup,
            visiting=visiting,
        )
        if upstream is None:
            return None
        cols = set(upstream)
        expressions = params.get("expressions")
        if isinstance(expressions, list):
            for expression in expressions:
                if isinstance(expression, dict):
                    name = str(expression.get("name") or "").strip()
                    if name:
                        cols.add(name)
        return cols or None

    if skill_name == "join":
        # silverone 2026-05-26 (SQL-1.1) — executor/skills/join.py와 동일 규칙:
        # join keys + left 비-키 + right 비-키 (left에 같은 이름 있으면 right_
        # prefix). 한쪽이라도 inference 불가면 전체 None.
        on_keys = params.get("on")
        left_ref = str(params.get("left") or "").strip()
        right_ref = str(params.get("right") or "").strip()
        left_cols = _infer_step_output_columns(left_ref, step_lookup, visiting=visiting)
        right_cols = _infer_step_output_columns(right_ref, step_lookup, visiting=visiting)
        if left_cols is None or right_cols is None:
            return None
        key_set: set[str] = set()
        if isinstance(on_keys, list):
            for col in on_keys:
                name = str(col or "").strip()
                if name:
                    key_set.add(name)
        cols = set(key_set)
        for col in left_cols:
            if col in key_set:
                continue
            cols.add(col)
        for col in right_cols:
            if col in key_set:
                continue
            if col in left_cols:
                cols.add(f"right_{col}")
            else:
                cols.add(col)
        return cols or None

    if skill_name == "sort":
        # silverone 2026-05-26 (SQL-1.1) — sort는 input output 그대로 pass-through.
        return _infer_step_output_columns(
            str(params.get("input") or "").strip(),
            step_lookup,
            visiting=visiting,
        )

    # 그 외 (present / summarize) — 보수적으로 None 반환.
    # present는 chain 끝에서만 쓰이므로 정의 안 함.
    return None


def _infer_step_output_string_columns(
    ref: str,
    step_lookup: dict[str, dict[str, Any]],
    *,
    visiting: set[str],
) -> set[str] | None:
    """ref output에서 *string 타입으로 확신할 수 있는* 컬럼 집합을 추론한다.

    silverone 2026-06-04 — calculate numeric operation이 string 컬럼을 참조하면
    executor SQL(COALESCE/산술)이 DuckDB Binder Error → 사용자에게 500으로 노출된다.
    이를 validator에서 사전 차단하기 위한 타입 추론.

    column-name 추론(``_infer_step_output_columns``)과 차이:
    - RESERVED root는 None이 아니라 *known string 컬럼 집합*을 반환한다. dataset별
      dynamic 컬럼은 타입을 모르므로 집합에 넣지 않는다 (모르는 컬럼 = over-block 안 함).
    - count/sum/avg metric은 numeric → 집합에서 제외. min/max는 source 컬럼이 string
      이면 string. group_by 컬럼은 source 타입을 유지.
    - join은 right_ prefix collision 판정이 복잡해 보수적으로 None(검증 skip).

    Returns:
        확신할 수 있는 string 컬럼 집합(빈 집합 가능), 또는 추론 불가 시 None.
    """
    ref = ref.strip()
    if not ref:
        return None
    if ref in RESERVED_INPUT_NAMES:
        # known string columns만. dynamic dataset 컬럼은 타입 불명 → 미포함.
        return set(RESERVED_STRING_COLUMNS.get(ref, frozenset()))
    if ref in visiting:
        return None
    if ref not in step_lookup:
        return None

    step = step_lookup[ref]
    if not isinstance(step, dict):
        return None
    skill_name = str(step.get("skill") or "").strip()
    params = step.get("params")
    if not isinstance(params, dict):
        return None

    visiting = visiting | {ref}

    if skill_name in ("filter", "sort"):
        return _infer_step_output_string_columns(
            str(params.get("input") or "").strip(), step_lookup, visiting=visiting
        )

    if skill_name == "calculate":
        # calculate는 numeric expression 컬럼만 추가 → string 집합은 upstream 그대로.
        return _infer_step_output_string_columns(
            str(params.get("input") or "").strip(), step_lookup, visiting=visiting
        )

    if skill_name == "aggregate":
        upstream = _infer_step_output_string_columns(
            str(params.get("input") or "").strip(), step_lookup, visiting=visiting
        )
        if upstream is None:
            return None
        strings: set[str] = set()
        group_by = params.get("group_by")
        if isinstance(group_by, list):
            for col in group_by:
                name = str(col or "").strip()
                if name and name in upstream:
                    strings.add(name)  # group 컬럼은 source 타입 유지
        metrics = params.get("metrics")
        if isinstance(metrics, list):
            for metric in metrics:
                if not isinstance(metric, dict):
                    continue
                out_name = str(metric.get("name") or "").strip()
                if not out_name:
                    continue
                func = str(metric.get("function") or "").strip()
                # count/sum/avg → numeric(제외). min/max는 source 컬럼이 string이면 string.
                if func in ("min", "max"):
                    src = str(metric.get("column") or "").strip()
                    if src and src in upstream:
                        strings.add(out_name)
        return strings

    if skill_name == "compare":
        left_label = str(params.get("left_label") or "").strip()
        right_label = str(params.get("right_label") or "").strip()
        if not left_label or not right_label:
            return None
        left_strings = _infer_step_output_string_columns(
            str(params.get("left") or "").strip(), step_lookup, visiting=visiting
        )
        right_strings = _infer_step_output_string_columns(
            str(params.get("right") or "").strip(), step_lookup, visiting=visiting
        )
        if left_strings is None or right_strings is None:
            return None
        join_key = params.get("join_key")
        join_set: set[str] = set()
        if isinstance(join_key, list):
            for col in join_key:
                name = str(col or "").strip()
                if name:
                    join_set.add(name)
        result: set[str] = set()
        for col in join_set:
            if col in left_strings or col in right_strings:
                result.add(col)
        for col in left_strings - join_set:
            result.add(f"{left_label}_{col}")
        for col in right_strings - join_set:
            result.add(f"{right_label}_{col}")
        return result

    # join / 그 외 — string 컬럼을 안전하게 확정하기 어려워 보수적으로 None(검증 skip).
    return None


__all__ = [
    "PlanValidationError",
    "ValidationIssue",
    "collect_plan_issues",
    "validate_plan",
]
