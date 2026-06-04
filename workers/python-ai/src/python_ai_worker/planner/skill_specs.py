from __future__ import annotations

"""Skill Contract v2 — declarative skill spec (pilot: calculate / present).

silverone 2026-06-04, Step 0. 목표는 skill/operation의 *선언적 사실*(required
params, 컬럼-ref 여부, output column 규칙, 단위, prompt 설명)을 한 곳에 모아
이후 validator / planner prompt params_schema가 여기서 파생되게 하는 것.

**Step 0 범위 (행동 변화 0)**: spec 정의 + ``render_params_schema`` 생성기까지만.
validator / executor / prompt 실제 출력 / Go display / OpenAPI는 *변경하지 않는다*.
:func:`render_params_schema`가 현 ``schema.SKILL_CATALOG[...].params_schema``와
동일한 dict를 생성함을 ``test_skill_specs``가 잠근다 (Step 2에서 SKILL_CATALOG가
이 생성기를 쓰도록 교체할 때 prompt 출력이 안 바뀌는 것을 보장).

calculate/present 외 skill, executor SQL 의미, OpenAPI는 본 모듈 범위 밖.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ParamSpecV2:
    """skill param 1개. ``schema_desc``는 planner prompt에 노출되는 문자열
    (현 SKILL_CATALOG.params_schema 값과 byte 동일). ``kind``/``required``/``enum``은
    Step 1+ validator가 generic 검증에 쓸 구조적 분류 — Step 0에서는 정의만 한다."""

    name: str
    schema_desc: str
    kind: str = "string"  # table_ref | column_ref | column_ref_list | enum | int | expression_list | string
    required: bool = True
    enum: tuple[str, ...] = ()


@dataclass(frozen=True)
class OperationSpecV2:
    """operation 가진 skill(calculate)의 operation 1개의 구조적 계약.

    Step 0에서는 검증에 사용하지 않는다 — 정의 + validator 상수와의 동등성
    교차검증(test)만. Step 1에서 이 데이터로 validator를 구동한다.
    ``alt_required``: '둘 중 한 묶음' 형태(ratio의 {num,denom}|{left,right})."""

    name: str
    required: tuple[str, ...] = ()
    alt_required: tuple[tuple[str, ...], ...] = ()
    column_refs: tuple[str, ...] = ()   # input output에 존재해야 하는 컬럼 param
    numeric_refs: tuple[str, ...] = ()  # 존재 + 수치 타입이어야 하는 컬럼 param
    unit: str = "raw"                   # raw | fraction_0_1 | percent_0_100


@dataclass(frozen=True)
class SkillSpecV2:
    name: str
    description: str
    input_type: str
    output_type: str
    params: tuple[ParamSpecV2, ...]
    operations: tuple[OperationSpecV2, ...] = ()
    output_rule: str = "passthrough"  # passthrough | passthrough_plus_names | projection


def render_params_schema(spec: SkillSpecV2) -> dict[str, str]:
    """spec → planner prompt용 params_schema dict ({param_name: schema_desc}).

    Step 2에서 SKILL_CATALOG가 이 함수 결과를 쓰도록 교체할 예정. Step 0에서는
    현 SKILL_CATALOG와 동일함을 test로 잠그기만 한다."""

    return {param.name: param.schema_desc for param in spec.params}


# ===== calculate =====
# expressions schema_desc는 현 schema.SKILL_CATALOG["calculate"]와 byte 동일하게
# 동일 분할로 작성 (test_skill_specs가 잠근다). 구조적 사실은 operations에 둔다.
CALCULATE_SPEC = SkillSpecV2(
    name="calculate",
    description="파생 컬럼을 추가한다. 사칙연산 + percent_change + ratio + share_of_total.",
    input_type="table",
    output_type="table",
    output_rule="passthrough_plus_names",  # SELECT *, <expr> AS name
    params=(
        ParamSpecV2("input", "table_or_step_id", kind="table_ref"),
        ParamSpecV2(
            "expressions",
            (
                "calculation[] — {name, operation, ...}. operation별 키: "
                "add|subtract|multiply|divide={left,right}, percent_change={base,current}, "
                "ratio={numerator,denominator} (같은 행 두 컬럼 나눗셈, 0~1), "
                "share_of_total={value, partition_by?} — value 컬럼의 전체 합 대비 비중(0~1). "
                "비율/구성비/비중/전체 대비 질문은 ratio가 아니라 share_of_total을 쓴다. "
                "partition_by(선택 string[])가 있으면 그 그룹 내 합 대비 비중."
            ),
            kind="expression_list",
        ),
    ),
    operations=(
        OperationSpecV2("add", required=("left", "right"), numeric_refs=("left", "right")),
        OperationSpecV2("subtract", required=("left", "right"), numeric_refs=("left", "right")),
        OperationSpecV2("multiply", required=("left", "right"), numeric_refs=("left", "right")),
        OperationSpecV2("divide", required=("left", "right"), numeric_refs=("left", "right")),
        OperationSpecV2(
            "percent_change", required=("base", "current"),
            numeric_refs=("base", "current"), unit="percent_0_100",
        ),
        OperationSpecV2(
            "ratio",
            alt_required=(("numerator", "denominator"), ("left", "right")),
            numeric_refs=("numerator", "denominator", "left", "right"),
            unit="fraction_0_1",
        ),
        OperationSpecV2(
            "share_of_total", required=("value",),
            numeric_refs=("value",), column_refs=("partition_by",),
            unit="fraction_0_1",
        ),
    ),
)


# ===== present =====
PRESENT_SPEC = SkillSpecV2(
    name="present",
    description="결과를 사용자에게 보여줄 형식 (표/차트/json)으로 변환한다. plan의 최종 결과 step에서 사용.",
    input_type="table",
    output_type="presentation",
    output_rule="projection",  # columns 있으면 그 컬럼만
    params=(
        ParamSpecV2("input", "table_or_step_id", kind="table_ref"),
        ParamSpecV2("format", "table|chart|json", kind="enum", enum=("table", "chart", "json")),
        ParamSpecV2("title", "string|null", kind="string", required=False),
        ParamSpecV2(
            "columns",
            "string[]|null — 사용자에게 보여줄 컬럼. 질문에 답하는 핵심 컬럼을 포함해야 한다.",
            kind="column_ref_list", required=False,
        ),
        ParamSpecV2(
            "limit",
            "integer|null — 반환 row 한도. null이면 default 1000. 1~10000 허용. (SQL-4)",
            kind="int", required=False,
        ),
    ),
)


SPECS: dict[str, SkillSpecV2] = {
    "calculate": CALCULATE_SPEC,
    "present": PRESENT_SPEC,
}


__all__ = [
    "ParamSpecV2",
    "OperationSpecV2",
    "SkillSpecV2",
    "render_params_schema",
    "CALCULATE_SPEC",
    "PRESENT_SPEC",
    "SPECS",
]
