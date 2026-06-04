from __future__ import annotations

"""Composite skill / recipe — R0 (silverone 2026-06-04).

recipe = 자주 쓰는 분석 패턴의 high-level skill. planner가 atomic skill
(filter/aggregate/calculate/sort/present 등)을 매번 직접 조립하지 않고 recipe
1개를 선택하게 해 **재현성**을 높인다. recipe는 실행 전 deterministic 하게 atomic
step들로 *lowering* 된다 (LLM 없음). atomic skill은 그대로 유지한다.

**R0 범위 (runtime behavior 변화 0)**: spec 정의(distribution / event_window_count
/ top_n) + **distribution / event_window_count lowering 함수 + 테스트**까지. planner
prompt / executor / Go / OpenAPI wiring 없음 — 아직 아무도 recipe를 내보내거나
실행하지 않는다. top_n은 spec 골격만 두고 lowering은 미구현(``lower_recipe`` 호출 시
RecipeError).

event_window_count R0 전제 / 정책 (silverone 2026-06-04):
  - **grain=day만 지원.** 현재 atomic skill에는 date_trunc/date_bucket이 없다.
    clean 단계 산출 ``created_at``이 day-granular(자정)라 ``group_by [created_at]``이
    곧 일자 집계로 동작한다. week/month는 date_trunc가 필요해 R0에서 제외(지연).
  - **window 경계는 inclusive 양끝**: ``filter.between [event-before_days,
    event+after_days]`` (DuckDB BETWEEN inclusive). 즉 before=after=7이면 기준일
    포함 총 15일(08-08 .. 08-22). 결과 row 수는 그 구간에서 *데이터가 있는 날 수*라
    질문/데이터에 따라 13~15 등으로 달라진다(옛 13/14건은 planner가 경계를 매번 다르게
    조립한 탓 — recipe가 경계를 결정적으로 고정한다).
"""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable


class RecipeError(ValueError):
    """recipe param 오류 / lowering 불가."""


@dataclass(frozen=True)
class RecipeParamSpec:
    name: str
    required: bool = False
    desc: str = ""


@dataclass(frozen=True)
class RecipeSpec:
    name: str
    description: str
    params: tuple[RecipeParamSpec, ...]
    # lowering으로 확장되는 atomic skill 목록 (문서/검증용).
    lowered_skills: tuple[str, ...]
    implemented: bool = False  # R0에서 lowering이 구현됐는지


# ===== spec 정의 =====

DISTRIBUTION_SPEC = RecipeSpec(
    name="distribution",
    description="group_by별 count(+전체 대비 share)를 계산한다. sentiment 비율, aspect 비중, 카테고리 구성비.",
    params=(
        RecipeParamSpec("input", required=True, desc="table_or_step_id"),
        RecipeParamSpec("group_by", required=True, desc="string[] — 분포 기준 컬럼"),
        RecipeParamSpec("metric", desc="count (R0는 count만)"),
        RecipeParamSpec("include_share", desc="bool — 전체 대비 share(0~1) 포함 (기본 true)"),
        RecipeParamSpec("count_column", desc="string — count 결과 컬럼명 (기본 count)"),
        RecipeParamSpec("share_column", desc="string — share 결과 컬럼명 (기본 ratio)"),
        RecipeParamSpec("title", desc="string|null — present 제목"),
    ),
    lowered_skills=("aggregate", "calculate", "present"),
    implemented=True,
)

EVENT_WINDOW_COUNT_SPEC = RecipeSpec(
    name="event_window_count",
    description="기준일(event_date) 전후 N일 문서 발생량. 축제일 기준 전/후 일주일 등.",
    params=(
        RecipeParamSpec("input", required=True, desc="table_or_step_id"),
        RecipeParamSpec("event_date", required=True, desc="YYYY-MM-DD 기준일"),
        RecipeParamSpec("date_column", desc="string — 날짜 컬럼 (기본 created_at)"),
        RecipeParamSpec("before_days", desc="int>=0 — 기준일 이전 일수 (기본 7)"),
        RecipeParamSpec("after_days", desc="int>=0 — 기준일 이후 일수 (기본 7)"),
        RecipeParamSpec("grain", desc="day (R0는 day만)"),
        RecipeParamSpec("count_column", desc="string — count 결과 컬럼명 (기본 count)"),
        RecipeParamSpec("title", desc="string|null"),
    ),
    lowered_skills=("filter", "aggregate", "sort", "present"),
    implemented=True,
)

TOP_N_SPEC = RecipeSpec(
    name="top_n",
    description="group_by + metric 상위 N개. 부정 후기 많은 aspect, 많이 언급된 항목 순위.",
    params=(
        RecipeParamSpec("input", required=True, desc="table_or_step_id"),
        RecipeParamSpec("group_by", required=True, desc="string[]"),
        RecipeParamSpec("metric", desc="count (R0 spec only)"),
        RecipeParamSpec("n", desc="int — 상위 개수 (기본 10)"),
        RecipeParamSpec("order", desc="asc|desc (기본 desc)"),
        RecipeParamSpec("title", desc="string|null"),
    ),
    lowered_skills=("aggregate", "sort", "present"),
    implemented=False,  # R0 미구현
)

RECIPE_SPECS: dict[str, RecipeSpec] = {
    DISTRIBUTION_SPEC.name: DISTRIBUTION_SPEC,
    EVENT_WINDOW_COUNT_SPEC.name: EVENT_WINDOW_COUNT_SPEC,
    TOP_N_SPEC.name: TOP_N_SPEC,
}


# ===== lowering =====


def lower_recipe(step: dict[str, Any]) -> list[dict[str, Any]]:
    """recipe step → atomic step 목록. R0는 distribution만 구현."""
    if not isinstance(step, dict):
        raise RecipeError("recipe step must be an object")
    skill = step.get("skill")
    spec = RECIPE_SPECS.get(skill)  # type: ignore[arg-type]
    if spec is None:
        raise RecipeError(f"unknown recipe: {skill!r}")
    lowerer = _LOWERERS.get(skill)  # type: ignore[arg-type]
    if lowerer is None:
        raise RecipeError(f"recipe '{skill}' lowering not implemented in R0")
    return lowerer(step)


def _required_str(params: dict[str, Any], recipe: str, key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RecipeError(f"{recipe}.{key} is required (non-empty string)")
    return value.strip()


def _opt_str(params: dict[str, Any], key: str, default: str) -> str:
    value = params.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return default


def _opt_nonneg_int(params: dict[str, Any], recipe: str, key: str, default: int) -> int:
    value = params.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RecipeError(f"{recipe}.{key} must be a non-negative integer")
    if value < 0:
        raise RecipeError(f"{recipe}.{key} must be >= 0")
    return value


def lower_distribution(step: dict[str, Any]) -> list[dict[str, Any]]:
    """distribution recipe → [aggregate, (calculate.share_of_total), present].

    deterministic — 같은 params는 항상 같은 atomic plan을 생성한다. step id는
    recipe id에서 파생(``<id>_agg`` / ``<id>_share`` / ``<id>_present``)."""
    params = step.get("params") or {}
    if not isinstance(params, dict):
        raise RecipeError("distribution.params must be an object")
    base = str(step.get("id") or "distribution").strip() or "distribution"

    input_ref = _required_str(params, "distribution", "input")

    group_by_raw = params.get("group_by")
    if (
        not isinstance(group_by_raw, list)
        or not group_by_raw
        or not all(isinstance(c, str) and c.strip() for c in group_by_raw)
    ):
        raise RecipeError("distribution.group_by must be a non-empty string list")
    group_by = [c.strip() for c in group_by_raw]

    metric = _opt_str(params, "metric", "count")
    if metric != "count":
        raise RecipeError(f"distribution.metric '{metric}' not supported in R0 (count only)")

    count_col = _opt_str(params, "count_column", "count")
    share_col = _opt_str(params, "share_column", "ratio")
    include_share = params.get("include_share", True)
    title = params.get("title")

    agg_id = f"{base}_agg"
    steps: list[dict[str, Any]] = [
        {
            "id": agg_id,
            "skill": "aggregate",
            "params": {
                "input": input_ref,
                "group_by": group_by,
                "metrics": [{"name": count_col, "function": "count", "column": "*"}],
            },
        }
    ]

    present_input = agg_id
    columns = [*group_by, count_col]
    if include_share:
        share_id = f"{base}_share"
        steps.append(
            {
                "id": share_id,
                "skill": "calculate",
                "params": {
                    "input": agg_id,
                    "expressions": [
                        {"name": share_col, "operation": "share_of_total", "value": count_col}
                    ],
                },
            }
        )
        present_input = share_id
        columns = [*group_by, count_col, share_col]

    present_params: dict[str, Any] = {
        "input": present_input,
        "format": "table",
        "columns": columns,
    }
    if isinstance(title, str) and title.strip():
        present_params["title"] = title.strip()
    steps.append({"id": f"{base}_present", "skill": "present", "params": present_params})
    return steps


def lower_event_window_count(step: dict[str, Any]) -> list[dict[str, Any]]:
    """event_window_count recipe → [filter(between), aggregate(by date), sort(asc), present].

    grain=day만 지원(모듈 docstring 정책 참조). window 경계는 inclusive 양끝.
    event_date ± days를 결정적으로 계산해 between 경계로 넣는다 — planner가 매번
    틀리던 날짜 경계를 고정한다. step id는 recipe id에서 파생."""
    params = step.get("params") or {}
    if not isinstance(params, dict):
        raise RecipeError("event_window_count.params must be an object")
    base = str(step.get("id") or "event_window_count").strip() or "event_window_count"

    input_ref = _required_str(params, "event_window_count", "input")
    date_column = _opt_str(params, "date_column", "created_at")
    count_col = _opt_str(params, "count_column", "count")

    grain = _opt_str(params, "grain", "day")
    if grain != "day":
        raise RecipeError(f"event_window_count.grain '{grain}' not supported in R0 (day only)")

    event_date = _required_str(params, "event_window_count", "event_date")
    try:
        event = date.fromisoformat(event_date)
    except ValueError as exc:
        raise RecipeError(
            f"event_window_count.event_date must be YYYY-MM-DD; got {event_date!r}"
        ) from exc

    before_days = _opt_nonneg_int(params, "event_window_count", "before_days", 7)
    after_days = _opt_nonneg_int(params, "event_window_count", "after_days", 7)
    lower_bound = (event - timedelta(days=before_days)).isoformat()
    upper_bound = (event + timedelta(days=after_days)).isoformat()

    title = params.get("title")

    window_id = f"{base}_window"
    by_date_id = f"{base}_by_date"
    sorted_id = f"{base}_sorted"
    steps: list[dict[str, Any]] = [
        {
            "id": window_id,
            "skill": "filter",
            "params": {
                "input": input_ref,
                "column": date_column,
                "operator": "between",
                "value": [lower_bound, upper_bound],
            },
        },
        {
            "id": by_date_id,
            "skill": "aggregate",
            "params": {
                "input": window_id,
                "group_by": [date_column],
                "metrics": [{"name": count_col, "function": "count", "column": "*"}],
            },
        },
        {
            "id": sorted_id,
            "skill": "sort",
            "params": {"input": by_date_id, "by": [date_column], "order": "asc"},
        },
    ]
    present_params: dict[str, Any] = {
        "input": sorted_id,
        "format": "table",
        "columns": [date_column, count_col],
    }
    if isinstance(title, str) and title.strip():
        present_params["title"] = title.strip()
    steps.append({"id": f"{base}_present", "skill": "present", "params": present_params})
    return steps


_LOWERERS: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
    "distribution": lower_distribution,
    "event_window_count": lower_event_window_count,
}


__all__ = [
    "RecipeError",
    "RecipeParamSpec",
    "RecipeSpec",
    "RECIPE_SPECS",
    "DISTRIBUTION_SPEC",
    "EVENT_WINDOW_COUNT_SPEC",
    "TOP_N_SPEC",
    "lower_recipe",
    "lower_distribution",
    "lower_event_window_count",
]
