from __future__ import annotations

"""Composite skill / recipe — deterministic lowering for planner recipes.

recipe = 자주 쓰는 분석 패턴의 high-level skill. planner가 atomic skill
(filter/aggregate/calculate/sort/present 등)을 매번 직접 조립하지 않고 recipe
1개를 선택하게 해 **재현성**을 높인다. recipe는 실행 전 deterministic 하게 atomic
step들로 *lowering* 된다 (LLM 없음). atomic skill은 그대로 유지한다.

3 recipe(distribution / event_window_count / top_n)의 spec + lowering 함수.
runtime 활성 recipe는 ``RUNTIME_ENABLED_RECIPES`` 하나로 validator/executor가 공유한다.
keyword_summary는 키워드 atomic 부재로 제외.

event_window_count 전제 / 정책 (silverone 2026-06-04):
  - **grain=day만 지원.** 현재 atomic skill에는 date_trunc/date_bucket이 없다.
    clean 단계 산출 ``created_at``이 day-granular(자정)라 ``group_by [created_at]``이
    곧 일자 집계로 동작한다. week/month는 date_trunc가 필요해 제외(지연).
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
    implemented: bool = False  # lowering이 구현됐는지


# ===== spec 정의 =====

DISTRIBUTION_SPEC = RecipeSpec(
    name="distribution",
    description="group_by별 count(+전체 대비 share)를 계산한다. sentiment 비율, aspect 비중, 카테고리 구성비.",
    params=(
        RecipeParamSpec("input", required=True, desc="table_or_step_id"),
        RecipeParamSpec("group_by", required=True, desc="string[] — 분포 기준 컬럼"),
        RecipeParamSpec("metric", desc="count (현재 count만)"),
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
        RecipeParamSpec("grain", desc="day (현재 day만)"),
        RecipeParamSpec("count_column", desc="string — count 결과 컬럼명 (기본 count)"),
        RecipeParamSpec("title", desc="string|null"),
    ),
    lowered_skills=("filter", "aggregate", "sort", "present"),
    implemented=True,
)

TOP_N_SPEC = RecipeSpec(
    name="top_n",
    description="(filters) + group_by + count → 상위 N개. 부정 후기 많은 aspect, 많이 언급된 항목 순위.",
    params=(
        RecipeParamSpec("input", required=True, desc="table_or_step_id"),
        RecipeParamSpec("group_by", required=True, desc="string[]"),
        RecipeParamSpec("metric", desc="count (현재 count만)"),
        RecipeParamSpec("filters", desc="[{column, op, value}] — 선택. op는 =,!=,>,>=,<,<=,in,contains"),
        RecipeParamSpec("sort", desc="{column, direction} — 기본 {count_column, desc}"),
        RecipeParamSpec("limit", desc="int>0 — 상위 개수 (기본 10)"),
        RecipeParamSpec("count_column", desc="string — count 결과 컬럼명 (기본 count)"),
        RecipeParamSpec("title", desc="string|null"),
    ),
    lowered_skills=("filter", "aggregate", "sort", "present"),
    implemented=True,
)

# silverone 2026-06-05 — 집계 없이 원문 근거(raw row)를 보여주는 가장 얇은 recipe.
# 데모에서 "집계 결과의 근거 예시"를 안정 진입점으로 제공. 연산축이 다름(집계 X,
# row projection + deterministic order + limit)이라 distribution/top_n과 구분된다.
SAMPLE_ROWS_SPEC = RecipeSpec(
    name="sample_rows",
    description="집계 없이 조건에 맞는 원문 row 몇 개를 결정적으로 보여준다. '예시/샘플/원문/근거 문장 보여줘'.",
    params=(
        RecipeParamSpec("input", required=True, desc="table_or_step_id"),
        RecipeParamSpec("columns", required=True, desc="string[] — 보여줄 컬럼(projection)"),
        RecipeParamSpec("filters", desc="[{column, op, value}] — 선택. op는 =,!=,>,>=,<,<=,in,not_in,contains"),
        RecipeParamSpec("sort", desc="{by: string[], direction: asc|desc} — 미지정 시 doc_id+columns asc"),
        RecipeParamSpec("limit", desc="int>0 — 행 수 (기본 10, 최대 100)"),
        RecipeParamSpec("title", desc="string|null"),
    ),
    lowered_skills=("filter", "sort", "present"),
    implemented=True,
)

RECIPE_SPECS: dict[str, RecipeSpec] = {
    DISTRIBUTION_SPEC.name: DISTRIBUTION_SPEC,
    EVENT_WINDOW_COUNT_SPEC.name: EVENT_WINDOW_COUNT_SPEC,
    TOP_N_SPEC.name: TOP_N_SPEC,
    SAMPLE_ROWS_SPEC.name: SAMPLE_ROWS_SPEC,
}

# sample_rows.limit 상한 (데모용 안전 cap). 초과 요청은 RecipeError.
SAMPLE_ROWS_MAX_LIMIT = 100
SAMPLE_ROWS_DEFAULT_LIMIT = 10


# ===== lowering =====


def lower_recipe(step: dict[str, Any]) -> list[dict[str, Any]]:
    """recipe step → atomic step 목록."""
    if not isinstance(step, dict):
        raise RecipeError("recipe step must be an object")
    skill = step.get("skill")
    spec = RECIPE_SPECS.get(skill)  # type: ignore[arg-type]
    if spec is None:
        raise RecipeError(f"unknown recipe: {skill!r}")
    lowerer = _LOWERERS.get(skill)  # type: ignore[arg-type]
    if lowerer is None:
        raise RecipeError(f"recipe '{skill}' lowering not implemented")
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
        raise RecipeError(f"distribution.metric '{metric}' not supported (count only)")

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
        raise RecipeError(f"event_window_count.grain '{grain}' not supported (day only)")

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


# recipe filter op(기호/별칭) → atomic filter operator.
_FILTER_OP_MAP = {
    "=": "eq", "==": "eq", "eq": "eq",
    "!=": "neq", "neq": "neq",
    ">": "gt", "gt": "gt",
    ">=": "gte", "gte": "gte",
    "<": "lt", "lt": "lt",
    "<=": "lte", "lte": "lte",
    "in": "in", "not_in": "not_in",
    "contains": "contains",
}


def lower_top_n(step: dict[str, Any]) -> list[dict[str, Any]]:
    """top_n recipe → [(filter…), aggregate, sort(limit), present].

    limit은 atomic sort.limit에 둔다(present.limit는 row-cap이라 별개). filters는
    각각 atomic filter step으로 체인. share 계산 없음. deterministic."""
    params = step.get("params") or {}
    if not isinstance(params, dict):
        raise RecipeError("top_n.params must be an object")
    base = str(step.get("id") or "top_n").strip() or "top_n"

    input_ref = _required_str(params, "top_n", "input")

    group_by_raw = params.get("group_by")
    if (
        not isinstance(group_by_raw, list)
        or not group_by_raw
        or not all(isinstance(c, str) and c.strip() for c in group_by_raw)
    ):
        raise RecipeError("top_n.group_by must be a non-empty string list")
    group_by = [c.strip() for c in group_by_raw]

    metric = _opt_str(params, "metric", "count")
    if metric != "count":
        raise RecipeError(f"top_n.metric '{metric}' not supported (count only)")
    count_col = _opt_str(params, "count_column", "count")

    limit = params.get("limit", 10)
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise RecipeError("top_n.limit must be a positive integer")

    sort_spec = params.get("sort") or {}
    if not isinstance(sort_spec, dict):
        raise RecipeError("top_n.sort must be an object {column, direction}")
    sort_col = _opt_str(sort_spec, "column", count_col)
    direction = _opt_str(sort_spec, "direction", "desc")
    if direction not in ("asc", "desc"):
        raise RecipeError(f"top_n.sort.direction must be asc|desc; got {direction!r}")

    steps: list[dict[str, Any]] = []
    chain_input = input_ref

    filters = params.get("filters") or []
    if not isinstance(filters, list):
        raise RecipeError("top_n.filters must be a list")
    for idx, flt in enumerate(filters, start=1):
        if not isinstance(flt, dict):
            raise RecipeError(f"top_n.filters[{idx - 1}] must be an object")
        column = _required_str(flt, "top_n.filters", "column")
        op_raw = _required_str(flt, "top_n.filters", "op")
        operator = _FILTER_OP_MAP.get(op_raw)
        if operator is None:
            raise RecipeError(f"top_n.filters op '{op_raw}' not supported")
        filter_id = f"{base}_filter{idx}"
        steps.append(
            {
                "id": filter_id,
                "skill": "filter",
                "params": {
                    "input": chain_input,
                    "column": column,
                    "operator": operator,
                    "value": flt.get("value"),
                },
            }
        )
        chain_input = filter_id

    agg_id = f"{base}_agg"
    sorted_id = f"{base}_sorted"
    steps.append(
        {
            "id": agg_id,
            "skill": "aggregate",
            "params": {
                "input": chain_input,
                "group_by": group_by,
                "metrics": [{"name": count_col, "function": "count", "column": "*"}],
            },
        }
    )
    steps.append(
        {
            "id": sorted_id,
            "skill": "sort",
            "params": {"input": agg_id, "by": [sort_col], "order": direction, "limit": limit},
        }
    )
    present_params: dict[str, Any] = {
        "input": sorted_id,
        "format": "table",
        "columns": [*group_by, count_col],
    }
    title = params.get("title")
    if isinstance(title, str) and title.strip():
        present_params["title"] = title.strip()
    steps.append({"id": f"{base}_present", "skill": "present", "params": present_params})
    return steps


def lower_sample_rows(step: dict[str, Any]) -> list[dict[str, Any]]:
    """sample_rows recipe → [(filter…), sort(by, order, limit), present(columns)].

    집계 없음. filters는 atomic filter step으로 체인, sort는 결정적 정렬 + limit으로
    상위 N행 선택, present는 columns projection. deterministic — random sampling 없음.

    기본 정렬(미지정 시): ``by = [doc_id, *columns]`` asc.
    - doc-level 입력(docs/genuineness, doc당 1행)은 doc_id만으로 전순서 → 결정적.
    - clause-level 입력(clauses, doc당 N행)은 doc_id tie를 projection 컬럼으로 깬다.
      텍스트 컬럼 정렬은 fallback 성격(알파벳순) — 안정성 확보용이지 의미순 아님.
    - TODO(후속): source_row_index / clause_id 같은 stable key가 생기면 그걸 우선 사용.
    atomic sort는 order가 컬럼 공통 1개라 컬럼별 방향 혼합은 불가(혼합 필요 시 미지원)."""
    params = step.get("params") or {}
    if not isinstance(params, dict):
        raise RecipeError("sample_rows.params must be an object")
    base = str(step.get("id") or "sample_rows").strip() or "sample_rows"

    input_ref = _required_str(params, "sample_rows", "input")

    columns_raw = params.get("columns")
    if (
        not isinstance(columns_raw, list)
        or not columns_raw
        or not all(isinstance(c, str) and c.strip() for c in columns_raw)
    ):
        raise RecipeError("sample_rows.columns must be a non-empty string list")
    columns = [c.strip() for c in columns_raw]

    limit = params.get("limit")
    if limit is None:  # 미지정(absent) 또는 null → 기본값
        limit = SAMPLE_ROWS_DEFAULT_LIMIT
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise RecipeError("sample_rows.limit must be a positive integer")
    if limit > SAMPLE_ROWS_MAX_LIMIT:
        raise RecipeError(f"sample_rows.limit must be <= {SAMPLE_ROWS_MAX_LIMIT}")

    # sort: 미지정 시 doc_id + projection 컬럼 asc (결정적 기본).
    sort_spec = params.get("sort") or {}
    if not isinstance(sort_spec, dict):
        raise RecipeError("sample_rows.sort must be an object {by, direction}")
    by_raw = sort_spec.get("by")
    if by_raw is None:
        # doc_id 우선 + projection 컬럼 tiebreak (중복 제거, 순서 보존).
        sort_by = list(dict.fromkeys(["doc_id", *columns]))
    elif (
        isinstance(by_raw, list)
        and by_raw
        and all(isinstance(c, str) and c.strip() for c in by_raw)
    ):
        sort_by = [c.strip() for c in by_raw]
    else:
        raise RecipeError("sample_rows.sort.by must be a non-empty string list")
    direction = _opt_str(sort_spec, "direction", "asc")
    if direction not in ("asc", "desc"):
        raise RecipeError(f"sample_rows.sort.direction must be asc|desc; got {direction!r}")

    steps: list[dict[str, Any]] = []
    chain_input = input_ref

    filters = params.get("filters") or []
    if not isinstance(filters, list):
        raise RecipeError("sample_rows.filters must be a list")
    for idx, flt in enumerate(filters, start=1):
        if not isinstance(flt, dict):
            raise RecipeError(f"sample_rows.filters[{idx - 1}] must be an object")
        column = _required_str(flt, "sample_rows.filters", "column")
        op_raw = _required_str(flt, "sample_rows.filters", "op")
        operator = _FILTER_OP_MAP.get(op_raw)
        if operator is None:
            raise RecipeError(f"sample_rows.filters op '{op_raw}' not supported")
        filter_id = f"{base}_filter{idx}"
        steps.append(
            {
                "id": filter_id,
                "skill": "filter",
                "params": {
                    "input": chain_input,
                    "column": column,
                    "operator": operator,
                    "value": flt.get("value"),
                },
            }
        )
        chain_input = filter_id

    sorted_id = f"{base}_sorted"
    steps.append(
        {
            "id": sorted_id,
            "skill": "sort",
            "params": {"input": chain_input, "by": sort_by, "order": direction, "limit": limit},
        }
    )
    present_params: dict[str, Any] = {
        "input": sorted_id,
        "format": "table",
        "columns": columns,
    }
    title = params.get("title")
    if isinstance(title, str) and title.strip():
        present_params["title"] = title.strip()
    steps.append({"id": f"{base}_present", "skill": "present", "params": present_params})
    return steps


_LOWERERS: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
    "distribution": lower_distribution,
    "event_window_count": lower_event_window_count,
    "top_n": lower_top_n,
    "sample_rows": lower_sample_rows,
}


# Runtime 실행 + validator 허용 recipe. validator/executor가 같은 이 집합을 참조해
# "validator 허용 == runtime 실행 가능"을 단일 source로 보장한다.
RUNTIME_ENABLED_RECIPES: frozenset[str] = frozenset(
    {"distribution", "event_window_count", "top_n", "sample_rows"}
)


def expand_recipes(plan: dict[str, Any]) -> dict[str, Any]:
    """plan의 recipe step을 실행 전 atomic step으로 expand한다.

    - recipe step이 없으면 **완전 no-op** (원본 plan 그대로 반환).
    - runtime 허용 recipe(distribution/event_window_count/top_n)는 lower_recipe로 atomic 치환.
    - 허용 안 된 recipe는 RecipeError.
    - non-recipe(atomic) step은 그대로. invalid recipe params는 RecipeError.

    expand 결과는 호출부(execute_analyze_plan)에서 기존 validator(execute_plan)로
    재검증된다 — recipe step은 여기서 사라지므로 validator는 atomic만 본다."""
    if not isinstance(plan, dict):
        return plan
    steps = plan.get("steps")
    if not isinstance(steps, list):
        return plan
    if not any(isinstance(s, dict) and s.get("skill") in RECIPE_SPECS for s in steps):
        return plan  # recipe 없음 → no-op

    new_steps: list[Any] = []
    for step in steps:
        skill = step.get("skill") if isinstance(step, dict) else None
        if skill in RECIPE_SPECS:
            if skill not in RUNTIME_ENABLED_RECIPES:
                enabled = ", ".join(sorted(RUNTIME_ENABLED_RECIPES))
                raise RecipeError(
                    f"recipe '{skill}' is not enabled for execution yet (enabled: {enabled})"
                )
            new_steps.extend(lower_recipe(step))
        else:
            new_steps.append(step)
    return {**plan, "steps": new_steps}


__all__ = [
    "RecipeError",
    "RecipeParamSpec",
    "RecipeSpec",
    "RECIPE_SPECS",
    "DISTRIBUTION_SPEC",
    "EVENT_WINDOW_COUNT_SPEC",
    "TOP_N_SPEC",
    "SAMPLE_ROWS_SPEC",
    "lower_recipe",
    "lower_distribution",
    "lower_event_window_count",
    "lower_top_n",
    "lower_sample_rows",
    "expand_recipes",
    "RUNTIME_ENABLED_RECIPES",
]
