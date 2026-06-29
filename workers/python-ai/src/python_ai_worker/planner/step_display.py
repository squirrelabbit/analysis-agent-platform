from __future__ import annotations

"""plan-step display(`{label, expression}`) 생성 — Skill Contract v2 Step 3.

plan step별 사용자 화면용 label/expression을 합성한다. 옛 source는 Go
control-plane ``service/plan_step_display.go``였고, Step 3에서 source of truth를
worker(Python)로 옮긴다. worker가 analyze 응답의 plan.steps[].display를 채우고,
Go는 display가 있으면 pass-through / 없으면 기존 builder로 fallback한다(이번 PR에서
Go fallback은 유지).

Go displayX 함수를 **byte 동일하게** 미러링한다 (parity는 test_step_display가
Go golden으로 잠금). expression은 SQL-like 직관 표현이며 실제 executor SQL과는
무관(디버그 노출 금지 정책). frontend 노출 shape는 정확히 ``{label, expression}``.
"""

from typing import Any

_ARITH_SYMBOLS = {"add": "+", "subtract": "-", "multiply": "*", "divide": "/"}


def build_step_display(step: dict[str, Any]) -> dict[str, str] | None:
    """plan step 1개 → ``{label, expression}`` 또는 None(알 수 없는 skill / 깨진 params).

    None이면 프론트는 raw params JSON으로 fallback (Go도 동일 정책)."""
    if not isinstance(step, dict):
        return None
    skill = step.get("skill")
    params = step.get("params")
    if not isinstance(params, dict):
        params = {}
    builder = _BUILDERS.get(skill)  # type: ignore[arg-type]
    return builder(params) if builder else None


def plan_with_step_display(plan: dict[str, Any]) -> dict[str, Any]:
    """plan을 비파괴 복사하면서 각 step에 display를 채워 반환. analyze 응답용.

    원본 plan/step dict는 변경하지 않는다 (reuse/로그 경로 보호). display 합성이
    None인 step은 display 키를 추가하지 않는다."""
    if not isinstance(plan, dict):
        return plan
    steps = plan.get("steps")
    if not isinstance(steps, list):
        return plan
    new_steps: list[Any] = []
    for step in steps:
        if isinstance(step, dict):
            display = build_step_display(step)
            if display is not None:
                step = {**step, "display": display}
        new_steps.append(step)
    return {**plan, "steps": new_steps}


# ===== param helpers (Go stringParam / stringListParam 미러) =====


def _str_param(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    return value.strip() if isinstance(value, str) else ""


def _str_list_param(params: dict[str, Any], key: str) -> list[str]:
    raw = params.get(key)
    if not isinstance(raw, list):
        return []
    return [item.strip() for item in raw if isinstance(item, str) and item.strip()]


def _non_empty(value: str, fallback: str) -> str:
    return value if value else fallback


# ===== literal formatting (Go formatLiteral / formatList / formatBetween 미러) =====


def _format_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):  # bool은 int subclass라 먼저 처리
        return "TRUE" if v else "FALSE"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        # Go: int-valued float64 → %d, else %g
        return str(int(v)) if v == int(v) else f"{v:g}"
    return str(v)


def _format_list(v: Any) -> str:
    if isinstance(v, list):
        parts = sorted(_format_literal(item) for item in v)  # Go sort.Strings = 결정성
        return "(" + ", ".join(parts) + ")"
    return "(" + _format_literal(v) + ")"


def _format_between(v: Any) -> str:
    if isinstance(v, list) and len(v) == 2:
        return _format_literal(v[0]) + " AND " + _format_literal(v[1])
    return _format_literal(v)


def _filter_op_to_sql(op: str, value: Any) -> str:
    if op == "eq":
        return "= " + _format_literal(value)
    if op == "neq":
        return "!= " + _format_literal(value)
    if op == "gt":
        return "> " + _format_literal(value)
    if op == "gte":
        return ">= " + _format_literal(value)
    if op == "lt":
        return "< " + _format_literal(value)
    if op == "lte":
        return "<= " + _format_literal(value)
    if op == "in":
        return "IN " + _format_list(value)
    if op == "not_in":
        return "NOT IN " + _format_list(value)
    if op == "contains":
        return "LIKE " + _format_literal(value)
    if op == "between":
        return "BETWEEN " + _format_between(value)
    if op == "is_null":
        return "IS NULL"
    if op == "not_null":
        return "IS NOT NULL"
    return op + " " + _format_literal(value)


def _positive_int_limit(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    return None


# ===== skill별 builder (Go displayX 미러) =====


def _display_filter(p: dict[str, Any]) -> dict[str, str] | None:
    column = _str_param(p, "column")
    if not column:
        return None
    op = _str_param(p, "operator")
    expr = "WHERE " + column + " " + _filter_op_to_sql(op, p.get("value"))
    return {"label": "조건 필터", "expression": expr}


def _display_join(p: dict[str, Any]) -> dict[str, str] | None:
    left = _str_param(p, "left")
    right = _str_param(p, "right")
    if not left or not right:
        return None
    how = _str_param(p, "how") or "inner"
    expr = f"{left} {how.upper()} JOIN {right}"
    on = _str_list_param(p, "on")
    if on:
        expr += " ON " + ", ".join(on)
    return {"label": "데이터 연결", "expression": expr}


def _display_aggregate(p: dict[str, Any]) -> dict[str, str] | None:
    group_by = _str_list_param(p, "group_by")
    metrics = p.get("metrics")
    metrics = metrics if isinstance(metrics, list) else []
    parts: list[str] = []
    if group_by:
        parts.append("GROUP BY " + ", ".join(group_by))
    if metrics:
        metric_exprs: list[str] = []
        for m in metrics:
            if not isinstance(m, dict):
                continue
            fn = _str_param(m, "function").upper()
            col = _str_param(m, "column")
            name = _str_param(m, "name")
            if not fn:
                continue
            call = f"{fn}({col})"
            if fn == "COUNT" and col == "":
                call = "COUNT(*)"
            if name:
                call += " AS " + name
            metric_exprs.append(call)
        if metric_exprs:
            parts.append(", ".join(metric_exprs))
    if not parts:
        return None
    label = (group_by[0] + "별 집계") if group_by else "집계"
    return {"label": label, "expression": " · ".join(parts)}


def _display_compare(p: dict[str, Any]) -> dict[str, str] | None:
    left = _str_param(p, "left")
    right = _str_param(p, "right")
    if not left or not right:
        return None
    left_name = _str_param(p, "left_label") or left
    right_name = _str_param(p, "right_label") or right
    expr = f"COMPARE {left_name} vs {right_name}"
    join_key = _str_list_param(p, "join_key")
    if join_key:
        expr += " ON " + ", ".join(join_key)
    return {"label": "두 결과 비교", "expression": expr}


def _display_calculate(p: dict[str, Any]) -> dict[str, str] | None:
    expressions = p.get("expressions")
    if not isinstance(expressions, list) or not expressions:
        return None
    exprs: list[str] = []
    label = "값 계산"
    for item in expressions:
        if not isinstance(item, dict):
            continue
        name = _str_param(item, "name")
        op = _str_param(item, "operation")
        left = _str_param(item, "left")
        right = _str_param(item, "right")
        if op == "ratio":
            label = "비율 계산"
            expr = f"{_non_empty(left, 'left')} / {_non_empty(right, 'right')} * 100"
        elif op == "percent_change":
            label = "증감률 계산"
            lo = _non_empty(left, "left")
            ro = _non_empty(right, "right")
            expr = f"({ro} - {lo}) / {lo} * 100"
        elif op == "share_of_total":
            label = "비중 계산"
            value = _str_param(item, "value")
            parts = _str_list_param(item, "partition_by")
            denom = (", ".join(parts) + "별 합계") if parts else "전체 합계"
            expr = f"{_non_empty(value, 'value')} / {denom} * 100"
        elif op in _ARITH_SYMBOLS:
            expr = f"{_non_empty(left, 'left')} {_ARITH_SYMBOLS[op]} {_non_empty(right, 'right')}"
        else:
            exprs.append(f"{name} = {op}" if name else op)
            continue
        exprs.append(f"{name} = {expr}" if name else expr)
    if not exprs:
        return None
    return {"label": label, "expression": ", ".join(exprs)}


def _display_sort(p: dict[str, Any]) -> dict[str, str] | None:
    by = _str_list_param(p, "by")
    if not by:
        return None
    order = _str_param(p, "order") or "desc"
    expr = "ORDER BY " + ", ".join(by) + " " + order.upper()
    limit = _positive_int_limit(p.get("limit"))
    if limit is not None:
        expr += f" LIMIT {limit}"
    return {"label": "정렬", "expression": expr}


def _display_present(p: dict[str, Any]) -> dict[str, str]:
    fmt = _str_param(p, "format") or "table"
    title = _str_param(p, "title")
    expr = fmt.upper()
    if title:
        expr = f"{fmt.upper()}: {title}"
    limit = _positive_int_limit(p.get("limit"))
    if limit is not None:
        expr += f" (LIMIT {limit})"
    return {"label": "결과 표시", "expression": expr}


_BUILDERS = {
    "filter": _display_filter,
    "join": _display_join,
    "aggregate": _display_aggregate,
    "compare": _display_compare,
    "calculate": _display_calculate,
    "sort": _display_sort,
    "present": _display_present,
}


__all__ = ["build_step_display", "plan_with_step_display"]
