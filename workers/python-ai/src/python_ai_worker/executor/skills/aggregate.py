from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import quote_identifier, safe_identifier

if TYPE_CHECKING:
    from ..context import ExecutorContext


def build_sql(params: dict[str, Any], context: "ExecutorContext") -> tuple[str, dict[str, Any]]:
    input_ref = safe_identifier(params["input"])
    # silverone 2026-06-05 — group_by=[] (total mode) 허용. 빈 리스트면 GROUP BY 없이
    # 전체 1행 집계(SELECT COUNT(*) AS count FROM input). non-empty 동작은 불변.
    group_by = [quote_identifier(c) for c in (params.get("group_by") or [])]
    metric_exprs: list[str] = []
    for metric in params["metrics"]:
        name = quote_identifier(metric["name"])
        function = str(metric["function"]).strip().lower()
        column = metric.get("column")
        if function == "count" and column in (None, "", "*"):
            metric_exprs.append(f"COUNT(*) AS {name}")
        else:
            col_ident = quote_identifier(column)
            metric_exprs.append(f"{function.upper()}({col_ident}) AS {name}")

    select_clause = ", ".join(group_by + metric_exprs)
    if group_by:
        sql = f"SELECT {select_clause} FROM {input_ref} GROUP BY {', '.join(group_by)}"
    else:
        sql = f"SELECT {select_clause} FROM {input_ref}"
    return sql, {}


__all__ = ["build_sql"]
