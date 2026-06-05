from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import quote_identifier, safe_identifier

if TYPE_CHECKING:
    from ..context import ExecutorContext


def build_sql(params: dict[str, Any], context: "ExecutorContext") -> tuple[str, dict[str, Any]]:
    input_ref = safe_identifier(params["input"])
    group_by = [quote_identifier(c) for c in params["group_by"]]
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
    group_clause = ", ".join(group_by)
    sql = f"SELECT {select_clause} FROM {input_ref} GROUP BY {group_clause}"
    return sql, {}


__all__ = ["build_sql"]
