from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import quote_identifier, safe_identifier

if TYPE_CHECKING:
    from ..context import ExecutorContext


def build_sql(params: dict[str, Any], context: "ExecutorContext") -> tuple[str, dict[str, Any]]:
    input_ref = safe_identifier(params["input"])
    by = [quote_identifier(c) for c in params["by"]]
    order = str(params.get("order") or "desc").strip().lower()
    direction = "ASC" if order == "asc" else "DESC"
    order_parts = [f"{c} {direction}" for c in by]
    sql = f"SELECT * FROM {input_ref} ORDER BY {', '.join(order_parts)}"
    limit = params.get("limit")
    if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
        sql += f" LIMIT {int(limit)}"
    return sql, {}


__all__ = ["build_sql"]
