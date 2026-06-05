from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import ExecutorError, quote_identifier, safe_identifier

if TYPE_CHECKING:
    from ..context import ExecutorContext


def build_sql(params: dict[str, Any], context: "ExecutorContext") -> tuple[str, dict[str, Any]]:
    """calculate skill SQL builder.

    NULL / zero 정책 (silverone 2026-05-26, SQL-2):
      - add / subtract  : NULL → 0 으로 본다 (COALESCE 0 wrapping).
        카운트 비교가 FULL OUTER JOIN으로 한쪽이 NULL인 경우(compare M5)에
        delta가 자연스럽게 떨어지도록.
      - multiply        : NULL 보존. 한쪽이 NULL이면 결과 NULL.
      - divide          : NULL 보존 + 분모 0 가드 (분모 NULL/0 → NULL). SQL-2.1.
        ratio와 동일한 패턴. integer ZeroDivisionError / INF 노출 방지.
      - percent_change  : base NULL/0 → NULL, current NULL → 0으로 본다.
      - ratio           : denominator NULL/0 → NULL, numerator NULL은 보존.
      - share_of_total  : value의 전체 합 대비 비중(0~1). window SUM OVER로 전체
        (또는 partition_by 그룹) 합을 구해 row별 value를 나눈다. SQL-2와 동일하게
        합이 NULL/0이면 NULL (NULLIF). silverone 2026-06-02.
    """

    input_ref = safe_identifier(params["input"])
    pieces: list[str] = ["*"]
    for expression in params["expressions"]:
        name = quote_identifier(expression["name"])
        operation = str(expression["operation"]).strip().lower()
        if operation in {"add", "subtract"}:
            left = quote_identifier(expression["left"])
            right = quote_identifier(expression["right"])
            op = "+" if operation == "add" else "-"
            pieces.append(f"(COALESCE({left}, 0) {op} COALESCE({right}, 0)) AS {name}")
        elif operation == "multiply":
            left = quote_identifier(expression["left"])
            right = quote_identifier(expression["right"])
            pieces.append(f"({left} * {right}) AS {name}")
        elif operation == "divide":
            # silverone 2026-05-26 (SQL-2.1, audit C3) — 분모 NULL/0 가드.
            left = quote_identifier(expression["left"])
            right = quote_identifier(expression["right"])
            pieces.append(
                f"(CASE WHEN {right} IS NULL OR {right} = 0 THEN NULL "
                f"ELSE {left} * 1.0 / {right} END) AS {name}"
            )
        elif operation == "percent_change":
            base = quote_identifier(expression["base"])
            current = quote_identifier(expression["current"])
            pieces.append(
                f"(CASE WHEN {base} IS NULL OR {base} = 0 THEN NULL "
                f"ELSE (COALESCE({current}, 0) - {base}) * 100.0 / {base} END) AS {name}"
            )
        elif operation == "ratio":
            numerator_key = "numerator" if "numerator" in expression else "left"
            denominator_key = "denominator" if "denominator" in expression else "right"
            numerator = quote_identifier(expression[numerator_key])
            denominator = quote_identifier(expression[denominator_key])
            pieces.append(
                f"(CASE WHEN {denominator} IS NULL OR {denominator} = 0 THEN NULL "
                f"ELSE {numerator} * 1.0 / {denominator} END) AS {name}"
            )
        elif operation == "share_of_total":
            # silverone 2026-06-02 — value의 전체(또는 partition) 합 대비 비중(0~1).
            # window SUM OVER (PARTITION BY ...) 로 합을 broadcast해 row별로 나눈다.
            value = quote_identifier(expression["value"])
            partition_by = expression.get("partition_by") or []
            if partition_by:
                cols = ", ".join(quote_identifier(c) for c in partition_by)
                window = f"SUM({value}) OVER (PARTITION BY {cols})"
            else:
                window = f"SUM({value}) OVER ()"
            pieces.append(f"({value} * 1.0 / NULLIF({window}, 0)) AS {name}")
        else:
            raise ExecutorError(f"calculate.operation unsupported: {operation}")
    sql = f"SELECT {', '.join(pieces)} FROM {input_ref}"
    return sql, {}


__all__ = ["build_sql"]
