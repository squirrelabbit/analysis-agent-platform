from __future__ import annotations

"""Sort skill contract (validator R5-sort, 2026-05-27).

R5 pilot(``present``)에서 검증된 SkillContract 패턴을 sort로 확장한 두 번째
skill. 다른 5 skill(filter/join/aggregate/compare/calculate)은 옛
``_validate_X`` 그대로.

issue code 그대로 유지: ``params.missing_keys`` / ``params.by_not_list`` /
``params.input_*`` / ``params.column_unknown`` / ``params.sort_by_unknown`` /
``params.order_invalid`` / ``params.limit_invalid``.
"""

from typing import TYPE_CHECKING, Any, Callable

from ..schema import SORT_ORDERS

if TYPE_CHECKING:
    from ..validator import _StepContext


class SortSkillContract:
    """plan_v2 ``sort`` skill의 contract."""

    name = "sort"

    def validate(self, params: dict[str, Any], ctx: "_StepContext") -> None:
        # cycle 회피 — validator helper는 함수 호출 시점에 lazy import.
        from ..validator import (
            _check_input_columns_exist,
            _check_input_ref,
            _check_required_keys,
        )

        if not _check_required_keys(params, ("input", "by"), ctx):
            return
        by = params.get("by")
        if not isinstance(by, list) or not by:
            ctx.issue(
                code="params.by_not_list",
                message="sort.by must be a non-empty list",
            )
            by_columns: list[str] = []
        else:
            by_columns = [str(col or "").strip() for col in by]
        _check_input_ref(
            params.get("input"), "input", ctx, require_column=by_columns or None
        )
        # silverone 2026-05-26 (SQL-3.4, audit M6) — step input의 경우도 inferred
        # output에 by_columns가 있는지 검증.
        _check_input_columns_exist(
            input_ref=str(params.get("input") or "").strip(),
            required_columns=by_columns,
            ctx=ctx,
            issue_code="params.sort_by_unknown",
            message_builder=lambda col, ref, available: (
                f"sort.by '{col}'가 step '{ref}'의 output에 없다. "
                f"available: {available}."
            ),
        )
        if "order" in params:
            order = str(params.get("order") or "").strip()
            if order and order not in SORT_ORDERS:
                ctx.issue(
                    code="params.order_invalid",
                    message=(
                        f"sort.order must be one of {sorted(SORT_ORDERS)}; got '{order}'"
                    ),
                )
        if "limit" in params:
            limit = params.get("limit")
            if limit is not None and (
                not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
            ):
                ctx.issue(
                    code="params.limit_invalid",
                    message="sort.limit must be null or a positive integer",
                )

    def infer_output_columns(
        self,
        params: dict[str, Any],
        upstream: Callable[[str], "set[str] | None"],
    ) -> "set[str] | None":
        # sort는 input rows의 순서만 바꾸므로 output columns == input columns.
        # upstream이 None 반환(추론 불가)이면 그대로 전파.
        input_ref = str(params.get("input") or "").strip()
        if not input_ref:
            return None
        return upstream(input_ref)


__all__ = ["SortSkillContract"]
