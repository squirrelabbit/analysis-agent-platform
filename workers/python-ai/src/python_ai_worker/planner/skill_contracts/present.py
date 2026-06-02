from __future__ import annotations

"""Present skill contract (validator R5 pilot, 2026-05-27).

옛 ``planner.validator._validate_present`` 본문을 contract 구조로 옮긴 첫
skill. validate/infer_output_columns 책임을 클래스 한 곳에 모은다.

R5 pilot 정책에 따라 *위치만 이동*. issue code (``params.format_invalid`` /
``params.limit_invalid`` / ``params.limit_cap_exceeded`` / ``params.missing_keys``
/ ``params.input_*``)는 모두 보존. executor builder 흡수는 R6 후속.
"""

from typing import TYPE_CHECKING, Any, Callable

from ..schema import PRESENT_FORMATS

if TYPE_CHECKING:
    from ..validator import _StepContext


# silverone 2026-05-26 (SQL-4) — present 응답 row 한도. validator는 1~10000
# 정수만 허용. executor/skills/present.py는 같은 cap을 runtime fallback으로
# 적용 (DEFAULT_PRESENT_MAX_ROWS / PRESENT_HARD_CAP_ROWS). R6에서 executor를
# 흡수할 때 두 cap 상수를 한 곳으로 통합 예정.
PRESENT_LIMIT_HARD_CAP = 10000


class PresentSkillContract:
    """plan_v2 ``present`` skill의 contract."""

    name = "present"

    def validate(self, params: dict[str, Any], ctx: "_StepContext") -> None:
        # cycle 회피 — validator helper는 함수 호출 시점에 lazy import.
        from ..validator import _check_input_ref, _check_required_keys

        if not _check_required_keys(params, ("input", "format"), ctx):
            return
        _check_input_ref(params.get("input"), "input", ctx)
        fmt = str(params.get("format") or "").strip()
        if fmt not in PRESENT_FORMATS:
            ctx.issue(
                code="params.format_invalid",
                message=(
                    f"present.format must be one of {sorted(PRESENT_FORMATS)}; got '{fmt}'"
                ),
            )
        # silverone 2026-05-26 (SQL-4, audit M7) — present.limit은 1~PRESENT_LIMIT_HARD_CAP
        # 정수 또는 누락(default 1000). 0/음수/비정수/cap 초과는 reject.
        if "limit" in params:
            limit = params.get("limit")
            if limit is not None:
                if isinstance(limit, bool) or not isinstance(limit, int):
                    ctx.issue(
                        code="params.limit_invalid",
                        message=(
                            "present.limit must be null or a positive integer in "
                            f"[1, {PRESENT_LIMIT_HARD_CAP}]"
                        ),
                    )
                elif limit <= 0:
                    ctx.issue(
                        code="params.limit_invalid",
                        message="present.limit must be a positive integer",
                    )
                elif limit > PRESENT_LIMIT_HARD_CAP:
                    ctx.issue(
                        code="params.limit_cap_exceeded",
                        message=(
                            f"present.limit {limit}는 hard cap {PRESENT_LIMIT_HARD_CAP}을 초과한다. "
                            "운영 안전 한도를 넘어가는 row 수는 단일 응답으로 반환하지 않는다."
                        ),
                    )

        # silverone 2026-06-02 — present.columns hard constraint. columns가 명시되면
        # input의 출력 컬럼에 모두 존재해야 한다. 없으면 repair 대상(issue)으로 표면화.
        # input이 RESERVED 테이블이면 TABLE_SCHEMAS로, prior step이면
        # _infer_step_output_columns로 컬럼 집합을 구해 검사한다. 추론 불가(None)면
        # 정적 검증을 건너뛴다 (executor 런타임이 DuckDB Binder Error로 잡는다).
        self._validate_columns(params, ctx)

    def _validate_columns(self, params: dict[str, Any], ctx: "_StepContext") -> None:
        if "columns" not in params or params.get("columns") is None:
            return
        columns = params.get("columns")
        if not isinstance(columns, list):
            ctx.issue(
                code="params.columns_not_list",
                message="present.columns must be a string array or null",
            )
            return
        names: list[str] = []
        for col in columns:
            text = str(col).strip() if isinstance(col, str) else ""
            if not text:
                ctx.issue(
                    code="params.columns_invalid",
                    message="present.columns entries must be non-empty strings",
                )
                return
            names.append(text)
        if not names:
            return

        from ..schema import TABLE_SCHEMAS
        from ..validator import _check_columns_on_table, _infer_step_output_columns

        input_ref = str(params.get("input") or "").strip()
        if input_ref in TABLE_SCHEMAS:
            _check_columns_on_table(input_ref, names, "columns", ctx)
            return
        inferred = _infer_step_output_columns(input_ref, ctx.step_lookup, visiting=set())
        if inferred is None:
            return  # 추론 불가 — executor 런타임 검증에 위임.
        missing = [c for c in names if c not in inferred]
        if missing:
            ctx.issue(
                code="params.columns_unknown",
                message=(
                    f"present.columns {missing} not in input '{input_ref}' output columns "
                    f"{sorted(inferred)}"
                ),
            )

    def infer_output_columns(
        self,
        params: dict[str, Any],
        upstream: Callable[[str], "set[str] | None"],
    ) -> "set[str] | None":
        # present는 chain 끝에서만 쓰이므로 후행 step이 없다. validator의
        # _infer_step_output_columns도 동일하게 None을 반환한다.
        return None


__all__ = ["PRESENT_LIMIT_HARD_CAP", "PresentSkillContract"]
