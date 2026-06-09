"""answer composition layer (ADR-020 PR-A — deterministic only).

executor result를 사용자-facing assistant_message로 변환한다. LLM 호출 없음.
ADR-020 §5 deterministic v1 정책의 5 템플릿을 한 함수로 구현.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "compose_answer",
    "FAILED_RUN_FALLBACK_CONTENT",
]


# composer 실패 또는 error_metadata 분기에서 사용. 운영 실제로는 control plane이
# `failedRunAssistantPlaceholder`로 덮어쓰지만 composer 자체도 같은 문구를 알고
# 있게 해서 정합성을 유지한다.
FAILED_RUN_FALLBACK_CONTENT = "분석 실행 중 오류가 발생했습니다. 조건을 조금 단순화해 다시 시도해 주세요."

_DEFAULT_MAX_ROWS = 1000


def compose_answer(
    *,
    user_question: str | None,
    present: dict[str, Any] | None,
    plan: dict[str, Any] | None = None,
    steps: list[dict[str, Any]] | None = None,
    reuse_metadata: dict[str, Any] | None = None,
    error_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """analyze result로부터 assistant_content + display + context_summary를 생성.

    실패 분기:
      - error_metadata가 있으면 placeholder 문구 + mode='error'.
      - present가 None이면 (executor가 present payload를 못 만든 경우) fallback.
      - 그 외에는 5 템플릿 중 하나로 분기.

    composer는 절대 raise하지 않는다 — caller가 답변 없는 상황에 안 빠지도록.
    """

    try:
        return _compose_safely(
            user_question=user_question,
            present=present,
            plan=plan,
            steps=steps,
            reuse_metadata=reuse_metadata,
            error_metadata=error_metadata,
        )
    except Exception as exc:  # noqa: BLE001 — composer 실패가 run 실패로 번지지 않게
        return _fallback_payload(
            user_question=user_question,
            reason=f"composer_exception: {type(exc).__name__}: {exc}",
        )


def _compose_safely(
    *,
    user_question: str | None,
    present: dict[str, Any] | None,
    plan: dict[str, Any] | None,
    steps: list[dict[str, Any]] | None,
    reuse_metadata: dict[str, Any] | None,
    error_metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    # 1. failed run — composer는 placeholder 문구 + mode=error.
    if error_metadata:
        return _error_payload(user_question=user_question, error_metadata=error_metadata)

    # 1.5 answerable=false 거절 plan (silverone 2026-06-01, PR1) — reason별 메시지를
    # 그대로 렌더하고 display=null. raw row 테이블/truncation warning을 만들지 않는다.
    if isinstance(plan, dict) and plan.get("answerable") is False:
        return _reject_payload(user_question=user_question, plan=plan)

    # 2. present 없음 — fallback.
    if not isinstance(present, dict):
        return _fallback_payload(user_question=user_question, reason="present_missing")

    total_rows = _safe_int(present.get("total_rows"), default=_safe_int(present.get("row_count"), 0))
    returned_rows = _safe_int(present.get("returned_rows"), default=len(present.get("rows") or []))
    truncated = bool(present.get("truncated", False))
    fmt = str(present.get("format") or "table").strip() or "table"

    # display를 먼저 만들어 최종 recommended_view(차트 다운그레이드 반영 후)를 얻고,
    # 그 값으로 본문 메시지를 만든다 — 영문 fmt(chart/json) 대신 한국어 view 표현 사용.
    display = _build_display(
        present=present,
        fmt=fmt,
        total_rows=total_rows,
        returned_rows=returned_rows,
        truncated=truncated,
        plan=plan,
    )

    template, content = _select_template(
        total_rows=total_rows,
        returned_rows=returned_rows,
        truncated=truncated,
        recommended_view=str(display.get("recommended_view") or ""),
        reuse_metadata=reuse_metadata,
    )
    # silverone 2026-06-09 — 기간/그룹 구성비 비교는 generic "N건 정리" 대신 핵심
    # 변화 요약으로 교체(가장 증가/감소/거의 변화 없음). empty/reuse/truncated가
    # 아닐 때만(=table_normal 류) 적용. 원인 추정은 하지 않는다.
    if template == "table_normal":
        change_summary = _compare_distribution_summary(
            present.get("rows") or [], display.get("columns") or []
        )
        if change_summary:
            content = change_summary
            template = "compare_distribution_summary"
    context_summary = _build_context_summary(
        user_question=user_question,
        present=present,
        total_rows=total_rows,
        returned_rows=returned_rows,
        answer_summary=content,
    )
    metadata = {
        "mode": "deterministic",
        "template": template,
        "fallback_reason": None,
    }
    return {
        "assistant_content": content,
        "display": display,
        "context_summary": context_summary,
        "metadata": metadata,
    }


# recommended_view enum → 한국어 표현 (silverone 2026-06-02). assistant_content에
# 영문 view/format 키워드(chart / table / line)가 한국어 문장에 박히지 않게 한다.
# unknown/기타는 form 표현을 생략해 일반 문장으로 둔다 (새 view 추가에도 안전).
_VIEW_LABEL_KO: dict[str, str] = {
    "table": "표로",
    "bar": "막대그래프로",
    "line": "선그래프로",
}


def _view_label_ko(recommended_view: str) -> str:
    return _VIEW_LABEL_KO.get(str(recommended_view or "").strip(), "")


def _select_template(
    *,
    total_rows: int,
    returned_rows: int,
    truncated: bool,
    recommended_view: str,
    reuse_metadata: dict[str, Any] | None,
) -> tuple[str, str]:
    """ADR-020 §5 우선순위 — empty → reuse → truncated → normal."""

    if total_rows <= 0:
        return "empty", "조건에 맞는 결과가 없습니다."
    if reuse_metadata and reuse_metadata.get("applied") is True:
        return "reuse_applied", "이전 분석 결과를 기준으로 요청한 표시 조건을 반영했습니다."
    if truncated:
        return (
            "table_truncated",
            f"전체 {total_rows}건 중 {returned_rows}건만 표시했습니다.",
        )
    label = _view_label_ko(recommended_view)
    if label:
        return ("table_normal", f"분석 결과 {returned_rows}건을 {label} 정리했습니다.")
    return ("table_normal", f"분석 결과 {returned_rows}건을 정리했습니다.")


def _build_display(
    *,
    present: dict[str, Any],
    fmt: str,
    total_rows: int,
    returned_rows: int,
    truncated: bool,
    plan: dict[str, Any] | None,
) -> dict[str, Any]:
    """present payload를 거의 그대로 frontend 렌더링 형태로 정리.
    PR-A에서는 변환 없음 — type=format만 매핑.

    silverone 2026-05-27 (display-columns) — 프론트가 table column 순서를
    안정적으로 렌더링할 수 있게 `columns` 필드 추가. v1은 string list만
    (key 순서). rows[0] key를 그대로 사용. rows가 비어있으면 [].
    label/type/format/chart_spec 등은 후속 PR.

    silverone 2026-05-27 (display-warnings v1) — 사용자 화면에 표시할 수 있는
    경고 문자열 list. empty / truncated / null ratio 3종 v1. analysis_display_
    contract_2026-05-27 §추가권장 1차."""

    rows = present.get("rows") or []
    max_rows = _safe_int(present.get("max_rows"), default=_DEFAULT_MAX_ROWS) or _DEFAULT_MAX_ROWS
    columns = _columns_from_rows(rows)
    recommended_view = _recommended_view(rows=rows, columns=columns, plan=plan)
    chart_spec = _build_chart_spec(
        recommended_view=recommended_view, rows=rows, columns=columns, plan=plan,
    )
    # chart_spec 검증(_build_chart_spec)을 통과하지 못하면 차트 추천을 철회하고
    # table로 내린다 — recommended_view와 chart_spec이 항상 일관되도록.
    if recommended_view in ("bar", "line") and chart_spec is None:
        recommended_view = "table"
    # silverone 2026-06-02 — line 차트는 x축이 시계열이라 행이 x 기준 정렬돼 있어야
    # 선이 올바르다. planner가 sort step을 넣었는지에 의존하지 않고(예: compare만
    # 쓴 plan은 행이 임의 순서) composer가 x 오름차순으로 보정한다. table/bar는
    # planner 의도(값 정렬 등)를 보존하기 위해 건드리지 않는다.
    if recommended_view == "line" and isinstance(chart_spec, dict):
        rows = _sort_rows_by_x(rows, str(chart_spec.get("x") or ""))
    result: dict[str, Any] = {
        "type": fmt,
        "title": present.get("title"),
        "columns": columns,
        "rows": rows,
        "total_rows": total_rows,
        "returned_rows": returned_rows,
        "truncated": truncated,
        "max_rows": max_rows,
        "warnings": _build_warnings(
            rows=rows,
            plan=plan,
            total_rows=total_rows,
            returned_rows=returned_rows,
            truncated=truncated,
        ),
        "recommended_view": recommended_view,
        "chart_spec": chart_spec,
    }
    # silverone 2026-06-09 — compare 결과(전/후 + delta)는 count/ratio/pp 단위가
    # 섞여 raw 값만으론 오해 소지가 크다. 컬럼별 표시 포맷/라벨을 contract로 내려
    # 프론트가 %·%p·정수로 렌더하게 한다(compare 결과에만 부여).
    if _has_compare_columns(columns):
        formats, labels = _compare_column_formats_labels(columns)
        if formats:
            result["column_formats"] = formats
        if labels:
            result["column_labels"] = labels
    return result


# silverone 2026-05-27 (display-warnings v1) — 사용자 화면 노출 가능한 짧은
# 경고 문자열 list. context_summary와 다름 — warnings는 화면 표시, context_
# summary는 LLM 다음 turn용으로 노출 금지.
_NULL_RATIO_OPERATIONS = frozenset({"ratio", "percent_change", "share_of_total"})
_NULL_RATIO_WARNING = "일부 비율 값은 기준값이 없어 계산되지 않았습니다."


def _build_warnings(
    *,
    rows: list[Any],
    plan: dict[str, Any] | None,
    total_rows: int,
    returned_rows: int,
    truncated: bool,
) -> list[str]:
    warnings: list[str] = []
    if total_rows <= 0:
        warnings.append("조건에 맞는 결과가 없습니다.")
        return warnings
    if truncated:
        warnings.append(
            f"전체 {total_rows}건 중 {returned_rows}건만 표시했습니다."
        )
    ratio_columns = _nullable_ratio_columns_from_plan(plan)
    if ratio_columns and _rows_have_null_in_columns(rows, ratio_columns):
        warnings.append(_NULL_RATIO_WARNING)
    return warnings


def _nullable_ratio_columns_from_plan(plan: dict[str, Any] | None) -> set[str]:
    """plan.steps에서 calculate.ratio / percent_change expression 이름 추출.
    SQL-2.1 (divide by zero 가드)로 분모 NULL/0이면 결과 NULL이라 row에 null이
    들어갈 수 있다. divide도 같은 가드를 받지만 사용자 명시는 ratio /
    percent_change 두 가지 (사용자 가시 빈도 ↑)."""

    cols: set[str] = set()
    if not isinstance(plan, dict):
        return cols
    steps = plan.get("steps")
    if not isinstance(steps, list):
        return cols
    for step in steps:
        if not isinstance(step, dict):
            continue
        if str(step.get("skill") or "").strip() != "calculate":
            continue
        params = step.get("params")
        if not isinstance(params, dict):
            continue
        expressions = params.get("expressions")
        if not isinstance(expressions, list):
            continue
        for expr in expressions:
            if not isinstance(expr, dict):
                continue
            operation = str(expr.get("operation") or "").strip()
            if operation not in _NULL_RATIO_OPERATIONS:
                continue
            name = str(expr.get("name") or "").strip()
            if name:
                cols.add(name)
    return cols


def _rows_have_null_in_columns(rows: list[Any], columns: set[str]) -> bool:
    for row in rows:
        if not isinstance(row, dict):
            continue
        for col in columns:
            if col in row and row[col] is None:
                return True
    return False


# silverone 2026-06-01 (chart-ready metadata v1) — display.recommended_view +
# chart_spec 추정. display.type은 "table" 유지하고 chart 힌트만 제공한다.
# 완전한 차트 spec(Vega-Lite 등)은 v2로 보류. 휴리스틱은 deterministic —
# 모호하면 table/null로 보수적 fallback.

# x축이 시계열 line chart로 추천되는 컬럼 prefix. created_at / 년월일 / date
# 계열을 라인으로 보고, 그 외 categorical은 bar.
_LINE_X_PREFIXES = ("created_at", "date", "year", "month", "day", "timestamp", "time", "ts")
# compare step output에서 자주 나오는 metric prefix/suffix. 두 시점/그룹 비교
# 결과는 bar로 추천 (group_by 컬럼 x축 + last/this/delta y축).
_COMPARE_COLUMN_PATTERNS = ("delta_", "last_", "this_")


def _recommended_view(
    *, rows: list[Any], columns: list[str], plan: dict[str, Any] | None,
) -> str:
    """rows + columns + plan 기반 chart 추천. 모호하면 "table"로 fallback.

    enum: table / bar / line. v1 — 정교화는 후속 PR.
    """
    if not rows or not columns:
        return "table"
    # 1 row만 있으면 차트로서 의미 약함 — 단 다중 group_by + 다중 metric은 추후
    # 가능하지만 v1은 보수적.
    if len(rows) == 1:
        return "table"
    # compare 결과 — delta_/last_/this_ 컬럼 존재 시 bar 우선.
    if _has_compare_columns(columns):
        # x축이 될 categorical group_by 컬럼이 있어야 한다.
        if _first_categorical_column(rows, columns) is not None:
            return "bar"
        return "table"
    # categorical group_by + numeric metric 조합 확인.
    x_col = _first_categorical_column(rows, columns)
    if x_col is None:
        return "table"
    if not _has_numeric_metric_column(rows, columns, exclude={x_col}):
        return "table"
    # x축이 시계열 컬럼이면 line, 아니면 bar.
    if _looks_like_time_column(x_col):
        return "line"
    return "bar"


def _sort_rows_by_x(rows: list[Any], x_col: str) -> list[Any]:
    """line 차트용 — x_col 기준 오름차순 정렬. x가 없는/None인 행은 끝으로.

    ISO 날짜 문자열은 문자열 정렬이 곧 시간순. 숫자(연/월)는 수치 정렬. 한 컬럼
    안 타입이 섞여 비교 불가하면(TypeError) 원래 순서를 보존(안전 fallback)."""
    if not x_col or not isinstance(rows, list) or len(rows) < 2:
        return rows
    with_x: list[Any] = []
    without_x: list[Any] = []
    for row in rows:
        if isinstance(row, dict) and row.get(x_col) is not None:
            with_x.append(row)
        else:
            without_x.append(row)
    try:
        with_x.sort(key=lambda r: r.get(x_col))
    except TypeError:
        return rows
    return with_x + without_x


def _build_chart_spec(
    *,
    recommended_view: str,
    rows: list[Any],
    columns: list[str],
    plan: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """recommended_view bar/line이고 차트 유효성 검증을 통과할 때만 spec 생성.

    검증 실패 시 None. 호출부는 None이면 recommended_view를 table로 내린다.

    검증 (analysis_display_contract — chart 추천 품질 보정 2026-06-01):
    - x 컬럼이 rows에 존재
    - y 컬럼이 rows에 존재 + 유효 숫자 값 _MIN_CHART_NUMERIC_VALUES개 이상
      (단일 값/대부분 null 결과는 차트 부적합 → table)
    - 다중 y는 동일 단위 그룹(compare wide-format)만. count와 ratio/rate/percent
      계열이 섞이거나 서로 다른 metric이면 table
    - line은 x축이 ordered 컬럼(날짜/연/월 등)일 때만
    """
    if recommended_view not in ("bar", "line"):
        return None
    if not rows or not columns:
        return None
    x_col = _first_categorical_column(rows, columns)
    if x_col is None or x_col not in columns:
        return None
    # y 후보: 유효 숫자 값이 충분하고 대부분 null이 아닌 numeric 컬럼만.
    y_cols = _valid_chart_y_columns(rows, columns, exclude={x_col})
    if not y_cols:
        return None
    # silverone 2026-06-09 — 두 기간/그룹 비교(compare wide-format)는 count/ratio가
    # 한 행에 섞여 다중 y로는 차트 불가 + 프론트가 단일 series만 렌더(다중 y는 첫
    # 값으로 좁아져 last_count/a_count만 보이는 오해). 변화량을 1차로 드러내기 위해
    # headline delta 컬럼 하나만 단일 series bar로 추천한다. distribution=
    # delta_ratio(pp), count=delta_count. delta가 없으면 차트 철회(table fallback).
    if _has_compare_columns(columns):
        headline = _headline_delta_column(y_cols)
        if headline is None:
            return None
        y_cols = [headline]
    # 다중 metric은 단위가 일치하는 compare 그룹만 허용 (count↔ratio 혼합 차단).
    if not _y_columns_chartable_together(y_cols):
        return None
    # line은 x축이 시계열/ordered여야 의미가 있다.
    if recommended_view == "line" and not _looks_like_time_column(x_col):
        return None
    # y는 단일 numeric이면 string, 다중(compare)이면 list.
    y_value: Any = y_cols[0] if len(y_cols) == 1 else y_cols
    return {
        "kind": recommended_view,
        "x": x_col,
        "y": y_value,
        # v1은 series 추정 안 함 — 단일 dimension만. compare도 같은 row에
        # last/this/delta가 묶이는 wide-format이라 series 분기 불필요.
        "series": None,
    }


# chart y축으로 쓸 numeric 컬럼이 갖춰야 할 최소 유효 값 개수. 1개뿐이면 차트로서
# 오해 소지가 커 table로 둔다.
_MIN_CHART_NUMERIC_VALUES = 2
# ratio/rate/percent 계열 컬럼명 — count 계열과 같은 y축에 섞으면 단위가 달라
# 차트 추천을 지양한다.
_RATIO_LIKE_PATTERNS = ("ratio", "rate", "percent", "pct", "비율", "율", "점유")
# silverone 2026-06-09 — 두 기간/그룹 비교(compare wide-format)의 "변화"를 단일
# series bar 1개로 보여줄 때 우선 채택할 delta 컬럼. 프론트가 단일 series만
# 렌더하므로(다중 y는 첫 값으로 좁혀 last_count만 보이는 오해 발생) 비교 결과는
# delta 하나만 그린다. distribution은 delta_ratio(pp), count는 delta_count 우선.
_DELTA_HEADLINE_PRIORITY = ("delta_ratio", "delta_count", "delta_rate")


def _is_finite_number(value: Any) -> bool:
    """bool/NaN/inf를 제외한 유한 숫자인지."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return value == value and value not in (float("inf"), float("-inf"))
    return False


def _numeric_value_count(rows: list[Any], col: str) -> int:
    count = 0
    for row in rows:
        if isinstance(row, dict) and _is_finite_number(row.get(col)):
            count += 1
    return count


def _valid_chart_y_columns(
    rows: list[Any], columns: list[str], *, exclude: set[str],
) -> list[str]:
    """차트 y축으로 안전한 numeric 컬럼만 추린다 (columns 순서 보존).

    - 유효 숫자 값이 _MIN_CHART_NUMERIC_VALUES개 미만이면 제외 (단일 값 방지).
    - 값이 대부분 null/누락이면(과반이 null) 제외.
    """
    total = len(rows)
    out: list[str] = []
    for col in columns:
        if col in exclude:
            continue
        valid = _numeric_value_count(rows, col)
        if valid < _MIN_CHART_NUMERIC_VALUES:
            continue
        if valid * 2 < total:  # 과반이 null/누락 → 차트 부적합
            continue
        out.append(col)
    return out


def _is_ratio_like_column(name: str) -> bool:
    if not isinstance(name, str):
        return False
    lower = name.lower()
    return any(pat in lower for pat in _RATIO_LIKE_PATTERNS)


def _is_compare_column(name: str) -> bool:
    return isinstance(name, str) and any(
        name.startswith(pat) for pat in _COMPARE_COLUMN_PATTERNS
    )


def _y_columns_chartable_together(y_cols: list[str]) -> bool:
    """다중 y축 허용 조건.

    v1에서 합쳐 그릴 수 있는 다중 metric은 compare wide-format(last_/this_/delta_)
    뿐이다. 그 외 서로 다른 metric(단위 상이 가능)은 table로 둔다. compare 안에서도
    ratio 계열과 count 계열이 섞이면(단위 상이) 차트를 지양한다.
    """
    if len(y_cols) <= 1:
        return True
    if not all(_is_compare_column(col) for col in y_cols):
        return False
    has_ratio = any(_is_ratio_like_column(col) for col in y_cols)
    has_non_ratio = any(not _is_ratio_like_column(col) for col in y_cols)
    return not (has_ratio and has_non_ratio)


# silverone 2026-06-09 — compare 결과 컬럼의 표시 포맷/라벨 contract. 백엔드가
# 의미(단위)를 선언하고 프론트가 렌더한다(프론트의 컬럼명 추측 금지). count/ratio/pp
# 혼동을 막는다. format enum: percent(0~1→%) / point(0~1→%p) / int(정수) / number.
_COMPARE_COLUMN_LABELS: dict[str, str] = {
    "a_count": "이전 건수",
    "b_count": "이후 건수",
    "delta_count": "Δ건수",
    "a_ratio": "이전 비율",
    "b_ratio": "이후 비율",
    "delta_ratio": "Δ비율(%p)",
    "delta_rate": "증감률(%)",
}

# delta_ratio abs가 이 값 미만이면 요약에서 "거의 변하지 않음"으로 표현 (2%p).
_NEAR_ZERO_DELTA_RATIO = 0.02
# 감성 enum → 한글 (요약 문구용 안정 3-value 매핑). 그 외 group 값은 raw 유지.
_SENTIMENT_LABEL_KO = {"positive": "긍정", "neutral": "중립", "negative": "부정"}


def _load_aspect_label_map() -> dict[str, str]:
    """taxonomy aspect key → 한글 label 맵. 요약 문구에서 영어 key(show_program 등)
    대신 한글(공연/프로그램)로 표기하기 위함 (silverone 2026-06-09). 차트 y축은
    프론트 taxonomy로 한글화되지만 요약 텍스트는 백엔드에서 한글화해야 한다.
    taxonomy load 실패 시 빈 맵 → raw key로 graceful fallback."""
    try:
        from ..taxonomies import DEFAULT_TAXONOMY_ID, load_taxonomy

        taxonomy = load_taxonomy(DEFAULT_TAXONOMY_ID)
        return {aspect.key: aspect.label for aspect in taxonomy.aspects}
    except Exception:
        return {}


_ASPECT_LABEL_KO = _load_aspect_label_map()


def _compare_column_format(col: str) -> str | None:
    """compare 결과 컬럼명 → 표시 포맷. delta_ratio는 %p(point), 그 외 ratio는 %,
    rate/percent_change는 %, count는 정수. 해당 없으면 None(포맷 미지정)."""
    if not isinstance(col, str):
        return None
    if col == "delta_ratio":
        return "point"
    if col == "percent_change" or col == "delta_rate" or col.endswith("_rate"):
        return "percent"
    if col == "ratio" or col.endswith("_ratio"):
        return "percent"
    if col == "count" or col.endswith("_count"):
        return "int"
    return None


def _compare_column_formats_labels(columns: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    formats: dict[str, str] = {}
    labels: dict[str, str] = {}
    for col in columns:
        fmt = _compare_column_format(col)
        if fmt:
            formats[col] = fmt
        if isinstance(col, str) and col in _COMPARE_COLUMN_LABELS:
            labels[col] = _COMPARE_COLUMN_LABELS[col]
    return formats, labels


def _group_label_for_summary(value: Any) -> str:
    if isinstance(value, str):
        if value in _SENTIMENT_LABEL_KO:
            return _SENTIMENT_LABEL_KO[value]
        if value in _ASPECT_LABEL_KO:
            return _ASPECT_LABEL_KO[value]
    return "—" if value is None else str(value)


def _compare_distribution_summary(rows: list[Any], columns: list[str]) -> str | None:
    """period_compare_distribution 결과면 핵심 변화 요약 문구를 만든다. 아니면 None.

    규칙(silverone 2026-06-09): delta_ratio 최대(+)=가장 증가, 최소(−)=가장 감소,
    abs(delta_ratio)가 작은 항목=거의 변화 없음. 원인 추정은 하지 않는다."""
    if "delta_ratio" not in columns:
        return None
    x_col = _first_categorical_column(rows, columns)
    if not x_col:
        return None
    items: list[tuple[Any, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        dr = row.get("delta_ratio")
        if _is_finite_number(dr):
            items.append((row.get(x_col), float(dr)))
    if not items:
        return None

    def pp(value: float) -> str:
        return f"{value * 100:+.1f}%p"

    inc = max(items, key=lambda t: t[1])
    dec = min(items, key=lambda t: t[1])
    near = min(items, key=lambda t: abs(t[1]))
    parts: list[str] = []
    if inc[1] > 0:
        parts.append(f"{_group_label_for_summary(inc[0])} 비율이 가장 크게 증가({pp(inc[1])})")
    if dec[1] < 0 and dec[0] != inc[0]:
        parts.append(f"{_group_label_for_summary(dec[0])} 비율은 감소({pp(dec[1])})")
    if abs(near[1]) < _NEAR_ZERO_DELTA_RATIO and near[0] not in (inc[0], dec[0]):
        parts.append(f"{_group_label_for_summary(near[0])} 비율은 거의 변하지 않았습니다")
    if not parts:
        return None
    return "두 기간 비교: " + ", ".join(parts) + "."


def _headline_delta_column(y_cols: list[str]) -> str | None:
    """compare wide-format에서 단일 series로 그릴 headline delta 컬럼 선택.

    우선순위: delta_ratio(pp) > delta_count > delta_rate > 기타 delta_*.
    유효 numeric으로 이미 걸러진 y_cols에서 고른다. delta가 없으면 None
    (호출부가 차트를 철회하고 table fallback)."""
    for name in _DELTA_HEADLINE_PRIORITY:
        if name in y_cols:
            return name
    for col in y_cols:
        if isinstance(col, str) and col.startswith("delta_"):
            return col
    return None


def _has_compare_columns(columns: list[str]) -> bool:
    for col in columns:
        if not isinstance(col, str):
            continue
        for pat in _COMPARE_COLUMN_PATTERNS:
            if col.startswith(pat):
                return True
    return False


def _looks_like_time_column(name: str) -> bool:
    if not isinstance(name, str):
        return False
    lower = name.lower()
    for prefix in _LINE_X_PREFIXES:
        if lower == prefix or lower.startswith(prefix):
            return True
    return False


# 차트 x축/그룹 라벨로 적절한 string 값의 최대 길이. 초과하면 본문(자유텍스트)으로
# 보고 x축 후보에서 제외한다 — silverone 2026-06-09. raw_text/cleaned_text/clause
# 같은 본문이 group_by/x축으로 뽑혀 막대 라벨이 문서 전문이 되는 것을 막는다.
# 컬럼명 하드코딩이 아니라 값 길이 기반(구조적) — dataset-specific 본문 컬럼도 커버.
_MAX_CHART_LABEL_LEN = 60


def _column_looks_free_text(rows: list[Any], col: str) -> bool:
    """col의 string 값이 차트 라벨로 쓰기엔 너무 긴(자유텍스트) 컬럼인지.

    상위 일부 row를 샘플링해 string 값 중 하나라도 _MAX_CHART_LABEL_LEN을 넘으면
    자유텍스트로 판단한다. categorical(aspect/sentiment/channel 등)은 짧아 통과."""
    for row in rows[:20]:
        if isinstance(row, dict):
            value = row.get(col)
            if isinstance(value, str) and len(value.strip()) > _MAX_CHART_LABEL_LEN:
                return True
    return False


def _first_categorical_column(rows: list[Any], columns: list[str]) -> str | None:
    """첫 row에서 string/None 또는 정수 시점 키(year 등)가 들어 있는 첫 컬럼.

    단, 본문(자유텍스트) string 컬럼은 차트 x축으로 부적합하므로 건너뛴다
    (planner가 raw_text 등으로 group_by한 plan이 무의미한 차트로 렌더되는 것 방지).
    """
    if not rows or not isinstance(rows[0], dict):
        return None
    for col in columns:
        value = rows[0].get(col)
        if isinstance(value, str):
            if _column_looks_free_text(rows, col):
                continue  # 자유텍스트 컬럼은 x축 부적합 → 다음 후보로 (없으면 table)
            return col
        # year 같은 정수가 시계열 x축이 될 수 있다 — 컬럼명이 시간 prefix면 채택.
        if isinstance(value, int) and not isinstance(value, bool) and _looks_like_time_column(col):
            return col
    return None


def _numeric_metric_columns(
    rows: list[Any], columns: list[str], *, exclude: set[str],
) -> list[str]:
    """첫 row 기준 numeric 값을 가진 컬럼 list. bool은 제외."""
    if not rows or not isinstance(rows[0], dict):
        return []
    out: list[str] = []
    for col in columns:
        if col in exclude:
            continue
        value = rows[0].get(col)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            out.append(col)
            continue
        # null이면 다음 row에서 확인 — compare 결과 첫 row의 this_count가 null
        # 일 수 있다 (data 부재).
        if value is None:
            for row in rows[1:]:
                if not isinstance(row, dict):
                    continue
                v2 = row.get(col)
                if isinstance(v2, bool):
                    break
                if isinstance(v2, (int, float)):
                    out.append(col)
                    break
    return out


def _has_numeric_metric_column(
    rows: list[Any], columns: list[str], *, exclude: set[str],
) -> bool:
    return len(_numeric_metric_columns(rows, columns, exclude=exclude)) > 0


def _build_context_summary(
    *,
    user_question: str | None,
    present: dict[str, Any],
    total_rows: int,
    returned_rows: int,
    answer_summary: str,
) -> dict[str, Any]:
    """analysis_api_model_2026-05-26 §6 whitelist + ADR-020 §3 context_summary v1.
    LLM 요약 없이 deterministic."""

    columns = _columns_from_rows(present.get("rows") or [])
    summary: dict[str, Any] = {
        "answer_summary": answer_summary,
        "returned_rows": returned_rows,
        "row_count": total_rows,
        "total_rows": total_rows,
    }
    question = (user_question or "").strip()
    if question:
        summary["question"] = question
    title = present.get("title")
    if isinstance(title, str) and title.strip():
        summary["present_title"] = title.strip()
    if columns:
        summary["columns"] = columns
        summary["key_dimensions"] = columns
    return summary


def _error_payload(*, user_question: str | None, error_metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "assistant_content": FAILED_RUN_FALLBACK_CONTENT,
        "display": None,
        "context_summary": {"question": (user_question or "").strip()} if user_question else {},
        "metadata": {
            "mode": "error",
            "template": "failed",
            "fallback_reason": "execution_failed",
            "error_metadata": error_metadata,
        },
    }


def _fallback_payload(*, user_question: str | None, reason: str) -> dict[str, Any]:
    """composer가 안전하게 만들 수 없을 때 — 비어있는 display + 일반 문구."""

    return {
        "assistant_content": "분석 결과를 정리하지 못했습니다. 잠시 후 다시 시도해 주세요.",
        "display": None,
        "context_summary": {"question": (user_question or "").strip()} if user_question else {},
        "metadata": {
            "mode": "fallback",
            "template": "fallback",
            "fallback_reason": reason,
        },
    }


# silverone 2026-06-01 (PR1) — answerable=false 거절 plan 렌더.
_DEFAULT_REJECT_MESSAGE = "이 질문은 현재 선택한 데이터셋으로는 답변할 수 없습니다."
# planner_validation_error: silverone 2026-06-05 — planner가 repair 후에도 유효한
# plan을 못 만든 경우. raw 500 대신 graceful 거절로 렌더하기 위한 structured reason.
_REJECT_REASONS = frozenset(
    {
        "out_of_dataset_scope",
        "unsupported_skill",
        "missing_data_or_artifact",
        "planner_validation_error",
        "execution_error",
    }
)


def _reject_payload(*, user_question: str | None, plan: dict[str, Any]) -> dict[str, Any]:
    """answerable=false plan → 사용자 노출 거절 메시지. display=null (테이블/경고 없음).

    metadata.mode=rejected, metadata.reason=<taxonomy>. reason=unsupported_skill의
    capability_gap은 metadata로 전달 — PR2(rejection event 저장)에서 사용한다.
    """
    reason = str(plan.get("reason") or "").strip()
    if reason not in _REJECT_REASONS:
        reason = "out_of_dataset_scope"
    message = str(plan.get("message") or "").strip() or _DEFAULT_REJECT_MESSAGE

    metadata: dict[str, Any] = {
        "mode": "rejected",
        "template": "rejected",
        "reason": reason,
        "fallback_reason": None,
    }
    capability_gap = plan.get("capability_gap")
    if isinstance(capability_gap, dict) and capability_gap:
        metadata["capability_gap"] = capability_gap

    # silverone 2026-06-02 — 멀티턴 clarify. reason=missing_data_or_artifact는
    # "분석은 가능하나 값(예: 축제 기준일)이 필요"한 clarify 요청이다. 이때 다음
    # 턴 planner가 짧은 후속 답("2024-08-15 야")을 직전 질문의 답으로 이어붙이도록
    # context_summary에 pending_clarification 신호 + 요청 문구(answer_summary)를
    # 남긴다. out_of_dataset_scope / unsupported_skill은 값 입력으로 풀리지 않으므로
    # 신호를 남기지 않는다 (후속 메시지를 잘못 끌어다 붙이지 않게).
    context_summary: dict[str, Any] = {}
    question_text = (user_question or "").strip()
    if question_text:
        context_summary["question"] = question_text
    if reason == "missing_data_or_artifact":
        context_summary["pending_clarification"] = True
        if message:
            context_summary["answer_summary"] = message

    # silverone 2026-06-08 (작업 1) — graceful 거절 시 plan이 제안한 대체 질문을
    # 그대로 노출(없으면 빈 리스트). 프론트가 "이렇게 물어보세요" 버튼으로 쓸 수 있다.
    suggested = plan.get("suggested_questions")
    suggested_questions = (
        [str(q).strip() for q in suggested if str(q).strip()]
        if isinstance(suggested, (list, tuple))
        else []
    )

    return {
        "assistant_content": message,
        "display": None,
        "context_summary": context_summary,
        "metadata": metadata,
        "suggested_questions": suggested_questions,
    }


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, bool):
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str) and value.strip():
            return int(float(value.strip()))
    except (ValueError, TypeError):
        pass
    return default


def _columns_from_rows(rows: list[Any]) -> list[str]:
    if not rows:
        return []
    first = rows[0]
    if isinstance(first, dict):
        return list(first.keys())
    return []
