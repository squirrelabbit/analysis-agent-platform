"""ADR-020 PR-A deterministic composer 잠금 테스트.

5 템플릿 + context_summary + display + 실패 분기를 잠근다.
"""

from __future__ import annotations

import unittest

from python_ai_worker.composer import FAILED_RUN_FALLBACK_CONTENT, compose_answer
from python_ai_worker.composer import _sort_rows_by_x


def _ts_present(rows):
    return {
        "step_id": "out", "format": "chart", "title": "축제 전후 발생량",
        "row_count": len(rows), "total_rows": len(rows), "returned_rows": len(rows),
        "max_rows": 1000, "truncated": False, "rows": rows,
    }


def _present(*, total=3, returned=3, truncated=False, fmt="table", title="작년 대비 aspect", rows=None):
    rows = rows if rows is not None else [
        {"aspect": "ambiance_scenery", "last_count": 1, "this_count": 2},
        {"aspect": "food", "last_count": 1, "this_count": 1},
        {"aspect": "show_program", "last_count": 0, "this_count": 1},
    ]
    return {
        "step_id": "out",
        "format": fmt,
        "title": title,
        "row_count": total,
        "total_rows": total,
        "returned_rows": returned,
        "max_rows": 1000,
        "truncated": truncated,
        "rows": rows,
    }


# silverone 2026-06-02 — assistant_content는 recommended_view를 한국어로 표현하고
# 영문 view/format 키워드(chart/table/line/bar/json)를 넣지 않는다.
_ENGLISH_VIEW_WORDS = ("chart", "table", "line", "bar", "json")


class NormalTemplateTests(unittest.TestCase):
    def _assert_no_english_view_word(self, content: str) -> None:
        for word in _ENGLISH_VIEW_WORDS:
            self.assertNotIn(word, content.lower(), f"본문에 영문 view 키워드 '{word}' 노출")

    def test_normal_table_view_korean(self) -> None:
        # 비-chartable rows → recommended_view=table → "표로".
        out = compose_answer(
            user_question="aspect 라벨 목록",
            present=_present(total=2, returned=2, rows=[{"aspect": "food", "label": "맛"}, {"aspect": "show", "label": "공연"}]),
        )
        self.assertEqual(out["metadata"]["template"], "table_normal")
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIn("표로", out["assistant_content"])
        self.assertIn("2건", out["assistant_content"])
        self._assert_no_english_view_word(out["assistant_content"])

    def test_normal_bar_view_korean(self) -> None:
        out = compose_answer(
            user_question="aspect별 건수",
            present=_present(rows=[{"aspect": "food", "count": 5}, {"aspect": "show", "count": 3}, {"aspect": "stage", "count": 7}]),
        )
        self.assertEqual(out["display"]["recommended_view"], "bar")
        self.assertIn("막대그래프로", out["assistant_content"])
        self._assert_no_english_view_word(out["assistant_content"])

    def test_normal_line_view_korean(self) -> None:
        out = compose_answer(
            user_question="날짜별 추이",
            present=_present(rows=[{"created_at": "2026-01", "count": 10}, {"created_at": "2026-02", "count": 15}, {"created_at": "2026-03", "count": 12}]),
        )
        self.assertEqual(out["display"]["recommended_view"], "line")
        self.assertIn("선그래프로", out["assistant_content"])
        self._assert_no_english_view_word(out["assistant_content"])

    def test_normal_chart_format_no_english(self) -> None:
        # present.format="chart"여도 본문은 한국어 view 표현만 사용 ("chart" 미노출).
        out = compose_answer(user_question="aspect 비율", present=_present(fmt="chart"))
        self._assert_no_english_view_word(out["assistant_content"])
        self.assertIn("정리했습니다", out["assistant_content"])


class TruncatedTemplateTests(unittest.TestCase):
    def test_truncated_template_chosen(self) -> None:
        out = compose_answer(
            user_question="대량 조회",
            present=_present(total=1234, returned=1000, truncated=True),
        )
        self.assertEqual(out["metadata"]["template"], "table_truncated")
        self.assertIn("1234", out["assistant_content"])
        self.assertIn("1000", out["assistant_content"])


class EmptyTemplateTests(unittest.TestCase):
    def test_empty_total_rows(self) -> None:
        out = compose_answer(
            user_question="없는 조건",
            present=_present(total=0, returned=0, truncated=False, rows=[]),
        )
        self.assertEqual(out["metadata"]["template"], "empty")
        self.assertIn("결과가 없", out["assistant_content"])


class ReuseTemplateTests(unittest.TestCase):
    def test_reuse_applied_takes_priority_over_truncated(self) -> None:
        out = compose_answer(
            user_question="표시 조건 변경",
            present=_present(total=2000, returned=1000, truncated=True),
            reuse_metadata={"applied": True, "action": "add_limit"},
        )
        self.assertEqual(out["metadata"]["template"], "reuse_applied")
        self.assertIn("이전 분석", out["assistant_content"])

    def test_reuse_not_applied_does_not_trigger(self) -> None:
        out = compose_answer(
            user_question="신규 질의",
            present=_present(total=3, returned=3),
            reuse_metadata={"applied": False, "fallback_reason": "classifier_no_match"},
        )
        self.assertEqual(out["metadata"]["template"], "table_normal")


class FailedTemplateTests(unittest.TestCase):
    def test_error_metadata_produces_placeholder(self) -> None:
        out = compose_answer(
            user_question="실패 사례",
            present=None,
            error_metadata={"reason": "executor failure"},
        )
        self.assertEqual(out["metadata"]["template"], "failed")
        self.assertEqual(out["metadata"]["mode"], "error")
        self.assertEqual(out["assistant_content"], FAILED_RUN_FALLBACK_CONTENT)


class ContextSummaryTests(unittest.TestCase):
    def test_context_summary_required_fields(self) -> None:
        out = compose_answer(
            user_question="작년 대비 aspect 증감",
            present=_present(total=3, returned=3),
        )
        summary = out["context_summary"]
        self.assertEqual(summary["question"], "작년 대비 aspect 증감")
        self.assertEqual(summary["present_title"], "작년 대비 aspect")
        self.assertEqual(summary["total_rows"], 3)
        self.assertEqual(summary["row_count"], 3)
        self.assertEqual(summary["returned_rows"], 3)
        self.assertEqual(summary["columns"], ["aspect", "last_count", "this_count"])
        self.assertEqual(summary["key_dimensions"], ["aspect", "last_count", "this_count"])
        self.assertEqual(summary["answer_summary"], out["assistant_content"])

    def test_context_summary_skips_columns_when_rows_empty(self) -> None:
        out = compose_answer(
            user_question="없는 조건",
            present=_present(total=0, returned=0, rows=[]),
        )
        summary = out["context_summary"]
        self.assertNotIn("columns", summary)
        self.assertNotIn("key_dimensions", summary)


class RejectClarifyContextTests(unittest.TestCase):
    """silverone 2026-06-02 — 멀티턴 clarify. reason=missing_data_or_artifact
    거절은 다음 턴이 후속 답을 이어받도록 pending_clarification + answer_summary를
    context_summary에 남긴다. 다른 reason은 남기지 않는다."""

    def _reject(self, reason: str, message: str = "축제 날짜(기준일)가 필요합니다."):
        return compose_answer(
            user_question="축제 전후 일주일 문서발생량",
            present=None,
            plan={"answerable": False, "reason": reason, "message": message, "steps": []},
        )

    def test_missing_data_sets_pending_clarification(self) -> None:
        out = self._reject("missing_data_or_artifact")
        summary = out["context_summary"]
        self.assertTrue(summary.get("pending_clarification"))
        self.assertEqual(summary["question"], "축제 전후 일주일 문서발생량")
        self.assertEqual(summary["answer_summary"], "축제 날짜(기준일)가 필요합니다.")
        self.assertEqual(out["metadata"]["reason"], "missing_data_or_artifact")

    def test_out_of_dataset_scope_has_no_pending_clarification(self) -> None:
        out = self._reject("out_of_dataset_scope", message="데이터셋과 무관한 질문입니다.")
        summary = out["context_summary"]
        self.assertNotIn("pending_clarification", summary)
        self.assertNotIn("answer_summary", summary)
        self.assertEqual(summary["question"], "축제 전후 일주일 문서발생량")

    def test_unsupported_skill_has_no_pending_clarification(self) -> None:
        out = self._reject("unsupported_skill", message="클러스터링은 아직 지원하지 않습니다.")
        self.assertNotIn("pending_clarification", out["context_summary"])


class LineChartSortTests(unittest.TestCase):
    """silverone 2026-06-02 — line 차트는 x(시계열) 기준 정렬돼야 한다. planner가
    sort step을 안 넣어 행이 임의 순서로 와도 composer가 보정."""

    def test_line_rows_sorted_by_x_ascending(self) -> None:
        rows = [
            {"created_at": "2024-08-18T00:00:00Z", "count": 44},
            {"created_at": "2024-08-16T00:00:00Z", "count": 38},
            {"created_at": "2024-08-17T00:00:00Z", "count": 28},
        ]
        out = compose_answer(user_question="축제 전후 일주일 발생량", present=_ts_present(rows))
        display = out["display"]
        self.assertEqual(display["recommended_view"], "line")
        dates = [r["created_at"] for r in display["rows"]]
        self.assertEqual(dates, sorted(dates), "line 차트 행이 x(날짜) 오름차순이어야 함")
        self.assertEqual(dates[0], "2024-08-16T00:00:00Z")

    def test_bar_rows_not_reordered(self) -> None:
        # 비-시계열 categorical x(aspect) → bar. planner 정렬 의도 보존(재정렬 X).
        rows = [
            {"aspect": "food", "count": 5},
            {"aspect": "ambiance_scenery", "count": 9},
            {"aspect": "show_program", "count": 1},
        ]
        out = compose_answer(user_question="aspect별 건수", present=_ts_present(rows))
        display = out["display"]
        self.assertEqual(display["recommended_view"], "bar")
        self.assertEqual([r["aspect"] for r in display["rows"]], ["food", "ambiance_scenery", "show_program"])


class SortRowsByXTests(unittest.TestCase):
    def test_nulls_pushed_to_end(self) -> None:
        rows = [{"x": "b"}, {"x": None}, {"x": "a"}]
        self.assertEqual(_sort_rows_by_x(rows, "x"), [{"x": "a"}, {"x": "b"}, {"x": None}])

    def test_single_row_passthrough(self) -> None:
        rows = [{"x": "a"}]
        self.assertEqual(_sort_rows_by_x(rows, "x"), rows)

    def test_mixed_types_fallback_preserves_order(self) -> None:
        rows = [{"x": "a"}, {"x": 3}]  # str vs int 비교 불가 → 원순서 보존
        self.assertEqual(_sort_rows_by_x(rows, "x"), rows)

    def test_numeric_sorted(self) -> None:
        rows = [{"x": 2023}, {"x": 2021}, {"x": 2022}]
        self.assertEqual([r["x"] for r in _sort_rows_by_x(rows, "x")], [2021, 2022, 2023])


class DisplayTests(unittest.TestCase):
    def test_display_mirrors_present_payload(self) -> None:
        present = _present(total=10, returned=5, truncated=True)
        out = compose_answer(user_question="q", present=present)
        display = out["display"]
        self.assertEqual(display["type"], "table")
        self.assertEqual(display["title"], present["title"])
        self.assertEqual(display["rows"], present["rows"])
        self.assertEqual(display["total_rows"], 10)
        self.assertEqual(display["returned_rows"], 5)
        self.assertTrue(display["truncated"])
        self.assertEqual(display["max_rows"], 1000)

    # ----- silverone 2026-05-27 display-columns -----

    def test_display_columns_present_with_rows(self) -> None:
        """rows가 있으면 display.columns는 rows[0]의 key 순서 그대로."""
        rows = [
            {"aspect": "ambiance_scenery", "last_count": 1, "this_count": 2},
            {"aspect": "food", "last_count": 1, "this_count": 1},
        ]
        out = compose_answer(user_question="q", present=_present(total=2, returned=2, rows=rows))
        self.assertEqual(out["display"]["columns"], ["aspect", "last_count", "this_count"])

    def test_display_columns_preserve_row_key_order(self) -> None:
        """columns 순서가 dict 삽입 순서를 따른다 (Python 3.7+ insertion-ordered)."""
        rows = [{"c": 3, "a": 1, "b": 2}]
        out = compose_answer(user_question="q", present=_present(total=1, returned=1, rows=rows))
        self.assertEqual(out["display"]["columns"], ["c", "a", "b"])

    def test_display_columns_empty_when_no_rows(self) -> None:
        out = compose_answer(user_question="없는 조건", present=_present(total=0, returned=0, rows=[]))
        self.assertEqual(out["display"]["columns"], [])


class DisplayWarningsTests(unittest.TestCase):
    """display-warnings v1 (silverone 2026-05-27) — 사용자 화면 노출 가능한
    경고 문자열 list. empty / truncated / null ratio 3종."""

    def _ratio_plan(self) -> dict[str, object]:
        # plan.steps에 calculate.ratio expression 1건 + percent_change 1건.
        # composer는 이 expression name들을 nullable ratio column으로 인식.
        return {
            "plan_version": "v2",
            "steps": [
                {
                    "id": "calc",
                    "skill": "calculate",
                    "params": {
                        "input": "compare_step",
                        "expressions": [
                            {
                                "name": "negative_ratio",
                                "operation": "ratio",
                                "numerator": "neg_count",
                                "denominator": "total_count",
                            },
                            {
                                "name": "delta_rate",
                                "operation": "percent_change",
                                "base": "last_count",
                                "current": "this_count",
                            },
                        ],
                    },
                },
            ],
        }

    def test_normal_result_no_warnings(self) -> None:
        out = compose_answer(
            user_question="aspect 증감",
            present=_present(),
            plan=self._ratio_plan(),
        )
        self.assertEqual(out["display"]["warnings"], [])

    def test_empty_result_warning(self) -> None:
        out = compose_answer(
            user_question="없는 조건",
            present=_present(total=0, returned=0, rows=[]),
        )
        self.assertIn("조건에 맞는 결과가 없습니다.", out["display"]["warnings"])

    def test_truncated_result_warning(self) -> None:
        out = compose_answer(
            user_question="많은 결과",
            present=_present(total=15000, returned=10000, truncated=True),
        )
        warnings = out["display"]["warnings"]
        self.assertTrue(
            any("15000" in w and "10000" in w for w in warnings),
            f"truncated warning expected with row counts in {warnings}",
        )

    def test_null_ratio_row_triggers_warning(self) -> None:
        rows = [
            {"aspect": "food", "neg_count": 2, "total_count": 10, "negative_ratio": 0.2},
            {"aspect": "ambiance_scenery", "neg_count": None, "total_count": 0, "negative_ratio": None},
        ]
        out = compose_answer(
            user_question="비율",
            present=_present(total=2, returned=2, rows=rows),
            plan=self._ratio_plan(),
        )
        self.assertIn(
            "일부 비율 값은 기준값이 없어 계산되지 않았습니다.",
            out["display"]["warnings"],
        )

    def test_null_percent_change_row_triggers_warning(self) -> None:
        rows = [
            {"aspect": "food", "last_count": 10, "this_count": 12, "delta_rate": 20.0},
            {"aspect": "new", "last_count": 0, "this_count": 5, "delta_rate": None},
        ]
        out = compose_answer(
            user_question="증감",
            present=_present(total=2, returned=2, rows=rows),
            plan=self._ratio_plan(),
        )
        self.assertIn(
            "일부 비율 값은 기준값이 없어 계산되지 않았습니다.",
            out["display"]["warnings"],
        )

    def test_null_in_non_ratio_column_no_warning(self) -> None:
        # neg_count는 ratio 결과 컬럼 아님 — null이 있어도 ratio warning 안 떠야.
        rows = [
            {"aspect": "food", "neg_count": None, "total_count": 10, "negative_ratio": 0.0},
        ]
        out = compose_answer(
            user_question="비율",
            present=_present(total=1, returned=1, rows=rows),
            plan=self._ratio_plan(),
        )
        self.assertEqual(out["display"]["warnings"], [])

    def test_warnings_present_when_plan_missing(self) -> None:
        # plan이 None이어도 empty/truncated warning은 동작. ratio warning만 skip.
        out = compose_answer(
            user_question="없음",
            present=_present(total=0, returned=0, rows=[]),
            plan=None,
        )
        self.assertIn("조건에 맞는 결과가 없습니다.", out["display"]["warnings"])

    def test_empty_short_circuits_other_warnings(self) -> None:
        # total_rows=0이면 empty warning만 반환. ratio/truncated는 추가 안 함.
        out = compose_answer(
            user_question="빈",
            present=_present(total=0, returned=0, rows=[], truncated=True),
            plan=self._ratio_plan(),
        )
        self.assertEqual(out["display"]["warnings"], ["조건에 맞는 결과가 없습니다."])


class ChartReadyMetadataTests(unittest.TestCase):
    """chart-ready metadata v1 (silverone 2026-06-01) — display.recommended_view
    + chart_spec 휴리스틱 잠금. enum: table / bar / line."""

    def _compose_with_rows(
        self,
        rows: list[dict[str, object]],
        *,
        plan: dict[str, object] | None = None,
    ) -> dict[str, object]:
        present = _present(
            total=len(rows), returned=len(rows), truncated=False, rows=rows,
        )
        kwargs: dict[str, object] = {"user_question": "q", "present": present}
        if plan is not None:
            kwargs["plan"] = plan
        return compose_answer(**kwargs)  # type: ignore[arg-type]

    def test_empty_rows_table_null(self) -> None:
        out = self._compose_with_rows([])
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_single_row_table_null(self) -> None:
        out = self._compose_with_rows([{"aspect": "food", "count": 5}])
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_single_column_table_null(self) -> None:
        out = self._compose_with_rows([{"aspect": "a"}, {"aspect": "b"}])
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_compare_columns_bar(self) -> None:
        """compare wide-format + categorical x → bar. silverone 2026-06-09:
        프론트가 단일 series만 렌더(다중 y는 첫 값으로 좁혀 last_count만 보임)하므로
        '변화'를 드러내는 headline delta(delta_count) 하나만 단일 series로 추천한다."""
        rows = [
            {"aspect": "food", "last_count": 1, "this_count": 2, "delta_count": 1},
            {"aspect": "show", "last_count": 0, "this_count": 1, "delta_count": 1},
        ]
        out = self._compose_with_rows(rows)
        display = out["display"]
        self.assertEqual(display["recommended_view"], "bar")
        spec = display["chart_spec"]
        self.assertIsNotNone(spec)
        self.assertEqual(spec["kind"], "bar")
        self.assertEqual(spec["x"], "aspect")
        self.assertEqual(spec["y"], "delta_count")
        self.assertIsNone(spec["series"])

    def test_distribution_compare_uses_delta_ratio(self) -> None:
        """period_compare_distribution wide-format(count+ratio 혼합) → delta_ratio(pp)
        단일 series bar. count↔ratio 혼합이라 다중 y는 불가, headline은 delta_ratio 우선."""
        rows = [
            {"sentiment": "negative", "a_count": 1, "a_ratio": 0.03, "b_count": 31, "b_ratio": 0.04, "delta_count": 30, "delta_ratio": 0.01},
            {"sentiment": "neutral", "a_count": 3, "a_ratio": 0.10, "b_count": 277, "b_ratio": 0.39, "delta_count": 274, "delta_ratio": 0.29},
            {"sentiment": "positive", "a_count": 27, "a_ratio": 0.87, "b_count": 402, "b_ratio": 0.57, "delta_count": 375, "delta_ratio": -0.30},
        ]
        out = self._compose_with_rows(rows)
        display = out["display"]
        self.assertEqual(display["recommended_view"], "bar")
        spec = display["chart_spec"]
        self.assertIsNotNone(spec)
        self.assertEqual(spec["kind"], "bar")
        self.assertEqual(spec["x"], "sentiment")
        self.assertEqual(spec["y"], "delta_ratio")
        self.assertIsNone(spec["series"])

    def test_compare_without_delta_falls_back_to_table(self) -> None:
        """compare 계열(last_/this_)이지만 delta 컬럼이 없으면 단일 headline을 못 골라
        차트 철회 → table."""
        rows = [
            {"aspect": "food", "last_count": 1, "this_count": 2},
            {"aspect": "show", "last_count": 0, "this_count": 1},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_time_x_axis_line(self) -> None:
        """created_at/year/month/date 계열 x축 + numeric metric → line."""
        rows = [
            {"created_at": "2026-01", "count": 10},
            {"created_at": "2026-02", "count": 15},
            {"created_at": "2026-03", "count": 12},
        ]
        out = self._compose_with_rows(rows)
        display = out["display"]
        self.assertEqual(display["recommended_view"], "line")
        spec = display["chart_spec"]
        self.assertIsNotNone(spec)
        self.assertEqual(spec["kind"], "line")
        self.assertEqual(spec["x"], "created_at")
        self.assertEqual(spec["y"], "count")
        self.assertIsNone(spec["series"])

    def test_year_int_x_axis_line(self) -> None:
        """year 정수 x축 + numeric metric → line (categorical은 정수도 채택)."""
        rows = [
            {"year": 2024, "total": 100},
            {"year": 2025, "total": 120},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "line")
        self.assertEqual(out["display"]["chart_spec"]["x"], "year")

    def test_single_categorical_with_numeric_bar(self) -> None:
        rows = [
            {"aspect": "food", "count": 5},
            {"aspect": "show", "count": 3},
            {"aspect": "stage", "count": 7},
        ]
        out = self._compose_with_rows(rows)
        display = out["display"]
        self.assertEqual(display["recommended_view"], "bar")
        spec = display["chart_spec"]
        self.assertEqual(spec["kind"], "bar")
        self.assertEqual(spec["x"], "aspect")
        self.assertEqual(spec["y"], "count")

    def test_no_numeric_metric_table(self) -> None:
        """categorical 컬럼만 있고 numeric metric 없으면 table."""
        rows = [
            {"aspect": "food", "label": "맛"},
            {"aspect": "show", "label": "공연"},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_no_categorical_x_table(self) -> None:
        """numeric 컬럼만 있고 categorical x가 없으면 table."""
        rows = [
            {"count": 1, "total": 5},
            {"count": 2, "total": 6},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_compare_without_categorical_x_table(self) -> None:
        """compare 컬럼은 있지만 categorical x가 없으면 table."""
        rows = [
            {"last_count": 1, "this_count": 2, "delta_count": 1},
            {"last_count": 2, "this_count": 3, "delta_count": 1},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_single_valid_numeric_value_table(self) -> None:
        """유효 numeric 값이 1개뿐이면(나머지 null) 차트 부적합 → table.

        chart 추천 품질 보정 2026-06-01 — 단일 값 차트 오해 방지."""
        rows = [
            {"aspect": "food", "count": None},
            {"aspect": "show", "count": 5},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_two_valid_numeric_after_null_first_row_bar(self) -> None:
        """첫 row가 null이어도 유효 numeric이 2개 이상이면 bar 채택."""
        rows = [
            {"aspect": "food", "count": None},
            {"aspect": "show", "count": 5},
            {"aspect": "stage", "count": 7},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "bar")
        self.assertEqual(out["display"]["chart_spec"]["y"], "count")

    def test_mostly_null_numeric_table(self) -> None:
        """유효 numeric이 과반 미만(대부분 null)이면 table.

        예: positive_ratio가 positive 행에만 있고 negative/neutral은 null."""
        rows = [
            {"label": "negative", "positive_ratio": None},
            {"label": "neutral", "positive_ratio": None},
            {"label": "positive", "positive_ratio": 1},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_count_and_ratio_mix_table(self) -> None:
        """compare 그룹이라도 count 계열과 ratio 계열이 섞이면 단위 불일치 → table."""
        rows = [
            {"aspect": "food", "last_count": 1, "this_count": 2, "last_ratio": 0.1, "this_ratio": 0.2},
            {"aspect": "show", "last_count": 0, "this_count": 1, "last_ratio": 0.0, "this_ratio": 0.3},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_multiple_non_compare_metrics_table(self) -> None:
        """compare 그룹이 아닌 서로 다른 metric이 여러 개면(단위 상이 가능) table."""
        rows = [
            {"aspect": "food", "count": 5, "avg_score": 4.1},
            {"aspect": "show", "count": 3, "avg_score": 3.8},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])

    def test_single_ratio_metric_bar(self) -> None:
        """단일 ratio metric에 유효 값이 2개 이상이면 bar (단위 혼합 아님)."""
        rows = [
            {"aspect": "food", "positive_ratio": 0.6},
            {"aspect": "show", "positive_ratio": 0.4},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "bar")
        self.assertEqual(out["display"]["chart_spec"]["y"], "positive_ratio")

    def test_bool_not_treated_as_numeric(self) -> None:
        rows = [
            {"aspect": "food", "is_active": True},
            {"aspect": "show", "is_active": False},
        ]
        out = self._compose_with_rows(rows)
        self.assertEqual(out["display"]["recommended_view"], "table")
        self.assertIsNone(out["display"]["chart_spec"])


class CompareColumnContractTests(unittest.TestCase):
    """silverone 2026-06-09 — compare 결과 표시 포맷/라벨 contract + 변화 요약.
    프론트가 %·%p·정수로 렌더하도록 백엔드가 column_formats/column_labels를 내린다."""

    def _present(self, rows: list[dict[str, object]]) -> dict[str, object]:
        from copy import deepcopy
        return {
            "total_rows": len(rows), "returned_rows": len(rows), "truncated": False,
            "format": "table", "title": "감성 비율 전후 변화", "rows": deepcopy(rows),
        }

    _DIST_ROWS = [
        {"sentiment": "negative", "a_count": 1, "a_ratio": 0.03, "b_count": 31, "b_ratio": 0.04, "delta_count": 30, "delta_ratio": 0.01},
        {"sentiment": "neutral", "a_count": 3, "a_ratio": 0.10, "b_count": 277, "b_ratio": 0.39, "delta_count": 274, "delta_ratio": 0.29},
        {"sentiment": "positive", "a_count": 27, "a_ratio": 0.87, "b_count": 402, "b_ratio": 0.57, "delta_count": 375, "delta_ratio": -0.30},
    ]

    def test_distribution_emits_column_formats(self) -> None:
        out = compose_answer(user_question="q", present=self._present(self._DIST_ROWS))
        fmts = out["display"]["column_formats"]
        self.assertEqual(fmts["a_ratio"], "percent")
        self.assertEqual(fmts["b_ratio"], "percent")
        self.assertEqual(fmts["delta_ratio"], "point")
        self.assertEqual(fmts["a_count"], "int")
        self.assertEqual(fmts["delta_count"], "int")
        self.assertNotIn("sentiment", fmts)  # group_by는 포맷 없음

    def test_distribution_emits_column_labels(self) -> None:
        out = compose_answer(user_question="q", present=self._present(self._DIST_ROWS))
        labels = out["display"]["column_labels"]
        self.assertEqual(labels["a_ratio"], "이전 비율")
        self.assertEqual(labels["b_ratio"], "이후 비율")
        self.assertEqual(labels["delta_ratio"], "Δ비율(%p)")
        self.assertEqual(labels["delta_count"], "Δ건수")

    def test_distribution_change_summary(self) -> None:
        out = compose_answer(user_question="q", present=self._present(self._DIST_ROWS))
        content = out["assistant_content"]
        self.assertEqual(out["metadata"]["template"], "compare_distribution_summary")
        # 가장 증가=중립(+29.0%p), 가장 감소=긍정(-30.0%p), 거의 변화 없음=부정.
        self.assertIn("중립", content)
        self.assertIn("가장 크게 증가", content)
        self.assertIn("+29.0%p", content)
        self.assertIn("긍정", content)
        self.assertIn("감소", content)
        self.assertIn("부정", content)
        self.assertIn("거의 변하지 않", content)
        # 단조로운 generic 문구가 아니어야 함.
        self.assertNotIn("분석 결과", content)

    def test_count_compare_formats_no_ratio(self) -> None:
        rows = [
            {"channel": "a", "a_count": 1, "b_count": 2, "delta_count": 1, "delta_rate": 100.0},
            {"channel": "b", "a_count": 4, "b_count": 2, "delta_count": -2, "delta_rate": -50.0},
        ]
        out = compose_answer(user_question="q", present=self._present(rows))
        fmts = out["display"]["column_formats"]
        self.assertEqual(fmts["delta_count"], "int")
        self.assertEqual(fmts["delta_rate"], "percent")
        self.assertNotIn("delta_ratio", fmts)
        # count 비교는 delta_ratio가 없어 distribution 요약을 안 만든다(generic 유지).
        self.assertNotEqual(out["metadata"]["template"], "compare_distribution_summary")

    def test_non_compare_has_no_column_formats(self) -> None:
        rows = [{"aspect": "food", "count": 5}, {"aspect": "show", "count": 3}]
        out = compose_answer(user_question="q", present=self._present(rows))
        self.assertNotIn("column_formats", out["display"])
        self.assertNotIn("column_labels", out["display"])


class GracefulFallbackTests(unittest.TestCase):
    def test_missing_present_uses_fallback(self) -> None:
        out = compose_answer(user_question="q", present=None)
        self.assertEqual(out["metadata"]["mode"], "fallback")
        self.assertEqual(out["metadata"]["fallback_reason"], "present_missing")
        self.assertTrue(out["assistant_content"])  # 비어있지 않은 문구

    def test_composer_never_raises_on_malformed_present(self) -> None:
        # present가 dict가 아닌 형태로 와도 fallback.
        out = compose_answer(user_question="q", present="not-a-dict")  # type: ignore[arg-type]
        self.assertIn(out["metadata"]["mode"], {"fallback", "error"})
        self.assertTrue(out["assistant_content"])


if __name__ == "__main__":
    unittest.main()
