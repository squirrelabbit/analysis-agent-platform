"""CSV 분석 컬럼 자동 추론 단위 테스트 (silverone 2026-06-08, 파일럿).

검증: type 추론(timestamp/integer/float/string + string fallback), SQL-safe alias
생성(한글→col_N, 충돌 방지, 표준 컬럼 회피), text/date 컬럼 제외, 값 coercion.
"""

from __future__ import annotations

import unittest

from python_ai_worker.dataset_build.schema_inference import (
    STANDARD_CLEAN_COLUMNS,
    coerce_timestamp,
    coerce_value,
    infer_analysis_columns,
    infer_column_type,
)


class InferColumnTypeTests(unittest.TestCase):
    def test_integer(self):
        self.assertEqual(infer_column_type(["0", "5", "120", ""]), "integer")

    def test_integer_with_thousands_separator(self):
        self.assertEqual(infer_column_type(["1,000", "2,345"]), "integer")

    def test_float(self):
        self.assertEqual(infer_column_type(["1.5", "2.0", "3"]), "float")

    def test_timestamp(self):
        self.assertEqual(infer_column_type(["2025-08-15", "2025-08-16"]), "timestamp")

    def test_timestamp_allows_minority_invalid(self):
        # 9/10 valid → timestamp (0.9 임계)
        vals = ["2025-08-%02d" % d for d in range(1, 10)] + ["Invalid date--"]
        self.assertEqual(infer_column_type(vals), "timestamp")

    def test_low_confidence_timestamp_falls_back_to_string(self):
        # 5/10만 날짜 → 임계 미달 → string
        vals = ["2025-08-01"] * 5 + ["not a date"] * 5
        self.assertEqual(infer_column_type(vals), "string")

    def test_mixed_numeric_text_is_string(self):
        self.assertEqual(infer_column_type(["10", "abc", "20"]), "string")

    def test_all_empty_is_string(self):
        self.assertEqual(infer_column_type(["", None, "  "]), "string")


class CoerceValueTests(unittest.TestCase):
    def test_integer(self):
        self.assertEqual(coerce_value("1,234", "integer"), 1234)
        self.assertIsInstance(coerce_value("5", "integer"), int)

    def test_float(self):
        self.assertEqual(coerce_value("3.5", "float"), 3.5)

    def test_timestamp_iso(self):
        self.assertEqual(coerce_value("2025-08-15", "timestamp"), "2025-08-15T00:00:00Z")

    def test_string_preserved(self):
        self.assertEqual(coerce_value("네이버카페", "string"), "네이버카페")

    def test_empty_and_fail_to_none(self):
        self.assertIsNone(coerce_value("", "integer"))
        self.assertIsNone(coerce_value(None, "string"))
        self.assertIsNone(coerce_value("not-int", "integer"))
        self.assertIsNone(coerce_value("Invalid date--", "timestamp"))


def _rows(*dicts):
    return list(dicts)


class InferAnalysisColumnsTests(unittest.TestCase):
    def _sample(self):
        # BOM 키 + 한글 + 괄호 + 영문 혼합.
        return _rows(
            {"﻿수집ID(고유)": "a", "게시일": "2025-08-15", "수집채널": "네이버카페",
             "본문": "후기1", "좋아요 수": "10", "URL": "http://x"},
            {"﻿수집ID(고유)": "b", "게시일": "2025-08-16", "수집채널": "인스타",
             "본문": "후기2", "좋아요 수": "3", "URL": "http://y"},
        )

    def test_excludes_text_and_date_columns(self):
        cols = infer_analysis_columns(self._sample(), exclude_source_columns=["본문", "게시일"])
        labels = {c.label for c in cols}
        self.assertNotIn("본문", labels)   # text_column 제외
        self.assertNotIn("게시일", labels)  # date_column(created_at) 제외
        # 나머지는 분석 컬럼으로 노출
        self.assertEqual(labels, {"수집ID(고유)", "수집채널", "좋아요 수", "URL"})

    def test_type_inference_per_column(self):
        cols = {c.label: c for c in infer_analysis_columns(self._sample(), ["본문", "게시일"])}
        self.assertEqual(cols["좋아요 수"].type, "integer")
        self.assertEqual(cols["수집채널"].type, "string")
        self.assertEqual(cols["URL"].type, "string")

    def test_alias_is_sql_safe_and_not_standard(self):
        cols = infer_analysis_columns(self._sample(), ["본문", "게시일"])
        import re
        ident = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
        for c in cols:
            self.assertRegex(c.name, ident, f"alias not SQL-safe: {c.name}")
            self.assertNotIn(c.name, STANDARD_CLEAN_COLUMNS, "alias collides with standard column")
        # alias 유일성
        names = [c.name for c in cols]
        self.assertEqual(len(names), len(set(names)))

    def test_ascii_columns_get_readable_alias(self):
        rows = _rows({"channel": "naver", "likes": "10", "text": "a"})
        cols = {c.label: c.name for c in infer_analysis_columns(rows, ["text"])}
        self.assertEqual(cols["channel"], "channel")
        self.assertEqual(cols["likes"], "likes")

    def test_korean_only_column_falls_back_to_col_position(self):
        rows = _rows({"채널": "naver", "본문": "a"})
        cols = infer_analysis_columns(rows, ["본문"])
        # 한글-only → ascii slug 비어 col_<position>. position=1(채널이 첫 컬럼).
        self.assertEqual(cols[0].name, "col_1")

    def test_alias_collision_gets_suffix(self):
        # 두 컬럼이 같은 slug("a/b" → "a_b")로 충돌 → suffix.
        rows = _rows({"a/b": "1", "a-b": "2", "본문": "x"})
        names = [c.name for c in infer_analysis_columns(rows, ["본문"])]
        self.assertEqual(len(names), len(set(names)), f"aliases must be unique: {names}")

    def test_empty_rows(self):
        self.assertEqual(infer_analysis_columns([], []), [])


if __name__ == "__main__":
    unittest.main()
