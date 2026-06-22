"""silverone 2026-05-28 — clean 정식화 잠금 test.

date_column → created_at 변환 + 표준 9 컬럼 schema + source_json 보존 +
한글/BOM source 컬럼이 top-level parquet column으로 남지 않음.
"""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from python_ai_worker.dataset_build.clean import (
    _clean_output_schema,
    _coerce_created_at,
    run_dataset_clean,
)


class CoerceCreatedAtTests(unittest.TestCase):
    def test_yyyy_mm_dd(self):
        self.assertEqual(_coerce_created_at("2025-01-15"), "2025-01-15T00:00:00Z")

    def test_iso_with_timezone(self):
        self.assertEqual(_coerce_created_at("2024-08-15T10:30:00Z"), "2024-08-15T10:30:00Z")

    def test_yyyy_mm_dd_with_space(self):
        self.assertEqual(_coerce_created_at("2025-01-15 12:00:00"), "2025-01-15T12:00:00Z")

    def test_invalid_date_marker_returns_none(self):
        self.assertIsNone(_coerce_created_at("Invalid date--"))

    def test_empty_returns_none(self):
        self.assertIsNone(_coerce_created_at(""))
        self.assertIsNone(_coerce_created_at("   "))

    def test_none_returns_none(self):
        self.assertIsNone(_coerce_created_at(None))

    def test_garbage_returns_none(self):
        self.assertIsNone(_coerce_created_at("not-a-date"))
        self.assertIsNone(_coerce_created_at("2025-13-99"))  # invalid month/day


class CleanOutputSchemaLockTests(unittest.TestCase):
    def test_standard_9_columns_exact(self):
        schema = _clean_output_schema()
        names = [field.name for field in schema]
        self.assertEqual(
            names,
            [
                "row_id",
                "doc_id",
                "source_row_index",
                "raw_text",
                "cleaned_text",
                "created_at",
                "clean_status",
                "clean_reason",
                "source_json",
            ],
        )

    def test_no_legacy_columns(self):
        """clean_disposition / clean_flags / clean_regex_applied_rules 제거."""
        schema = _clean_output_schema()
        names = {field.name for field in schema}
        for legacy in ("clean_disposition", "clean_flags", "clean_regex_applied_rules"):
            self.assertNotIn(legacy, names)


class CleanRunIntegrationTests(unittest.TestCase):
    """run_dataset_clean을 small fixture로 호출. 표준 schema + date 변환 +
    source_json 보존 + 한글/BOM source 컬럼이 top-level에서 사라지는지 확인.
    """

    def _write_csv(self, dir_path: Path, rows: list[dict[str, str]], header: list[str]) -> Path:
        csv_path = dir_path / "source.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=header)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return csv_path

    def _run_clean(self, payload: dict) -> dict:
        return run_dataset_clean(payload)

    def test_date_column_translates_to_created_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = self._write_csv(
                tmp_path,
                rows=[
                    {"﻿수집ID(고유)": "abc", "게시일": "2025-01-15", "본문": "후기 본문 1", "URL": "http://x"},
                    {"﻿수집ID(고유)": "def", "게시일": "Invalid date--", "본문": "후기 본문 2", "URL": "http://y"},
                    {"﻿수집ID(고유)": "ghi", "게시일": "2024-08-15", "본문": "후기 본문 3", "URL": "http://z"},
                ],
                header=["﻿수집ID(고유)", "게시일", "본문", "URL"],
            )
            output_path = tmp_path / "cleaned.parquet"

            self._run_clean(
                {
                    "dataset_version_id": "test-v",
                    "dataset_name": str(csv_path),
                    "text_columns": ["본문"],
                    "output_path": str(output_path),
                    "progress_path": str(tmp_path / "progress.json"),
                    "date_column": "게시일",
                }
            )

            table = pq.read_table(output_path)
            cols = table.column_names
            # 표준 9 컬럼은 항상 존재 (subset).
            self.assertTrue(
                {
                    "row_id",
                    "doc_id",
                    "source_row_index",
                    "raw_text",
                    "cleaned_text",
                    "created_at",
                    "clean_status",
                    "clean_reason",
                    "source_json",
                }.issubset(set(cols))
            )
            # silverone 2026-06-08 (파일럿) — 원본 메타 컬럼은 SQL-safe alias로
            # materialize된다: URL→url, 수집ID(고유)→id. (게시일=date_column→created_at,
            # 본문=text_column → 분석 컬럼에서 제외)
            self.assertIn("url", cols)
            self.assertIn("id", cols)
            # 한글/BOM/괄호 원본명은 절대 top-level이 아니다(SQL identifier 위반 방지).
            for forbidden in ("﻿수집ID(고유)", "수집ID(고유)", "게시일", "본문", "URL"):
                self.assertNotIn(forbidden, cols, f"forbidden top-level column: {forbidden!r}")

            rows = table.to_pylist()
            self.assertEqual(len(rows), 3, "row_count must be preserved")

            # created_at: 정상 / null / 정상.
            self.assertEqual(rows[0]["created_at"], "2025-01-15T00:00:00Z")
            self.assertIsNone(rows[1]["created_at"], "Invalid date-- → null")
            self.assertEqual(rows[2]["created_at"], "2024-08-15T00:00:00Z")

            # doc_id == row_id alias.
            for row in rows:
                self.assertEqual(row["row_id"], row["doc_id"])

            # source_json에 원본 row 보존 (한글/BOM 키 포함).
            source0 = json.loads(rows[0]["source_json"])
            self.assertIn("게시일", source0)
            self.assertEqual(source0["게시일"], "2025-01-15")
            self.assertIn("﻿수집ID(고유)", source0)
            self.assertEqual(source0["URL"], "http://x")

            # clean_status rename 잠금 (옛 clean_disposition 아님).
            self.assertEqual(rows[0]["clean_status"], "keep")

    def test_no_date_column_keeps_created_at_null(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = self._write_csv(
                tmp_path,
                rows=[
                    {"id": "1", "text": "본문 내용"},
                ],
                header=["id", "text"],
            )
            output_path = tmp_path / "cleaned.parquet"

            self._run_clean(
                {
                    "dataset_version_id": "test-v",
                    "dataset_name": str(csv_path),
                    "text_columns": ["text"],
                    "output_path": str(output_path),
                    "progress_path": str(tmp_path / "progress.json"),
                    # date_column 명시 안 함.
                }
            )

            table = pq.read_table(output_path)
            rows = table.to_pylist()
            self.assertEqual(len(rows), 1)
            self.assertIsNone(rows[0]["created_at"])
            # 표준 schema는 그대로 적용.
            self.assertIn("created_at", table.column_names)
            self.assertEqual(rows[0]["doc_id"], rows[0]["row_id"])


class AnalysisColumnMaterializationTests(CleanRunIntegrationTests):
    """silverone 2026-06-08 (파일럿) — CSV 메타 컬럼이 cleaned.parquet의 typed
    queryable column으로 materialize되고 summary.analysis_columns로 기록되는지.
    advertised type == parquet 적재 type 확인.
    """

    def test_materializes_typed_analysis_columns(self):
        import pyarrow as pa

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = self._write_csv(
                tmp_path,
                rows=[
                    {"게시일": "2025-08-15", "본문": "후기1", "수집채널": "네이버카페", "좋아요 수": "10", "평점": "4.5"},
                    {"게시일": "2025-08-16", "본문": "후기2", "수집채널": "인스타", "좋아요 수": "3", "평점": "3.0"},
                ],
                header=["게시일", "본문", "수집채널", "좋아요 수", "평점"],
            )
            output_path = tmp_path / "cleaned.parquet"
            result = self._run_clean(
                {
                    "dataset_version_id": "test-v",
                    "dataset_name": str(csv_path),
                    "text_columns": ["본문"],
                    "output_path": str(output_path),
                    "progress_path": str(tmp_path / "progress.json"),
                    "date_column": "게시일",
                }
            )

            table = pq.read_table(output_path)
            schema = table.schema
            ac = result["artifact"]["summary"]["analysis_columns"]
            by_label = {c["label"]: c for c in ac}
            # 게시일(date_column)·본문(text_column)은 분석 컬럼에서 제외.
            self.assertEqual(set(by_label), {"수집채널", "좋아요 수", "평점"})
            for c in ac:
                self.assertEqual(c["source_column"], c["label"])
                # advertised type == 실제 parquet 컬럼 type
                self.assertIn(c["name"], table.column_names)

            chan = by_label["수집채널"]
            likes = by_label["좋아요 수"]
            rating = by_label["평점"]
            self.assertEqual(chan["type"], "string")
            self.assertEqual(likes["type"], "integer")
            self.assertEqual(rating["type"], "float")

            # parquet physical type이 advertised type과 일치
            self.assertTrue(pa.types.is_string(schema.field(chan["name"]).type))
            self.assertTrue(pa.types.is_integer(schema.field(likes["name"]).type))
            self.assertTrue(pa.types.is_floating(schema.field(rating["name"]).type))

            # 값 coercion 확인 (실제 숫자 → 집계 가능)
            rows = table.to_pylist()
            self.assertEqual(rows[0][likes["name"]], 10)
            self.assertEqual(rows[0][rating["name"]], 4.5)
            self.assertEqual(rows[0][chan["name"]], "네이버카페")
            # source_json은 유지 (원본 보존)
            self.assertIn("수집채널", json.loads(rows[0]["source_json"]))

    def test_no_analysis_columns_when_only_text_and_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = self._write_csv(
                tmp_path,
                rows=[{"게시일": "2025-08-15", "본문": "후기1"}],
                header=["게시일", "본문"],
            )
            output_path = tmp_path / "cleaned.parquet"
            result = self._run_clean(
                {
                    "dataset_version_id": "test-v",
                    "dataset_name": str(csv_path),
                    "text_columns": ["본문"],
                    "output_path": str(output_path),
                    "progress_path": str(tmp_path / "progress.json"),
                    "date_column": "게시일",
                }
            )
            self.assertEqual(result["artifact"]["summary"]["analysis_columns"], [])

    def test_dedup_removes_duplicate_content(self):
        # silverone 2026-06-22 — 검색 키워드 팬아웃 재현: 같은 본문이 keyword만
        # 다르게 3번 + 고유 1건. 본문(cleaned_text) 기준 dedup으로 2건만 유지.
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = self._write_csv(
                tmp_path,
                rows=[
                    {"uuid": "p1", "keyword": "kw_a", "본문": "행사장 준비 잘 해놨네요"},
                    {"uuid": "p1", "keyword": "kw_b", "본문": "행사장 준비 잘 해놨네요"},
                    {"uuid": "p1", "keyword": "kw_c", "본문": "행사장 준비 잘 해놨네요"},
                    {"uuid": "p2", "keyword": "kw_a", "본문": "맥주가 맛있네요"},
                ],
                header=["uuid", "keyword", "본문"],
            )
            output_path = tmp_path / "cleaned.parquet"
            result = self._run_clean(
                {
                    "dataset_version_id": "test-dedup",
                    "dataset_name": str(csv_path),
                    "text_columns": ["본문"],
                    "output_path": str(output_path),
                    "progress_path": str(tmp_path / "progress.json"),
                }
            )
            kept = pq.read_table(output_path).to_pylist()
            self.assertEqual(len(kept), 2, "본문 중복 제거 후 2건이어야 함")
            self.assertEqual(
                sorted(r["cleaned_text"] for r in kept),
                ["맥주가 맛있네요", "행사장 준비 잘 해놨네요"],
            )
            summary = result["artifact"]["summary"]
            self.assertEqual(summary["deduped_count"], 2)
            self.assertEqual(summary["kept_count"], 2)
            self.assertTrue(summary["dedup_enabled"])

    def test_dedup_disabled_keeps_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            csv_path = self._write_csv(
                tmp_path,
                rows=[
                    {"uuid": "p1", "keyword": "kw_a", "본문": "같은 본문"},
                    {"uuid": "p1", "keyword": "kw_b", "본문": "같은 본문"},
                ],
                header=["uuid", "keyword", "본문"],
            )
            output_path = tmp_path / "cleaned.parquet"
            result = self._run_clean(
                {
                    "dataset_version_id": "test-nodedup",
                    "dataset_name": str(csv_path),
                    "text_columns": ["본문"],
                    "output_path": str(output_path),
                    "progress_path": str(tmp_path / "progress.json"),
                    "dedup": False,
                }
            )
            kept = pq.read_table(output_path).to_pylist()
            self.assertEqual(len(kept), 2, "dedup=False면 본문 중복 유지")
            self.assertEqual(result["artifact"]["summary"]["deduped_count"], 0)
            self.assertFalse(result["artifact"]["summary"]["dedup_enabled"])


if __name__ == "__main__":
    unittest.main()
