import json
import tempfile
import unittest
from pathlib import Path

from python_ai_worker.source_summary import build_source_summary, run_source_summary


class SourceSummaryContractTests(unittest.TestCase):
    """source_summary 응답 shape — Go domain.DatasetSourceSummary JSON 계약 잠금."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)

    def _write_csv(self, name: str = "sample.csv") -> Path:
        path = self.root / name
        # 따옴표 안 개행 포함 — 줄 수 세기가 아니라 CSV record 수를 세는지 검증.
        path.write_text(
            'doc_id,text,score\n1,"first\nmultiline",10\n2,plain,20\n3,third,30\n',
            encoding="utf-8",
        )
        return path

    def test_csv_ready_summary(self) -> None:
        path = self._write_csv()
        summary = build_source_summary(str(path), 0)
        self.assertTrue(summary["available"])
        self.assertEqual(summary["status"], "ready")
        self.assertEqual(summary["format"], "csv")
        self.assertEqual(summary["row_count"], 3)
        self.assertEqual(summary["column_count"], 3)
        self.assertEqual([c["name"] for c in summary["columns"]], ["doc_id", "text", "score"])
        # sample_limit=0 — sample_rows / sample_limit 키 생략 (Go omitempty).
        self.assertNotIn("sample_rows", summary)
        self.assertNotIn("sample_limit", summary)

    def test_csv_sample_rows(self) -> None:
        path = self._write_csv()
        summary = build_source_summary(str(path), 2)
        self.assertEqual(summary["sample_limit"], 2)
        self.assertEqual(len(summary["sample_rows"]), 2)
        self.assertEqual(summary["sample_rows"][1]["text"], "plain")

    def test_jsonl_ready_summary(self) -> None:
        path = self.root / "rows.jsonl"
        lines = [json.dumps({"doc_id": i, "text": f"t{i}"}) for i in range(4)]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        summary = build_source_summary(str(path), 0)
        self.assertEqual(summary["status"], "ready")
        self.assertEqual(summary["format"], "jsonl")
        self.assertEqual(summary["row_count"], 4)
        self.assertEqual([c["name"] for c in summary["columns"]], ["doc_id", "text"])

    def test_xlsx_ready_summary(self) -> None:
        from openpyxl import Workbook

        path = self.root / "book.xlsx"
        workbook = Workbook()
        sheet = workbook.active
        sheet.append(["col_a", "", "col_b"])  # 빈 헤더 열 무시
        sheet.append(["v1", "ignored", "v2"])
        sheet.append([None, None, None])  # 완전 빈 행 skip
        sheet.append(["v3", None, None])
        workbook.save(str(path))

        summary = build_source_summary(str(path), 5)
        self.assertEqual(summary["status"], "ready")
        self.assertEqual(summary["format"], "xlsx")
        self.assertEqual(summary["row_count"], 2)
        self.assertEqual(
            summary["columns"],
            [{"name": "col_a", "type": "VARCHAR"}, {"name": "col_b", "type": "VARCHAR"}],
        )
        self.assertEqual(summary["sample_rows"][0], {"col_a": "v1", "col_b": "v2"})

    def test_missing_file(self) -> None:
        summary = build_source_summary(str(self.root / "absent.csv"), 0)
        self.assertFalse(summary["available"])
        self.assertEqual(summary["status"], "missing")
        self.assertEqual(summary["error_message"], "source file not found")
        self.assertEqual(summary["format"], "csv")
        self.assertNotIn("row_count", summary)

    def test_empty_storage_uri(self) -> None:
        summary = build_source_summary("", 0)
        self.assertEqual(summary["status"], "missing")
        self.assertEqual(summary["error_message"], "storage_uri is required")

    def test_unsupported_format(self) -> None:
        summary = build_source_summary(str(self.root / "notes.txt"), 0)
        self.assertEqual(summary["status"], "unsupported")
        self.assertEqual(summary["error_message"], "unsupported source format")

    def test_directory_path_is_error(self) -> None:
        directory = self.root / "dir.csv"
        directory.mkdir()
        summary = build_source_summary(str(directory), 0)
        self.assertEqual(summary["status"], "error")
        self.assertEqual(summary["error_message"], "source path must be a file")

    def test_run_task_payload_validation(self) -> None:
        with self.assertRaises(ValueError):
            run_source_summary({"storage_uri": "x.csv", "sample_limit": "not-int"})
        summary = run_source_summary({"storage_uri": str(self._write_csv())})
        self.assertEqual(summary["status"], "ready")


if __name__ == "__main__":
    unittest.main()
