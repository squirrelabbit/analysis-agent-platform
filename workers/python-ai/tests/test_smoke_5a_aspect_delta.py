"""5-A unit smoke — "작년과 올해의 aspect 증감수치 계산해줘"를 수동 plan으로
실행해 service → validator → executor → present 경로가 deterministic하게
끝까지 동작하는지 확인한다.

silverone 2026-05-21 5단계 결정으로 fixture는 ``tests/fixtures/plan_v2_smoke/``에
committed.

시나리오 (festival 6 clause 미니 fixture):
- 2025 docs 2개: d1(atmosphere 1 clause) / d2(food 1 clause)
- 2026 docs 2개: d3(atmosphere 2 clauses) / d4(contents 1 clause)
- 예상 aspect별 결과:
  - atmosphere: last=1, this=2, delta=+1, rate=+100.0%
  - food:       last=1, this=NULL, delta=-1, rate=-100.0%   ← 사라진 aspect 유지
  - contents:   last=NULL, this=1, delta=+1, rate=NULL      ← 새 aspect 유지 + 분모 0 안전
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any

from python_ai_worker.executor import (
    ArtifactPaths,
    ExecutorContext,
    execute_analyze_plan,
)


_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "plan_v2_smoke"


def _fixture_paths() -> ArtifactPaths:
    return ArtifactPaths(
        docs=_FIXTURE_DIR / "cleaned.parquet",
        clauses=_FIXTURE_DIR / "clause_label.jsonl",
        genuineness=_FIXTURE_DIR / "doc_genuineness.jsonl",
    )


def _load_plan() -> dict[str, Any]:
    return json.loads((_FIXTURE_DIR / "aspect_delta_plan.json").read_text(encoding="utf-8"))


class YearOverYearAspectDeltaSmokeTests(unittest.TestCase):
    """silverone 5-A unit smoke — calculation path가 end-to-end 동작하는지."""

    def test_full_path_runs_to_present(self) -> None:
        result = execute_analyze_plan(
            "v1",
            _load_plan(),
            artifact_paths=_fixture_paths(),
        )

        self.assertEqual(result["plan_version"], "v2")
        self.assertEqual(len(result["steps"]), 8)
        self.assertIsNotNone(result["present"])
        self.assertEqual(result["present"]["step_id"], "out")
        self.assertEqual(result["present"]["format"], "table")
        self.assertEqual(result["present"]["title"], "작년 대비 올해 aspect 증감")

    def test_present_row_count_includes_disappeared_and_new_aspects(self) -> None:
        result = execute_analyze_plan("v1", _load_plan(), artifact_paths=_fixture_paths())

        self.assertEqual(result["present"]["row_count"], 3)
        aspects = {row["aspect"] for row in result["present"]["rows"]}
        self.assertEqual(aspects, {"ambiance_scenery", "food", "show_program"})

    def test_per_aspect_counts_and_delta_match_expected(self) -> None:
        result = execute_analyze_plan("v1", _load_plan(), artifact_paths=_fixture_paths())

        by_aspect = {row["aspect"]: row for row in result["present"]["rows"]}

        self.assertEqual(by_aspect["ambiance_scenery"]["last_year_count"], 1)
        self.assertEqual(by_aspect["ambiance_scenery"]["this_year_count"], 2)
        self.assertEqual(by_aspect["ambiance_scenery"]["delta_count"], 1)
        self.assertAlmostEqual(by_aspect["ambiance_scenery"]["delta_rate"], 100.0, places=4)

        self.assertEqual(by_aspect["food"]["last_year_count"], 1)
        self.assertIsNone(by_aspect["food"]["this_year_count"])
        self.assertEqual(by_aspect["food"]["delta_count"], -1)
        self.assertAlmostEqual(by_aspect["food"]["delta_rate"], -100.0, places=4)

        self.assertIsNone(by_aspect["show_program"]["last_year_count"])
        self.assertEqual(by_aspect["show_program"]["this_year_count"], 1)
        self.assertEqual(by_aspect["show_program"]["delta_count"], 1)
        self.assertIsNone(by_aspect["show_program"]["delta_rate"])

    def test_clause_id_generated_on_load(self) -> None:
        with ExecutorContext(_fixture_paths()) as context:
            rows = context.fetch_rows("clauses")
            clause_ids = [row["clause_id"] for row in rows]
            self.assertEqual(len(clause_ids), len(set(clause_ids)))
            for row in rows:
                self.assertTrue(
                    row["clause_id"].startswith(row["doc_id"] + "__"),
                    f"clause_id must start with doc_id__: {row}",
                )
            d3_ids = sorted(row["clause_id"] for row in rows if row["doc_id"] == "d3")
            self.assertEqual(d3_ids, ["d3__1", "d3__2"])

    def test_division_by_zero_does_not_crash(self) -> None:
        result = execute_analyze_plan("v1", _load_plan(), artifact_paths=_fixture_paths())
        by_aspect = {row["aspect"]: row for row in result["present"]["rows"]}
        self.assertIsNone(by_aspect["show_program"]["delta_rate"])
        self.assertIsNotNone(by_aspect["ambiance_scenery"]["delta_rate"])
        self.assertIsNotNone(by_aspect["food"]["delta_rate"])


if __name__ == "__main__":
    unittest.main()
