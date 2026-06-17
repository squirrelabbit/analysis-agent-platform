"""clause_label 진성 필터 effective label 우선순위 잠금 (ADR-026, step 4b).

override > final_label > genuineness/label. verify artifact(final_label)와 단일
모델 artifact(genuineness) 모두 처리하고, payload genuineness_overrides가 최상위.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from python_ai_worker.dataset_build.clause_label import _load_genuineness_filter


def _write(path: Path, recs: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


class GenuinenessFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ref = Path(self.tmp.name) / "dg.jsonl"

    def test_final_label_priority_over_genuineness(self) -> None:
        # verify artifact: final_label이 권위. genuineness(원본 모델)는 무시.
        _write(self.ref, [
            {"doc_id": "d1", "final_label": "non_review", "genuineness": "genuine_review"},
            {"doc_id": "d2", "genuineness": "genuine_review"},  # 단일 모델(final_label 없음)
        ])
        tiers, tier_by_doc, _spans = _load_genuineness_filter({
            "include_genuineness": ["genuine_review"],
            "doc_genuineness_ref": str(self.ref),
        })
        self.assertEqual(tiers, {"genuine_review"})
        self.assertEqual(tier_by_doc["d1"], "non_review")    # final_label 우선
        self.assertEqual(tier_by_doc["d2"], "genuine_review")  # genuineness fallback

    def test_override_is_top_priority(self) -> None:
        _write(self.ref, [
            {"doc_id": "d1", "final_label": "non_review"},
        ])
        _tiers, tier_by_doc, _spans = _load_genuineness_filter({
            "include_genuineness": ["genuine_review"],
            "doc_genuineness_ref": str(self.ref),
            "genuineness_overrides": {"d1": "genuine_review"},  # 사람 보정 최상위
        })
        self.assertEqual(tier_by_doc["d1"], "genuine_review")  # override > final_label

    def test_legacy_label_field_fallback(self) -> None:
        _write(self.ref, [{"doc_id": "d1", "label": "non_review"}])
        _tiers, tier_by_doc, _spans = _load_genuineness_filter({
            "include_genuineness": ["non_review"],
            "doc_genuineness_ref": str(self.ref),
        })
        self.assertEqual(tier_by_doc["d1"], "non_review")


if __name__ == "__main__":
    unittest.main()
