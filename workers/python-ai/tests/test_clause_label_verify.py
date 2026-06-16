"""clause_label verify mode 잠금 (ADR-028) — 문장 앵커 교차모델 + reconciliation + judge.

splitter는 결정론적으로 patch하고, LLOA는 model별 urlopen 주입으로 mock한다.
classify(번호 문장 plaintext)와 judge(candidate_1 포함 JSON)를 user payload로 구분한다.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


def _completion(content: str) -> dict:
    return {
        "choices": [{"message": {"content": content, "reasoning_content": ""}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


def _fake_config():
    from python_ai_worker.config import WorkerConfig

    return WorkerConfig(
        lloa_api_key="k",
        lloa_api_url="http://lloa.example/v1/chat/completions",
        lloa_model="model-a",
        lloa_max_tokens=2048,
        lloa_timeout_sec=30,
        lloa_reasoning_effort=None,
        lloa_prepend_no_think=True,
    )


# 3 문장 고정 fixture:
#  s1 합의(positive/food)            → agree, 1 row
#  s2 sentiment neutral↔positive + aspect subset → sentiment_auto + union
#  s3 aspect disjoint(food vs show) → judge
SENTENCES = ["맛집 음식이 최고였다", "야경 보며 푸드트럭도 들렀다", "공연인지 먹거리인지 애매"]

# model별 classify 출력 (index 1-based)
LABELS_A = [
    {"index": 1, "relevant": True, "sentiment": "positive", "aspects": ["food"]},
    {"index": 2, "relevant": True, "sentiment": "neutral", "aspects": ["food"]},
    {"index": 3, "relevant": True, "sentiment": "positive", "aspects": ["food"]},
]
LABELS_B = [
    {"index": 1, "relevant": True, "sentiment": "positive", "aspects": ["food"]},
    {"index": 2, "relevant": True, "sentiment": "positive", "aspects": ["food", "ambiance_scenery"]},
    {"index": 3, "relevant": True, "sentiment": "positive", "aspects": ["show_program"]},
]


class ClauseLabelVerifyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        with self.clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "본문(분할은 patch)"}, ensure_ascii=False) + "\n")

    def _run(self, *, judge_item: dict | None):
        from python_ai_worker.dataset_build import clause_label_verify as v

        def make_urlopen(model: str):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                user = json.loads(req.data.decode("utf-8"))["messages"][1]["content"]
                if "candidate_1" in user:  # judge 호출
                    return _Resp(_completion(json.dumps([judge_item] if judge_item else [], ensure_ascii=False)))
                labels = LABELS_A if model == "model-a" else LABELS_B
                return _Resp(_completion(json.dumps(labels, ensure_ascii=False)))

            return _fake

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=make_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1",
            "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out),
            "verify": True,
            "classify_models": ["model-a", "model-b"],
            "judge_model": "model-judge",
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init), \
             patch.object(v, "_split_anchor_sentences", return_value=SENTENCES):
            result = v.run_dataset_clause_label_verify(payload)
        rows = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines() if line.strip()]
        return result, rows

    def test_agree_union_sentiment_auto_and_judge(self) -> None:
        # judge가 s3을 food로 확정.
        result, rows = self._run(judge_item={
            "sentence_index": 3, "relevant": True, "sentiment": "positive",
            "aspects": ["food"], "chosen": "candidate_1", "reason": "음식 맥락",
        })
        by_sent: dict[str, list[dict]] = {}
        for r in rows:
            by_sent.setdefault(r["clause"], []).append(r)

        # s1 agree → 1 row food/positive
        s1 = by_sent[SENTENCES[0]]
        self.assertEqual(len(s1), 1)
        self.assertEqual(s1[0]["resolution"], "agree")
        self.assertEqual(s1[0]["aspect"], "food")
        self.assertEqual(s1[0]["sentiment"], "positive")
        self.assertFalse(s1[0]["needs_review"])

        # s2: sentiment neutral↔positive→positive, aspect {food}⊂{food,ambiance}→union → 2 rows
        s2 = by_sent[SENTENCES[1]]
        self.assertEqual({r["aspect"] for r in s2}, {"food", "ambiance_scenery"})
        self.assertTrue(all(r["sentiment"] == "positive" for r in s2))  # non-neutral 채택
        self.assertTrue(all(r["resolution"] == "union" for r in s2))

        # s3: disjoint → judge → food
        s3 = by_sent[SENTENCES[2]]
        self.assertEqual(len(s3), 1)
        self.assertEqual(s3[0]["resolution"], "judge")
        self.assertEqual(s3[0]["aspect"], "food")
        self.assertFalse(s3[0]["needs_review"])

        summary = result["artifact"]["summary"]
        self.assertEqual(summary["mode"], "verify")
        rc = summary["resolution_counts"]
        self.assertEqual(rc["agree"], 1)
        self.assertEqual(rc["union"], 1)
        self.assertEqual(rc["judge"], 1)

    def test_judge_invalid_aspect_marks_needs_review(self) -> None:
        # judge가 s3에 invalid aspect → fallback 없이 needs_review
        _result, rows = self._run(judge_item={
            "sentence_index": 3, "relevant": True, "sentiment": "positive",
            "aspects": ["not_a_real_aspect"], "chosen": "other", "reason": "x",
        })
        s3 = [r for r in rows if r["clause"] == SENTENCES[2]]
        self.assertTrue(all(r["needs_review"] for r in s3))
        self.assertTrue(all(r["resolution"] == "needs_review" for r in s3))

    def test_judge_missing_isolates_needs_review(self) -> None:
        # judge가 s3 결과를 안 줌 → needs_review + union aspects 보존
        _result, rows = self._run(judge_item=None)
        s3 = [r for r in rows if r["clause"] == SENTENCES[2]]
        self.assertTrue(s3)
        self.assertTrue(all(r["needs_review"] for r in s3))
        self.assertTrue(all(r["resolution"] == "needs_review" for r in s3))
        self.assertEqual({r["aspect"] for r in s3}, {"food", "show_program"})  # union 보존

    def test_classify_models_validation(self) -> None:
        from python_ai_worker.dataset_build import clause_label_verify as v

        with patch.object(v, "load_config", return_value=_fake_config()):
            with self.assertRaises(ValueError):
                v.run_dataset_clause_label_verify({
                    "dataset_version_id": "x", "clean_artifact_ref": str(self.clean),
                    "output_path": str(self.out), "verify": True,
                    "classify_models": ["only-one"],
                    "doc_genuineness": {"subject_name": "x"},
                })


if __name__ == "__main__":
    unittest.main()
