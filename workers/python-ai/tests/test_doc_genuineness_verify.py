"""doc_genuineness verify mode 잠금 (ADR-026) — 교차 분류 + 불일치 judge.

LloaClient는 model별 urlopen 주입으로 mock한다. classify 두 모델 + judge를
요청 body의 system/모델로 구분해 정해진 응답을 돌려준다.
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


class DocGenuinenessVerifyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        rows = [
            {"row_id": "d1", "cleaned_text": "강릉야행 다녀왔어요. 야경 환상적."},  # 합의(genuine)
            {"row_id": "d2", "cleaned_text": "행사 안내문입니다. 일정 확인."},      # 불일치 → judge
            {"row_id": "d3", "cleaned_text": ""},                                   # empty shortcut
        ]
        with self.clean.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    def _run(self, *, judge_chosen: str, judge_label: str | None, judge_conf: float):
        from python_ai_worker.dataset_build import doc_genuineness_verify as v

        # model별 응답: a/b classify는 doc_id로, judge는 candidate 포함 여부로 구분.
        def make_urlopen(model: str):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                body = json.loads(req.data.decode("utf-8"))
                user = body["messages"][1]["content"]
                obj = json.loads(user)
                if "candidate_1" in obj:  # judge 호출
                    return _Resp(_completion(json.dumps({
                        "chosen": judge_chosen,
                        "final_label": judge_label,
                        "confidence": judge_conf,
                        "reason": "judge reason",
                    }, ensure_ascii=False)))
                doc_id = obj["doc_id"]
                # d1은 둘 다 genuine, d2는 a=genuine b=non_review로 불일치.
                if doc_id == "d1":
                    label = "genuine_review"
                else:  # d2
                    label = "genuine_review" if model == "model-a" else "non_review"
                return _Resp(_completion(json.dumps({"genuineness": label, "reason": "r"}, ensure_ascii=False)))

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
             patch.object(v.LloaClient, "__init__", _init):
            result = v.run_dataset_doc_genuineness_verify(payload)
        records = {}
        with self.out.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rec = json.loads(line)
                    records[rec["doc_id"]] = rec
        return result, records

    def test_agreement_and_judge_accept(self) -> None:
        # judge가 candidate 중 non_review 쪽(=model-b) 채택하도록.
        # d2: a=genuine(candidate_? ), b=non. judge_label은 매핑 후 검증하므로
        # chosen=candidate_1/2 어느 쪽이든 코드가 winner.genuineness로 final 설정.
        result, recs = self._run(judge_chosen="candidate_1", judge_label="genuine_review", judge_conf=0.9)
        # d1 합의.
        self.assertEqual(recs["d1"]["resolution"], "model_agreement")
        self.assertEqual(recs["d1"]["final_label"], "genuine_review")
        self.assertFalse(recs["d1"]["needs_review"])
        self.assertFalse(recs["d1"]["is_disagreement"])
        # d3 empty shortcut.
        self.assertEqual(recs["d3"]["resolution"], "empty_text_shortcut")
        self.assertEqual(recs["d3"]["final_label"], "non_review")
        # d2 불일치 → judge.
        self.assertTrue(recs["d2"]["is_disagreement"])
        self.assertEqual(recs["d2"]["resolution"], "judge_on_disagreement")
        self.assertIn(recs["d2"]["judge_result"]["decision"], {"accept_a", "accept_b"})
        self.assertIn(recs["d2"]["final_label"], {"genuine_review", "non_review"})
        self.assertFalse(recs["d2"]["needs_review"])  # conf 0.9 >= 0.85
        # summary.
        s = result["artifact"]["summary"]
        self.assertEqual(s["mode"], "verify")
        self.assertEqual(s["agreement_count"], 1)
        self.assertEqual(s["disagreement_count"], 1)
        self.assertEqual(s["judge_count"], 1)
        self.assertEqual(s["models"], {"a": "model-a", "b": "model-b", "judge": "model-judge"})

    def test_low_confidence_needs_review(self) -> None:
        _result, recs = self._run(judge_chosen="candidate_2", judge_label="non_review", judge_conf=0.5)
        self.assertTrue(recs["d2"]["needs_review"])  # conf 0.5 < 0.85

    def test_judge_review_decision_nulls_final(self) -> None:
        _result, recs = self._run(judge_chosen="review", judge_label=None, judge_conf=0.4)
        self.assertEqual(recs["d2"]["judge_result"]["decision"], "review")
        self.assertIsNone(recs["d2"]["final_label"])
        self.assertTrue(recs["d2"]["needs_review"])

    def test_revise_third_label(self) -> None:
        _result, recs = self._run(judge_chosen="other", judge_label="uncertain", judge_conf=0.9)
        self.assertEqual(recs["d2"]["judge_result"]["decision"], "revise")
        self.assertEqual(recs["d2"]["final_label"], "uncertain")

    def _run_with_urlopen(self, make_urlopen):
        from python_ai_worker.dataset_build import doc_genuineness_verify as v

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=make_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1", "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out), "verify": True,
            "classify_models": ["model-a", "model-b"], "judge_model": "model-judge",
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init):
            v.run_dataset_doc_genuineness_verify(payload)
        recs = {}
        with self.out.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    recs[r["doc_id"]] = r
        return recs

    def test_partial_classify_isolation(self) -> None:
        # model-b는 항상 빈 content(실패), model-a는 정상 → partial_classify.
        def make_urlopen(model):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                obj = json.loads(json.loads(req.data.decode("utf-8"))["messages"][1]["content"])
                if "candidate_1" in obj:
                    return _Resp(_completion(json.dumps({"chosen": "candidate_1", "final_label": "genuine_review", "confidence": 0.9, "reason": "r"})))
                if model == "model-b":
                    return _Resp(_completion(""))  # 빈 content → LloaResponseParseError
                return _Resp(_completion(json.dumps({"genuineness": "genuine_review", "reason": "a ok"})))
            return _fake

        recs = self._run_with_urlopen(make_urlopen)
        for doc in ("d1", "d2"):
            self.assertEqual(recs[doc]["resolution"], "partial_classify", doc)
            self.assertEqual(recs[doc]["final_label"], "genuine_review", doc)  # a 라벨 채택
            self.assertTrue(recs[doc]["needs_review"], doc)
            self.assertIsNone(recs[doc]["model_b_result"], doc)  # b 실패

    def test_both_classify_fail_uncertain(self) -> None:
        # 두 모델 모두 빈 content → classify_error + uncertain(빈칸 아님).
        def make_urlopen(_model):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                return _Resp(_completion(""))
            return _fake

        recs = self._run_with_urlopen(make_urlopen)
        self.assertEqual(recs["d1"]["resolution"], "classify_error")
        self.assertEqual(recs["d1"]["final_label"], "uncertain")
        self.assertTrue(recs["d1"]["needs_review"])

    def test_classify_models_validation(self) -> None:
        from python_ai_worker.dataset_build import doc_genuineness_verify as v

        with patch.object(v, "load_config", return_value=_fake_config()):
            with self.assertRaises(ValueError):
                v.run_dataset_doc_genuineness_verify({
                    "dataset_version_id": "x", "clean_artifact_ref": str(self.clean),
                    "output_path": str(self.out), "verify": True,
                    "classify_models": ["only-one"],
                    "doc_genuineness": {"subject_name": "x"},
                })


if __name__ == "__main__":
    unittest.main()
