"""빌드 협조적 취소 (silverone 2026-06-29).

_cancel 레지스트리 + 빌드 task가 루프에서 event를 확인해 남은 doc를 멈추고 거기까지
결과를 보존(summary.cancelled=True)하는지 검증한다.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from python_ai_worker.dataset_build import _cancel


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


class CancelRegistryTests(unittest.TestCase):
    def test_begin_request_end(self) -> None:
        _cancel.end("v")  # 깨끗한 시작
        # 등록 안 됐으면 request는 False.
        self.assertFalse(_cancel.request("v"))
        event = _cancel.begin("v")
        self.assertFalse(event.is_set())
        self.assertTrue(_cancel.request("v"))  # 등록됐으니 True + set
        self.assertTrue(event.is_set())
        # begin 재호출은 직전 event를 clear(재사용).
        event2 = _cancel.begin("v")
        self.assertFalse(event2.is_set())
        _cancel.end("v")
        self.assertFalse(_cancel.request("v"))


class ClauseLabelCancelTests(unittest.TestCase):
    SENTENCES = [f"s{i}" for i in range(1, 5)]

    def test_cancel_mid_run_preserves_partial(self) -> None:
        from python_ai_worker.dataset_build import clause_label as cl
        from python_ai_worker.dataset_build import clause_label_verify as v

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        clean = base / "clean.jsonl"
        out = base / "out.jsonl"
        with clean.open("w", encoding="utf-8") as f:
            for i in range(1, 6):  # 5 doc
                f.write(json.dumps({"row_id": f"d{i}", "cleaned_text": "x" * 30}, ensure_ascii=False) + "\n")

        calls = {"n": 0}

        def _fake_labels(client, system_prompt, sub, max_tokens, allowed, fallback):
            calls["n"] += 1
            if calls["n"] == 2:
                # 둘째 doc 처리 중 취소 요청 → 다음 루프 반복에서 멈춘다(첫 doc은 보존).
                _cancel.request("ver-cancel")
            return {i: {"relevant": True, "sentiment": "positive", "aspects": ["food"]} for i in range(1, len(sub) + 1)}

        payload = {
            "dataset_version_id": "ver-cancel", "clean_artifact_ref": str(clean),
            "output_path": str(out),
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1, "include_genuineness": [],  # 필터 off → 5 doc 전부 대상
        }
        with patch.object(cl, "load_config", return_value=_fake_config()), \
             patch.object(cl.LloaClient, "__init__", lambda self, config, **kw: None), \
             patch.object(cl, "split_anchor_sentences", return_value=self.SENTENCES), \
             patch.object(v, "_label_sentences", _fake_labels):
            result = cl.run_dataset_clause_label(payload)

        summary = result["artifact"]["summary"]
        self.assertTrue(summary["cancelled"], "summary.cancelled True여야")
        # 거기까지 결과 보존 — 일부 doc은 출력되지만 5 doc 전부는 아님.
        rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines() if line.strip()]
        doc_ids = {r["doc_id"] for r in rows}
        self.assertGreaterEqual(len(doc_ids), 1)
        self.assertLess(len(doc_ids), 5)
        # 레지스트리 정리됨.
        self.assertFalse(_cancel.request("ver-cancel"))


if __name__ == "__main__":
    unittest.main()
