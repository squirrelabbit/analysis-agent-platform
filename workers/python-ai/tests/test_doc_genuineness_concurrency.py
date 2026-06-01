"""silverone 2026-05-28 (D2) — dataset_doc_genuineness concurrency 잠금.

clause_label과 동일한 ThreadPoolExecutor 패턴 이식 후 동작 검증:
- summary에 concurrency / reasoning_effort 노출
- payload concurrency override
- output jsonl은 원본 row 순서로 write (concurrent 호출이어도 ordering 보장)
- 모든 doc_id가 record_by_doc에 들어가 누락 없음
"""

from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch


def _llm_completion(content: str, *, finish_reason: str = "stop") -> dict:
    return {
        "choices": [{
            "message": {"content": content, "reasoning_content": ""},
            "finish_reason": finish_reason,
        }],
        "usage": {"prompt_tokens": 50, "completion_tokens": 20, "total_tokens": 70},
    }


def _fake_urlopen_factory_with_delay(responses_by_doc, delay_sec: float = 0.05):
    """LLOA 호출마다 delay를 주고 호출된 thread name을 기록 — 병렬성 검증용."""
    call_log: list[dict] = []
    log_lock = threading.Lock()

    class _Resp:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return json.dumps(self._payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

    def _fake(req, timeout=None):
        body = json.loads(req.data.decode("utf-8"))
        user_text = body["messages"][1]["content"]
        user_obj = json.loads(user_text)
        doc_id = user_obj["doc_id"]
        with log_lock:
            call_log.append({
                "doc_id": doc_id,
                "thread": threading.current_thread().name,
                "started_at": time.monotonic(),
            })
        time.sleep(delay_sec)
        if doc_id not in responses_by_doc:
            raise AssertionError(f"unexpected doc_id in test: {doc_id}")
        return _Resp(responses_by_doc[doc_id])

    return _fake, call_log


class DocGenuinenessConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean_path = Path(self.tmpdir.name) / "clean.jsonl"
        self.output_path = Path(self.tmpdir.name) / "doc_genuineness.jsonl"
        # 8 docs (concurrency=8과 동일 — full saturation 검증)
        self.rows = [
            {"row_id": f"row:{i}", "cleaned_text": f"본문 {i}번째 doc의 실제 후기 텍스트 내용입니다."}
            for i in range(8)
        ]
        with self.clean_path.open("w", encoding="utf-8") as f:
            for row in self.rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _payload(self, **overrides) -> dict:
        payload = {
            "dataset_version_id": "dvid:test",
            "clean_artifact_ref": str(self.clean_path),
            "output_path": str(self.output_path),
            "doc_genuineness": {
                "subject_type": "festival",
                "subject_name": "강릉 국가유산야행",
                "subject_aliases": ["문화유산야행"],
                "recruitment_keywords": [],
            },
        }
        payload.update(overrides)
        return payload

    def _run_with_concurrency(self, urlopen_fn, payload_overrides=None):
        from python_ai_worker.dataset_build import doc_genuineness
        from python_ai_worker.config import WorkerConfig

        fake_config = WorkerConfig(
            lloa_api_key="test-key",
            lloa_api_url="http://lloa.example/v1/chat/completions",
            lloa_model="wisenut/wise-lloa-max-v1.2.1",
            lloa_max_tokens=2048,
            lloa_timeout_sec=30,
            lloa_reasoning_effort="low",
            lloa_prepend_no_think=True,
        )

        original_init = doc_genuineness.LloaClient.__init__

        def _init_with_fake(self, config, *, urlopen=None):
            original_init(self, config, urlopen=urlopen_fn)

        payload = self._payload(**(payload_overrides or {}))
        with patch.object(doc_genuineness, "load_config", return_value=fake_config), \
             patch.object(doc_genuineness.LloaClient, "__init__", _init_with_fake):
            return doc_genuineness.run_dataset_doc_genuineness(payload)

    def test_default_concurrency_exposed_in_summary(self):
        responses = {
            f"row:{i}": _llm_completion(
                f'{{"doc_id":"row:{i}","genuineness":"genuine_review","reason":"r{i}"}}'
            )
            for i in range(8)
        }
        urlopen_fn, _ = _fake_urlopen_factory_with_delay(responses, delay_sec=0)
        result = self._run_with_concurrency(urlopen_fn)
        summary = result["artifact"]["summary"]
        self.assertEqual(summary["concurrency"], 8, "default concurrency must be 8")
        self.assertEqual(summary["reasoning_effort"], "low")
        self.assertEqual(summary["processed_row_count"], 8)
        self.assertEqual(summary["parse_failures"], 0)

    def test_payload_concurrency_override(self):
        responses = {
            f"row:{i}": _llm_completion(
                f'{{"doc_id":"row:{i}","genuineness":"non_review","reason":"r"}}'
            )
            for i in range(8)
        }
        urlopen_fn, _ = _fake_urlopen_factory_with_delay(responses, delay_sec=0)
        result = self._run_with_concurrency(urlopen_fn, payload_overrides={"concurrency": 2})
        self.assertEqual(result["artifact"]["summary"]["concurrency"], 2)

    def test_output_preserves_original_row_order_under_concurrency(self):
        # 의도적으로 LLOA 호출에 delay를 주고 ordering 확인.
        # ThreadPoolExecutor는 as_completed로 처리하지만 jsonl write는 원본
        # row 순서로 한다.
        responses = {
            f"row:{i}": _llm_completion(
                f'{{"doc_id":"row:{i}","genuineness":"genuine_review","reason":"r{i}"}}'
            )
            for i in range(8)
        }
        urlopen_fn, log = _fake_urlopen_factory_with_delay(responses, delay_sec=0.02)
        self._run_with_concurrency(urlopen_fn)

        # 출력 파일 line 순서 == 원본 row 순서.
        lines = self.output_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 8)
        for idx, line in enumerate(lines):
            record = json.loads(line)
            self.assertEqual(record["doc_id"], f"row:{idx}",
                             f"line {idx} doc_id mismatch — concurrency가 order를 깨면 안 됨")

        # 병렬 호출이 실제로 일어났는지 — 동일 thread만 쓰면 sequential과 동일.
        unique_threads = {entry["thread"] for entry in log}
        self.assertGreater(len(unique_threads), 1,
                           f"concurrency 적용 안 됨 — 단일 thread만 사용됨: {unique_threads}")

    def test_empty_docs_skipped_from_lloa_calls(self):
        # rows[7]을 empty로 만들면 LLOA 호출은 7건만 일어나야.
        empty_row_path = Path(self.tmpdir.name) / "clean_with_empty.jsonl"
        rows_with_empty = self.rows[:7] + [{"row_id": "row:7", "cleaned_text": ""}]
        with empty_row_path.open("w", encoding="utf-8") as f:
            for row in rows_with_empty:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        responses = {
            f"row:{i}": _llm_completion(
                f'{{"doc_id":"row:{i}","genuineness":"genuine_review","reason":"r"}}'
            )
            for i in range(7)
        }
        urlopen_fn, log = _fake_urlopen_factory_with_delay(responses, delay_sec=0)

        from python_ai_worker.dataset_build import doc_genuineness
        from python_ai_worker.config import WorkerConfig
        fake_config = WorkerConfig(
            lloa_api_key="test-key",
            lloa_api_url="http://lloa.example/v1/chat/completions",
            lloa_model="wisenut/wise-lloa-max-v1.2.1",
            lloa_max_tokens=2048,
            lloa_timeout_sec=30,
            lloa_reasoning_effort="low",
            lloa_prepend_no_think=True,
        )
        original_init = doc_genuineness.LloaClient.__init__

        def _init_with_fake(self, config, *, urlopen=None):
            original_init(self, config, urlopen=urlopen_fn)

        payload = self._payload(clean_artifact_ref=str(empty_row_path))
        with patch.object(doc_genuineness, "load_config", return_value=fake_config), \
             patch.object(doc_genuineness.LloaClient, "__init__", _init_with_fake):
            result = doc_genuineness.run_dataset_doc_genuineness(payload)

        # 7건만 LLOA에 도달, 1건은 empty shortcut.
        self.assertEqual(len(log), 7)
        self.assertNotIn("row:7", {e["doc_id"] for e in log})

        summary = result["artifact"]["summary"]
        self.assertEqual(summary["processed_row_count"], 8)
        # row:7 empty shortcut + row:0~6 genuine_review
        self.assertEqual(summary["tier_counts"]["genuine_review"], 7)
        self.assertEqual(summary["tier_counts"]["non_review"], 1)


if __name__ == "__main__":
    unittest.main()
