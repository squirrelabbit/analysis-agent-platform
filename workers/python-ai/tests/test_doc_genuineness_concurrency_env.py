"""silverone 2026-06-01 — doc_genuineness concurrency env fallback 잠금.

resolve 우선순위: payload > env LLOA_DOC_GENUINENESS_CONCURRENCY > default 8.
invalid / 0 / negative는 silent fallback. cap 32.

helper level 6 case + run_dataset_doc_genuineness 호출 시 summary.concurrency가
최종 적용값으로 기록되는지 fixture로 잠금.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from python_ai_worker.dataset_build.doc_genuineness import (
    _DEFAULT_CONCURRENCY,
    _DOC_GENUINENESS_CONCURRENCY_ENV,
    _MAX_CONCURRENCY,
    _resolve_concurrency,
)


class ResolveConcurrencyEnvTests(unittest.TestCase):
    def setUp(self) -> None:
        # 다른 test에서 set한 env가 새지 않도록 항상 깨끗하게 시작.
        self._saved_env = os.environ.pop(_DOC_GENUINENESS_CONCURRENCY_ENV, None)

    def tearDown(self) -> None:
        os.environ.pop(_DOC_GENUINENESS_CONCURRENCY_ENV, None)
        if self._saved_env is not None:
            os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = self._saved_env

    def test_env_absent_uses_default(self):
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)
        self.assertEqual(_DEFAULT_CONCURRENCY, 8, "default constant 변경 잠금")

    def test_env_overrides_default(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "4"
        self.assertEqual(_resolve_concurrency({}), 4)

    def test_payload_overrides_env(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "4"
        self.assertEqual(_resolve_concurrency({"concurrency": 2}), 2)

    def test_env_invalid_falls_back_to_default(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "abc"
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = ""
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "3.5"
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)

    def test_env_zero_or_negative_falls_back_to_default(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "0"
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "-3"
        self.assertEqual(_resolve_concurrency({}), _DEFAULT_CONCURRENCY)

    def test_env_too_large_caps_at_max(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "999"
        self.assertEqual(_resolve_concurrency({}), _MAX_CONCURRENCY)
        self.assertEqual(_MAX_CONCURRENCY, 32, "max cap 변경 잠금")

    def test_payload_invalid_then_env_fallback(self):
        # payload가 invalid면 env로 fallback (payload value 없는 것으로 취급).
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "5"
        self.assertEqual(_resolve_concurrency({"concurrency": -1}), 5)
        self.assertEqual(_resolve_concurrency({"concurrency": "abc"}), 5)
        self.assertEqual(_resolve_concurrency({"concurrency": 0}), 5)

    def test_payload_too_large_caps_at_max(self):
        self.assertEqual(_resolve_concurrency({"concurrency": 999}), _MAX_CONCURRENCY)

    def test_payload_bool_rejected(self):
        # bool은 int subclass라 silent로 1/0이 될 위험 — reject 확인.
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "5"
        self.assertEqual(_resolve_concurrency({"concurrency": True}), 5)
        self.assertEqual(_resolve_concurrency({"concurrency": False}), 5)


class SummaryReflectsResolvedConcurrencyTests(unittest.TestCase):
    """run_dataset_doc_genuineness가 _resolve_concurrency 결과를
    summary.concurrency에 그대로 기록하는지 fixture로 확인.
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean_path = Path(self.tmpdir.name) / "clean.jsonl"
        self.output_path = Path(self.tmpdir.name) / "doc_genuineness.jsonl"
        # 1 doc — concurrency 값 잠금만 확인하면 충분.
        with self.clean_path.open("w", encoding="utf-8") as f:
            f.write(json.dumps(
                {"row_id": "row:0", "cleaned_text": "본문 텍스트."},
                ensure_ascii=False,
            ) + "\n")
        self._saved_env = os.environ.pop(_DOC_GENUINENESS_CONCURRENCY_ENV, None)

    def tearDown(self) -> None:
        os.environ.pop(_DOC_GENUINENESS_CONCURRENCY_ENV, None)
        if self._saved_env is not None:
            os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = self._saved_env

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

    def _run(self, urlopen_fn, payload_overrides=None):
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

    def _stub_urlopen(self):
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
            return _Resp({
                "choices": [{
                    "message": {"content": '{"doc_id":"row:0","genuineness":"genuine_review","reason":"r"}', "reasoning_content": ""},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            })

        return _fake

    def test_summary_records_env_concurrency(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "4"
        result = self._run(self._stub_urlopen())
        self.assertEqual(result["artifact"]["summary"]["concurrency"], 4)

    def test_summary_records_capped_value(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "999"
        result = self._run(self._stub_urlopen())
        self.assertEqual(result["artifact"]["summary"]["concurrency"], _MAX_CONCURRENCY)

    def test_summary_records_payload_over_env(self):
        os.environ[_DOC_GENUINENESS_CONCURRENCY_ENV] = "4"
        result = self._run(self._stub_urlopen(), payload_overrides={"concurrency": 2})
        self.assertEqual(result["artifact"]["summary"]["concurrency"], 2)


if __name__ == "__main__":
    unittest.main()
