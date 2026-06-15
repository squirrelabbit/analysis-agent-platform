"""전처리 빌드 모델 선택 (2026-06-12) — payload model_id가 LloaConfig.model로
반영되고 summary.model에 실사용 모델이 기록되는지 잠금.

control-plane이 allowlist(LLOA_MODELS) 검증 후 payload로 넘기는 계약이므로
worker는 받은 값을 그대로 쓴다. 생략 시 env(LLOA_MODEL) default 유지.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


def _llm_completion(content: str) -> dict:
    return {
        "choices": [{
            "message": {"content": content, "reasoning_content": ""},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 100, "completion_tokens": 30, "total_tokens": 130},
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


def _fake_worker_config():
    from python_ai_worker.config import WorkerConfig

    return WorkerConfig(
        lloa_api_key="test-key",
        lloa_api_url="http://lloa.example/v1/chat/completions",
        lloa_model="wisenut/wise-lloa-max-v1.2.1",
        lloa_max_tokens=2048,
        lloa_timeout_sec=30,
        lloa_reasoning_effort=None,
        lloa_prepend_no_think=True,
    )


class DocGenuinenessModelSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean_path = Path(self.tmpdir.name) / "clean.jsonl"
        self.output_path = Path(self.tmpdir.name) / "doc_genuineness.jsonl"
        self.clean_path.write_text(
            json.dumps({"row_id": "row:1", "cleaned_text": "야경이 환상적이었습니다."}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    def _run(self, payload_overrides: dict) -> tuple[dict, list]:
        from python_ai_worker.dataset_build import doc_genuineness

        captured_configs: list = []
        original_init = doc_genuineness.LloaClient.__init__

        def _fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            return _Resp(_llm_completion(
                '{"doc_id":"row:1","genuineness":"genuine_review","reason":"후기."}'
            ))

        def _init_with_fake(self, config, *, urlopen=None):
            captured_configs.append(config)
            original_init(self, config, urlopen=_fake_urlopen)

        payload = {
            "dataset_version_id": "dvid:test",
            "clean_artifact_ref": str(self.clean_path),
            "output_path": str(self.output_path),
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
        }
        payload.update(payload_overrides)
        with patch.object(doc_genuineness, "load_config", return_value=_fake_worker_config()), \
             patch.object(doc_genuineness.LloaClient, "__init__", _init_with_fake):
            result = doc_genuineness.run_dataset_doc_genuineness(payload)
        return result, captured_configs

    def test_model_id_overrides_config_model(self) -> None:
        result, configs = self._run({"model_id": "wisenut/wise-lloa-ultra-v1.1.0"})
        self.assertEqual(configs[0].model, "wisenut/wise-lloa-ultra-v1.1.0")
        self.assertEqual(
            result["artifact"]["summary"]["model"], "wisenut/wise-lloa-ultra-v1.1.0"
        )

    def test_model_id_omitted_keeps_env_default(self) -> None:
        result, configs = self._run({})
        self.assertEqual(configs[0].model, "wisenut/wise-lloa-max-v1.2.1")
        self.assertEqual(
            result["artifact"]["summary"]["model"], "wisenut/wise-lloa-max-v1.2.1"
        )


class ClauseLabelModelSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.clean_path = Path(self.tmpdir.name) / "clean.jsonl"
        self.output_path = Path(self.tmpdir.name) / "clause_label.jsonl"
        self.clean_path.write_text(
            json.dumps(
                {"row_id": "row:1", "doc_title": "후기", "cleaned_text": "야경은 환상적이었어요."},
                ensure_ascii=False,
            ) + "\n",
            encoding="utf-8",
        )

    def _run(self, payload_overrides: dict) -> tuple[dict, list]:
        from python_ai_worker.dataset_build import clause_label

        captured_configs: list = []
        original_init = clause_label.LloaClient.__init__

        def _fake_urlopen(req, timeout=None):  # type: ignore[no-untyped-def]
            clauses = [{"clause": "야경은 환상적이었어요", "sentiment": "positive", "aspect": "ambiance_scenery"}]
            return _Resp(_llm_completion(json.dumps(clauses, ensure_ascii=False)))

        def _init_with_fake(self, config, *, urlopen=None):
            captured_configs.append(config)
            original_init(self, config, urlopen=_fake_urlopen)

        payload = {
            "dataset_version_id": "dvid:test",
            "clean_artifact_ref": str(self.clean_path),
            "output_path": str(self.output_path),
            "concurrency": 1,
            "include_genuineness": [],  # doc_genuineness artifact 없는 fixture — opt-out
        }
        payload.update(payload_overrides)
        with patch.object(clause_label, "load_config", return_value=_fake_worker_config()), \
             patch.object(clause_label.LloaClient, "__init__", _init_with_fake):
            result = clause_label.run_dataset_clause_label(payload)
        return result, captured_configs

    def test_model_id_overrides_config_model(self) -> None:
        result, configs = self._run({"model_id": "wisenut/wise-lloa-ultra-v1.1.0"})
        self.assertEqual(configs[0].model, "wisenut/wise-lloa-ultra-v1.1.0")
        self.assertEqual(
            result["artifact"]["summary"]["model"], "wisenut/wise-lloa-ultra-v1.1.0"
        )

    def test_model_id_omitted_keeps_env_default(self) -> None:
        result, configs = self._run({})
        self.assertEqual(configs[0].model, "wisenut/wise-lloa-max-v1.2.1")
        self.assertEqual(
            result["artifact"]["summary"]["model"], "wisenut/wise-lloa-max-v1.2.1"
        )


if __name__ == "__main__":
    unittest.main()
