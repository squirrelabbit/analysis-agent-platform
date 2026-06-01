"""plan_and_execute_analyze / plan_from_question — 6단계 service 검증.

silverone 2026-05-21 6단계 — LLM planner + executor end-to-end. anthropic은 fake
client으로 주입해 committed fixture에서 결정적으로 실행되는지 확인.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any

from python_ai_worker.executor import (
    ArtifactPaths,
    plan_and_execute_analyze,
    plan_from_question,
)


_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "plan_v2_smoke"


def _fixture_paths() -> ArtifactPaths:
    return ArtifactPaths(
        docs=_FIXTURE_DIR / "cleaned.parquet",
        clauses=_FIXTURE_DIR / "clause_label.jsonl",
        genuineness=_FIXTURE_DIR / "doc_genuineness.jsonl",
    )


def _committed_plan() -> dict[str, Any]:
    return json.loads((_FIXTURE_DIR / "aspect_delta_plan.json").read_text(encoding="utf-8"))


class _FakeDecision:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = body
        self.usage = {"input_tokens": 12, "output_tokens": 34}


class _FakeClient:
    def __init__(self, plan_obj: dict[str, Any]) -> None:
        self._plan_obj = plan_obj
        self._config = type("Config", (), {"model": "fake-model"})()
        self.call_count = 0

    def is_enabled(self) -> bool:
        return True

    def create_json_response(self, **kwargs: Any) -> _FakeDecision:
        self.call_count += 1
        return _FakeDecision(body={"plan_json": json.dumps(self._plan_obj, ensure_ascii=False)})


class PlanV2FromQuestionTests(unittest.TestCase):
    def test_returns_plan_and_planner_metadata(self) -> None:
        client = _FakeClient(_committed_plan())
        result = plan_from_question(
            "smoke-6",
            "작년과 올해의 aspect 증감수치 계산해줘",
            anthropic_client=client,
        )
        self.assertEqual(result["dataset_version_id"], "smoke-6")
        self.assertEqual(result["plan_version"], "v2")
        self.assertEqual(len(result["plan"]["steps"]), 8)
        self.assertEqual(result["planner"]["prompt_version"], "planner-v2-anthropic-v1")
        self.assertEqual(result["planner"]["usage"]["input_tokens"], 12)
        self.assertEqual(client.call_count, 1)


class PlanAndExecuteTests(unittest.TestCase):
    def test_end_to_end_runs_committed_fixture(self) -> None:
        client = _FakeClient(_committed_plan())
        result = plan_and_execute_analyze(
            "smoke-6",
            "작년과 올해의 aspect 증감수치 계산해줘",
            artifact_paths=_fixture_paths(),
            anthropic_client=client,
        )
        self.assertEqual(result["plan_version"], "v2")
        self.assertEqual(len(result["steps"]), 8)
        self.assertEqual(result["present"]["row_count"], 3)
        # planner metadata가 응답에 함께
        self.assertIn("planner", result)
        self.assertEqual(result["planner"]["prompt_version"], "planner-v2-anthropic-v1")
        self.assertEqual(result["planner"]["usage"]["output_tokens"], 34)
        self.assertEqual(client.call_count, 1)

        # 핵심 결과 검증 (5-A와 같은 값)
        by_aspect = {row["aspect"]: row for row in result["present"]["rows"]}
        self.assertEqual(by_aspect["ambiance_scenery"]["delta_count"], 1)
        self.assertEqual(by_aspect["food"]["delta_count"], -1)
        self.assertEqual(by_aspect["show_program"]["delta_count"], 1)
        self.assertIsNone(by_aspect["show_program"]["delta_rate"])


if __name__ == "__main__":
    unittest.main()
