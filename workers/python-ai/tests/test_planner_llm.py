"""planner.llm tests — mock anthropic client로 generate_plan 흐름 검증.

silverone 2026-05-21 6단계 결정:
- parse 실패 시 1회 repair retry
- validator 실패 시 1회 self-correct retry
- 둘 다 실패하면 PlannerParseError / PlannerValidationError
"""

from __future__ import annotations

import json
import unittest
from typing import Any

from python_ai_worker.planner import (
    DatasetSpecificColumn,
    PlannerCallError,
    PlannerParseError,
    PlannerValidationError,
    generate_plan,
)


def _good_plan() -> dict[str, Any]:
    return {
        "plan_version": "v2",
        "steps": [
            {
                "id": "positive_clauses",
                "skill": "filter",
                "params": {"input": "clauses", "column": "sentiment", "operator": "eq", "value": "positive"},
            },
            {
                "id": "out",
                "skill": "present",
                "params": {"input": "positive_clauses", "format": "table", "title": "긍정 절"},
            },
        ],
    }


def _bad_plan_invalid_skill() -> dict[str, Any]:
    plan = _good_plan()
    plan["steps"][0]["skill"] = "wave_hands"
    return plan


class _FakeDecision:
    """anthropic.create_json_response 응답 stand-in.

    silverone 2026-05-26 (cost-opt) — usage dict에 ``cache_creation_input_tokens`` /
    ``cache_read_input_tokens`` 도 노출 가능."""

    def __init__(self, body: dict[str, Any], usage: dict[str, int] | None = None) -> None:
        self.body = body
        self.usage = dict(usage or {"input_tokens": 10, "output_tokens": 5})


class _FakeClient:
    """순차적으로 미리 준비한 response를 돌려주는 fake anthropic client.

    silverone 2026-05-26 (cost-opt) — system / cache_system 인자도 호출별로
    capture해서 잠금에 사용한다. 각 응답에 별도 usage를 지정하고 싶으면
    ``responses``에 ``(body_dict, usage_dict)`` 튜플을 넣어도 된다.
    """

    def __init__(self, responses: list[Any]) -> None:
        # responses 항목은 (a) plain dict (= body) 또는 (b) (body, usage) tuple.
        normalized: list[tuple[dict[str, Any], dict[str, int] | None]] = []
        for item in responses:
            if isinstance(item, tuple):
                normalized.append((item[0], item[1] if len(item) > 1 else None))
            else:
                normalized.append((item, None))
        self._responses = normalized
        self.calls: list[str] = []
        self.invocations: list[dict[str, Any]] = []
        self._config = type("Config", (), {"model": "fake-model"})()

    def is_enabled(self) -> bool:
        return True

    def create_json_response(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        max_tokens: int | None = None,
        system: str = "",
        cache_system: bool = False,
    ) -> _FakeDecision:
        self.calls.append(prompt)
        self.invocations.append({
            "prompt": prompt,
            "system": system,
            "cache_system": cache_system,
            "max_tokens": max_tokens,
        })
        if not self._responses:
            raise AssertionError("FakeClient exhausted")
        body, usage = self._responses.pop(0)
        return _FakeDecision(body=body, usage=usage)


def _plan_body(plan: dict[str, Any]) -> dict[str, Any]:
    return {"plan_json": json.dumps(plan, ensure_ascii=False)}


class HappyPathTests(unittest.TestCase):
    def test_single_call_returns_valid_plan(self) -> None:
        client = _FakeClient([_plan_body(_good_plan())])
        result = generate_plan(
            user_question="긍정 절을 보여줘",
            anthropic_client=client,
        )
        self.assertEqual(result.plan["plan_version"], "v2")
        self.assertEqual(len(result.plan["steps"]), 2)
        self.assertEqual(result.prompt_version, "planner-v2-anthropic-v1")
        self.assertEqual(len(result.attempts), 1)
        self.assertEqual(result.attempts[0]["phase"], "initial")
        self.assertEqual(result.attempts[0]["parsed"], True)
        self.assertEqual(result.attempts[0]["validation_issues"], [])
        self.assertEqual(result.usage["input_tokens"], 10)
        self.assertEqual(result.usage["output_tokens"], 5)
        self.assertEqual(client.calls and len(client.calls), 1)

    def test_docs_extra_columns_passed_to_user_prompt(self) -> None:
        # silverone 2026-05-26 (cost-opt) — docs_extra_columns는 dataset마다
        # 달라지므로 user_prompt에 들어가야 한다. system은 cache 영역이므로
        # 깨지면 안 됨.
        client = _FakeClient([_plan_body(_good_plan())])
        generate_plan(
            user_question="채널별 분포",
            anthropic_client=client,
            docs_extra_columns=[DatasetSpecificColumn(name="channel", type="string", description="유입 채널")],
        )
        invocation = client.invocations[0]
        self.assertIn("`channel`", invocation["prompt"])
        self.assertNotIn("`channel`", invocation["system"])

    def test_system_prompt_passed_with_cache_system_true(self) -> None:
        # silverone 2026-05-26 (cost-opt) — planner는 system_prompt를 Anthropic
        # prompt cache의 ephemeral 블록으로 보내야 cache hit가 발생한다.
        # cache_system=True가 client에 전달되는지를 잠근다.
        client = _FakeClient([_plan_body(_good_plan())])
        generate_plan(user_question="dummy", anthropic_client=client)
        invocation = client.invocations[0]
        self.assertTrue(invocation["cache_system"], "cache_system must be True for planner calls")
        self.assertGreater(len(invocation["system"]), 500, "system_prompt가 비어 있으면 cache 영역이 깨졌다는 신호")
        # standard table + skill catalog + examples는 system 안에 있어야 함
        self.assertIn("### docs", invocation["system"])
        self.assertIn("### join", invocation["system"])
        self.assertIn("### 예시 1", invocation["system"])

    def test_usage_includes_cache_token_keys(self) -> None:
        client = _FakeClient([
            (
                _plan_body(_good_plan()),
                {
                    "input_tokens": 100,
                    "output_tokens": 20,
                    "cache_creation_input_tokens": 1500,
                    "cache_read_input_tokens": 0,
                },
            ),
        ])
        result = generate_plan(user_question="dummy", anthropic_client=client)
        for key in (
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        ):
            with self.subTest(key=key):
                self.assertIn(key, result.usage)
        self.assertEqual(result.usage["cache_creation_input_tokens"], 1500)
        self.assertEqual(result.usage["cache_read_input_tokens"], 0)

    def test_usage_accumulates_cache_tokens_across_retries(self) -> None:
        # parse retry 케이스에서 두 호출의 cache 토큰이 누적되는지 잠금.
        client = _FakeClient([
            (
                {"plan_json": "not-json"},
                {
                    "input_tokens": 50,
                    "output_tokens": 5,
                    "cache_creation_input_tokens": 1500,
                    "cache_read_input_tokens": 0,
                },
            ),
            (
                _plan_body(_good_plan()),
                {
                    "input_tokens": 80,
                    "output_tokens": 30,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 1500,
                },
            ),
        ])
        result = generate_plan(user_question="dummy", anthropic_client=client)
        self.assertEqual(result.usage["cache_creation_input_tokens"], 1500)
        # 두 번째 호출이 cache hit — 누적도 1500.
        self.assertEqual(result.usage["cache_read_input_tokens"], 1500)
        self.assertEqual(result.usage["input_tokens"], 130)
        self.assertEqual(result.usage["output_tokens"], 35)


class ParseRetryTests(unittest.TestCase):
    def test_parse_retry_succeeds(self) -> None:
        client = _FakeClient(
            [
                {"plan_json": "not-json-at-all"},
                _plan_body(_good_plan()),
            ]
        )
        result = generate_plan(user_question="dummy", anthropic_client=client)
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(len(result.attempts), 2)
        self.assertEqual(result.attempts[0]["phase"], "initial")
        self.assertEqual(result.attempts[0]["parsed"], False)
        self.assertEqual(result.attempts[1]["phase"], "parse_repair")
        self.assertEqual(result.attempts[1]["parsed"], True)
        # usage 누적
        self.assertEqual(result.usage["input_tokens"], 20)
        self.assertEqual(result.usage["output_tokens"], 10)

    def test_parse_retry_repair_prompt_includes_error_and_raw(self) -> None:
        client = _FakeClient(
            [
                {"plan_json": "bad"},
                _plan_body(_good_plan()),
            ]
        )
        generate_plan(user_question="dummy", anthropic_client=client)
        repair_prompt = client.calls[1]
        self.assertIn("이전 응답 (JSON parse 실패)", repair_prompt)
        self.assertIn("bad", repair_prompt)
        self.assertIn("raw JSON만 출력", repair_prompt)

    def test_parse_retry_fails_raises(self) -> None:
        client = _FakeClient(
            [
                {"plan_json": "not-json"},
                {"plan_json": "still-not-json"},
            ]
        )
        with self.assertRaises(PlannerParseError) as ctx:
            generate_plan(user_question="dummy", anthropic_client=client)
        self.assertEqual(len(ctx.exception.attempts), 2)


class ValidatorRetryTests(unittest.TestCase):
    def test_validator_retry_succeeds(self) -> None:
        client = _FakeClient(
            [
                _plan_body(_bad_plan_invalid_skill()),
                _plan_body(_good_plan()),
            ]
        )
        result = generate_plan(user_question="dummy", anthropic_client=client)
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(len(result.attempts), 2)
        self.assertEqual(result.attempts[0]["phase"], "initial")
        self.assertTrue(len(result.attempts[0]["validation_issues"]) > 0)
        self.assertEqual(result.attempts[1]["phase"], "validator_repair")
        self.assertEqual(result.attempts[1]["validation_issues"], [])

    def test_validator_repair_prompt_includes_issues(self) -> None:
        client = _FakeClient(
            [
                _plan_body(_bad_plan_invalid_skill()),
                _plan_body(_good_plan()),
            ]
        )
        generate_plan(user_question="dummy", anthropic_client=client)
        repair_prompt = client.calls[1]
        self.assertIn("plan_v2 validator 위반", repair_prompt)
        self.assertIn("step.skill_unknown", repair_prompt)

    def test_validator_retry_still_bad_raises(self) -> None:
        client = _FakeClient(
            [
                _plan_body(_bad_plan_invalid_skill()),
                _plan_body(_bad_plan_invalid_skill()),
            ]
        )
        with self.assertRaises(PlannerValidationError) as ctx:
            generate_plan(user_question="dummy", anthropic_client=client)
        codes = {i.code for i in ctx.exception.issues}
        self.assertIn("step.skill_unknown", codes)

    def test_validator_retry_parse_fail_raises(self) -> None:
        client = _FakeClient(
            [
                _plan_body(_bad_plan_invalid_skill()),
                {"plan_json": "garbage"},
            ]
        )
        with self.assertRaises(PlannerValidationError):
            generate_plan(user_question="dummy", anthropic_client=client)


class GuardrailTests(unittest.TestCase):
    def test_empty_user_question_raises(self) -> None:
        client = _FakeClient([])
        with self.assertRaises(PlannerCallError):
            generate_plan(user_question="   ", anthropic_client=client)

    def test_disabled_client_raises(self) -> None:
        class DisabledClient(_FakeClient):
            def is_enabled(self) -> bool:
                return False

        with self.assertRaises(PlannerCallError):
            generate_plan(user_question="dummy", anthropic_client=DisabledClient([]))


if __name__ == "__main__":
    unittest.main()
