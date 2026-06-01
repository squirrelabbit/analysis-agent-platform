"""task_router canonical task name + legacy alias 잠금 (silverone 2026-06-01).

rename PR A 결정:
- canonical task name: `analyze` / `plan`
- backward-compatible alias: `analyze_v2` / `plan_v2`
- 둘 다 같은 handler를 호출
- /health capability 목록에는 canonical만 노출 (alias 은닉)
- alias 호출 시 obs warning 1줄
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from python_ai_worker import task_router


class CanonicalCapabilityListTests(unittest.TestCase):
    def test_capabilities_expose_canonical_names(self) -> None:
        names = set(task_router.capability_names())
        self.assertIn("analyze", names)
        self.assertIn("plan", names)

    def test_capabilities_do_not_expose_legacy_alias(self) -> None:
        # alias는 dispatch에서만 받아주고 capability 광고에는 노출하지 않는다.
        names = set(task_router.capability_names())
        self.assertNotIn("analyze_v2", names)
        self.assertNotIn("plan_v2", names)


class CanonicalAndAliasDispatchTests(unittest.TestCase):
    """run_task가 canonical / alias 양쪽을 같은 handler로 라우팅하는지 잠금.

    실제 _run_analyze / _run_plan은 LLM/DuckDB 의존이라 unit test에서 직접
    호출 안 함. patch로 대체해서 어떤 name이 들어와도 같은 mock이 호출되는지만
    확인.
    """

    def test_analyze_canonical_calls_handler(self) -> None:
        with patch.object(task_router, "_run_analyze", return_value={"ok": True}) as mock:
            result = task_router.run_task("analyze", {"dataset_version_id": "v"})
        self.assertEqual(result, {"ok": True})
        mock.assert_called_once_with({"dataset_version_id": "v"})

    def test_analyze_legacy_alias_routes_to_canonical_handler(self) -> None:
        with patch.object(task_router, "_run_analyze", return_value={"ok": True}) as mock:
            result = task_router.run_task("analyze_v2", {"dataset_version_id": "v"})
        self.assertEqual(result, {"ok": True})
        mock.assert_called_once_with({"dataset_version_id": "v"})

    def test_plan_canonical_calls_handler(self) -> None:
        with patch.object(task_router, "_run_plan", return_value={"plan": {}}) as mock:
            result = task_router.run_task("plan", {"dataset_version_id": "v", "user_question": "q"})
        self.assertEqual(result, {"plan": {}})
        mock.assert_called_once()

    def test_plan_legacy_alias_routes_to_canonical_handler(self) -> None:
        with patch.object(task_router, "_run_plan", return_value={"plan": {}}) as mock:
            result = task_router.run_task("plan_v2", {"dataset_version_id": "v", "user_question": "q"})
        self.assertEqual(result, {"plan": {}})
        mock.assert_called_once()

    def test_alias_emits_legacy_warning(self) -> None:
        """alias 호출 시 task.dispatch.legacy_alias warning이 남아야 한다."""
        with patch.object(task_router, "_run_analyze", return_value={}):
            with patch.object(task_router._LOG, "warning") as warn:
                task_router.run_task("analyze_v2", {"dataset_version_id": "v"})
        self.assertTrue(warn.called)
        event = warn.call_args.args[0]
        self.assertEqual(event, "task.dispatch.legacy_alias")
        kwargs = warn.call_args.kwargs
        self.assertEqual(kwargs.get("requested"), "analyze_v2")
        self.assertEqual(kwargs.get("canonical"), "analyze")

    def test_canonical_does_not_emit_legacy_warning(self) -> None:
        with patch.object(task_router, "_run_analyze", return_value={}):
            with patch.object(task_router._LOG, "warning") as warn:
                task_router.run_task("analyze", {"dataset_version_id": "v"})
        # canonical 호출은 legacy warning을 남기지 않아야.
        for call in warn.call_args_list:
            self.assertNotEqual(call.args[0], "task.dispatch.legacy_alias")

    def test_unknown_task_still_raises(self) -> None:
        with self.assertRaises(ValueError):
            task_router.run_task("nope_task", {})


if __name__ == "__main__":
    unittest.main()
