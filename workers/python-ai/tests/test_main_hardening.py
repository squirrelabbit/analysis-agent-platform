"""silverone 2026-06-04 — worker 운영 안정화 (Codex review #2) 잠금.

Content-Length 상한(413) / 동시 처리 ceiling(503) / readiness 분리(/readyz) /
_env_int·_readiness 단위 동작을 검증한다. 실제 ThreadingHTTPServer를 랜덤
포트에 띄워 HTTP로 호출한다 (test_main_http.py 패턴).
"""
from __future__ import annotations

import json
import threading
import unittest
from http.client import HTTPConnection
from types import SimpleNamespace
from unittest.mock import patch

from python_ai_worker import main as worker_main


def _cfg(**over) -> SimpleNamespace:
    base = dict(
        role="worker",
        queue="q",
        llm_provider="anthropic",
        anthropic_model="claude-test",
        openai_embedding_model="m",
        openai_embedding_dimensions=1536,
        local_embedding_model="",
        host="127.0.0.1",
        port=0,
        anthropic_api_key="test-key",
        lloa_api_key=None,
    )
    base.update(over)
    return SimpleNamespace(**base)


class MainHardeningHTTPTests(unittest.TestCase):
    def _start(self, config=None, **handler_kwargs):
        config = config or _cfg()
        server = worker_main.ThreadingHTTPServer(
            ("127.0.0.1", 0),
            worker_main.make_handler(config, **handler_kwargs),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        # cleanup은 LIFO — shutdown → server_close → join 순으로 실행.
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def _post(self, server, path, body, headers=None):
        conn = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
        conn.request("POST", path, body=body, headers=headers or {"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = resp.read().decode("utf-8")
        status = resp.status
        retry_after = resp.getheader("Retry-After")
        conn.close()
        return status, data, retry_after

    def _get(self, server, path):
        conn = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
        conn.request("GET", path)
        resp = conn.getresponse()
        data = resp.read().decode("utf-8")
        status = resp.status
        conn.close()
        return status, data

    def test_body_over_limit_returns_413(self) -> None:
        server = self._start(max_request_bytes=100)
        big = json.dumps({"x": "a" * 500})
        # run_task는 호출되면 안 됨 (413으로 먼저 거절).
        with patch("python_ai_worker.main.run_task", side_effect=AssertionError("run_task should not run")):
            status, data, _ = self._post(server, "/tasks/analyze", big)
        self.assertEqual(status, 413)
        self.assertIn("exceeds limit", data)

    def test_body_under_limit_ok(self) -> None:
        server = self._start(max_request_bytes=10_000)
        with patch("python_ai_worker.main.run_task", return_value={"ok": True}):
            status, data, _ = self._post(server, "/tasks/analyze", json.dumps({"a": 1}))
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(data)["ok"])

    def test_concurrency_limit_returns_503(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def blocking_run_task(name, payload):
            started.set()
            release.wait(timeout=5)
            return {"ok": True}

        sem = threading.BoundedSemaphore(1)
        server = self._start(semaphore=sem)

        with patch("python_ai_worker.main.run_task", side_effect=blocking_run_task):
            holder_result: dict = {}

            def hold():
                holder_result["status"], _, _ = self._post(
                    server, "/tasks/analyze", json.dumps({"a": 1})
                )

            holder = threading.Thread(target=hold)
            holder.start()
            # 첫 요청이 permit을 잡고 run_task에 진입할 때까지 대기.
            self.assertTrue(started.wait(timeout=5), "first request did not enter run_task")

            # 두 번째 요청은 permit 없음 → 즉시 503.
            status2, data2, retry_after = self._post(server, "/tasks/analyze", json.dumps({"a": 2}))
            self.assertEqual(status2, 503)
            self.assertEqual(retry_after, "1")
            self.assertIn("max concurrency", data2)

            release.set()
            holder.join(timeout=5)
        self.assertEqual(holder_result.get("status"), 200)

    def test_readyz_ready_when_all_checks_pass(self) -> None:
        server = self._start(_cfg(anthropic_api_key="k"))
        with patch("python_ai_worker.registries.prompt.available_prompt_versions", return_value=["planner-v2-anthropic-v1"]), \
             patch("python_ai_worker.taxonomies.load_taxonomy", return_value=object()):
            status, data = self._get(server, "/readyz")
        self.assertEqual(status, 200)
        body = json.loads(data)
        self.assertEqual(body["status"], "ready")
        self.assertTrue(body["checks"]["any_llm_key"])

    @classmethod
    def _metric_value_or_zero(cls, metrics_text: str, name: str) -> int:
        try:
            return cls._metric_value(metrics_text, name)
        except AssertionError:
            return 0

    def test_metrics_request_counters_by_task_status(self) -> None:
        server = self._start()
        ok_key = 'python_worker_requests_total{task="analyze",status="ok"}'
        bad_key = 'python_worker_requests_total{task="analyze",status="bad_request"}'
        err_key = 'python_worker_requests_total{task="analyze",status="error"}'
        dur_key = 'python_worker_request_duration_ms_count{task="analyze"}'

        _, base = self._get(server, "/metrics")
        ok0 = self._metric_value_or_zero(base, ok_key)
        bad0 = self._metric_value_or_zero(base, bad_key)
        err0 = self._metric_value_or_zero(base, err_key)
        dur0 = self._metric_value_or_zero(base, dur_key)

        # 성공 → ok + duration count.
        with patch("python_ai_worker.main.run_task", return_value={"ok": True}):
            code, _, _ = self._post(server, "/tasks/analyze", json.dumps({"a": 1}))
        self.assertEqual(code, 200)
        # ValueError → 400 bad_request.
        with patch("python_ai_worker.main.run_task", side_effect=ValueError("bad input")):
            code, _, _ = self._post(server, "/tasks/analyze", json.dumps({"a": 1}))
        self.assertEqual(code, 400)
        # Exception → 500 error.
        with patch("python_ai_worker.main.run_task", side_effect=RuntimeError("boom")):
            code, _, _ = self._post(server, "/tasks/analyze", json.dumps({"a": 1}))
        self.assertEqual(code, 500)

        _, after = self._get(server, "/metrics")
        self.assertEqual(self._metric_value(after, ok_key), ok0 + 1)
        self.assertEqual(self._metric_value(after, bad_key), bad0 + 1)
        self.assertEqual(self._metric_value(after, err_key), err0 + 1)
        # duration count는 처리된 3건 모두 증가.
        self.assertEqual(self._metric_value(after, dur_key), dur0 + 3)
        # duration sum 라인도 노출돼야 한다(값은 가변).
        self.assertIn('python_worker_request_duration_ms_sum{task="analyze"}', after)

    def test_readyz_not_ready_without_llm_key(self) -> None:
        server = self._start(_cfg(anthropic_api_key=None, lloa_api_key=None))
        status, data = self._get(server, "/readyz")
        self.assertEqual(status, 503)
        body = json.loads(data)
        self.assertEqual(body["status"], "not_ready")
        self.assertFalse(body["checks"]["any_llm_key"])

    @staticmethod
    def _metric_value(metrics_text: str, name: str) -> int:
        for line in metrics_text.splitlines():
            if line.startswith(name + " "):
                return int(line.rsplit(" ", 1)[1])
        raise AssertionError(f"metric {name!r} not found in:\n{metrics_text}")

    def test_metrics_endpoint_exposes_worker_counters(self) -> None:
        # singleton 카운터는 프로세스 전역 → delta로 검증.
        server = self._start(max_request_bytes=100)
        status, body = self._get(server, "/metrics")
        self.assertEqual(status, 200)
        for name in (
            "python_worker_active_requests",
            "python_worker_rejected_body_too_large_total",
            "python_worker_rejected_concurrency_total",
        ):
            self.assertIn(name, body)
        self.assertIn("# TYPE python_worker_active_requests gauge", body)

        before = self._metric_value(body, "python_worker_rejected_body_too_large_total")
        code, _, _ = self._post(server, "/tasks/analyze", "a" * 200)  # >100 → 413
        self.assertEqual(code, 413)
        _, body2 = self._get(server, "/metrics")
        after = self._metric_value(body2, "python_worker_rejected_body_too_large_total")
        self.assertEqual(after, before + 1)

    def test_metrics_concurrency_counter_increments_on_503(self) -> None:
        import threading

        sem = threading.BoundedSemaphore(1)
        server = self._start(semaphore=sem)
        _, body0 = self._get(server, "/metrics")
        before = self._metric_value(body0, "python_worker_rejected_concurrency_total")

        started = threading.Event()
        release = threading.Event()

        def blocking_run_task(name, payload):
            started.set()
            release.wait(timeout=5)
            return {"ok": True}

        with patch("python_ai_worker.main.run_task", side_effect=blocking_run_task):
            holder = threading.Thread(
                target=lambda: self._post(server, "/tasks/analyze", json.dumps({"a": 1}))
            )
            holder.start()
            self.assertTrue(started.wait(timeout=5))
            code, _, _ = self._post(server, "/tasks/analyze", json.dumps({"a": 2}))
            self.assertEqual(code, 503)
            release.set()
            holder.join(timeout=5)

        _, body1 = self._get(server, "/metrics")
        after = self._metric_value(body1, "python_worker_rejected_concurrency_total")
        self.assertEqual(after, before + 1)


class ReadinessUnitTests(unittest.TestCase):
    def test_env_int_fallbacks(self) -> None:
        import os

        with patch.dict(os.environ, {"X_INT": "42"}):
            self.assertEqual(worker_main._env_int("X_INT", 7), 42)
        with patch.dict(os.environ, {"X_INT": ""}):
            self.assertEqual(worker_main._env_int("X_INT", 7), 7)
        with patch.dict(os.environ, {"X_INT": "abc"}):
            self.assertEqual(worker_main._env_int("X_INT", 7), 7)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("X_INT", None)
            self.assertEqual(worker_main._env_int("X_INT", 7), 7)

    def test_readiness_requires_any_llm_key(self) -> None:
        cfg = _cfg(anthropic_api_key=None, lloa_api_key=None)
        with patch("python_ai_worker.registries.prompt.available_prompt_versions", return_value=["planner-v2-anthropic-v1"]), \
             patch("python_ai_worker.taxonomies.load_taxonomy", return_value=object()):
            ready, checks = worker_main._readiness(cfg)
        self.assertFalse(ready)
        self.assertFalse(checks["any_llm_key"])
        self.assertTrue(checks["planner_prompt"])
        self.assertTrue(checks["taxonomy"])

    def test_readiness_handles_loader_failure(self) -> None:
        cfg = _cfg(anthropic_api_key="k")
        with patch("python_ai_worker.registries.prompt.available_prompt_versions", side_effect=RuntimeError("boom")), \
             patch("python_ai_worker.taxonomies.load_taxonomy", side_effect=RuntimeError("boom")):
            ready, checks = worker_main._readiness(cfg)
        self.assertFalse(ready)
        self.assertFalse(checks["planner_prompt"])
        self.assertFalse(checks["taxonomy"])


if __name__ == "__main__":
    unittest.main()
