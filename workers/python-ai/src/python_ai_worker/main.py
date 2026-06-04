from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .config import load_config
from .obs import bind_request_context, clear_request_context, get, init
from .runtime.rule_config import rule_config_status
from .task_router import capability_names, capability_payload, run_task

# silverone 2026-06-04 — worker 운영 안정화 (Codex review #2).
# 무제한 Content-Length / 무제한 동시 스레드 / readiness 미분리 / graceful
# shutdown 부재를 제한 장치로 방어한다. FastAPI 이전 없이 현 ThreadingHTTPServer에
# ceiling만 얹는 1차 조치.
_DEFAULT_MAX_REQUEST_BYTES = 10 * 1024 * 1024  # 10MB
_DEFAULT_MAX_CONCURRENT_REQUESTS = 32
_DEFAULT_SHUTDOWN_GRACE_SEC = 10
_MAX_REQUEST_BYTES_ENV = "PYTHON_AI_MAX_REQUEST_BYTES"
_MAX_CONCURRENT_REQUESTS_ENV = "PYTHON_AI_MAX_CONCURRENT_REQUESTS"
_SHUTDOWN_GRACE_SEC_ENV = "PYTHON_AI_SHUTDOWN_GRACE_SEC"


def _env_int(name: str, default: int) -> int:
    """env 정수 파싱. 미설정/빈값/invalid는 default. 음수도 그대로 반환
    (caller가 <=0을 '무제한'으로 해석)."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _readiness(config: Any) -> tuple[bool, dict[str, bool]]:
    """프로세스가 요청을 실제로 처리할 수 있는지 점검.

    /healthz(생존)와 분리. config 로딩 + LLM key(최소 1종) + planner prompt +
    taxonomy 로딩 가능 여부를 본다. 어느 하나라도 실패면 not_ready(503).
    """
    anthropic_key = bool(getattr(config, "anthropic_api_key", None) or "")
    lloa_key = bool(getattr(config, "lloa_api_key", None) or "")
    checks: dict[str, bool] = {
        "config": config is not None,
        "anthropic_key": anthropic_key,
        "lloa_key": lloa_key,
        "any_llm_key": anthropic_key or lloa_key,
    }
    try:
        from .registries.prompt import available_prompt_versions

        checks["planner_prompt"] = any(
            str(v).startswith("planner-v2") for v in available_prompt_versions()
        )
    except Exception:  # noqa: BLE001 — readiness probe는 raise하지 않는다
        checks["planner_prompt"] = False
    try:
        from .taxonomies import DEFAULT_TAXONOMY_ID, load_taxonomy

        load_taxonomy(DEFAULT_TAXONOMY_ID)
        checks["taxonomy"] = True
    except Exception:  # noqa: BLE001
        checks["taxonomy"] = False
    ready = (
        checks["config"]
        and checks["any_llm_key"]
        and checks["planner_prompt"]
        and checks["taxonomy"]
    )
    return ready, checks


def describe() -> None:
    config = load_config()
    payload = {
        "role": config.role,
        "queue": config.queue,
        "host": config.host,
        "port": config.port,
        "llm_provider": config.llm_provider,
        "anthropic_model": config.anthropic_model,
        "openai_embedding_model": config.openai_embedding_model,
        "openai_embedding_dimensions": config.openai_embedding_dimensions,
        "local_embedding_model": config.local_embedding_model,
        "rule_config": rule_config_status(),
        "capabilities": capability_names(),
    }
    print(json.dumps(payload, ensure_ascii=False))


class WorkerMetrics:
    """worker 최소 메트릭 (silverone 2026-06-04, metrics 1차). ThreadingHTTPServer는
    요청마다 스레드라 Lock으로 보호한다. Prometheus client 의존 없이 text exposition."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active = 0
        self._rejected_body_too_large = 0
        self._rejected_concurrency = 0

    def inc_active(self) -> None:
        with self._lock:
            self._active += 1

    def dec_active(self) -> None:
        with self._lock:
            self._active -= 1

    def inc_body_too_large(self) -> None:
        with self._lock:
            self._rejected_body_too_large += 1

    def inc_concurrency(self) -> None:
        with self._lock:
            self._rejected_concurrency += 1

    def render(self) -> str:
        with self._lock:
            active, body, conc = self._active, self._rejected_body_too_large, self._rejected_concurrency
        return "\n".join(
            [
                "# HELP python_worker_active_requests In-flight /tasks/ requests being processed.",
                "# TYPE python_worker_active_requests gauge",
                f"python_worker_active_requests {active}",
                "# HELP python_worker_rejected_body_too_large_total Requests rejected with 413 (Content-Length over limit).",
                "# TYPE python_worker_rejected_body_too_large_total counter",
                f"python_worker_rejected_body_too_large_total {body}",
                "# HELP python_worker_rejected_concurrency_total Requests rejected with 503 (max concurrency).",
                "# TYPE python_worker_rejected_concurrency_total counter",
                f"python_worker_rejected_concurrency_total {conc}",
            ]
        ) + "\n"


# 프로세스 전역 singleton — make_handler가 매 요청 새 Handler를 만들어도 카운터는 공유.
_WORKER_METRICS = WorkerMetrics()


def make_handler(
    config: Any,
    *,
    max_request_bytes: int = _DEFAULT_MAX_REQUEST_BYTES,
    semaphore: threading.BoundedSemaphore | None = None,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            clear_request_context()
            request_id = bind_request_context(self.headers)
            started_at = time.monotonic()
            self._request_started()

            if self.path in {"/health", "/healthz"}:
                self._write_json(
                    HTTPStatus.OK,
                    {
                        "status": "ok",
                        "role": config.role,
                        "queue": config.queue,
                        "llm_provider": config.llm_provider,
                        "anthropic_model": config.anthropic_model,
                        "openai_embedding_model": config.openai_embedding_model,
                        "openai_embedding_dimensions": config.openai_embedding_dimensions,
                        "local_embedding_model": config.local_embedding_model,
                        "rule_config": rule_config_status(),
                        "capabilities": capability_names(),
                    },
                    request_id=request_id,
                    started_at=started_at,
                )
                return
            if self.path in {"/ready", "/readyz"}:
                ready, checks = _readiness(config)
                self._write_json(
                    HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE,
                    {"status": "ready" if ready else "not_ready", "checks": checks},
                    request_id=request_id,
                    started_at=started_at,
                )
                return
            if self.path == "/capabilities":
                self._write_json(HTTPStatus.OK, capability_payload(), request_id=request_id, started_at=started_at)
                return
            if self.path == "/metrics":
                self._write_text(
                    HTTPStatus.OK,
                    _WORKER_METRICS.render(),
                    request_id=request_id,
                    started_at=started_at,
                    content_type="text/plain; version=0.0.4; charset=utf-8",
                )
                return
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"}, request_id=request_id, started_at=started_at)

        def do_POST(self) -> None:
            clear_request_context()
            request_id = bind_request_context(self.headers)
            started_at = time.monotonic()
            self._request_started()

            prefix = "/tasks/"
            if not self.path.startswith(prefix):
                self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"}, request_id=request_id, started_at=started_at)
                return

            # Content-Length 파싱 + 상한. 초과/invalid는 body를 읽지 않고 거절하고
            # connection을 닫아 body-desync를 피한다.
            try:
                size = int(self.headers.get("Content-Length") or "0")
            except ValueError:
                size = -1
            if size < 0:
                self.close_connection = True
                self._write_json(HTTPStatus.BAD_REQUEST, {"error": "invalid Content-Length"}, request_id=request_id, started_at=started_at)
                return
            if max_request_bytes > 0 and size > max_request_bytes:
                _WORKER_METRICS.inc_body_too_large()
                self.close_connection = True
                self._write_json(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    {"error": f"request body {size} bytes exceeds limit {max_request_bytes}"},
                    request_id=request_id,
                    started_at=started_at,
                )
                return

            # 동시 처리 ceiling. 초과 시 load shedding(503 + Retry-After).
            acquired = semaphore.acquire(blocking=False) if semaphore is not None else True
            if not acquired:
                _WORKER_METRICS.inc_concurrency()
                self._write_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"error": "worker at max concurrency, retry later"},
                    request_id=request_id,
                    started_at=started_at,
                    extra_headers={"Retry-After": "1"},
                )
                return
            _WORKER_METRICS.inc_active()
            try:
                raw_body = self.rfile.read(size)
                payload = json.loads(raw_body or b"{}")
                response = run_task(self.path[len(prefix):], payload)
            except ValueError as exc:
                self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)}, request_id=request_id, started_at=started_at)
                return
            except Exception as exc:  # pragma: no cover
                self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)}, request_id=request_id, started_at=started_at)
                return
            finally:
                _WORKER_METRICS.dec_active()
                if semaphore is not None and acquired:
                    semaphore.release()

            self._write_json(HTTPStatus.OK, response, request_id=request_id, started_at=started_at)

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _request_started(self) -> None:
            get("http").info(
                "http.request.started",
                method=self.command,
                path=self.path,
                remote_ip=self.client_address[0] if self.client_address else "",
            )

        def _write_text(
            self,
            status: HTTPStatus,
            text: str,
            *,
            request_id: str = "",
            started_at: float | None = None,
            content_type: str = "text/plain; charset=utf-8",
        ) -> None:
            body = text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if request_id:
                self.send_header("X-Request-ID", request_id)
            self.end_headers()
            self.wfile.write(body)
            if started_at is not None:
                get("http").info(
                    "http.request.completed",
                    method=self.command,
                    path=self.path,
                    status=int(status),
                    duration_ms=int((time.monotonic() - started_at) * 1000),
                    response_size=len(body),
                )

        def _write_json(
            self,
            status: HTTPStatus,
            payload: dict[str, Any],
            *,
            request_id: str = "",
            started_at: float | None = None,
            extra_headers: dict[str, str] | None = None,
        ) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            if request_id:
                self.send_header("X-Request-ID", request_id)
            for key, value in (extra_headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)
            if started_at is not None:
                get("http").info(
                    "http.request.completed",
                    method=self.command,
                    path=self.path,
                    status=int(status),
                    duration_ms=int((time.monotonic() - started_at) * 1000),
                    response_size=len(body),
                )

    return Handler


def serve() -> None:
    init("python-ai-worker")
    logger = get(__name__)
    logger.info("service.boot.started", msg="python-ai-worker starting")

    config = load_config()

    max_request_bytes = _env_int(_MAX_REQUEST_BYTES_ENV, _DEFAULT_MAX_REQUEST_BYTES)
    max_concurrent = _env_int(_MAX_CONCURRENT_REQUESTS_ENV, _DEFAULT_MAX_CONCURRENT_REQUESTS)
    semaphore = threading.BoundedSemaphore(max_concurrent) if max_concurrent > 0 else None

    server = ThreadingHTTPServer(
        (config.host, config.port),
        make_handler(config, max_request_bytes=max_request_bytes, semaphore=semaphore),
    )

    logger.info(
        "service.boot.completed",
        msg="listening",
        pid=os.getpid(),
        python_version=platform.python_version(),
        host=config.host,
        port=config.port,
        max_request_bytes=max_request_bytes,
        max_concurrent_requests=max_concurrent if max_concurrent > 0 else "unlimited",
        llm_provider=config.llm_provider,
        anthropic_model=config.anthropic_model,
        openai_embedding_model=config.openai_embedding_model,
        openai_embedding_dimensions=config.openai_embedding_dimensions,
        local_embedding_model=config.local_embedding_model,
    )

    # SIGTERM/SIGINT graceful shutdown — 새 accept 중단 후 in-flight 요청에
    # grace 시간을 준다 (orchestrator가 SIGKILL 보내기 전 정리).
    shutdown_by_signal = {"value": False}

    def _graceful(signum: int, _frame: Any) -> None:
        shutdown_by_signal["value"] = True
        logger.info("service.shutdown.signal", signal_num=int(signum))
        # serve_forever 루프 안에서 shutdown()을 호출하면 deadlock → 별도 스레드.
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if shutdown_by_signal["value"]:
            grace = _env_int(_SHUTDOWN_GRACE_SEC_ENV, _DEFAULT_SHUTDOWN_GRACE_SEC)
            if grace > 0:
                logger.info("service.shutdown.draining", grace_sec=grace)
                time.sleep(grace)
        server.server_close()
        logger.info("service.shutdown.completed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Print worker metadata and exit.",
    )
    args = parser.parse_args()

    if args.describe:
        describe()
        return
    serve()


if __name__ == "__main__":
    main()
