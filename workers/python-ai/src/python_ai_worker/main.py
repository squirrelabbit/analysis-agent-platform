from __future__ import annotations

import argparse
import json
import os
import platform
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .config import load_config
from .obs import bind_request_context, clear_request_context, get, init
from .runtime.rule_config import rule_config_status
from .skill_policy_registry import skill_policy_status, validate_skill_policies
from .skill_bundle import bundle_version
from .task_router import capability_names, capability_payload, run_task


def describe() -> None:
    config = load_config()
    payload = {
        "role": config.role,
        "queue": config.queue,
        "host": config.host,
        "port": config.port,
        "llm_provider": config.llm_provider,
        "anthropic_model": config.anthropic_model,
        "anthropic_prepare_model": config.anthropic_prepare_model,
        "anthropic_sentiment_model": config.anthropic_sentiment_model,
        "openai_embedding_model": config.openai_embedding_model,
        "openai_embedding_dimensions": config.openai_embedding_dimensions,
        "local_embedding_model": config.local_embedding_model,
        "rule_config": rule_config_status(),
        "skill_policy": skill_policy_status(),
        "skill_policy_validation": validate_skill_policies(),
        "skill_bundle_version": bundle_version(),
        "capabilities": capability_names(),
    }
    print(json.dumps(payload, ensure_ascii=False))


def make_handler(config: Any) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            clear_request_context()
            request_id = bind_request_context(self.headers)
            started_at = time.monotonic()
            self._request_started()

            if self.path == "/health":
                self._write_json(
                    HTTPStatus.OK,
                    {
                        "status": "ok",
                        "role": config.role,
                        "queue": config.queue,
                        "llm_provider": config.llm_provider,
                        "anthropic_model": config.anthropic_model,
                        "anthropic_prepare_model": config.anthropic_prepare_model,
                        "anthropic_sentiment_model": config.anthropic_sentiment_model,
                        "openai_embedding_model": config.openai_embedding_model,
                        "openai_embedding_dimensions": config.openai_embedding_dimensions,
                        "local_embedding_model": config.local_embedding_model,
                        "rule_config": rule_config_status(),
                        "skill_policy": skill_policy_status(),
                        "skill_policy_validation": validate_skill_policies(),
                        "skill_bundle_version": bundle_version(),
                        "capabilities": capability_names(),
                    },
                    request_id=request_id,
                    started_at=started_at,
                )
                return
            if self.path == "/capabilities":
                self._write_json(HTTPStatus.OK, capability_payload(), request_id=request_id, started_at=started_at)
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

            size = int(self.headers.get("Content-Length") or "0")
            raw_body = self.rfile.read(size)
            try:
                payload = json.loads(raw_body or b"{}")
                response = run_task(self.path[len(prefix):], payload)
            except ValueError as exc:
                self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)}, request_id=request_id, started_at=started_at)
                return
            except Exception as exc:  # pragma: no cover
                self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)}, request_id=request_id, started_at=started_at)
                return

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

        def _write_json(
            self,
            status: HTTPStatus,
            payload: dict[str, Any],
            *,
            request_id: str = "",
            started_at: float | None = None,
        ) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
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

    return Handler


def serve() -> None:
    init("python-ai-worker")
    logger = get(__name__)
    logger.info("service.boot.started", msg="python-ai-worker starting")

    config = load_config()
    server = ThreadingHTTPServer((config.host, config.port), make_handler(config))

    logger.info(
        "service.boot.completed",
        msg="listening",
        pid=os.getpid(),
        python_version=platform.python_version(),
        host=config.host,
        port=config.port,
        llm_provider=config.llm_provider,
        anthropic_model=config.anthropic_model,
        anthropic_prepare_model=config.anthropic_prepare_model,
        anthropic_sentiment_model=config.anthropic_sentiment_model,
        openai_embedding_model=config.openai_embedding_model,
        openai_embedding_dimensions=config.openai_embedding_dimensions,
        local_embedding_model=config.local_embedding_model,
        skill_bundle_version=bundle_version(),
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


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
