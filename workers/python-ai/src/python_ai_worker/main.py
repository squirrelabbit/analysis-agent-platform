from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .config import load_config
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
        "openai_embedding_model": config.openai_embedding_model,
        "openai_embedding_dimensions": config.openai_embedding_dimensions,
        "local_embedding_model": config.local_embedding_model,
        "skill_bundle_version": bundle_version(),
        "capabilities": capability_names(),
    }
    print(json.dumps(payload, ensure_ascii=False))


def make_handler(config: Any) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
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
                        "openai_embedding_model": config.openai_embedding_model,
                        "openai_embedding_dimensions": config.openai_embedding_dimensions,
                        "local_embedding_model": config.local_embedding_model,
                        "skill_bundle_version": bundle_version(),
                        "capabilities": capability_names(),
                    },
                )
                return
            if self.path == "/capabilities":
                self._write_json(HTTPStatus.OK, capability_payload())
                return
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def do_POST(self) -> None:
            prefix = "/tasks/"
            if not self.path.startswith(prefix):
                self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return

            size = int(self.headers.get("Content-Length") or "0")
            raw_body = self.rfile.read(size)
            try:
                payload = json.loads(raw_body or b"{}")
                response = run_task(self.path[len(prefix) :], payload)
            except ValueError as exc:
                self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except Exception as exc:  # pragma: no cover
                self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
                return

            self._write_json(HTTPStatus.OK, response)

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _write_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def serve() -> None:
    config = load_config()
    server = ThreadingHTTPServer((config.host, config.port), make_handler(config))
    print(
        json.dumps(
            {
                "status": "listening",
                "host": config.host,
                "port": config.port,
                "llm_provider": config.llm_provider,
                "anthropic_model": config.anthropic_model,
                "anthropic_prepare_model": config.anthropic_prepare_model,
                "openai_embedding_model": config.openai_embedding_model,
                "openai_embedding_dimensions": config.openai_embedding_dimensions,
                "local_embedding_model": config.local_embedding_model,
                "skill_bundle_version": bundle_version(),
                "capabilities": capability_names(),
            },
            ensure_ascii=False,
        )
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
