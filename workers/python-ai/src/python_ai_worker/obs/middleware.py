"""Request-context binding for the Python AI worker's BaseHTTPRequestHandler server.

Usage in each request handler:
    clear_request_context()          # reset any inherited context from thread pool
    rid = bind_request_context(self.headers)
    self.send_header("X-Request-ID", rid)
"""
from __future__ import annotations

import uuid
from http.client import HTTPMessage
from typing import Union

import structlog.contextvars

from .logger import service_name


def bind_request_context(headers: Union[HTTPMessage, dict]) -> str:
    """Extract X-Request-ID from headers (or generate a UUID4) and bind to structlog context.

    Returns the request ID so callers can echo it in the response.
    """
    if isinstance(headers, dict):
        request_id = headers.get("X-Request-ID", "") or headers.get("x-request-id", "")
    else:
        request_id = headers.get("X-Request-ID") or ""
    request_id = request_id.strip()
    if not request_id:
        request_id = str(uuid.uuid4())
    service = service_name()
    if service:
        structlog.contextvars.bind_contextvars(service=service, request_id=request_id)
    else:
        structlog.contextvars.bind_contextvars(request_id=request_id)
    return request_id


def clear_request_context() -> None:
    """Clear all per-request context vars (call at the start of each request)."""
    structlog.contextvars.clear_contextvars()
