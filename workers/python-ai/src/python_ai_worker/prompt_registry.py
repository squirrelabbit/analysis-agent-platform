from __future__ import annotations

import json
import os
import re
from pathlib import Path

DEFAULT_PREPARE_PROMPT_VERSION = "dataset-prepare-anthropic-v1"
DEFAULT_PREPARE_BATCH_PROMPT_VERSION = "dataset-prepare-anthropic-batch-v1"
DEFAULT_SENTIMENT_PROMPT_VERSION = "sentiment-anthropic-v1"
DEFAULT_SENTIMENT_BATCH_PROMPT_VERSION = "sentiment-anthropic-batch-v1"
PROMPTS_DIR_ENV = "PYTHON_AI_PROMPTS_DIR"

_PROMPT_DEFAULT_GROUPS = {
    DEFAULT_PREPARE_PROMPT_VERSION: ["prepare"],
    DEFAULT_PREPARE_BATCH_PROMPT_VERSION: ["prepare_batch"],
    DEFAULT_SENTIMENT_PROMPT_VERSION: ["sentiment"],
    DEFAULT_SENTIMENT_BATCH_PROMPT_VERSION: ["sentiment_batch"],
}
_PROMPT_DIR_EXCLUDE = {"README", "CHANGELOG"}


def _prompt_templates_dir() -> Path:
    override = os.getenv(PROMPTS_DIR_ENV, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[4] / "config" / "prompts"


def _available_prompt_versions() -> list[str]:
    templates_dir = _prompt_templates_dir()
    if not templates_dir.exists():
        return []
    return sorted(
        path.stem
        for path in templates_dir.glob("*.md")
        if path.is_file() and path.stem not in _PROMPT_DIR_EXCLUDE
    )


def available_prompt_versions() -> list[str]:
    return _available_prompt_versions()


def _parse_front_matter(raw_text: str) -> tuple[dict[str, str], str]:
    content = raw_text.strip()
    if not content.startswith("---\n"):
        return {}, content
    lines = content.splitlines()
    metadata: dict[str, str] = {}
    closing_index = -1
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            closing_index = index
            break
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip()
    if closing_index < 0:
        return {}, content
    body = "\n".join(lines[closing_index + 1 :]).strip()
    return metadata, body


def _infer_prompt_operation(version: str) -> str:
    normalized = version.strip()
    if "prepare-anthropic-batch" in normalized:
        return "prepare_batch"
    if "prepare-anthropic" in normalized:
        return "prepare"
    if "sentiment-anthropic-batch" in normalized:
        return "sentiment_batch"
    if "sentiment-anthropic" in normalized:
        return "sentiment"
    return "custom"


def _prompt_default_groups(version: str) -> list[str]:
    return list(_PROMPT_DEFAULT_GROUPS.get(version, []))


def _resolve_prompt_version(version: str, *, batch: bool, operation: str) -> str:
    requested = version.strip()
    if not requested:
        if operation == "prepare":
            return DEFAULT_PREPARE_BATCH_PROMPT_VERSION if batch else DEFAULT_PREPARE_PROMPT_VERSION
        return DEFAULT_SENTIMENT_BATCH_PROMPT_VERSION if batch else DEFAULT_SENTIMENT_PROMPT_VERSION

    if batch and "-batch-" not in requested:
        candidate = requested.replace("-anthropic-", "-anthropic-batch-", 1)
        if candidate in _available_prompt_versions():
            return candidate
    if not batch and "-batch-" in requested:
        candidate = requested.replace("-anthropic-batch-", "-anthropic-", 1)
        if candidate in _available_prompt_versions():
            return candidate
    return requested


def prompt_catalog() -> list[dict[str, object]]:
    catalog: list[dict[str, object]] = []
    for version in _available_prompt_versions():
        template_path = _prompt_templates_dir() / f"{version}.md"
        metadata, _ = _parse_front_matter(template_path.read_text(encoding="utf-8"))
        title = str(metadata.get("title") or version).strip()
        operation = str(metadata.get("operation") or _infer_prompt_operation(version)).strip()
        status = str(metadata.get("status") or "active").strip()
        summary = str(metadata.get("summary") or "").strip()
        catalog.append(
            {
                "version": version,
                "title": title,
                "operation": operation,
                "status": status,
                "summary": summary,
                "default_groups": _prompt_default_groups(version),
            }
        )
    return catalog


def _load_prompt_template(version: str, kind: str) -> str:
    normalized_version = version.strip()
    template_path = _prompt_templates_dir() / f"{normalized_version}.md"
    if not template_path.is_file():
        available = ", ".join(_available_prompt_versions())
        raise ValueError(f"unsupported {kind} prompt version: {normalized_version} (available: {available})")
    _, body = _parse_front_matter(template_path.read_text(encoding="utf-8"))
    return body.strip()


def _render_template(template: str, replacements: dict[str, str], version: str) -> str:
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    unresolved = sorted(set(re.findall(r"{{\s*([a-zA-Z0-9_]+)\s*}}", rendered)))
    if unresolved:
        raise ValueError(f"prompt template {version} has unresolved placeholders: {', '.join(unresolved)}")
    return rendered


def render_prepare_prompt(raw_text: str, version: str = "") -> tuple[str, str]:
    prompt_version = _resolve_prompt_version(version, batch=False, operation="prepare")
    template = _load_prompt_template(prompt_version, "prepare")
    prompt = _render_template(template, {"raw_text": raw_text}, prompt_version)
    return prompt_version, prompt


def render_prepare_batch_prompt(raw_texts: list[str], version: str = "") -> tuple[str, str]:
    prompt_version = _resolve_prompt_version(version, batch=True, operation="prepare")
    template = _load_prompt_template(prompt_version, "prepare batch")
    rows_json = json.dumps(
        [{"row_index": index, "raw_text": raw_text} for index, raw_text in enumerate(raw_texts)],
        ensure_ascii=False,
    )
    prompt = _render_template(template, {"rows_json": rows_json}, prompt_version)
    return prompt_version, prompt


def render_sentiment_prompt(text: str, version: str = "") -> tuple[str, str]:
    prompt_version = _resolve_prompt_version(version, batch=False, operation="sentiment")
    template = _load_prompt_template(prompt_version, "sentiment")
    prompt = _render_template(template, {"text": text}, prompt_version)
    return prompt_version, prompt


def render_sentiment_batch_prompt(texts: list[str], version: str = "") -> tuple[str, str]:
    prompt_version = _resolve_prompt_version(version, batch=True, operation="sentiment")
    template = _load_prompt_template(prompt_version, "sentiment batch")
    rows_json = json.dumps(
        [{"row_index": index, "text": text} for index, text in enumerate(texts)],
        ensure_ascii=False,
    )
    prompt = _render_template(template, {"rows_json": rows_json}, prompt_version)
    return prompt_version, prompt


__all__ = [
    "DEFAULT_PREPARE_BATCH_PROMPT_VERSION",
    "DEFAULT_PREPARE_PROMPT_VERSION",
    "DEFAULT_SENTIMENT_BATCH_PROMPT_VERSION",
    "DEFAULT_SENTIMENT_PROMPT_VERSION",
    "PROMPTS_DIR_ENV",
    "available_prompt_versions",
    "prompt_catalog",
    "render_prepare_batch_prompt",
    "render_prepare_prompt",
    "render_sentiment_batch_prompt",
    "render_sentiment_prompt",
]
