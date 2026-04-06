from __future__ import annotations

import json
import os
import re
from pathlib import Path

DEFAULT_PREPARE_PROMPT_VERSION = "dataset-prepare-anthropic-v1"
DEFAULT_PREPARE_BATCH_PROMPT_VERSION = "dataset-prepare-anthropic-batch-v1"
DEFAULT_SENTIMENT_PROMPT_VERSION = "sentiment-anthropic-v1"
PROMPTS_DIR_ENV = "PYTHON_AI_PROMPTS_DIR"


def _prompt_templates_dir() -> Path:
    override = os.getenv(PROMPTS_DIR_ENV, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[4] / "config" / "prompts"


def _available_prompt_versions() -> list[str]:
    templates_dir = _prompt_templates_dir()
    if not templates_dir.exists():
        return []
    return sorted(path.stem for path in templates_dir.glob("*.md") if path.is_file() and path.stem != "README")


def _load_prompt_template(version: str, kind: str) -> str:
    normalized_version = version.strip()
    template_path = _prompt_templates_dir() / f"{normalized_version}.md"
    if not template_path.is_file():
        available = ", ".join(_available_prompt_versions())
        raise ValueError(f"unsupported {kind} prompt version: {normalized_version} (available: {available})")
    return template_path.read_text(encoding="utf-8").strip()


def _render_template(template: str, replacements: dict[str, str], version: str) -> str:
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    unresolved = sorted(set(re.findall(r"{{\s*([a-zA-Z0-9_]+)\s*}}", rendered)))
    if unresolved:
        raise ValueError(f"prompt template {version} has unresolved placeholders: {', '.join(unresolved)}")
    return rendered


def render_prepare_prompt(raw_text: str, version: str = "") -> tuple[str, str]:
    prompt_version = version.strip() or DEFAULT_PREPARE_PROMPT_VERSION
    template = _load_prompt_template(prompt_version, "prepare")
    prompt = _render_template(template, {"raw_text": raw_text}, prompt_version)
    return prompt_version, prompt


def render_prepare_batch_prompt(raw_texts: list[str], version: str = "") -> tuple[str, str]:
    prompt_version = version.strip() or DEFAULT_PREPARE_BATCH_PROMPT_VERSION
    template = _load_prompt_template(prompt_version, "prepare batch")
    rows_json = json.dumps(
        [{"row_index": index, "raw_text": raw_text} for index, raw_text in enumerate(raw_texts)],
        ensure_ascii=False,
    )
    prompt = _render_template(template, {"rows_json": rows_json}, prompt_version)
    return prompt_version, prompt


def render_sentiment_prompt(text: str, version: str = "") -> tuple[str, str]:
    prompt_version = version.strip() or DEFAULT_SENTIMENT_PROMPT_VERSION
    template = _load_prompt_template(prompt_version, "sentiment")
    prompt = _render_template(template, {"text": text}, prompt_version)
    return prompt_version, prompt


__all__ = [
    "DEFAULT_PREPARE_BATCH_PROMPT_VERSION",
    "DEFAULT_PREPARE_PROMPT_VERSION",
    "DEFAULT_SENTIMENT_PROMPT_VERSION",
    "PROMPTS_DIR_ENV",
    "render_prepare_batch_prompt",
    "render_prepare_prompt",
    "render_sentiment_prompt",
]
