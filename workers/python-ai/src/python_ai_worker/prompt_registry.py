from __future__ import annotations

import json
from typing import Callable, TypeVar

DEFAULT_PREPARE_PROMPT_VERSION = "dataset-prepare-anthropic-v1"
DEFAULT_PREPARE_BATCH_PROMPT_VERSION = "dataset-prepare-anthropic-batch-v1"
DEFAULT_SENTIMENT_PROMPT_VERSION = "sentiment-anthropic-v1"


def _prepare_row_prompt_v1(raw_text: str) -> str:
    return "\n".join(
        [
            "You are preparing raw VOC or issue text for deterministic downstream analysis.",
            "Keep the original meaning. Remove only obvious noise, duplicated punctuation, and boilerplate.",
            "Do not summarize beyond a short normalization. Do not invent facts.",
            "Choose disposition keep, review, or drop.",
            "Use drop only for empty, unreadable noise, or clear non-content rows.",
            "Use review when the text is partially readable but low quality or mixed.",
            "",
            f"raw_text: {raw_text}",
        ]
    )


def _prepare_row_prompt_v2(raw_text: str) -> str:
    return "\n".join(
        [
            "You are preparing raw VOC or issue text for deterministic downstream analysis.",
            "Preserve the original language, issue symptom, product name, and user intent.",
            "Normalize only obvious noise, duplicated punctuation, spacing, and boilerplate.",
            "Do not summarize, generalize, or remove key complaint details.",
            "Choose exactly one disposition: keep, review, or drop.",
            "Use drop only for empty rows, unreadable noise, or clear non-content.",
            "Use review when the content is partially readable, mixed, or quality is uncertain.",
            "",
            f"raw_text: {raw_text}",
        ]
    )


def _prepare_batch_prompt_v1(raw_texts: list[str]) -> str:
    return "\n".join(
        [
            "You are preparing raw VOC or issue text for deterministic downstream analysis.",
            "Process each row independently and preserve ordering.",
            "Keep the original meaning. Remove only obvious noise, duplicated punctuation, and boilerplate.",
            "Do not summarize beyond a short normalization. Do not invent facts.",
            "Choose disposition keep, review, or drop for each row.",
            "",
            "rows:",
            json.dumps(
                [{"row_index": index, "raw_text": raw_text} for index, raw_text in enumerate(raw_texts)],
                ensure_ascii=False,
            ),
        ]
    )


def _prepare_batch_prompt_v2(raw_texts: list[str]) -> str:
    return "\n".join(
        [
            "You are preparing raw VOC or issue text for deterministic downstream analysis.",
            "Process each row independently, preserve ordering, and preserve issue-specific details.",
            "Normalize only obvious noise, duplicated punctuation, spacing, and boilerplate.",
            "Do not summarize, merge rows, or infer missing context.",
            "Choose exactly one disposition keep, review, or drop for each row.",
            "",
            "rows:",
            json.dumps(
                [{"row_index": index, "raw_text": raw_text} for index, raw_text in enumerate(raw_texts)],
                ensure_ascii=False,
            ),
        ]
    )


def _sentiment_prompt_v1(text: str) -> str:
    return "\n".join(
        [
            "You are labeling sentiment for customer feedback or issue text.",
            "Classify one label only: positive, negative, neutral, mixed, or unknown.",
            "negative: complaint, failure, error, dissatisfaction, delay, refund, or blocked experience.",
            "positive: satisfaction, appreciation, successful resolution, or clearly favorable experience.",
            "neutral: factual report without clear positive or negative sentiment.",
            "mixed: both positive and negative signals are explicit in the same text.",
            "unknown: the text is too ambiguous or too short to classify reliably.",
            "Do not invent context beyond the text.",
            "",
            f"text: {text}",
        ]
    )


def _sentiment_prompt_v2(text: str) -> str:
    return "\n".join(
        [
            "You are labeling sentiment for customer feedback or issue text.",
            "Classify exactly one label: positive, negative, neutral, mixed, or unknown.",
            "negative: complaint, failure, error, dissatisfaction, delay, refund, blocked experience, or explicit frustration.",
            "positive: satisfaction, appreciation, successful resolution, gratitude, or clearly favorable experience.",
            "neutral: factual status reporting without clear positive or negative sentiment.",
            "mixed: explicit positive and negative signals coexist in the same text.",
            "unknown: the text is too ambiguous, too short, or too fragmentary to classify reliably.",
            "Prefer neutral over negative when the text only reports status or handling progress without explicit dissatisfaction.",
            "Do not invent context beyond the text.",
            "",
            f"text: {text}",
        ]
    )


PREPARE_ROW_PROMPTS: dict[str, Callable[[str], str]] = {
    "dataset-prepare-anthropic-v1": _prepare_row_prompt_v1,
    "dataset-prepare-anthropic-v2": _prepare_row_prompt_v2,
}

PREPARE_BATCH_PROMPTS: dict[str, Callable[[list[str]], str]] = {
    "dataset-prepare-anthropic-batch-v1": _prepare_batch_prompt_v1,
    "dataset-prepare-anthropic-batch-v2": _prepare_batch_prompt_v2,
}

SENTIMENT_PROMPTS: dict[str, Callable[[str], str]] = {
    "sentiment-anthropic-v1": _sentiment_prompt_v1,
    "sentiment-anthropic-v2": _sentiment_prompt_v2,
}

T = TypeVar("T")

def _render_prompt(registry: dict[str, Callable[[T], str]], value: T, requested_version: str, default_version: str, kind: str) -> tuple[str, str]:
    version = requested_version.strip() or default_version
    renderer = registry.get(version)
    if renderer is None:
        available = ", ".join(sorted(registry))
        raise ValueError(f"unsupported {kind} prompt version: {version} (available: {available})")
    return version, renderer(value)


def render_prepare_prompt(raw_text: str, version: str = "") -> tuple[str, str]:
    return _render_prompt(PREPARE_ROW_PROMPTS, raw_text, version, DEFAULT_PREPARE_PROMPT_VERSION, "prepare")


def render_prepare_batch_prompt(raw_texts: list[str], version: str = "") -> tuple[str, str]:
    return _render_prompt(PREPARE_BATCH_PROMPTS, raw_texts, version, DEFAULT_PREPARE_BATCH_PROMPT_VERSION, "prepare batch")


def render_sentiment_prompt(text: str, version: str = "") -> tuple[str, str]:
    return _render_prompt(SENTIMENT_PROMPTS, text, version, DEFAULT_SENTIMENT_PROMPT_VERSION, "sentiment")


__all__ = [
    "DEFAULT_PREPARE_BATCH_PROMPT_VERSION",
    "DEFAULT_PREPARE_PROMPT_VERSION",
    "DEFAULT_SENTIMENT_PROMPT_VERSION",
    "PREPARE_BATCH_PROMPTS",
    "PREPARE_ROW_PROMPTS",
    "SENTIMENT_PROMPTS",
    "render_prepare_batch_prompt",
    "render_prepare_prompt",
    "render_sentiment_prompt",
]
