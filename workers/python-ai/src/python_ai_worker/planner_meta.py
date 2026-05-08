from __future__ import annotations

import json
from dataclasses import dataclass

from .skill_bundle import planner_layer_hints
from .runtime.llm import _create_json_logged

DEFAULT_ACTIVE_LAYERS = frozenset({"preprocess", "aggregate", "summarize"})
META_PLANNER_LAYERS = ("preprocess", "aggregate", "retrieve", "summarize", "presentation", "structured")


@dataclass(frozen=True)
class MetaPlanResult:
    active_layers: frozenset[str]
    confidence: str
    trigger_matches: tuple[str, ...]


def select_active_layers(question: str, *, anthropic_client=None) -> MetaPlanResult:
    normalized_question = str(question or "").strip().lower()
    matched_layers: set[str] = set()
    matched_sequences: list[str] = []
    for hint in planner_layer_hints():
        triggers = [str(trigger or "").strip().lower() for trigger in list(hint.get("trigger") or [])]
        if not triggers:
            continue
        if not any(trigger and trigger in normalized_question for trigger in triggers):
            continue
        matched_layers.update(_valid_layer_values(hint.get("layers") or []))
        sequence_name = str(hint.get("sequence_name") or "").strip()
        if sequence_name and sequence_name not in matched_sequences:
            matched_sequences.append(sequence_name)
    if matched_layers:
        return MetaPlanResult(
            active_layers=frozenset(sorted(matched_layers)),
            confidence="rule",
            trigger_matches=tuple(matched_sequences),
        )

    if (
        anthropic_client is not None
        and anthropic_client.is_enabled()
        and callable(getattr(anthropic_client, "create_json", None))
    ):
        fallback = _select_layers_with_llm(anthropic_client, question)
        if fallback is not None:
            return fallback

    return MetaPlanResult(
        active_layers=DEFAULT_ACTIVE_LAYERS,
        confidence="default",
        trigger_matches=(),
    )


def _valid_layer_values(values: list[str]) -> list[str]:
    layers: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if normalized in META_PLANNER_LAYERS:
            layers.append(normalized)
    return layers


def _select_layers_with_llm(anthropic_client, question: str) -> MetaPlanResult | None:
    prompt = "\n".join(
        [
            "You are a planner meta-router for an analysis platform.",
            "Choose the smallest set of runtime layers needed for the user's question.",
            f"Allowed layers: {', '.join(META_PLANNER_LAYERS)}.",
            "Return JSON with a single 'layers' array.",
            f"question: {question}",
        ]
    )
    response = _create_json_logged(
        anthropic_client,
        operation="planner_meta",
        prompt=prompt,
        schema={
            "type": "object",
            "properties": {
                "layers": {
                    "type": "array",
                    "items": {"type": "string"},
                }
            },
            "required": ["layers"],
            "additionalProperties": False,
        },
        max_tokens=200,
    )
    if not isinstance(response, dict):
        logger.warning("llm.meta_planner.response_malformed", response=repr(response))
        return None
    selected = _valid_layer_values(list((response or {}).get("layers") or []))
    if not selected:
        return None
    return MetaPlanResult(
        active_layers=frozenset(sorted(selected)),
        confidence="llm",
        trigger_matches=("llm_fallback",),
    )


__all__ = [
    "DEFAULT_ACTIVE_LAYERS",
    "META_PLANNER_LAYERS",
    "MetaPlanResult",
    "select_active_layers",
]
