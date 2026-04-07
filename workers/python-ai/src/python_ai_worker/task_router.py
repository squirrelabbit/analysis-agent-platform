from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .planner import run_planner
from .prompt_registry import prompt_catalog
from .runtime.rule_config import (
    resolve_default_garbage_rule_names,
    resolve_default_prepare_regex_rule_names,
    resolve_garbage_rules,
    resolve_prepare_regex_rules,
)
from .skill_bundle import bundle_version, capability_skills
from .skills.core import (
    run_evidence_pack,
    run_issue_breakdown_summary,
    run_issue_cluster_summary,
    run_issue_evidence_summary,
    run_issue_period_compare,
    run_issue_sentiment_summary,
    run_issue_taxonomy_summary,
    run_issue_trend_summary,
    run_unstructured_issue_summary,
)
from .skills.dataset_build import run_dataset_prepare, run_embedding, run_sentiment_label
from .skills.presentation import run_execution_final_answer
from .skills.support import (
    run_cluster_label_candidates,
    run_deduplicate_documents,
    run_dictionary_tagging,
    run_document_filter,
    run_document_sample,
    run_embedding_cluster,
    run_garbage_filter,
    run_keyword_frequency,
    run_meta_group_count,
    run_noun_frequency,
    run_semantic_search,
    run_sentence_split,
    run_time_bucket_count,
)


@dataclass(frozen=True)
class TaskCapability:
    name: str
    description: str


PLANNER_CAPABILITY = TaskCapability(name="planner", description="Generate replayable skill plans.")
FINAL_ANSWER_CAPABILITY = TaskCapability(
    name="execution_final_answer",
    description="Generate grounded final answers from completed execution results.",
)


def capability_names() -> list[str]:
    return [item.name for item in supported_capabilities()]


def capability_payload() -> dict[str, Any]:
    return {
        "skill_bundle_version": bundle_version(),
        "capabilities": [
            {"name": item.name, "description": item.description}
            for item in supported_capabilities()
        ],
        "prompt_catalog": prompt_catalog(),
        "rule_catalog": {
            "available_prepare_regex_rule_names": sorted(resolve_prepare_regex_rules().keys()),
            "default_prepare_regex_rule_names": resolve_default_prepare_regex_rule_names(),
            "available_garbage_rule_names": sorted(resolve_garbage_rules().keys()),
            "default_garbage_rule_names": resolve_default_garbage_rule_names(),
        },
    }


def supported_capabilities() -> list[TaskCapability]:
    capabilities = [PLANNER_CAPABILITY, FINAL_ANSWER_CAPABILITY]
    for skill in capability_skills():
        name = str(skill.get("name") or "").strip()
        description = str(skill.get("description") or "").strip()
        if not name:
            continue
        capabilities.append(TaskCapability(name=name, description=description))
    return capabilities


def task_handlers() -> dict[str, Any]:
    return {
        "planner": run_planner,
        "execution_final_answer": run_execution_final_answer,
        "dataset_prepare": run_dataset_prepare,
        "sentiment_label": run_sentiment_label,
        "embedding": run_embedding,
        "garbage_filter": run_garbage_filter,
        "document_filter": run_document_filter,
        "deduplicate_documents": run_deduplicate_documents,
        "keyword_frequency": run_keyword_frequency,
        "noun_frequency": run_noun_frequency,
        "sentence_split": run_sentence_split,
        "time_bucket_count": run_time_bucket_count,
        "meta_group_count": run_meta_group_count,
        "document_sample": run_document_sample,
        "dictionary_tagging": run_dictionary_tagging,
        "embedding_cluster": run_embedding_cluster,
        "cluster_label_candidates": run_cluster_label_candidates,
        "issue_breakdown_summary": run_issue_breakdown_summary,
        "issue_cluster_summary": run_issue_cluster_summary,
        "issue_trend_summary": run_issue_trend_summary,
        "issue_period_compare": run_issue_period_compare,
        "issue_sentiment_summary": run_issue_sentiment_summary,
        "issue_taxonomy_summary": run_issue_taxonomy_summary,
        "semantic_search": run_semantic_search,
        "issue_evidence_summary": run_issue_evidence_summary,
        "evidence_pack": run_evidence_pack,
        "unstructured_issue_summary": run_unstructured_issue_summary,
    }


def run_task(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    handler = task_handlers().get(name)
    if handler is None:
        raise ValueError(f"unsupported capability: {name}")
    return handler(payload)
