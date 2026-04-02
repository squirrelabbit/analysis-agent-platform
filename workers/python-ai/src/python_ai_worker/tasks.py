from __future__ import annotations

"""Compatibility exports for legacy imports from python_ai_worker.tasks."""

from .planner import run_planner  # noqa: F401
from .skills.core import (  # noqa: F401
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
from .skills.dataset_build import run_dataset_prepare, run_embedding, run_sentiment_label  # noqa: F401
from .skills.support import (  # noqa: F401
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
from .task_router import (  # noqa: F401
    PLANNER_CAPABILITY,
    TaskCapability,
    capability_names,
    capability_payload,
    run_task,
    supported_capabilities,
    task_handlers,
)

__all__ = [
    "PLANNER_CAPABILITY",
    "TaskCapability",
    "capability_names",
    "capability_payload",
    "run_cluster_label_candidates",
    "run_dataset_prepare",
    "run_deduplicate_documents",
    "run_dictionary_tagging",
    "run_document_filter",
    "run_document_sample",
    "run_embedding",
    "run_embedding_cluster",
    "run_evidence_pack",
    "run_garbage_filter",
    "run_issue_breakdown_summary",
    "run_issue_cluster_summary",
    "run_issue_evidence_summary",
    "run_issue_period_compare",
    "run_issue_sentiment_summary",
    "run_issue_taxonomy_summary",
    "run_issue_trend_summary",
    "run_keyword_frequency",
    "run_meta_group_count",
    "run_noun_frequency",
    "run_planner",
    "run_semantic_search",
    "run_sentiment_label",
    "run_sentence_split",
    "run_task",
    "run_time_bucket_count",
    "supported_capabilities",
    "task_handlers",
    "run_unstructured_issue_summary",
]
