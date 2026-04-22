from __future__ import annotations

"""Compatibility exports for legacy imports from python_ai_worker.tasks."""

from .planner import run_planner  # noqa: F401
from .skills.aggregate import (  # noqa: F401
    run_dictionary_tagging,
    run_keyword_frequency,
    run_meta_group_count,
    run_noun_frequency,
    run_time_bucket_count,
)
from .skills.dataset_build import run_dataset_clean, run_dataset_cluster_build, run_dataset_prepare, run_embedding, run_sentiment_label  # noqa: F401
from .skills.preprocess import (  # noqa: F401
    run_deduplicate_documents,
    run_document_filter,
    run_document_sample,
    run_garbage_filter,
    run_sentence_split,
)
from .skills.presentation import run_execution_final_answer  # noqa: F401
from .skills.retrieve import (  # noqa: F401
    run_cluster_label_candidates,
    run_embedding_cluster,
    run_semantic_search,
)
from .skills.summarize import (  # noqa: F401
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
    "run_dataset_clean",
    "run_dataset_cluster_build",
    "run_dataset_prepare",
    "run_deduplicate_documents",
    "run_dictionary_tagging",
    "run_document_filter",
    "run_document_sample",
    "run_embedding",
    "run_embedding_cluster",
    "run_execution_final_answer",
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
