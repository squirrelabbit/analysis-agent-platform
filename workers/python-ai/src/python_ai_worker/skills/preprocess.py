from __future__ import annotations

"""Preprocess-layer skill handlers."""

from ..obs import skill_handler
from ._preprocess_impl import (
    run_deduplicate_documents as _run_deduplicate_documents,
    run_document_filter as _run_document_filter,
    run_document_sample as _run_document_sample,
    run_garbage_filter as _run_garbage_filter,
    run_sentence_split as _run_sentence_split,
)

run_garbage_filter = skill_handler("python-ai")(_run_garbage_filter)
run_document_filter = skill_handler("python-ai")(_run_document_filter)
run_document_sample = skill_handler("python-ai")(_run_document_sample)
run_deduplicate_documents = skill_handler("python-ai")(_run_deduplicate_documents)
run_sentence_split = skill_handler("python-ai")(_run_sentence_split)

__all__ = [
    "run_deduplicate_documents",
    "run_document_filter",
    "run_document_sample",
    "run_garbage_filter",
    "run_sentence_split",
]
