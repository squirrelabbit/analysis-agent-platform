from __future__ import annotations

"""Aggregate-layer skill handlers."""

from ..obs import skill_handler
from ._aggregate_impl import (
    run_dictionary_tagging as _run_dictionary_tagging,
    run_keyword_frequency as _run_keyword_frequency,
    run_meta_group_count as _run_meta_group_count,
    run_noun_frequency as _run_noun_frequency,
    run_time_bucket_count as _run_time_bucket_count,
)

run_keyword_frequency = skill_handler("python-ai")(_run_keyword_frequency)
run_noun_frequency = skill_handler("python-ai")(_run_noun_frequency)
run_time_bucket_count = skill_handler("python-ai")(_run_time_bucket_count)
run_meta_group_count = skill_handler("python-ai")(_run_meta_group_count)
run_dictionary_tagging = skill_handler("python-ai")(_run_dictionary_tagging)

__all__ = [
    "run_dictionary_tagging",
    "run_keyword_frequency",
    "run_meta_group_count",
    "run_noun_frequency",
    "run_time_bucket_count",
]
