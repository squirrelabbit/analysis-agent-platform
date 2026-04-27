"""Migration target registry for ADR-009 Skill Surface Consolidation.

Single source of truth for the 17 legacy skill names being consolidated
and the deprecated→canonical alias mapping during the T4 deprecation
period.

Phase 1 (this commit) establishes only the
``keyword_frequency → term_frequency`` alias per ADR-009 F1.
Later phases extend ``DEPRECATED_ALIASES`` as additional renames or
primitive consolidations are decided.

References
----------
- ADR-009 (개발기록부_LLM분석플랫폼TF.md, 2026-04-25)
- workers/python-ai/docs/investigations/2026-04-24-bundle-prune-crossref-audit.md
"""

from __future__ import annotations

from typing import Mapping

# 17 legacy skill names enumerated by the 2026-04-24 audit (§0).
# This set is the canonical migration scope for T4 — all consolidation
# phases reference this constant rather than re-listing names inline.
LEGACY_SKILL_NAMES: frozenset[str] = frozenset(
    {
        "noun_frequency",
        "meta_group_count",
        "time_bucket_count",
        "garbage_filter",
        "issue_breakdown_summary",
        "issue_taxonomy_summary",
        "issue_sentiment_summary",
        "issue_trend_summary",
        "issue_period_compare",
        "sentence_split",
        "deduplicate_documents",
        "dictionary_tagging",
        "evidence_pack",
        "dataset_prepare",
        "sentiment_label",
        "embedding",
        "keyword_frequency",
    }
)

# Maps deprecated skill name → canonical replacement.
# Phase 1 introduces the first entry per ADR-009 F1 (alias period for
# the keyword_frequency rename). Subsequent phases extend this mapping
# as more renames land.
DEPRECATED_ALIASES: Mapping[str, str] = {
    "keyword_frequency": "term_frequency",
    "evidence_pack": "issue_evidence_summary",
}


def canonical_skill_name(name: str) -> str:
    """Return the canonical skill name, resolving deprecated aliases.

    Identity for non-deprecated names. Used by routing and contract
    validation to treat alias and canonical names as equivalent during
    the deprecation period.
    """

    return DEPRECATED_ALIASES.get(name, name)


__all__ = [
    "LEGACY_SKILL_NAMES",
    "DEPRECATED_ALIASES",
    "canonical_skill_name",
]
