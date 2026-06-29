from __future__ import annotations

"""artifact result_scope 정규화 헬퍼.

ADR-018 retrieve layer 삭제 후, 이 모듈의 evidence/embedding/retrieve/cluster/
build_* 계열 함수(~1056줄)는 모두 죽어 제거했다(2026-06-29 전체구조검토 정리).
dataset_build(clean/doc_genuineness/clause_label/clause_keywords + verify)이
artifact의 result_scope/runtime_result_scope 필드를 정규화할 때 쓰는 함수만 남긴다.
"""

from typing import Any

_RESULT_SCOPE_ALIASES = {
    "subset_filtered": "document_subset",
    "sample_n": "document_subset",
    "single_record": "document_subset",
    "subset_selection": "cluster_subset",
}


def _normalize_result_scope(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    return _RESULT_SCOPE_ALIASES.get(normalized, normalized)


def _set_scope_fields(
    artifact: dict[str, Any],
    *,
    declared_result_scope: str,
    runtime_result_scope: str | None = None,
) -> dict[str, Any]:
    normalized_declared_result_scope = _normalize_result_scope(declared_result_scope)
    if not normalized_declared_result_scope:
        raise ValueError("declared_result_scope is required")
    normalized_runtime_result_scope = _normalize_result_scope(
        runtime_result_scope or normalized_declared_result_scope
    )
    if not normalized_runtime_result_scope:
        raise ValueError("runtime_result_scope is required")
    artifact["result_scope"] = normalized_declared_result_scope
    artifact["runtime_result_scope"] = normalized_runtime_result_scope
    return artifact


__all__ = ["_normalize_result_scope", "_set_scope_fields"]
