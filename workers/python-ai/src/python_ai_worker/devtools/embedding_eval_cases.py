from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EmbeddingSearchEvalCase:
    case_id: str
    description: str
    query: str
    documents: tuple[str, ...]
    expected_top_indices: tuple[int, ...]


@dataclass(frozen=True)
class EmbeddingClusterEvalCase:
    case_id: str
    description: str
    documents: tuple[str, ...]
    expected_clusters: tuple[tuple[int, ...], ...]
    similarity_threshold: float = 0.2


SEARCH_EVAL_CASES: tuple[EmbeddingSearchEvalCase, ...] = (
    EmbeddingSearchEvalCase(
        case_id="payment_error_priority",
        description="결제 오류 질의는 결제 오류 문서를 최상위로 올려야 한다.",
        query="결제 오류 관련 근거를 찾아줘",
        documents=(
            "결제 오류가 반복 발생했습니다",
            "로그인이 자주 실패하고 오류가 보입니다",
            "배송 문의가 계속 들어옵니다",
        ),
        expected_top_indices=(0, 1),
    ),
    EmbeddingSearchEvalCase(
        case_id="login_failure_focus",
        description="로그인 실패 질의는 로그인/인증 계열 문서를 우선해야 한다.",
        query="로그인 실패 원인을 찾고 싶어",
        documents=(
            "결제 승인 오류가 다시 발생했습니다",
            "로그인이 계속 실패합니다",
            "로그인 인증 오류가 반복됩니다",
            "배송 문의가 계속 들어옵니다",
        ),
        expected_top_indices=(1, 2),
    ),
)


CLUSTER_EVAL_CASES: tuple[EmbeddingClusterEvalCase, ...] = (
    EmbeddingClusterEvalCase(
        case_id="generic_overlap_topics",
        description="공통 표현이 많아도 결제/로그인/배송 군집이 분리되어야 한다.",
        documents=(
            "결제 오류가 계속 발생합니다",
            "결제 승인 문제가 반복됩니다",
            "로그인 오류가 계속 발생합니다",
            "로그인 인증 문제가 반복됩니다",
            "배송 오류가 계속 발생합니다",
            "배송 조회 문제가 반복됩니다",
        ),
        expected_clusters=((0, 1), (2, 3), (4, 5)),
    ),
    EmbeddingClusterEvalCase(
        case_id="short_issue_groups",
        description="짧은 VOC도 주제별로 안정적으로 묶여야 한다.",
        documents=(
            "결제 오류",
            "인증 오류",
            "배송 문의",
        ),
        expected_clusters=((0, 1), (2,)),
    ),
)


__all__ = [
    "CLUSTER_EVAL_CASES",
    "SEARCH_EVAL_CASES",
    "EmbeddingClusterEvalCase",
    "EmbeddingSearchEvalCase",
]
