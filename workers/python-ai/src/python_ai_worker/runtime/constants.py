from __future__ import annotations

import re
from typing import Any

TOKEN_PATTERN = re.compile(r"[0-9A-Za-z가-힣]{2,}")
STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "this",
    "that",
    "from",
    "have",
    "were",
    "will",
    "about",
    "error",
    "issue",
    "please",
    "there",
    "있습니다",
    "합니다",
    "계속",
    "문의",
    "내용",
    "확인",
    "처리",
    "대한",
    "관련",
}
DEFAULT_EMBEDDING_MODEL = "token-overlap-v1"
DEFAULT_DUPLICATE_THRESHOLD = 0.85
DEFAULT_CLUSTER_SIMILARITY_THRESHOLD = 0.3
DEFAULT_MAX_TAGS_PER_DOCUMENT = 3
DEFAULT_PREPARE_BATCH_SIZE = 8
SENTIMENT_LABELS = {"positive", "negative", "neutral", "mixed", "unknown"}
POSITIVE_SENTIMENT_TERMS = {
    "good",
    "great",
    "excellent",
    "fast",
    "resolved",
    "fixed",
    "satisfied",
    "thanks",
    "좋다",
    "만족",
    "편리",
    "빠르",
    "정상",
    "해결",
    "감사",
    "원활",
}
NEGATIVE_SENTIMENT_TERMS = {
    "bad",
    "issue",
    "error",
    "fail",
    "failed",
    "broken",
    "slow",
    "delay",
    "refund",
    "complaint",
    "문제",
    "오류",
    "실패",
    "불만",
    "불편",
    "지연",
    "환불",
    "반복",
    "안됨",
    "안돼",
    "끊김",
}
DEFAULT_TAXONOMY_RULES: dict[str, dict[str, Any]] = {
    "payment_billing": {
        "label": "결제/정산",
        "patterns": ["결제", "승인", "주문", "환불", "billing", "payment", "checkout", "refund"],
    },
    "login_account": {
        "label": "로그인/계정",
        "patterns": ["로그인", "인증", "계정", "비밀번호", "otp", "login", "account", "password"],
    },
    "delivery_fulfillment": {
        "label": "배송/이행",
        "patterns": ["배송", "배달", "출고", "도착", "tracking", "shipment", "delivery"],
    },
    "system_failure": {
        "label": "시스템 장애",
        "patterns": ["오류", "장애", "실패", "에러", "버그", "fail", "error", "broken", "안됨", "안돼"],
    },
    "service_quality": {
        "label": "품질/성능",
        "patterns": ["지연", "느림", "끊김", "latency", "slow", "performance", "timeout"],
    },
    "support_request": {
        "label": "문의/지원",
        "patterns": ["문의", "상담", "도움", "안내", "support", "help", "ticket"],
    },
}

__all__ = [
    "DEFAULT_CLUSTER_SIMILARITY_THRESHOLD",
    "DEFAULT_DUPLICATE_THRESHOLD",
    "DEFAULT_EMBEDDING_MODEL",
    "DEFAULT_MAX_TAGS_PER_DOCUMENT",
    "DEFAULT_PREPARE_BATCH_SIZE",
    "DEFAULT_TAXONOMY_RULES",
    "NEGATIVE_SENTIMENT_TERMS",
    "POSITIVE_SENTIMENT_TERMS",
    "SENTIMENT_LABELS",
    "STOPWORDS",
    "TOKEN_PATTERN",
]
