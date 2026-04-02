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
PREPARE_REGEX_RULES: dict[str, dict[str, Any]] = {
    "media_placeholder": {
        "description": "이미지/스티커 placeholder 문구 제거",
        "patterns": [
            r"존재하지 않는 이미지입니다",
            r"존재하지 않는 스티커입니다",
            r"Previous imageNext image",
        ],
        "replacement": " ",
    },
    "html_artifact": {
        "description": "HTML break 및 nbsp 정리",
        "patterns": [
            r"<br\s*/?>",
            r"&nbsp;",
        ],
        "replacement": " ",
    },
    "url_cleanup": {
        "description": "URL 문자열 제거",
        "patterns": [
            r"https?://\S+",
            r"www\.\S+",
        ],
        "replacement": " ",
    },
    "zero_width_cleanup": {
        "description": "zero-width/BOM 문자 제거",
        "patterns": [
            r"[\u200b-\u200d\ufeff]",
        ],
        "replacement": "",
    },
}
DEFAULT_PREPARE_REGEX_RULE_NAMES = [
    "media_placeholder",
    "html_artifact",
    "url_cleanup",
    "zero_width_cleanup",
]
GARBAGE_RULES: dict[str, dict[str, Any]] = {
    "ad_marker": {
        "description": "광고/협찬/원고료 고지 패턴",
        "patterns": [
            r"#?\s*광고",
            r"#?\s*협찬",
            r"체험단",
            r"원고료",
            r"유료\s*광고",
            r"소정의\s*수수료",
            r"쿠팡\s*파트너스",
            r"파트너스\s*활동",
            r"sponsored",
            r"advertisement",
        ],
    },
    "promotion_link": {
        "description": "링크 클릭/프로필 이동 유도 패턴",
        "patterns": [
            r"프로필\s*링크",
            r"링크\s*클릭",
            r"구매\s*링크",
            r"자세한\s*내용은\s*링크",
            r"문의는?\s*dm",
            r"상담은?\s*dm",
        ],
    },
    "platform_placeholder": {
        "description": "플랫폼 placeholder 또는 미디어 안내 문구",
        "patterns": [
            r"존재하지 않는 이미지입니다",
            r"존재하지 않는 스티커입니다",
            r"Previous imageNext image",
        ],
    },
    "empty_or_noise": {
        "description": "정제 후 비어 있거나 noise-only 텍스트",
        "patterns": [],
    },
}
DEFAULT_GARBAGE_RULE_NAMES = [
    "ad_marker",
    "promotion_link",
    "platform_placeholder",
    "empty_or_noise",
]
TOKEN_OVERLAP_EMBEDDING_MODEL = "token-overlap-v1"
DEFAULT_LOCAL_EMBEDDING_MODEL = "intfloat/multilingual-e5-small"
DEFAULT_EMBEDDING_MODEL = DEFAULT_LOCAL_EMBEDDING_MODEL
DEFAULT_DENSE_EMBEDDING_BATCH_SIZE = 32
DEFAULT_DUPLICATE_THRESHOLD = 0.85
DEFAULT_CLUSTER_SIMILARITY_THRESHOLD = 0.3
DEFAULT_MAX_TAGS_PER_DOCUMENT = 3
DEFAULT_NOUN_ALLOWED_POS_PREFIXES = ["N"]
DEFAULT_NOUN_MIN_TOKEN_LENGTH = 2
DEFAULT_PREPARE_BATCH_SIZE = 8
DEFAULT_SENTENCE_PREVIEW_PER_ROW = 3
DEFAULT_SENTENCE_SPLIT_LANGUAGE = "ko"
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
    "DEFAULT_DENSE_EMBEDDING_BATCH_SIZE",
    "DEFAULT_DUPLICATE_THRESHOLD",
    "DEFAULT_EMBEDDING_MODEL",
    "DEFAULT_GARBAGE_RULE_NAMES",
    "DEFAULT_LOCAL_EMBEDDING_MODEL",
    "DEFAULT_MAX_TAGS_PER_DOCUMENT",
    "DEFAULT_NOUN_ALLOWED_POS_PREFIXES",
    "DEFAULT_NOUN_MIN_TOKEN_LENGTH",
    "DEFAULT_PREPARE_BATCH_SIZE",
    "DEFAULT_PREPARE_REGEX_RULE_NAMES",
    "DEFAULT_SENTENCE_PREVIEW_PER_ROW",
    "DEFAULT_SENTENCE_SPLIT_LANGUAGE",
    "DEFAULT_TAXONOMY_RULES",
    "GARBAGE_RULES",
    "NEGATIVE_SENTIMENT_TERMS",
    "POSITIVE_SENTIMENT_TERMS",
    "PREPARE_REGEX_RULES",
    "SENTIMENT_LABELS",
    "STOPWORDS",
    "TOKEN_OVERLAP_EMBEDDING_MODEL",
    "TOKEN_PATTERN",
]
