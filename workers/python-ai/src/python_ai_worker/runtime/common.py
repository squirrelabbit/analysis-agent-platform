from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised in environments without parquet support
    pa = None
    pq = None

from .constants import (
    DEFAULT_GARBAGE_RULE_NAMES,
    DEFAULT_PREPARE_REGEX_RULE_NAMES,
    DEFAULT_TAXONOMY_RULES,
    GARBAGE_RULES,
    PREPARE_REGEX_RULES,
    STOPWORDS,
    TOKEN_PATTERN,
)


def _iter_documents(dataset_name: str, text_column: str) -> list[str]:
    return [str(row.get(text_column) or "").strip() for row in _iter_rows(dataset_name)]


def _iter_rows(dataset_name: str) -> list[dict[str, Any]]:
    path = Path(dataset_name)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _read_csv_rows(path)
    if suffix == ".jsonl":
        return _read_jsonl_rows(path)
    if suffix == ".parquet":
        return _read_parquet_rows(path)
    if suffix == ".txt":
        return [{"text": line.strip()} for line in path.read_text(encoding="utf-8").splitlines()]
    raise ValueError("dataset_name must point to a .csv, .jsonl, .parquet, or .txt file")


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    _, parquet = _require_pyarrow()
    table = parquet.read_table(path)
    return [dict(row) for row in table.to_pylist()]


def _write_parquet_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    arrow, parquet = _require_pyarrow()
    table = arrow.Table.from_pylist(rows) if rows else arrow.table({})
    parquet.write_table(table, path)


def _require_pyarrow() -> tuple[Any, Any]:
    if pa is None or pq is None:
        raise RuntimeError("pyarrow is required for parquet dataset support")
    return pa, pq


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _normalize_prepared_text(text: str) -> str:
    normalized = text.strip()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"[!?.]{2,}", ".", normalized)
    normalized = re.sub(r"[_\-=/]{3,}", " ", normalized)
    return normalized.strip()


def _normalize_prepare_regex_rule_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return list(DEFAULT_PREPARE_REGEX_RULE_NAMES)
    normalized: list[str] = []
    for item in value:
        name = str(item or "").strip()
        if not name or name not in PREPARE_REGEX_RULES or name in normalized:
            continue
        normalized.append(name)
    return normalized or list(DEFAULT_PREPARE_REGEX_RULE_NAMES)


def _normalize_garbage_rule_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return list(DEFAULT_GARBAGE_RULE_NAMES)
    normalized: list[str] = []
    for item in value:
        name = str(item or "").strip()
        if not name or name not in GARBAGE_RULES or name in normalized:
            continue
        normalized.append(name)
    return normalized or list(DEFAULT_GARBAGE_RULE_NAMES)


def _apply_prepare_regex_rules(text: str, rule_names: list[str]) -> tuple[str, list[str]]:
    current = str(text or "")
    applied: list[str] = []
    for name in _normalize_prepare_regex_rule_names(rule_names):
        rule = PREPARE_REGEX_RULES.get(name) or {}
        replacement = str(rule.get("replacement") or " ")
        before = current
        for pattern in list(rule.get("patterns") or []):
            current = re.sub(str(pattern), replacement, current, flags=re.IGNORECASE)
        if current != before:
            applied.append(name)
    return current, applied


def _match_garbage_rules(text: str, rule_names: list[str]) -> list[str]:
    raw_text = str(text or "")
    normalized_text, _ = _apply_prepare_regex_rules(raw_text, DEFAULT_PREPARE_REGEX_RULE_NAMES)
    prepared_text = _normalize_prepared_text(normalized_text)
    matched: list[str] = []
    for name in _normalize_garbage_rule_names(rule_names):
        if name == "empty_or_noise":
            if not prepared_text or _looks_noise_only(prepared_text):
                matched.append(name)
            continue
        rule = GARBAGE_RULES.get(name) or {}
        for pattern in list(rule.get("patterns") or []):
            if re.search(str(pattern), raw_text, flags=re.IGNORECASE):
                matched.append(name)
                break
    return matched


def _looks_noise_only(text: str) -> bool:
    if not text:
        return True
    tokens = TOKEN_PATTERN.findall(text.lower())
    return not tokens


def _tokenize(text: str) -> list[str]:
    tokens = []
    for match in TOKEN_PATTERN.findall(text.lower()):
        normalized = _normalize_token(match)
        if normalized in STOPWORDS:
            continue
        tokens.append(normalized)
    return tokens


def _normalize_token(token: str) -> str:
    normalized = token.strip().lower()
    if len(normalized) >= 3 and normalized[-1] in {"이", "가", "은", "는", "을", "를", "와", "과", "도", "에"}:
        candidate = normalized[:-1]
        if len(candidate) >= 2:
            normalized = candidate
    return normalized


def _looks_unstructured(goal: str) -> bool:
    keywords = ("issue", "voc", "text", "document", "review", "이슈", "문의", "리뷰", "문서", "텍스트")
    return any(keyword in goal for keyword in keywords)


def _looks_semantic_search_goal(goal: str) -> bool:
    keywords = ("search", "evidence", "find", "relevant", "근거", "찾아", "검색", "관련 문서")
    return any(keyword in goal for keyword in keywords)


def _looks_trend_goal(goal: str) -> bool:
    keywords = ("trend", "increase", "decrease", "change", "recent", "over time", "추세", "증가", "감소", "변화", "급증", "최근")
    return any(keyword in goal for keyword in keywords)


def _looks_compare_goal(goal: str) -> bool:
    keywords = ("compare", "versus", "vs", "difference", "period compare", "전주", "전월", "지난주", "지난달", "대비", "비교", "달라졌", "얼마나 달라")
    return any(keyword in goal for keyword in keywords)


def _looks_breakdown_goal(goal: str) -> bool:
    keywords = ("breakdown", "group by", "channel", "source", "product", "region", "채널별", "제품별", "상태별", "분해", "어디서", "어느 채널")
    return any(keyword in goal for keyword in keywords)


def _looks_cluster_goal(goal: str) -> bool:
    keywords = ("cluster", "clustering", "theme", "topic group", "군집", "클러스터", "토픽", "주제별", "묶음")
    return any(keyword in goal for keyword in keywords)


def _looks_taxonomy_goal(goal: str) -> bool:
    keywords = ("taxonomy", "tag", "category", "categorize", "분류", "분류체계", "카테고리", "태그")
    return any(keyword in goal for keyword in keywords)


def _looks_duplicate_goal(goal: str) -> bool:
    keywords = ("duplicate", "dedup", "중복", "반복 문서", "같은 이슈", "유사 문서")
    return any(keyword in goal for keyword in keywords)


def _looks_sentiment_goal(goal: str) -> bool:
    keywords = ("sentiment", "positive", "negative", "neutral", "긍정", "부정", "중립", "감성", "감정", "호감", "불만", "만족")
    return any(keyword in goal for keyword in keywords)


def _iter_embedding_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise ValueError(f"embedding_uri does not exist: {path}")
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def _vector_norm(token_counts: Counter[str]) -> float:
    total = sum(value * value for value in token_counts.values())
    return math.sqrt(total)


def _cosine_similarity(query_counts: Counter[str], doc_counts: dict[str, int], doc_norm: float) -> float:
    if not query_counts or not doc_counts or doc_norm <= 0:
        return 0.0
    query_norm = _vector_norm(query_counts)
    if query_norm <= 0:
        return 0.0
    dot_product = 0
    for token, query_value in query_counts.items():
        dot_product += query_value * int(doc_counts.get(token) or 0)
    return dot_product / (query_norm * doc_norm)


def _rank_documents(documents: list[str], query: str) -> list[dict[str, Any]]:
    query_tokens = set(_tokenize(query))
    ranked = []
    for index, document in enumerate(documents):
        tokens = _tokenize(document)
        overlap = sum(1 for token in tokens if token in query_tokens)
        ranked.append(
            {
                "rank": 0,
                "source_index": index,
                "score": overlap,
                "text": document,
            }
        )

    ranked.sort(key=lambda item: (-item["score"], item["source_index"]))
    for order, item in enumerate(ranked, start=1):
        item["rank"] = order
    return ranked


def _fallback_evidence_summary(query: str, snippets: list[dict[str, Any]], top_terms: Counter[str]) -> str:
    if not snippets:
        return "선택된 근거 문서가 없습니다."
    top_term_list = ", ".join(term for term, _ in top_terms.most_common(5))
    if query:
        return f"질문 '{query}' 기준으로 관련 문서를 추렸고, 주요 용어는 {top_term_list} 입니다."
    return f"대표 문서를 추렸고, 주요 용어는 {top_term_list} 입니다."


def _fallback_follow_up_questions(query: str) -> list[str]:
    if query:
        return [
            f"'{query}'와 직접 연결되는 메타 컬럼은 무엇인가?",
            "기간별 변화도 같이 비교할 것인가?",
        ]
    return [
        "기간별 변화도 같이 비교할 것인가?",
        "제품/채널별 분해가 필요한가?",
    ]


def _evidence_rationale(item: dict[str, Any], selection_source: str) -> str:
    if selection_source == "semantic_search":
        score = float(item.get("score") or 0)
        return f"selected by semantic similarity (score={score:.3f})"
    if selection_source == "document_sample":
        score = float(item.get("score") or 0)
        if score > 0:
            return f"selected by document_sample support skill (score={score:.3f})"
        return "selected by document_sample support skill"
    if float(item.get("score") or 0) > 0:
        return "selected by lexical overlap"
    return "selected by source order"


def _resolve_compare_periods(bucket_labels: list[str], normalized: dict[str, Any]) -> tuple[list[str], list[str]]:
    current_start = normalized["current_start_bucket"]
    current_end = normalized["current_end_bucket"]
    previous_start = normalized["previous_start_bucket"]
    previous_end = normalized["previous_end_bucket"]
    if current_start or current_end or previous_start or previous_end:
        current = [label for label in bucket_labels if _in_bucket_range(label, current_start, current_end)]
        previous = [label for label in bucket_labels if _in_bucket_range(label, previous_start, previous_end)]
        return current, previous

    window_size = normalized["window_size"]
    if not bucket_labels:
        return [], []
    current = bucket_labels[-window_size:]
    previous_end_index = max(0, len(bucket_labels) - window_size)
    previous_start_index = max(0, previous_end_index - window_size)
    previous = bucket_labels[previous_start_index:previous_end_index]
    return current, previous


def _in_bucket_range(label: str, start: str, end: str) -> bool:
    if start and label < start:
        return False
    if end and label > end:
        return False
    return True


def _collect_bucket_documents(bucket_documents: dict[str, list[str]], buckets: list[str]) -> list[str]:
    documents: list[str] = []
    for bucket in buckets:
        documents.extend(bucket_documents.get(bucket, []))
    return documents


def _build_period_payload(
    buckets: list[str],
    documents: list[str],
    terms: Counter[str],
    top_n: int,
    sample_n: int,
) -> dict[str, Any]:
    return {
        "start_bucket": _period_start(buckets),
        "end_bucket": _period_end(buckets),
        "bucket_count": len(buckets),
        "document_count": len(documents),
        "top_terms": [
            {"term": term, "count": count}
            for term, count in terms.most_common(top_n)
        ],
        "samples": documents[:sample_n],
    }


def _build_term_deltas(current_terms: Counter[str], previous_terms: Counter[str], top_n: int) -> list[dict[str, Any]]:
    candidates = set(current_terms.keys()) | set(previous_terms.keys())
    rows = []
    for term in candidates:
        current_count = current_terms.get(term, 0)
        previous_count = previous_terms.get(term, 0)
        delta = current_count - previous_count
        rows.append(
            {
                "term": term,
                "current_count": current_count,
                "previous_count": previous_count,
                "delta": delta,
            }
        )
    rows.sort(key=lambda item: (-abs(item["delta"]), -item["current_count"], item["term"]))
    return rows[:top_n]


def _period_start(buckets: list[str]) -> str | None:
    if not buckets:
        return None
    return buckets[0]


def _period_end(buckets: list[str]) -> str | None:
    if not buckets:
        return None
    return buckets[-1]


def _parse_timestamp(raw: str) -> datetime | None:
    value = raw.strip()
    if not value:
        return None

    candidates = [value]
    if value.endswith("Z"):
        candidates.insert(0, value[:-1] + "+00:00")

    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue

    for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(value, pattern)
        except ValueError:
            continue
    return None


def _bucket_label(timestamp: datetime, bucket: str) -> str:
    if bucket == "week":
        week_start = timestamp - timedelta(days=timestamp.weekday())
        return week_start.date().isoformat()
    if bucket == "month":
        return f"{timestamp.year:04d}-{timestamp.month:02d}"
    return timestamp.date().isoformat()


def _token_counter(value: Any) -> Counter[str]:
    counter: Counter[str] = Counter()
    if not isinstance(value, dict):
        return counter
    for key, count in value.items():
        try:
            normalized_count = int(count)
        except (TypeError, ValueError):
            continue
        if normalized_count <= 0:
            continue
        counter[str(key)] = normalized_count
    return counter


def _duplicate_similarity(
    normalized_text: str,
    token_set: set[str],
    canonical_text: str,
    canonical_tokens: set[str],
) -> float:
    if normalized_text and normalized_text == canonical_text:
        return 1.0
    if not token_set or not canonical_tokens:
        return 0.0
    intersection = len(token_set & canonical_tokens)
    union = len(token_set | canonical_tokens)
    if union <= 0:
        return 0.0
    return intersection / union


def _normalize_taxonomy_rules(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return dict(DEFAULT_TAXONOMY_RULES)
    normalized: dict[str, dict[str, Any]] = {}
    for taxonomy_id, raw_rule in value.items():
        taxonomy_key = str(taxonomy_id).strip()
        if not taxonomy_key:
            continue
        label = taxonomy_key
        patterns: list[str] = []
        if isinstance(raw_rule, dict):
            label = str(raw_rule.get("label") or taxonomy_key).strip() or taxonomy_key
            patterns = [str(item).strip().lower() for item in list(raw_rule.get("patterns") or []) if str(item).strip()]
        elif isinstance(raw_rule, list):
            patterns = [str(item).strip().lower() for item in raw_rule if str(item).strip()]
        if not patterns:
            continue
        normalized[taxonomy_key] = {
            "label": label,
            "patterns": patterns,
        }
    if not normalized:
        return dict(DEFAULT_TAXONOMY_RULES)
    return normalized


def _match_taxonomies(text: str, taxonomy_rules: dict[str, dict[str, Any]], max_tags_per_document: int) -> list[str]:
    tokens = _tokenize(text)
    lowered_text = text.lower()
    scored: list[tuple[str, int]] = []
    for taxonomy_id, rule in taxonomy_rules.items():
        score = 0
        for pattern in list(rule.get("patterns") or []):
            normalized_pattern = str(pattern).strip().lower()
            if not normalized_pattern:
                continue
            if " " in normalized_pattern:
                if normalized_pattern in lowered_text:
                    score += 1
                continue
            for token in tokens:
                if token == normalized_pattern or normalized_pattern in token or token in normalized_pattern:
                    score += 1
        if score > 0:
            scored.append((taxonomy_id, score))
    scored.sort(key=lambda item: (-item[1], item[0]))
    return [taxonomy_id for taxonomy_id, _ in scored[:max_tags_per_document]]


def _cluster_candidate_labels(top_terms: list[dict[str, Any]]) -> list[str]:
    terms = [str(item.get("term") or "").strip() for item in top_terms if str(item.get("term") or "").strip()]
    labels: list[str] = []
    if len(terms) >= 2:
        labels.append(f"{terms[0]} / {terms[1]}")
    if len(terms) >= 3:
        labels.append(f"{terms[0]}, {terms[1]}, {terms[2]}")
    if terms:
        labels.append(terms[0])
    unique = []
    seen: set[str] = set()
    for label in labels:
        if label in seen:
            continue
        unique.append(label)
        seen.add(label)
    return unique


def _cluster_label_rationale(top_terms: list[dict[str, Any]]) -> str:
    terms = [str(item.get("term") or "").strip() for item in top_terms if str(item.get("term") or "").strip()]
    if not terms:
        return "대표 용어가 부족해 기본 레이블을 사용했습니다."
    return f"상위 용어 {', '.join(terms[:3])} 기준으로 레이블 후보를 만들었습니다."


__all__ = [
    "_bucket_label",
    "_build_period_payload",
    "_build_term_deltas",
    "_cluster_candidate_labels",
    "_cluster_label_rationale",
    "_coerce_string_list",
    "_collect_bucket_documents",
    "_cosine_similarity",
    "_duplicate_similarity",
    "_evidence_rationale",
    "_fallback_evidence_summary",
    "_fallback_follow_up_questions",
    "_apply_prepare_regex_rules",
    "_in_bucket_range",
    "_iter_documents",
    "_iter_embedding_records",
    "_iter_rows",
    "_looks_breakdown_goal",
    "_looks_cluster_goal",
    "_looks_compare_goal",
    "_looks_duplicate_goal",
    "_looks_noise_only",
    "_looks_semantic_search_goal",
    "_looks_sentiment_goal",
    "_looks_taxonomy_goal",
    "_looks_trend_goal",
    "_looks_unstructured",
    "_match_taxonomies",
    "_match_garbage_rules",
    "_normalize_garbage_rule_names",
    "_normalize_prepared_text",
    "_normalize_prepare_regex_rule_names",
    "_normalize_taxonomy_rules",
    "_normalize_token",
    "_parse_timestamp",
    "_period_end",
    "_period_start",
    "_rank_documents",
    "_read_csv_rows",
    "_read_jsonl_rows",
    "_read_parquet_rows",
    "_resolve_compare_periods",
    "_token_counter",
    "_tokenize",
    "_vector_norm",
    "_write_parquet_rows",
]
