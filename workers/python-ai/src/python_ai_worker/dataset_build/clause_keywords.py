from __future__ import annotations

"""dataset_clause_keywords build step (silverone 2026-06-10).

clause_label.jsonl(절 단위 sentiment/aspect 라벨) → **long-format** clause_keywords
(절-키워드 1행). 키워드 TOP/aspect별 키워드 순위 같은 집계가 기존 atomic skill
(top_n / aggregate / filter / sample_rows)로 바로 되도록 한다. list 컬럼은 집계가
어려우므로 keyword 단위로 펼친다.

extractor는 Kiwi 기반(keyword_extractor.KiwiKeywordExtractor) — JVM 불필요. LLOA 호출
없음(결정론적). extractor/stopword를 바꿔도 clause_label(LLOA)을 다시 돌릴 필요 없이
이 step만 재실행하면 된다.

clause_id는 executor의 ``clauses`` view와 **동일 규칙**으로 생성해 join 정합을 맞춘다:
``{doc_id}__{ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY clause, source, prompt_version)}``.
"""

import json
import time
from pathlib import Path
from typing import Any

from .. import runtime as rt
from ._common import write_progress
from .keyword_extractor import DEFAULT_KEYWORD_STOPWORDS_RULE, KiwiKeywordExtractor


def _read_clause_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as src:
        for line in src:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                records.append(obj)
    return records


def _assign_clause_ids(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """executor ``clauses`` view와 동일하게 doc_id별 ROW_NUMBER로 clause_id 부여.

    정렬 키 (clause, source, prompt_version)는 executor SQL과 동일해야 clause_id가
    일치한다 (clause_keywords ↔ clauses join 정합)."""
    by_doc: dict[str, list[dict[str, Any]]] = {}
    for rec in records:
        by_doc.setdefault(str(rec.get("doc_id") or ""), []).append(rec)
    enriched: list[dict[str, Any]] = []
    for doc_id, group in by_doc.items():
        ordered = sorted(
            group,
            key=lambda r: (
                str(r.get("clause") or ""),
                str(r.get("source") or ""),
                str(r.get("prompt_version") or ""),
            ),
        )
        for index, rec in enumerate(ordered, start=1):
            row = dict(rec)
            row["clause_id"] = f"{doc_id}__{index}"
            enriched.append(row)
    return enriched


def run_dataset_clause_keywords(payload: dict[str, Any]) -> dict[str, Any]:
    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    input_ref = str(
        payload.get("clause_label_ref") or payload.get("input_ref") or ""
    ).strip()
    output_path_raw = str(payload.get("output_path") or "").strip()
    if not dataset_version_id or not input_ref or not output_path_raw:
        raise ValueError(
            "dataset_clause_keywords requires dataset_version_id, clause_label_ref, output_path"
        )
    min_len = max(1, int(payload.get("keyword_min_len") or 2))
    progress_path = str(payload.get("progress_path") or "").strip()

    # 키워드 정제 사전 baked-in (silverone 2026-06-25, Phase 2). control-plane이
    # 활성 규칙(block/synonym)을 payload로 넘긴다. 재빌드 시 처음부터 제외/병합돼
    # artifact 자체가 정제본 → 보고서/analyze에도 그대로 흘러간다(overlay 불필요).
    block_terms: set[str] = set()
    synonym_map: dict[str, str] = {}
    for rule in payload.get("keyword_dictionary_rules") or []:
        if not isinstance(rule, dict):
            continue
        source_term = str(rule.get("source_term") or "").strip()
        if not source_term:
            continue
        rule_type = str(rule.get("rule_type") or "").strip()
        if rule_type == "block":
            block_terms.add(source_term)
        elif rule_type == "synonym":
            target_term = str(rule.get("target_term") or "").strip()
            if target_term:
                synonym_map[source_term] = target_term
    stopwords_rule = (
        str(payload.get("keyword_stopwords_rule_name") or "").strip()
        or DEFAULT_KEYWORD_STOPWORDS_RULE
    )

    input_path = Path(input_ref)
    if not input_path.exists():
        raise ValueError(f"dataset_clause_keywords input missing: {input_path}")
    output_path = Path(output_path_raw)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = time.monotonic()
    records = _assign_clause_ids(_read_clause_records(input_path))
    extractor = KiwiKeywordExtractor(
        min_len=min_len,
        stopwords_rule_name=stopwords_rule,
        block_terms=block_terms,
        synonym_map=synonym_map,
    )

    # 진행률은 status=running 동안만 화면에 표시되므로(BuildRunningBanner) 초기 0% +
    # 루프 중 주기적 갱신을 남겨야 빌드 진행 중에 진행률 바가 채워진다. 최종 100%는
    # 루프 후에 기록한다. (silverone 2026-06-15 — 다른 build 단계와 동일 패턴)
    total_rows = len(records)
    if progress_path:
        write_progress(
            progress_path,
            processed_rows=0,
            total_rows=total_rows,
            started_at=started_at,
            message="clause_keywords queued",
        )

    clause_count = 0
    clauses_with_keywords = 0
    keyword_row_count = 0
    keyword_counts: dict[str, int] = {}

    with output_path.open("w", encoding="utf-8") as dst:
        for rec in records:
            clause_count += 1
            keywords = extractor.extract(str(rec.get("clause") or ""))
            if keywords:
                clauses_with_keywords += 1
            for rank, keyword in enumerate(keywords, start=1):
                row = {
                    "doc_id": rec.get("doc_id"),
                    "clause_id": rec.get("clause_id"),
                    "clause": rec.get("clause"),
                    "aspect": rec.get("aspect"),
                    "sentiment": rec.get("sentiment"),
                    "keyword": keyword,
                    "source": "kiwi",
                    "extractor_version": extractor.version,
                    "keyword_rank_in_clause": rank,
                }
                dst.write(json.dumps(row, ensure_ascii=False))
                dst.write("\n")
                keyword_row_count += 1
                keyword_counts[keyword] = keyword_counts.get(keyword, 0) + 1

            # 200절마다(또는 마지막) 진행률 갱신 — 큰 데이터셋에서 진행률 바가
            # 실시간으로 채워지게. 결정론적 Kiwi 처리라 파일 I/O만 소폭 추가된다.
            if progress_path and (clause_count % 200 == 0 or clause_count == total_rows):
                write_progress(
                    progress_path,
                    processed_rows=clause_count,
                    total_rows=total_rows,
                    started_at=started_at,
                    message="clause_keywords processing",
                )

    if progress_path:
        write_progress(
            progress_path,
            processed_rows=total_rows,
            total_rows=total_rows,
            started_at=started_at,
            message="clause_keywords completed",
        )

    top_keywords = sorted(keyword_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:20]
    summary = {
        "input_artifact_ref": input_ref,
        "clause_count": clause_count,
        "clauses_with_keywords": clauses_with_keywords,
        "keyword_row_count": keyword_row_count,
        "unique_keyword_count": len(keyword_counts),
        "extractor_version": extractor.version,
        "keyword_min_len": min_len,
        "keyword_stopwords_rule": stopwords_rule,
        "dictionary_block_count": len(block_terms),
        "dictionary_synonym_count": len(synonym_map),
        "top_keywords": [{"keyword": k, "count": c} for k, c in top_keywords],
    }
    return {
        "notes": [
            f"dataset_clause_keywords — {keyword_row_count} keyword rows from {clause_count} clauses "
            f"({len(keyword_counts)} unique, {clauses_with_keywords} clauses with keywords)",
            f"extractor: {extractor.version}, min_len: {min_len}",
        ],
        "artifact": rt._set_scope_fields(
            {
                "skill_name": "dataset_clause_keywords",
                "dataset_version_id": dataset_version_id,
                "clause_keywords_uri": str(output_path),
                "clause_keywords_ref": str(output_path),
                "clause_keywords_input_source": "clause_label",
                "progress_ref": progress_path,
                "summary": summary,
            },
            declared_result_scope="full_dataset",
            runtime_result_scope="full_dataset",
        ),
    }
