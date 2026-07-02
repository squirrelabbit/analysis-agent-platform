from __future__ import annotations

"""clause_keywords view task — 키워드 대시보드/표 집계 (Go loadClauseKeywordsArtifact).

Go control-plane의 clause_keywords artifact DuckDB 집계(worker 이전, ADR-024).
키워드 정제 사전(block/synonym) overlay는 rules payload로 받아 서브쿼리 source에
적용한다 — 원본 artifact 불변, 모든 하위 집계(빈도/랭크/대표문장)에 자동 반영.

응답: {summary, total, items}
- summary: 대시보드 집계(필터 미적용 전체) + aspect 필터 시 selected_aspect 3종.
- items 값은 Go scanArtifactRows(NullString) 대응 — count/ratio/weight도 문자열.
- group=='clause'면 items가 절 중심 {clause, keywords[], occurrence_count}.
"""

import json
from typing import Any

from .artifact_views import (
    _aggregate_grouped_counts,
    _connect,
    _esc,
    _jsonl_source,
    _normalize_key,
)
from .obs import get

_LOG = get(__name__)

# 전체 긍/부 카드용 상위 키워드 수 / aspect별 워드클라우드용 상위 키워드 수 (Go 상수).
_TOP_KEYWORD_CARD_N = 5
_WORD_CLOUD_TOP_N = 30


def run_clause_keywords_view(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("clause_keywords view payload must be an object")
    ref = str(payload.get("ref") or "").strip()
    if not ref:
        raise ValueError("clause_keywords view payload requires 'ref'")
    try:
        limit = int(payload.get("limit") or 100)
        offset = int(payload.get("offset") or 0)
    except (TypeError, ValueError):
        raise ValueError("clause_keywords view 'limit'/'offset' must be integers")
    aspect = str(payload.get("aspect") or "").strip()
    sentiment = str(payload.get("sentiment") or "").strip()
    q = str(payload.get("q") or "").strip()
    group = str(payload.get("group") or "").strip()
    rules = payload.get("rules") if isinstance(payload.get("rules"), list) else []

    result = _load_clause_keywords(ref, limit, offset, aspect, sentiment, q, group, rules)
    _LOG.info(
        "artifact_view.completed",
        view="clause_keywords",
        group=group or "keyword",
        total=result["total"],
        item_count=len(result["items"]),
    )
    return result


def _load_clause_keywords(
    ref: str, limit: int, offset: int,
    aspect: str, sentiment: str, q: str, group: str,
    rules: list[Any],
) -> dict[str, Any]:
    con = _connect()
    try:
        source = _dictionary_source(ref, rules)

        # ── dashboard summary (필터 미적용 전체) ──────────────────────────
        total, by_aspect = _aggregate_grouped_counts(con, source, "aspect")
        _, by_sentiment = _aggregate_grouped_counts(con, source, "sentiment")
        unique_keywords = _scalar_count(
            con, f"SELECT COUNT(DISTINCT keyword) FROM {source} WHERE keyword IS NOT NULL"
        )
        clauses_with_keywords = _scalar_count(
            con, f"SELECT COUNT(DISTINCT clause_id) FROM {source}"
        )
        summary: dict[str, Any] = {
            "total_keyword_count": total,
            "unique_keyword_count": unique_keywords,
            "clause_count": clauses_with_keywords,
            "aspect": by_aspect,
            "sentiment": by_sentiment,
            "top_keywords_positive": _top_keywords_by_sentiment(con, source, "positive"),
            "top_keywords_negative": _top_keywords_by_sentiment(con, source, "negative"),
            "aspect_sentiment_keywords": _aggregate_aspect_sentiment_keywords(con, source),
        }
        if aspect:
            sel_total, sel_sentiment = _aggregate_grouped_counts_where(
                con, source, "sentiment", f"WHERE aspect = '{_esc(aspect)}'"
            )
            summary["selected_aspect"] = aspect
            summary["selected_aspect_total"] = sel_total
            summary["selected_aspect_sentiment"] = sel_sentiment

        # ── group=clause: 절 중심 item table ─────────────────────────────
        if group == "clause":
            clause_total, clause_items = _load_clause_grouped(con, source, limit, offset, q)
            return {"summary": summary, "total": clause_total, "items": clause_items}

        # ── 키워드 집계 item table ────────────────────────────────────────
        where = _keyword_filter(aspect, sentiment, q)
        glue = "WHERE " if not where else " AND "
        filtered_total = _scalar_count(
            con,
            f"SELECT COUNT(DISTINCT keyword) FROM {source} {where}{glue}keyword IS NOT NULL",
        )
        # dominant_*/top_aspect/대표 절은 ROW_NUMBER + 명시 tie-break(동률 시 사전순) —
        # arg_max는 동률 승자가 비결정적이라 호출마다 값이 흔들린다 (Go도 동일하게 수정).
        item_query = f"""
            WITH filtered AS (
               SELECT * FROM {source} {where}{glue}keyword IS NOT NULL
            ),
            ks AS (SELECT keyword, sentiment, COUNT(*) c FROM filtered GROUP BY keyword, sentiment),
            ka AS (SELECT keyword, aspect, COUNT(*) c FROM filtered GROUP BY keyword, aspect),
            base AS (
               SELECT keyword,
                      COUNT(*) AS count,
                      COUNT(DISTINCT doc_id) AS document_count
               FROM filtered GROUP BY keyword
            ),
            rep AS (
               SELECT keyword, clause AS representative_clause FROM (
                  SELECT keyword, clause,
                         ROW_NUMBER() OVER (PARTITION BY keyword ORDER BY length(clause) DESC, clause) AS rn
                  FROM filtered
               ) WHERE rn = 1
            ),
            dom_sent AS (
               SELECT keyword, sentiment AS dominant_sentiment, c AS dom_c FROM (
                  SELECT keyword, sentiment, c,
                         ROW_NUMBER() OVER (PARTITION BY keyword ORDER BY c DESC, sentiment) AS rn
                  FROM ks
               ) WHERE rn = 1
            ),
            dom_asp AS (
               SELECT keyword, aspect AS top_aspect FROM (
                  SELECT keyword, aspect, c,
                         ROW_NUMBER() OVER (PARTITION BY keyword ORDER BY c DESC, aspect) AS rn
                  FROM ka
               ) WHERE rn = 1
            )
            SELECT b.keyword,
                   b.count,
                   b.document_count,
                   s.dominant_sentiment,
                   ROUND(CAST(s.dom_c AS DOUBLE) / b.count, 4) AS dominant_sentiment_ratio,
                   a.top_aspect,
                   r.representative_clause
            FROM base b
            JOIN dom_sent s USING (keyword)
            JOIN dom_asp a USING (keyword)
            JOIN rep r USING (keyword)
            ORDER BY b.count DESC, b.keyword
            LIMIT {limit} OFFSET {offset}"""
        items = _fetch_text_rows(con, item_query)
        return {"summary": summary, "total": filtered_total, "items": items}
    finally:
        con.close()


def _load_clause_grouped(
    con, source: str, limit: int, offset: int, q: str
) -> tuple[int, list[dict[str, Any]]]:
    """Go loadClauseGroupedKeywords — 절 텍스트 dedup + 키워드 배열(절 내 순서)."""
    clause_filter = ""
    if q:
        esc = _esc(q)
        clause_filter = (
            f"AND clause IN (SELECT DISTINCT clause FROM {source} "
            f"WHERE keyword IS NOT NULL AND (clause ILIKE '%{esc}%' OR keyword ILIKE '%{esc}%'))"
        )
    total = _scalar_count(
        con,
        f"SELECT COUNT(DISTINCT clause) FROM {source} WHERE keyword IS NOT NULL {clause_filter}",
    )
    item_query = f"""
        WITH base AS (
           SELECT clause, keyword, clause_id, keyword_rank_in_clause AS rk
           FROM {source}
           WHERE keyword IS NOT NULL {clause_filter}
        ),
        kw AS (
           SELECT clause, keyword, MIN(rk) AS rk FROM base GROUP BY clause, keyword
        ),
        occ AS (
           SELECT clause, COUNT(DISTINCT clause_id) AS occurrence_count FROM base GROUP BY clause
        ),
        grouped AS (
           SELECT clause,
                  CAST(to_json(list(keyword ORDER BY rk, keyword)) AS VARCHAR) AS keywords,
                  COUNT(*) AS keyword_count
           FROM kw GROUP BY clause
        )
        SELECT g.clause, g.keywords, o.occurrence_count
        FROM grouped g JOIN occ o ON g.clause = o.clause
        ORDER BY g.keyword_count DESC, g.clause
        LIMIT {limit} OFFSET {offset}"""
    rows = _fetch_text_rows(con, item_query)
    items = []
    for row in rows:
        items.append(
            {
                "clause": row["clause"],
                "keywords": _decode_string_array(row["keywords"]),
                "occurrence_count": row["occurrence_count"],
            }
        )
    return total, items


def _decode_string_array(value: Any) -> list[str]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        decoded = json.loads(value)
    except ValueError:
        return []
    if not isinstance(decoded, list):
        return []
    return [item for item in decoded if isinstance(item, str)]


def _keyword_filter(aspect: str, sentiment: str, q: str) -> str:
    conds = []
    if aspect:
        conds.append(f"aspect = '{_esc(aspect)}'")
    if sentiment:
        conds.append(f"sentiment = '{_esc(sentiment)}'")
    if q:
        esc = _esc(q)
        conds.append(f"(keyword ILIKE '%{esc}%' OR clause ILIKE '%{esc}%')")
    return ("WHERE " + " AND ".join(conds)) if conds else ""


def _top_keywords_by_sentiment(con, source: str, sentiment: str) -> list[dict[str, Any]]:
    query = f"""
        SELECT keyword, COUNT(*) AS count
        FROM {source}
        WHERE sentiment = '{_esc(sentiment)}' AND keyword IS NOT NULL
        GROUP BY keyword
        ORDER BY count DESC, keyword
        LIMIT {_TOP_KEYWORD_CARD_N}"""
    return _fetch_text_rows(con, query)


def _aggregate_aspect_sentiment_keywords(con, source: str) -> dict[str, Any]:
    """Go aggregateAspectSentimentKeywords — aspect × 긍/부 top30 + weight(0~1)."""
    query = f"""
        WITH counts AS (
           SELECT aspect, sentiment, keyword, COUNT(*) AS c
           FROM {source}
           WHERE sentiment IN ('positive', 'negative') AND keyword IS NOT NULL
           GROUP BY aspect, sentiment, keyword
        ),
        ranked AS (
           SELECT aspect, sentiment, keyword, c,
                  ROW_NUMBER() OVER (PARTITION BY aspect, sentiment ORDER BY c DESC, keyword) AS rn,
                  MAX(c) OVER (PARTITION BY aspect, sentiment) AS maxc
           FROM counts
        )
        SELECT aspect, sentiment, keyword, c AS count,
               ROUND(CAST(c AS DOUBLE) / maxc, 4) AS weight
        FROM ranked
        WHERE rn <= {_WORD_CLOUD_TOP_N}
        ORDER BY aspect, sentiment, c DESC, keyword"""
    rows = _fetch_text_rows(con, query)
    out: dict[str, Any] = {}
    for row in rows:
        aspect = str(row["aspect"])
        sentiment = str(row["sentiment"])
        out.setdefault(aspect, {}).setdefault(sentiment, []).append(
            {"keyword": row["keyword"], "count": row["count"], "weight": row["weight"]}
        )
    return out


def _aggregate_grouped_counts_where(
    con, source: str, group_column: str, where: str
) -> tuple[int, dict[str, int]]:
    rows = con.execute(
        f"SELECT {group_column} AS grp, COUNT(*) AS cnt FROM {source} {where} GROUP BY {group_column}"
    ).fetchall()
    result: dict[str, int] = {}
    total = 0
    for key_raw, cnt in rows:
        result[_normalize_key(key_raw)] = int(cnt)
        total += int(cnt)
    return total, result


def _scalar_count(con, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def _fetch_text_rows(con, query: str) -> list[dict[str, Any]]:
    """Go scanArtifactRows(NullString) 대응 — 모든 값을 문자열/null로 (count/
    ratio/weight 포함, database/sql convertAssign과 동일 문자열화)."""
    cursor = con.execute(query)
    names = [desc[0] for desc in cursor.description]
    rows = []
    for values in cursor.fetchall():
        rows.append({name: _null_string(value) for name, value in zip(names, values)})
    return rows


def _null_string(value: Any) -> Any:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        # Go strconv.FormatFloat(v,'g',-1,64) — 정수값은 소수부 없이("1"), 그 외 최단 표현.
        return str(int(value)) if value.is_integer() else repr(value)
    return str(value)


def _dictionary_source(ref: str, rules: list[Any]) -> str:
    """Go buildKeywordDictionarySource — block(WHERE NOT IN) + synonym(CASE REPLACE)."""
    base = _jsonl_source(ref)
    blocked: list[str] = []
    syn_by_target: dict[str, list[str]] = {}
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("active") is not True:
            continue
        rule_type = str(rule.get("rule_type") or "")
        source_term = str(rule.get("source_term") or "")
        if not source_term:
            continue
        if rule_type == "block":
            blocked.append(source_term)
        elif rule_type == "synonym":
            target_term = str(rule.get("target_term") or "")
            if target_term:
                syn_by_target.setdefault(target_term, []).append(source_term)
    if not blocked and not syn_by_target:
        return base

    select_clause = "*"
    if syn_by_target:
        whens = [
            f"WHEN keyword IN ({_quote_terms(sources)}) THEN '{_esc(target)}'"
            for target, sources in syn_by_target.items()
        ]
        select_clause = f"* REPLACE (CASE {' '.join(whens)} ELSE keyword END AS keyword)"
    where = ""
    if blocked:
        where = f" WHERE keyword IS NULL OR keyword NOT IN ({_quote_terms(blocked)})"
    return f"(SELECT {select_clause} FROM {base}{where})"


def _quote_terms(terms: list[str]) -> str:
    return ", ".join(f"'{_esc(term)}'" for term in terms)
