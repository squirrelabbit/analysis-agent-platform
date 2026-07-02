from __future__ import annotations

"""artifact_views task — doc_genuineness / clause_label artifact 화면 view 집계.

Go control-plane ``loadDocGenuinenessArtifact`` / ``loadDocGenuinenessVerifyArtifact`` /
``loadClauseLabelArtifact`` / ``loadClauseLabelVerifyArtifact``(DuckDB on-demand)의
worker 이전 (ADR-024: artifact 파일 집계는 worker). Node control-plane이
GET /versions/{vid}/doc_genuineness · /clause_label에서 호출한다.

worker는 artifact 파일 집계(summary/items/total)만 담당한다 — status/progress/
applied 합성과 override overlay는 control-plane(Node)이 DB를 보고 수행한다.

응답: {summary, prompt_version, total, items}
- summary: 필터 미적용 전체 분포. total은 필터 적용 행 수(서버 페이징 기준).
- items 값 타입은 Go 로더와 동일 — 단일 모드는 문자열/null(Go NullString scan),
  verify 모드는 bool/int/object 합성 후.
"""

import json
import math
from typing import Any

from .obs import get

_LOG = get(__name__)


def run_doc_genuineness_view(payload: dict[str, Any]) -> dict[str, Any]:
    ref, clean_ref, limit, offset, mode = _common_args(payload)
    genuineness = str(payload.get("genuineness") or "").strip()
    disagreement_only = payload.get("disagreement_only") is True
    needs_review_only = payload.get("needs_review_only") is True
    if mode == "verify":
        result = _load_doc_genuineness_verify(
            ref, clean_ref, limit, offset, genuineness, disagreement_only, needs_review_only
        )
    else:
        result = _load_doc_genuineness(ref, clean_ref, limit, offset, genuineness)
    _LOG.info(
        "artifact_view.completed",
        view="doc_genuineness",
        mode=mode,
        total=result["total"],
        item_count=len(result["items"]),
    )
    return result


def run_clause_label_view(payload: dict[str, Any]) -> dict[str, Any]:
    ref, clean_ref, limit, offset, mode = _common_args(payload)
    aspect = str(payload.get("aspect") or "").strip()
    sentiment = str(payload.get("sentiment") or "").strip()
    disagreement_only = payload.get("disagreement_only") is True
    needs_review_only = payload.get("needs_review_only") is True
    if mode == "verify":
        result = _load_clause_label_verify(
            ref, clean_ref, limit, offset, aspect, sentiment, disagreement_only, needs_review_only
        )
    else:
        result = _load_clause_label(ref, clean_ref, limit, offset, aspect, sentiment)
    _LOG.info(
        "artifact_view.completed",
        view="clause_label",
        mode=mode,
        total=result["total"],
        item_count=len(result["items"]),
    )
    return result


def _common_args(payload: dict[str, Any]) -> tuple[str, str, int, int, str]:
    if not isinstance(payload, dict):
        raise ValueError("artifact view payload must be an object")
    ref = str(payload.get("ref") or "").strip()
    if not ref:
        raise ValueError("artifact view payload requires 'ref'")
    clean_ref = str(payload.get("clean_ref") or "").strip()
    try:
        limit = int(payload.get("limit") or 100)
        offset = int(payload.get("offset") or 0)
    except (TypeError, ValueError):
        raise ValueError("artifact view 'limit'/'offset' must be integers")
    mode = str(payload.get("mode") or "single").strip()
    return ref, clean_ref, limit, offset, mode


# ── DuckDB 공통 (Go dataset_artifact_views.go helpers 대응) ──────────────────


def _connect():
    import duckdb  # heavy dep — lazy import

    return duckdb.connect()


def _esc(value: str) -> str:
    return value.replace("'", "''")


def _jsonl_source(ref: str) -> str:
    return f"read_json('{_esc(ref)}', format='newline_delimited')"


def _parquet_source(ref: str) -> str:
    return f"read_parquet('{_esc(ref)}')"


def _aggregate_grouped_counts(con, source: str, group_column: str) -> tuple[int, dict[str, int]]:
    """Go aggregateGroupedCounts — NULL/빈 키는 'unknown'으로 정규화 + 병합."""
    rows = con.execute(
        f"SELECT {group_column}, COUNT(*) AS cnt FROM {source} GROUP BY {group_column}"
    ).fetchall()
    result: dict[str, int] = {}
    total = 0
    for key_raw, cnt in rows:
        key = "unknown"
        if key_raw is not None:
            trimmed = str(key_raw).strip()
            if trimmed:
                key = trimmed
        result[key] = result.get(key, 0) + int(cnt)
        total += int(cnt)
    return total, result


_STANDARD_SENTIMENTS = ("positive", "negative", "neutral")


def _percent_of(count: int, total: int) -> Any:
    """Go percentOf — 소수 1자리 half-away-from-zero 반올림. 정수값은 int로
    (Go float64 marshal이 50.0을 '50'으로 내보내는 것과 일치)."""
    if total <= 0:
        return 0
    value = math.floor(count / total * 1000 + 0.5) / 10
    return int(value) if float(value).is_integer() else value


def _aggregate_aspect_sentiment(con, source: str) -> dict[str, Any]:
    """Go aggregateAspectSentiment — aspect × sentiment 교차 분포 + percent."""
    rows = con.execute(
        f"SELECT aspect, sentiment, COUNT(*) AS cnt FROM {source} GROUP BY aspect, sentiment"
    ).fetchall()
    counts: dict[str, dict[str, int]] = {}
    totals: dict[str, int] = {}
    for aspect_raw, sentiment_raw, cnt in rows:
        aspect = _normalize_key(aspect_raw)
        sentiment = _normalize_key(sentiment_raw)
        counts.setdefault(aspect, {})
        counts[aspect][sentiment] = counts[aspect].get(sentiment, 0) + int(cnt)
        totals[aspect] = totals.get(aspect, 0) + int(cnt)

    result: dict[str, Any] = {}
    for aspect, sentiment_counts in counts.items():
        total = totals[aspect]
        merged = {s: 0 for s in _STANDARD_SENTIMENTS}
        merged.update(sentiment_counts)
        dist = {
            s: {"count": c, "percent": _percent_of(c, total)} for s, c in merged.items()
        }
        result[aspect] = {"total": total, "sentiment": dist}
    return result


def _normalize_key(raw: Any) -> str:
    if raw is not None:
        trimmed = str(raw).strip()
        if trimmed:
            return trimmed
    return "unknown"


def _first_string_value(con, source: str, column: str) -> str:
    row = con.execute(f"SELECT {column} FROM {source} LIMIT 1").fetchone()
    if row is None or row[0] is None:
        return ""
    return str(row[0]).strip()


def _count_rows_where(con, source: str, where: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {source} {where}").fetchone()[0])


def _parquet_source_json_expr(con, clean_source: str) -> str:
    """Go parquetSourceJSONExpr — source_json 컬럼 부재 parquet(legacy)이면 NULL로."""
    n = con.execute(
        f"SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM {clean_source}) WHERE column_name = 'source_json'"
    ).fetchone()[0]
    return "c.source_json" if n else "NULL AS source_json"


def _fetch_rows(con, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    names = [desc[0] for desc in cursor.description]
    return [dict(zip(names, values)) for values in cursor.fetchall()]


def _text_or_none(value: Any) -> Any:
    """Go scanArtifactRows(NullString)와 동일 — 값을 문자열/null로."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def extract_source_url(source_json: Any) -> str:
    """Go extractSourceURL — 값 전체가 http(s) URL인 첫 컬럼(키 정렬)을 원문 URL로."""
    if not isinstance(source_json, str) or not source_json.strip():
        return ""
    try:
        record = json.loads(source_json)
    except ValueError:
        return ""
    if not isinstance(record, dict):
        return ""
    for key in sorted(record.keys()):
        value = record[key]
        if not isinstance(value, str):
            continue
        value = value.strip()
        if value.startswith(("https://", "http://")) and not any(
            ch in value for ch in " \t\r\n"
        ):
            return value
    return ""


def _verify_object(value: Any) -> dict[str, Any] | None:
    """Go docVerifyObject — to_json 문자열을 객체로. null/전부-null 객체는 None."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text == "null":
        return None
    try:
        obj = json.loads(text)
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    if not any(v is not None for v in obj.values()):
        return None
    return obj


def _verify_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return False


def _verify_int(value: Any) -> Any:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


# ── doc_genuineness (단일 모드) — Go loadDocGenuinenessArtifact ───────────────


def _load_doc_genuineness(
    ref: str, clean_ref: str, limit: int, offset: int, genuineness: str
) -> dict[str, Any]:
    con = _connect()
    try:
        source = _jsonl_source(ref)
        total, by_genuineness = _aggregate_grouped_counts(con, source, "genuineness")
        summary = {"total": total, "genuineness": by_genuineness}
        prompt = _first_string_value(con, source, "prompt_version")

        where_source, where_join = "", ""
        filtered_total = total
        if genuineness:
            esc = _esc(genuineness)
            where_source = f"WHERE genuineness = '{esc}'"
            where_join = f"WHERE dg.genuineness = '{esc}'"
            filtered_total = _count_rows_where(con, source, where_source)

        if clean_ref:
            clean_source = _parquet_source(clean_ref)
            source_json_expr = _parquet_source_json_expr(con, clean_source)
            item_query = f"""
                SELECT dg.doc_id, dg.genuineness, dg.reason, dg.source, c.cleaned_text, {source_json_expr}
                FROM {source} AS dg
                LEFT JOIN {clean_source} AS c ON dg.doc_id = c.row_id
                {where_join}
                ORDER BY dg.doc_id
                LIMIT {limit} OFFSET {offset}"""
            try:
                raw_items = _fetch_rows(con, item_query)
            except Exception as exc:
                # Go와 동일 — JOIN 실패(row_id 부재 등) 시 본문 없이 fallback.
                _LOG.warning(
                    "artifact_view.doc_genuineness.cleaned_text_join_failed",
                    clean_ref=clean_ref,
                    error_message=str(exc),
                )
                return _doc_genuineness_without_body(
                    con, source, summary, prompt, filtered_total, limit, offset, where_source
                )
            items = []
            for raw in raw_items:
                item = {
                    "doc_id": _text_or_none(raw["doc_id"]),
                    "genuineness": _text_or_none(raw["genuineness"]),
                    "reason": _text_or_none(raw["reason"]),
                    "source": _text_or_none(raw["source"]),
                    "cleaned_text": _text_or_none(raw["cleaned_text"]),
                    "source_url": extract_source_url(raw["source_json"]),
                }
                items.append(item)
            return {"summary": summary, "prompt_version": prompt, "total": filtered_total, "items": items}

        return _doc_genuineness_without_body(
            con, source, summary, prompt, filtered_total, limit, offset, where_source
        )
    finally:
        con.close()


def _doc_genuineness_without_body(
    con, source: str, summary: dict[str, Any], prompt: str,
    filtered_total: int, limit: int, offset: int, where: str,
) -> dict[str, Any]:
    item_query = f"""
        SELECT doc_id, genuineness, reason, source
        FROM {source}
        {where}
        ORDER BY doc_id
        LIMIT {limit} OFFSET {offset}"""
    raw_items = _fetch_rows(con, item_query)
    items = [
        {
            "doc_id": _text_or_none(raw["doc_id"]),
            "genuineness": _text_or_none(raw["genuineness"]),
            "reason": _text_or_none(raw["reason"]),
            "source": _text_or_none(raw["source"]),
        }
        for raw in raw_items
    ]
    return {"summary": summary, "prompt_version": prompt, "total": filtered_total, "items": items}


# ── doc_genuineness (verify 모드, ADR-026) — Go loadDocGenuinenessVerifyArtifact ─


def _load_doc_genuineness_verify(
    ref: str, clean_ref: str, limit: int, offset: int,
    genuineness: str, disagreement_only: bool, needs_review_only: bool,
) -> dict[str, Any]:
    con = _connect()
    try:
        source = _jsonl_source(ref)
        total, by_final = _aggregate_grouped_counts(con, source, "final_label")
        summary = {"total": total, "genuineness": by_final}
        prompt = _first_string_value(con, source, "prompt_version")

        conds_source, conds_join = [], []
        if genuineness:
            esc = _esc(genuineness)
            conds_source.append(f"final_label = '{esc}'")
            conds_join.append(f"dg.final_label = '{esc}'")
        if disagreement_only:
            conds_source.append("is_disagreement = true")
            conds_join.append("dg.is_disagreement = true")
        if needs_review_only:
            conds_source.append("needs_review = true")
            conds_join.append("dg.needs_review = true")
        where_source, where_join = "", ""
        filtered_total = total
        if conds_source:
            where_source = "WHERE " + " AND ".join(conds_source)
            where_join = "WHERE " + " AND ".join(conds_join)
            filtered_total = _count_rows_where(con, source, where_source)

        def select_expr(prefix: str) -> str:
            p = prefix
            return (
                f"{p}doc_id, {p}final_label, CAST({p}needs_review AS VARCHAR) AS needs_review, "
                f"{p}resolution, CAST({p}is_disagreement AS VARCHAR) AS is_disagreement, "
                f"CAST(to_json({p}model_a_result) AS VARCHAR) AS model_a_result, "
                f"CAST(to_json({p}model_b_result) AS VARCHAR) AS model_b_result, "
                f"CAST(to_json({p}judge_result) AS VARCHAR) AS judge_result"
            )

        if clean_ref:
            clean_source = _parquet_source(clean_ref)
            source_json_expr = _parquet_source_json_expr(con, clean_source)
            item_query = f"""
                SELECT {select_expr('dg.')}, c.cleaned_text, {source_json_expr}
                FROM {source} AS dg LEFT JOIN {clean_source} AS c ON dg.doc_id = c.row_id
                {where_join} ORDER BY dg.doc_id LIMIT {limit} OFFSET {offset}"""
        else:
            item_query = f"""
                SELECT {select_expr('')}, NULL AS cleaned_text, NULL AS source_json
                FROM {source} {where_source} ORDER BY doc_id LIMIT {limit} OFFSET {offset}"""
        raw_items = _fetch_rows(con, item_query)

        items = []
        for raw in raw_items:
            final = _text_or_none(raw["final_label"])
            judge = _verify_object(raw["judge_result"])
            model_a = _verify_object(raw["model_a_result"])
            reason = ""
            if judge is not None:
                r = judge.get("reason")
                reason = r if isinstance(r, str) else ""
            if not reason and model_a is not None:
                r = model_a.get("reason")
                reason = r if isinstance(r, str) else ""
            item = {
                "doc_id": _text_or_none(raw["doc_id"]),
                "final_label": final,
                "genuineness": final if isinstance(final, str) else "",  # 화면 호환 — effective label
                "resolution": _text_or_none(raw["resolution"]),
                "needs_review": _verify_bool(raw["needs_review"]),
                "is_disagreement": _verify_bool(raw["is_disagreement"]),
                "cleaned_text": _text_or_none(raw["cleaned_text"]),
                "source_url": extract_source_url(raw["source_json"]) if isinstance(raw["source_json"], str) else "",
                "model_a_result": model_a,
                "model_b_result": _verify_object(raw["model_b_result"]),
                "judge_result": judge,
                "reason": reason,
            }
            items.append(item)
        return {"summary": summary, "prompt_version": prompt, "total": filtered_total, "items": items}
    finally:
        con.close()


# ── clause_label (단일 모드) — Go loadClauseLabelArtifact ─────────────────────


def _clause_summary(con, source: str) -> dict[str, Any]:
    total, by_sentiment = _aggregate_grouped_counts(con, source, "sentiment")
    _, by_aspect = _aggregate_grouped_counts(con, source, "aspect")
    aspect_sentiment = _aggregate_aspect_sentiment(con, source)
    return {
        "total": total,
        "sentiment": by_sentiment,
        "aspect": by_aspect,
        "aspect_sentiment": aspect_sentiment,
    }


def _clause_filter(aspect: str, sentiment: str) -> str:
    conds = []
    if aspect:
        conds.append(f"aspect = '{_esc(aspect)}'")
    if sentiment:
        conds.append(f"sentiment = '{_esc(sentiment)}'")
    return ("WHERE " + " AND ".join(conds)) if conds else ""


def _load_clause_label(
    ref: str, clean_ref: str, limit: int, offset: int, aspect: str, sentiment: str
) -> dict[str, Any]:
    con = _connect()
    try:
        source = _jsonl_source(ref)
        summary = _clause_summary(con, source)
        total = summary["total"]
        prompt = _first_string_value(con, source, "prompt_version")

        where = _clause_filter(aspect, sentiment)
        filtered_total = total
        if where:
            filtered_total = _count_rows_where(con, source, where)

        select_clause = "SELECT doc_id, clause_id, clause, sentiment, aspect, source FROM numbered"
        columns = ["doc_id", "clause_id", "clause", "sentiment", "aspect", "source"]
        if clean_ref:
            clean_source = _parquet_source(clean_ref)
            select_clause = (
                "SELECT n.doc_id AS doc_id, n.clause_id, n.clause, n.sentiment, n.aspect, n.source, c.cleaned_text"
                f" FROM numbered AS n LEFT JOIN {clean_source} AS c ON n.doc_id = c.row_id"
            )
            columns.append("cleaned_text")
        item_query = f"""
            WITH ordered AS (
               SELECT *, ROW_NUMBER() OVER () AS _rn
               FROM {source}
            ),
            numbered AS (
               SELECT
                  doc_id,
                  doc_id || '-' || CAST(ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY _rn) - 1 AS VARCHAR) AS clause_id,
                  clause, sentiment, aspect, source, _rn
               FROM ordered
            )
            {select_clause}
            {where}
            ORDER BY _rn
            LIMIT {limit} OFFSET {offset}"""
        raw_items = _fetch_rows(con, item_query)
        items = [{col: _text_or_none(raw[col]) for col in columns} for raw in raw_items]
        return {"summary": summary, "prompt_version": prompt, "total": filtered_total, "items": items}
    finally:
        con.close()


# ── clause_label (verify 모드, ADR-028) — Go loadClauseLabelVerifyArtifact ────


def _load_clause_label_verify(
    ref: str, clean_ref: str, limit: int, offset: int,
    aspect: str, sentiment: str, disagreement_only: bool, needs_review_only: bool,
) -> dict[str, Any]:
    con = _connect()
    try:
        source = _jsonl_source(ref)
        summary = _clause_summary(con, source)
        total = summary["total"]
        _, by_resolution = _aggregate_grouped_counts(con, source, "resolution")
        needs_review_count = _count_rows_where(con, source, "WHERE needs_review = true")
        summary["resolution"] = by_resolution
        summary["needs_review_count"] = needs_review_count

        conds = []
        if aspect:
            conds.append(f"aspect = '{_esc(aspect)}'")
        if sentiment:
            conds.append(f"sentiment = '{_esc(sentiment)}'")
        if disagreement_only:
            conds.append("resolution <> 'agree'")
        if needs_review_only:
            conds.append("needs_review = true")
        where = ("WHERE " + " AND ".join(conds)) if conds else ""

        filtered_total = total
        if where:
            filtered_total = _count_rows_where(con, source, where)

        select_clause = (
            "SELECT doc_id, clause_id, clause, sentiment, aspect, source, resolution,"
            " needs_review, sentence_index, chunk_index,"
            " model_a_result, model_b_result, judge_result FROM numbered"
        )
        with_clean = bool(clean_ref)
        if with_clean:
            clean_source = _parquet_source(clean_ref)
            select_clause = (
                "SELECT n.doc_id AS doc_id, n.clause_id, n.clause, n.sentiment, n.aspect, n.source, n.resolution,"
                " n.needs_review, n.sentence_index, n.chunk_index,"
                " n.model_a_result, n.model_b_result, n.judge_result, c.cleaned_text"
                f" FROM numbered AS n LEFT JOIN {clean_source} AS c ON n.doc_id = c.row_id"
            )
        item_query = f"""
            WITH ordered AS (
               SELECT *, ROW_NUMBER() OVER () AS _rn
               FROM {source}
            ),
            numbered AS (
               SELECT
                  doc_id,
                  doc_id || '-' || CAST(ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY _rn) - 1 AS VARCHAR) AS clause_id,
                  clause, sentiment, aspect, source, resolution,
                  CAST(needs_review AS VARCHAR) AS needs_review,
                  CAST(sentence_index AS VARCHAR) AS sentence_index,
                  CAST(chunk_index AS VARCHAR) AS chunk_index,
                  CAST(to_json(model_a_result) AS VARCHAR) AS model_a_result,
                  CAST(to_json(model_b_result) AS VARCHAR) AS model_b_result,
                  CAST(to_json(judge_result) AS VARCHAR) AS judge_result,
                  _rn
               FROM ordered
            )
            {select_clause}
            {where}
            ORDER BY _rn
            LIMIT {limit} OFFSET {offset}"""
        raw_items = _fetch_rows(con, item_query)

        items = []
        for raw in raw_items:
            item = {
                "doc_id": _text_or_none(raw["doc_id"]),
                "clause_id": _text_or_none(raw["clause_id"]),
                "clause": _text_or_none(raw["clause"]),
                "sentiment": _text_or_none(raw["sentiment"]),
                "aspect": _text_or_none(raw["aspect"]),
                "source": _text_or_none(raw["source"]),
                "resolution": _text_or_none(raw["resolution"]),
                "needs_review": _verify_bool(raw["needs_review"]),
                "sentence_index": _verify_int(raw["sentence_index"]),
                "chunk_index": _verify_int(raw["chunk_index"]),
                "model_a_result": _verify_object(raw["model_a_result"]),
                "model_b_result": _verify_object(raw["model_b_result"]),
                "judge_result": _verify_object(raw["judge_result"]),
            }
            if with_clean:
                item["cleaned_text"] = _text_or_none(raw["cleaned_text"])
            items.append(item)
        # verify artifact는 top-level prompt_version이 없어 빈 값 반환(Go와 동일 —
        # control-plane이 summary metadata에서 회수).
        return {"summary": summary, "prompt_version": "", "total": filtered_total, "items": items}
    finally:
        con.close()
