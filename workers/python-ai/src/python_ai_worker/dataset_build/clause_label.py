from __future__ import annotations

"""dataset_clause_label entry point — cleaned doc(title + body)을 LLOA 한 호출로
festival-related 절 추출 + sentiment + aspect(7종) 라벨링.

ADR-018 (β2 / 5/19) 5-step pipeline STEP 3. silverone 5/20 prompt 결정으로
schema를 clause/sentiment/aspect 3 필드로 단순화 (clause_index / *_reason 제거).
input source는 clean_artifact_ref (cleaned doc parquet/jsonl).

성능: ThreadPoolExecutor(max_workers=8) 병렬 호출 — 50 docs sequential ~11분
→ 병렬 ~1.5분. silverone 5/20 결정. ``concurrency`` payload key로 override 가능.

Optional: ``include_genuineness=["genuine_review","mixed"]``로 doc_genuineness
artifact를 읽어 필터링한다. default는 *모든 doc 처리*.
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .. import runtime as rt
from ..config import load_config
from ..config_paths import resolve_config_dir
from ..prompt_options import load_prompt_body
from ..clients.lloa import LloaClient, LloaConfig, LloaResponseParseError
from ..obs import get, skill_handler
from ..taxonomies import load_taxonomy, render_aspect_taxonomy_block
from ._common import write_progress

LOGGER = get(__name__)

# silverone 2026-06-02 — prompt는 task-folder(config/prompts/clause_label/)에서
# resolve. default version은 그 폴더의 index.yaml. _PROMPT_VERSION_DEFAULT는
# artifact 저장 라벨로 기존 계약 유지(파일 stem 'v3'과 별개).
_PROMPT_TASK = "clause_label"
_PROMPT_VERSION_DEFAULT = "dataset-clause-label-v3"
_ALLOWED_SENTIMENT = {"positive", "negative", "neutral"}

# taxonomy-driven config Phase 2-A (2026-05-27) — _ALLOWED_ASPECT를
# config/taxonomies/festival-v2.json에서 derive. Phase 3에서 dataset_version
# metadata 기반 동적 lookup으로 전환 예정. 현재는 single taxonomy 고정.
DEFAULT_CLAUSE_LABEL_TAXONOMY_ID = "festival-v2"
_TAXONOMY = load_taxonomy(DEFAULT_CLAUSE_LABEL_TAXONOMY_ID)
_ALLOWED_ASPECT: frozenset[str] = _TAXONOMY.aspect_keys_set
_FALLBACK_ASPECT: str = _TAXONOMY.fallback_aspect
_ALLOWED_GENUINENESS_FILTER = {"genuine_review", "mixed", "non_review", "uncertain"}
# 5/20 결정 — default ON. doc_genuineness 결과 중 genuine_review + mixed만
# clause_label로 보낸다 (non_review는 LLOA 호출 절약 + 분석 가치 0). caller가
# 명시적으로 ``include_genuineness=[]`` (빈 list) 또는 ``include_genuineness=
# ["genuine_review","mixed","non_review"]`` 보내면 모든 doc 처리 가능.
_DEFAULT_INCLUDE_GENUINENESS: list[str] = ["genuine_review", "mixed"]
_DEFAULT_CONCURRENCY = 8


def _find_prompt_path(name: str) -> Path | None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "config" / "prompts" / name
        if candidate.is_file():
            return candidate
    cfg = resolve_config_dir()
    if cfg:
        candidate = cfg / "prompts" / name
        if candidate.is_file():
            return candidate
    return None


def _strip_front_matter(template: str) -> str:
    text = template.lstrip()
    if not text.startswith("---"):
        return template
    body = text[3:]
    end = body.find("\n---")
    if end < 0:
        return template
    return body[end + 4 :].lstrip("\n")


_ASPECT_TAXONOMY_PLACEHOLDER = "{{ASPECT_TAXONOMY}}"


def _inject_taxonomy(template: str) -> str:
    """Phase 2-B (2026-05-27) — prompt template의 ``{{ASPECT_TAXONOMY}}``를
    config/taxonomies/festival-v2.json에서 렌더한 markdown table로 치환한다.
    placeholder가 없는 inline prompt(옛 호환)는 그대로 통과시킨다."""

    if _ASPECT_TAXONOMY_PLACEHOLDER not in template:
        return template
    rendered = render_aspect_taxonomy_block(_TAXONOMY)
    return template.replace(_ASPECT_TAXONOMY_PLACEHOLDER, rendered)


# silverone 2026-05-28 — clause_label subject 변수화. doc_genuineness PR-α2
# 패턴을 그대로 이식. 다만 doc_genuineness와 달리 subject metadata가 없으면
# festival default로 fallback (옛 dataset 호환). recruitment_keywords는
# doc_genuineness 전용이라 본 prompt에는 inject하지 않는다.
_DEFAULT_SUBJECT_CONFIG: dict[str, Any] = {
    "subject_name": "축제",
    "subject_aliases": [],
    "subject_type": "festival",
}


def _extract_subject_config(payload: dict[str, Any]) -> dict[str, Any]:
    """payload['doc_genuineness']에서 subject 변수를 추출한다.

    공유 정책 (2026-05-28): dataset.metadata.doc_genuineness 키를 doc_genuineness
    skill과 그대로 공유한다. control plane이 payload['doc_genuineness']로 inject
    하면 여기서 정규화한다. metadata가 없거나 subject_name이 비면 festival
    default로 fallback — clause_label은 옛 dataset도 처리해야 한다 (5/28 결정).
    """
    raw = payload.get("doc_genuineness")
    if not isinstance(raw, dict):
        return dict(_DEFAULT_SUBJECT_CONFIG)
    subject_name = str(raw.get("subject_name") or "").strip()
    if not subject_name:
        return dict(_DEFAULT_SUBJECT_CONFIG)
    aliases = [str(item).strip() for item in raw.get("subject_aliases") or [] if str(item).strip()]
    subject_type = str(raw.get("subject_type") or "generic").strip() or "generic"
    return {
        "subject_name": subject_name,
        "subject_aliases": aliases,
        "subject_type": subject_type,
    }


def _render_quoted_list(values: list[str]) -> str:
    if not values:
        return ""
    return ", ".join(f"'{v}'" for v in values)


_CONDITIONAL_BLOCK_PATTERN = re.compile(
    r"\{\{#if (?P<var>\w+)\}\}(?P<body>.*?)\{\{/if\}\}", re.DOTALL
)


def _render_subject_prompt(template: str, config: dict[str, Any]) -> str:
    """``{{subject_name}}`` / ``{{#if subject_aliases}}...{{/if}}`` 치환.

    doc_genuineness ``_render_prompt``와 같은 문법. subject_type은 prompt 본문에
    노출되지 않으며 summary.applied snapshot용으로만 사용된다.
    """
    truthy = {
        "subject_name": bool(config.get("subject_name")),
        "subject_aliases": bool(config.get("subject_aliases")),
    }

    def repl_block(match: re.Match) -> str:
        var = match.group("var")
        return match.group("body") if truthy.get(var, False) else ""

    rendered = _CONDITIONAL_BLOCK_PATTERN.sub(repl_block, template)

    substitutions = {
        "subject_name": str(config.get("subject_name") or ""),
        "subject_aliases": _render_quoted_list(list(config.get("subject_aliases") or [])),
    }
    for key, value in substitutions.items():
        rendered = rendered.replace("{{" + key + "}}", value)
    return rendered


def _load_prompt_template(payload: dict[str, Any]) -> tuple[str, str]:
    inline = payload.get("clause_label_prompt_content")
    if isinstance(inline, str) and inline.strip():
        version = str(payload.get("clause_label_prompt_version") or "request_inline").strip()
        return _inject_taxonomy(inline), version
    # silverone 2026-06-02 — task-folder prompt resolver로 전환. 기본 version은
    # config/prompts/clause_label/index.yaml의 default. artifact 저장용
    # prompt_version 라벨(_PROMPT_VERSION_DEFAULT)은 기존 계약 유지.
    body, _stem = load_prompt_body(_PROMPT_TASK)
    return _inject_taxonomy(body), _PROMPT_VERSION_DEFAULT


def _decode_clauses_response(body: Any) -> list[dict[str, Any]]:
    """LLOA response body를 clause list로 변환. silverone 5/20 prompt는 *최외곽
    array*를 반환하지만, LLM이 가끔 ``{"clauses": [...]}`` 또는 ``{"result": [...]}``
    형태로 wrap하는 경우도 hint로 받아줌."""
    if isinstance(body, list):
        return [item for item in body if isinstance(item, dict)]
    if isinstance(body, dict):
        for key in ("clauses", "result", "data"):
            value = body.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    raise ValueError(f"clause_label expected JSON array, got {type(body).__name__}")


def _load_genuineness_filter(payload: dict[str, Any]) -> tuple[set[str] | None, dict[str, str]]:
    """doc_genuineness artifact를 읽어 doc_id -> tier map을 반환한다.

    Default 동작 (5/20 결정): payload에 ``include_genuineness`` 키가 없으면
    ``["genuine_review", "mixed"]``로 필터링 (non_review는 LLOA 호출 절약 + 분석
    가치 0). 명시적으로 빈 list ``[]`` 보내면 모든 doc 처리 (filter off).
    명시적으로 3 tier 모두 포함하면 사실상 모든 doc 처리지만 doc_genuineness ref는
    여전히 필요.
    """
    if "include_genuineness" in payload:
        raw_filter = payload.get("include_genuineness")
        if not isinstance(raw_filter, list):
            raise ValueError("include_genuineness must be a list of genuineness tiers")
        if not raw_filter:
            # explicit opt-out — 모든 doc 처리
            return None, {}
    else:
        raw_filter = _DEFAULT_INCLUDE_GENUINENESS

    tiers: set[str] = set()
    for item in raw_filter:
        normalized = str(item or "").strip()
        if normalized not in _ALLOWED_GENUINENESS_FILTER:
            raise ValueError(
                f"invalid include_genuineness tier: {item!r} (expected one of {sorted(_ALLOWED_GENUINENESS_FILTER)})"
            )
        tiers.add(normalized)
    if not tiers:
        return None, {}
    ref = str(payload.get("doc_genuineness_ref") or "").strip()
    if not ref:
        raise ValueError(
            "include_genuineness filter active but doc_genuineness_ref missing — clause_label requires doc_genuineness artifact for filtering (5/20 default ON)"
        )
    tier_by_doc: dict[str, str] = {}
    with Path(ref).open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                LOGGER.warning(
                    "dataset_clause_label.genuineness_parse_failed",
                    error_category=type(exc).__name__,
                    error_message=str(exc),
                )
                continue
            doc_id = str(rec.get("doc_id") or "").strip()
            tier = str(rec.get("genuineness") or "").strip()
            if doc_id and tier:
                tier_by_doc[doc_id] = tier
    return tiers, tier_by_doc


def _label_doc(
    client: LloaClient,
    *,
    system_prompt: str,
    doc_id: str,
    doc_title: str,
    doc_text: str,
    max_tokens: int,
) -> list[dict[str, Any]]:
    """단일 doc LLOA 호출 + 응답 파싱. 5/20 prompt는 user에 ``제목: ...\\n본문: ...``
    plaintext 형식, output은 *최외곽 JSON array* (clauses_json wrapper 없음)."""
    user_payload = f"제목: {doc_title}\n본문: {doc_text}"
    response = client.create_json_response(
        system=system_prompt,
        user=user_payload,
        max_tokens=max_tokens,
    )
    raw_clauses = _decode_clauses_response(response.body)
    out: list[dict[str, Any]] = []
    for raw in raw_clauses:
        clause_text = str(raw.get("clause") or "").strip()
        if not clause_text:
            continue
        sentiment = str(raw.get("sentiment") or "neutral").strip()
        if sentiment not in _ALLOWED_SENTIMENT:
            sentiment = "neutral"
        aspect = str(raw.get("aspect") or _FALLBACK_ASPECT).strip()
        if aspect not in _ALLOWED_ASPECT:
            aspect = _FALLBACK_ASPECT
        out.append(
            {
                "doc_id": doc_id,
                "clause": clause_text,
                "sentiment": sentiment,
                "aspect": aspect,
            }
        )
    return out


@skill_handler("python-ai")
def run_dataset_clause_label(payload: dict[str, Any]) -> dict[str, Any]:
    """ADR-018 (β2) 5-step pipeline STEP 3 (5/20 schema 단순화 + concurrency 8).

    cleaned doc(title + body) 단위로 LLOA 병렬 호출 (default concurrency 8)에
    festival-related 절 추출 + sentiment + aspect 라벨링까지 처리. schema:
    {doc_id, clause, sentiment, aspect, prompt_version, source}.
    """
    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    clean_artifact_ref = str(payload.get("clean_artifact_ref") or "").strip()
    output_path_raw = str(payload.get("output_path") or "").strip()
    progress_path = str(payload.get("progress_path") or "").strip()
    if not dataset_version_id or not clean_artifact_ref or not output_path_raw:
        raise ValueError(
            "dataset_clause_label requires dataset_version_id, clean_artifact_ref, output_path"
        )

    output_path = Path(output_path_raw)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = time.monotonic()

    config = load_config()
    if not (config.lloa_api_key or "").strip():
        raise ValueError(
            "dataset_clause_label requires LLOA API key — set LLOA_API_KEY / WISENUT_LLOA_API_KEY / WISENUT_LLOA_MAX_V1_2_1_API_KEY"
        )

    lloa_config = LloaConfig(
        api_key=config.lloa_api_key,
        api_url=config.lloa_api_url,
        model=config.lloa_model,
        max_tokens=config.lloa_max_tokens,
        timeout_sec=config.lloa_timeout_sec,
        reasoning_effort=config.lloa_reasoning_effort,
        prepend_no_think=config.lloa_prepend_no_think,
    )
    client = LloaClient(lloa_config)

    template, prompt_version = _load_prompt_template(payload)
    subject_config = _extract_subject_config(payload)
    system_prompt = _render_subject_prompt(template, subject_config)
    max_tokens = int(payload.get("max_tokens") or 8192)
    concurrency = max(1, int(payload.get("concurrency") or _DEFAULT_CONCURRENCY))

    include_tiers, tier_by_doc = _load_genuineness_filter(payload)

    rows = rt._iter_rows(clean_artifact_ref)
    total_rows = len(rows)
    if progress_path:
        write_progress(
            progress_path,
            processed_rows=0,
            total_rows=total_rows,
            started_at=started_at,
            message="clause_label queued",
        )

    # 1) 처리 대상 doc 사전 분류 (filter / empty 처리)
    target_docs: list[tuple[int, str, str, str]] = []  # (index, doc_id, doc_title, doc_text)
    skipped_by_filter = 0
    skipped_empty = 0
    for index, row in enumerate(rows):
        doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
        cleaned_text = str(row.get("cleaned_text") or "").strip()
        if not cleaned_text:
            skipped_empty += 1
            continue
        if include_tiers is not None:
            tier = tier_by_doc.get(doc_id)
            if tier not in include_tiers:
                skipped_by_filter += 1
                continue
        doc_title = str(row.get("doc_title") or "").strip()
        target_docs.append((index, doc_id, doc_title, cleaned_text))

    # 2) 병렬 LLOA 호출 — ThreadPoolExecutor
    parse_failures = 0
    clause_count = 0
    sentiment_counts: dict[str, int] = {tier: 0 for tier in _ALLOWED_SENTIMENT}
    aspect_counts: dict[str, int] = {asp: 0 for asp in _ALLOWED_ASPECT}
    completed_docs = 0
    clauses_by_doc: dict[str, list[dict[str, Any]]] = {}

    def _process(item: tuple[int, str, str, str]) -> tuple[str, list[dict[str, Any]], Exception | None]:
        _, doc_id, doc_title, doc_text = item
        try:
            clauses = _label_doc(
                client,
                system_prompt=system_prompt,
                doc_id=doc_id,
                doc_title=doc_title,
                doc_text=doc_text,
                max_tokens=max_tokens,
            )
            return doc_id, clauses, None
        except (LloaResponseParseError, ValueError) as exc:
            return doc_id, [], exc

    if target_docs:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(_process, item): item for item in target_docs}
            for future in as_completed(futures):
                doc_id, clauses, exc = future.result()
                if exc is not None:
                    LOGGER.warning(
                        "dataset_clause_label.doc_parse_failed",
                        doc_id=doc_id,
                        error_category=type(exc).__name__,
                        error_message=str(exc),
                    )
                    parse_failures += 1
                    clauses = []
                clauses_by_doc[doc_id] = clauses
                completed_docs += 1
                if progress_path and (completed_docs % 5 == 0 or completed_docs == len(target_docs)):
                    write_progress(
                        progress_path,
                        processed_rows=completed_docs + skipped_by_filter + skipped_empty,
                        total_rows=total_rows,
                        started_at=started_at,
                        message="clause_label processing",
                    )

    # 3) 결과 jsonl 출력 (원본 row 순서 보존)
    with output_path.open("w", encoding="utf-8") as dst:
        for index, row in enumerate(rows):
            doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
            for clause in clauses_by_doc.get(doc_id, []):
                clause_record = dict(clause)
                clause_record["prompt_version"] = prompt_version
                clause_record["source"] = "lloa"
                dst.write(json.dumps(clause_record, ensure_ascii=False))
                dst.write("\n")
                clause_count += 1
                sentiment_counts[clause["sentiment"]] = sentiment_counts.get(clause["sentiment"], 0) + 1
                aspect_counts[clause["aspect"]] = aspect_counts.get(clause["aspect"], 0) + 1

    processed_docs = skipped_by_filter + skipped_empty + completed_docs

    if progress_path:
        write_progress(
            progress_path,
            processed_rows=processed_docs,
            total_rows=processed_docs,
            started_at=started_at,
            message="clause_label completed",
        )

    summary = {
        "input_artifact_ref": clean_artifact_ref,
        "input_row_count": total_rows,
        "processed_doc_count": processed_docs,
        "skipped_by_filter": skipped_by_filter,
        "skipped_empty": skipped_empty,
        "parse_failures": parse_failures,
        "clause_count": clause_count,
        "sentiment_counts": sentiment_counts,
        "aspect_counts": aspect_counts,
        "include_genuineness": sorted(include_tiers) if include_tiers else None,
        "prompt_version": prompt_version,
        "model": lloa_config.model,
        "concurrency": concurrency,
        "reasoning_effort": lloa_config.reasoning_effort,
        # taxonomy-driven config Phase 2-B (2026-05-27) — artifact가 어떤
        # aspect taxonomy로 빌드됐는지 추적. Phase 3에서 analyze 시 artifact
        # taxonomy_id ↔ planner taxonomy_id 정합성 체크에 사용.
        "taxonomy_id": _TAXONOMY.taxonomy_id,
        "taxonomy_hash": _TAXONOMY.taxonomy_hash,
        # silverone 2026-05-28 — 실행 당시 적용된 subject variables snapshot.
        # doc_genuineness PR-α2와 동일한 패턴. subject metadata가 누락된 옛
        # dataset이면 festival default값이 기록된다.
        "applied": {
            "prompt_version": prompt_version,
            "subject_name": subject_config["subject_name"],
            "subject_aliases": list(subject_config["subject_aliases"]),
            "subject_type": subject_config["subject_type"],
        },
    }
    return {
        "notes": [
            f"dataset_clause_label — {clause_count} clauses from {completed_docs} docs "
            f"(skipped_by_filter={skipped_by_filter}, skipped_empty={skipped_empty}, parse_failures={parse_failures})",
            f"prompt: {prompt_version}, model: {lloa_config.model}, concurrency: {concurrency}, reasoning_effort: {lloa_config.reasoning_effort}",
        ],
        "artifact": rt._set_scope_fields(
            {
                "skill_name": "dataset_clause_label",
                "dataset_version_id": dataset_version_id,
                "clause_label_uri": str(output_path),
                "clause_label_ref": str(output_path),
                "clause_label_input_source": "clean",
                "progress_ref": progress_path,
                "summary": summary,
            },
            declared_result_scope="full_dataset",
            runtime_result_scope="full_dataset",
        ),
    }
