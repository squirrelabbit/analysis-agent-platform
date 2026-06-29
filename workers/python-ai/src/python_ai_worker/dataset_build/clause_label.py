from __future__ import annotations

"""dataset_clause_label entry point — cleaned doc(title + body)을 LLOA 한 호출로
festival-related 절 추출 + sentiment + aspect(7종) 라벨링.

ADR-018 (β2 / 5/19) 5-step pipeline STEP 3. silverone 5/20 prompt 결정으로
schema를 clause/sentiment/aspect 3 필드로 단순화 (clause_index / *_reason 제거).
input source는 clean_artifact_ref (cleaned doc parquet/jsonl).

성능: ThreadPoolExecutor(max_workers=8) 병렬 호출 — 50 docs sequential ~11분
→ 병렬 ~1.5분. silverone 5/20 결정. ``concurrency`` payload key로 override 가능.

Optional: ``include_genuineness=["genuine_review","uncertain"]``로 doc_genuineness
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
from ..taxonomies import DEFAULT_TAXONOMY_ID, Taxonomy, load_taxonomy, render_aspect_taxonomy_block
from . import _cancel
from ._chunking import build_sentence_chunks, split_anchor_sentences
from ._common import write_progress
from ._prompt_slots import extract_extra_slot

LOGGER = get(__name__)

# silverone 2026-06-02 — prompt는 task-folder(config/prompts/clause_label/)에서
# resolve. version은 payload(/prompt_options에서 고른 stem) > index.yaml default.
# artifact prompt_version은 resolve된 stem(예 "v3")을 그대로 기록한다.
_PROMPT_TASK = "clause_label"
_ALLOWED_SENTIMENT = {"positive", "negative", "neutral"}

# taxonomy-driven config (2026-05-27~) — aspect 정의를 config/taxonomies/<id>.json
# 에서 derive. 2026-06-17 (Phase 3) — taxonomy_id가 더는 전역 고정이 아니라 빌드
# payload(`taxonomy_id`, control-plane이 dataset.metadata에서 주입)로 per-request
# 선택된다. 아래 모듈 전역(_TAXONOMY/_ALLOWED_ASPECT/_FALLBACK_ASPECT)은 payload에
# taxonomy_id가 없을 때의 **DEFAULT** 및 옛 호환(test / verify import)용으로만 남는다.
DEFAULT_CLAUSE_LABEL_TAXONOMY_ID = DEFAULT_TAXONOMY_ID
_TAXONOMY = load_taxonomy(DEFAULT_CLAUSE_LABEL_TAXONOMY_ID)
_ALLOWED_ASPECT: frozenset[str] = _TAXONOMY.aspect_keys_set
_FALLBACK_ASPECT: str = _TAXONOMY.fallback_aspect


def resolve_clause_label_taxonomy(payload: dict[str, Any]) -> Taxonomy:
    """payload['taxonomy_id'](control-plane이 dataset.metadata.taxonomy_id에서 주입)로
    taxonomy를 per-request 로드. 미지정이면 DEFAULT. clause_label / clause_label_verify
    공용."""
    taxonomy_id = str(payload.get("taxonomy_id") or "").strip() or DEFAULT_CLAUSE_LABEL_TAXONOMY_ID
    return load_taxonomy(taxonomy_id)
_ALLOWED_GENUINENESS_FILTER = {"genuine_review", "non_review", "uncertain"}
# 5/20 결정 — default ON. doc_genuineness 결과 중 genuine_review + uncertain만
# clause_label로 보낸다 (non_review는 LLOA 호출 절약 + 분석 가치 0). caller가
# 명시적으로 ``include_genuineness=[]`` (빈 list) 또는 ``include_genuineness=
# ["genuine_review","uncertain","non_review"]`` 보내면 모든 doc 처리 가능.
# silverone 2026-06-16 — legacy mixed tier 제거, uncertain으로 통합.
_DEFAULT_INCLUDE_GENUINENESS: list[str] = ["genuine_review", "uncertain"]
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


def _inject_taxonomy(template: str, taxonomy: Taxonomy = _TAXONOMY) -> str:
    """prompt template의 ``{{ASPECT_TAXONOMY}}``를 주어진 taxonomy에서 렌더한 markdown
    table로 치환한다. taxonomy 미지정 시 모듈 DEFAULT(_TAXONOMY) 사용(옛 호환).
    placeholder가 없는 inline prompt(옛 호환)는 그대로 통과시킨다."""

    if _ASPECT_TAXONOMY_PLACEHOLDER not in template:
        return template
    rendered = render_aspect_taxonomy_block(taxonomy)
    return template.replace(_ASPECT_TAXONOMY_PLACEHOLDER, rendered)


# ADR-030 — primary_area "대상 축". aspect taxonomy처럼 config-driven으로 프롬프트에
# {{PRIMARY_AREA}} 주입(하드코딩 금지). 도메인 공유라 config/primary_area/<ver>.json 단일 파일.
_PRIMARY_AREA_PLACEHOLDER = "{{PRIMARY_AREA}}"


def _load_primary_area(version: str = "v1") -> dict[str, Any]:
    path = resolve_config_dir("PYTHON_AI_PRIMARY_AREA_DIR", __file__, "primary_area") / f"{version}.json"
    return json.loads(path.read_text(encoding="utf-8"))


_PRIMARY_AREA = _load_primary_area()
# allowed key set + 프롬프트 inject 목록을 한 source(config)에서 — 검증/프롬프트 불일치 방지.
PRIMARY_AREA_KEYS = frozenset(a["key"] for a in _PRIMARY_AREA.get("areas", []))


def render_primary_area_block(data: dict[str, Any] | None = None) -> str:
    """config/primary_area를 프롬프트 inject용 목록 markdown으로 렌더. deterministic."""
    data = data or _PRIMARY_AREA
    lines = [f"- {a['key']}: {a['desc']}" for a in data.get("areas", [])]
    rules = data.get("boundary_rules", [])
    if rules:
        lines.append("- 경계: " + " / ".join(rules))
    return "\n".join(lines)


def _inject_primary_area(template: str, data: dict[str, Any] | None = None) -> str:
    """prompt의 ``{{PRIMARY_AREA}}``를 config/primary_area 목록으로 치환.
    placeholder 없는 프롬프트(judge 등)는 그대로 통과."""
    if _PRIMARY_AREA_PLACEHOLDER not in template:
        return template
    return template.replace(_PRIMARY_AREA_PLACEHOLDER, render_primary_area_block(data))


# silverone 2026-05-28 — clause_label subject 변수화. doc_genuineness PR-α2
# 패턴을 그대로 이식. 다만 doc_genuineness와 달리 subject metadata가 없으면
# festival default로 fallback (옛 dataset 호환). recruitment_keywords는
# doc_genuineness 전용이라 본 prompt에는 inject하지 않는다.
_DEFAULT_SUBJECT_CONFIG: dict[str, Any] = {
    "subject_name": "축제",
    "subject_aliases": [],
    "subject_type": "festival",
    "extra_instructions": "",
    "extra_examples": "",
}


def _extract_subject_config(payload: dict[str, Any]) -> dict[str, Any]:
    """subject 변수 + 행사별 추가 슬롯을 추출한다.

    - subject_name/aliases/type: payload['doc_genuineness']에서. doc_genuineness
      skill과 공유 (2026-05-28). 없거나 subject_name이 비면 festival default로
      fallback — clause_label은 옛 dataset도 처리해야 한다 (5/28 결정).
    - extra_instructions/extra_examples: payload['clause_label']에서 (task별 분리,
      2026-06-25). doc_genuineness 예시와 출력 스키마가 달라 공용 금지. subject가
      default여도 extra는 독립으로 읽는다. verify 경로도 이 함수를 재사용하므로
      자동 반영된다.
    """
    extra = extract_extra_slot(payload.get("clause_label"))
    raw = payload.get("doc_genuineness")
    if not isinstance(raw, dict):
        return {**_DEFAULT_SUBJECT_CONFIG, **extra}
    subject_name = str(raw.get("subject_name") or "").strip()
    if not subject_name:
        return {**_DEFAULT_SUBJECT_CONFIG, **extra}
    aliases = [str(item).strip() for item in raw.get("subject_aliases") or [] if str(item).strip()]
    subject_type = str(raw.get("subject_type") or "generic").strip() or "generic"
    return {
        "subject_name": subject_name,
        "subject_aliases": aliases,
        "subject_type": subject_type,
        **extra,
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
        "extra_instructions": bool(config.get("extra_instructions")),
        "extra_examples": bool(config.get("extra_examples")),
    }

    def repl_block(match: re.Match) -> str:
        var = match.group("var")
        return match.group("body") if truthy.get(var, False) else ""

    rendered = _CONDITIONAL_BLOCK_PATTERN.sub(repl_block, template)

    substitutions = {
        "subject_name": str(config.get("subject_name") or ""),
        "subject_aliases": _render_quoted_list(list(config.get("subject_aliases") or [])),
        "extra_instructions": str(config.get("extra_instructions") or ""),
        "extra_examples": str(config.get("extra_examples") or ""),
    }
    for key, value in substitutions.items():
        rendered = rendered.replace("{{" + key + "}}", value)
    return rendered


def _load_prompt_template(payload: dict[str, Any], taxonomy: Taxonomy = _TAXONOMY) -> tuple[str, str]:
    inline = payload.get("clause_label_prompt_content")
    if isinstance(inline, str) and inline.strip():
        version = str(payload.get("clause_label_prompt_version") or "request_inline").strip()
        return _inject_primary_area(_inject_taxonomy(inline, taxonomy)), version
    # silverone 2026-06-02 — 카탈로그 빌드. /prompt_options에서 고른 version(stem)을
    # payload['clause_label_prompt_version']로 받아 그 version 파일을 로드. 미지정이면
    # index.yaml default. unknown version은 load_prompt_body가 400으로 reject.
    # artifact prompt_version은 resolve된 stem을 기록 (감사 가능).
    requested = str(payload.get("clause_label_prompt_version") or "").strip() or None
    body, stem = load_prompt_body(_PROMPT_TASK, requested)
    return _inject_primary_area(_inject_taxonomy(body, taxonomy)), stem


def _load_genuineness_filter(payload: dict[str, Any]) -> tuple[set[str] | None, dict[str, str], dict[str, list]]:
    """doc_genuineness artifact를 읽어 (include_tiers, doc_id→tier, doc_id→genuine_spans).

    Default 동작 (5/20 결정): payload에 ``include_genuineness`` 키가 없으면
    ``["genuine_review", "uncertain"]``로 필터링 (non_review는 LLOA 호출 절약 + 분석
    가치 0). 명시적으로 빈 list ``[]`` 보내면 모든 doc 처리 (filter off).
    명시적으로 3 tier 모두 포함하면 사실상 모든 doc 처리지만 doc_genuineness ref는
    여전히 필요.

    genuine_spans (ADR-029): doc_genuineness chunk aggregate가 남긴 진성 chunk의
    sentence span. verify가 이 구간만 처리하는 데 쓴다(없으면 전체 doc). filter off면
    spans도 비운다(전체 처리).
    """
    if "include_genuineness" in payload:
        raw_filter = payload.get("include_genuineness")
        if not isinstance(raw_filter, list):
            raise ValueError("include_genuineness must be a list of genuineness tiers")
        if not raw_filter:
            # explicit opt-out — 모든 doc 처리
            return None, {}, {}
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
        return None, {}, {}
    ref = str(payload.get("doc_genuineness_ref") or "").strip()
    if not ref:
        raise ValueError(
            "include_genuineness filter active but doc_genuineness_ref missing — clause_label requires doc_genuineness artifact for filtering (5/20 default ON)"
        )
    tier_by_doc: dict[str, str] = {}
    spans_by_doc: dict[str, list] = {}
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
            # ADR-026 — verify artifact는 권위 라벨이 final_label. 단일 모델
            # artifact는 genuineness(또는 옛 label). effective label 우선순위:
            # final_label > genuineness > label.
            tier = str(
                rec.get("final_label") or rec.get("genuineness") or rec.get("label") or ""
            ).strip()
            if doc_id and tier:
                tier_by_doc[doc_id] = tier
            spans = rec.get("genuine_spans")
            if doc_id and isinstance(spans, list) and spans:
                spans_by_doc[doc_id] = spans
    # 사람 보정(override)은 최상위 — control-plane이 payload로 넘긴 doc_id→tier로
    # 덮는다(override > final_label > genuineness). overrides 없으면 무효.
    overrides = payload.get("genuineness_overrides")
    if isinstance(overrides, dict):
        for raw_doc, raw_tier in overrides.items():
            doc_id = str(raw_doc or "").strip()
            tier = str(raw_tier or "").strip()
            if doc_id and tier:
                tier_by_doc[doc_id] = tier
                # override가 tier를 바꾸면 옛 genuine_spans는 무효 — 전체 처리로 fallback.
                spans_by_doc.pop(doc_id, None)
    return tiers, tier_by_doc, spans_by_doc


@skill_handler("python-ai")
def run_dataset_clause_label(payload: dict[str, Any]) -> dict[str, Any]:
    """ADR-018 (β2) 5-step pipeline STEP 3 (5/20 schema 단순화 + concurrency 8).

    cleaned doc(title + body) 단위로 LLOA 병렬 호출 (default concurrency 8)에
    festival-related 절 추출 + sentiment + aspect 라벨링까지 처리. schema:
    {doc_id, clause, sentiment, aspect, prompt_version, source}.

    payload['verify']가 참이면 문장 앵커 교차모델 검증 경로(ADR-028)로 위임한다.
    """
    if payload.get("verify"):
        from .clause_label_verify import run_dataset_clause_label_verify

        return run_dataset_clause_label_verify(payload)

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

    # 전처리 모델 선택 (2026-06-12) — control-plane이 allowlist(LLOA_MODELS) 검증
    # 후 payload로 넘긴다. 생략 시 env(LLOA_MODEL) default.
    requested_model = str(payload.get("model_id") or "").strip()
    lloa_config = LloaConfig(
        api_key=config.lloa_api_key,
        api_url=config.lloa_api_url,
        model=requested_model or config.lloa_model,
        max_tokens=config.lloa_max_tokens,
        timeout_sec=config.lloa_timeout_sec,
        reasoning_effort=config.lloa_reasoning_effort,
        prepend_no_think=config.lloa_prepend_no_think,
        retry_max_attempts=config.lloa_retry_max_attempts,
        retry_base_delay_sec=config.lloa_retry_base_delay_sec,
        retry_max_delay_sec=config.lloa_retry_max_delay_sec,
    )
    client = LloaClient(lloa_config)

    # taxonomy per-request (Phase 3) — payload['taxonomy_id'](control-plane이
    # dataset.metadata.taxonomy_id에서 주입)로 선택. 미지정 시 DEFAULT.
    taxonomy = resolve_clause_label_taxonomy(payload)
    template, prompt_version = _load_prompt_template(payload, taxonomy)
    subject_config = _extract_subject_config(payload)
    system_prompt = _render_subject_prompt(template, subject_config)
    max_tokens = int(payload.get("max_tokens") or 8192)
    concurrency = max(1, int(payload.get("concurrency") or _DEFAULT_CONCURRENCY))

    # 단일 모드 clause_label은 문장 단위가 아니라 doc 통째 LLM 추출이라 genuine_spans는
    # 적용 안 함(span 제한은 문장앵커 verify 경로 전용, ADR-029).
    include_tiers, tier_by_doc, _genuine_spans = _load_genuineness_filter(payload)

    rows = rt._iter_rows(clean_artifact_ref)
    total_rows = len(rows)  # 전체 입력 수(summary input_row_count 용)

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

    # 진행률 분모 = 실제 LLM 처리 대상(진성 필터/빈 텍스트 제외) 수. 전체 모수가 아니라
    # 진성 데이터 기준으로 보여야 한다(verify 모드와 동일). (silverone 2026-06-26)
    target_count = len(target_docs)
    if progress_path:
        write_progress(
            progress_path,
            processed_rows=0,
            total_rows=target_count,
            started_at=started_at,
            message="clause_label queued",
        )

    # 2) 병렬 LLOA 호출 — ThreadPoolExecutor
    parse_failures = 0
    clause_count = 0
    sentiment_counts: dict[str, int] = {tier: 0 for tier in _ALLOWED_SENTIMENT}
    aspect_counts: dict[str, int] = {asp: 0 for asp in taxonomy.aspect_keys_set}
    completed_docs = 0
    clauses_by_doc: dict[str, list[dict[str, Any]]] = {}
    # 빌드 중단(silverone 2026-06-29) — control-plane이 /tasks/cancel 보내면 event set.
    # 루프에서 확인 → 남은 doc 멈추고 거기까지 결과만 보존. begin은 직전 잔여 event를 clear.
    cancelled = False
    cancel_event = _cancel.begin(dataset_version_id)

    # 단일 모드도 문장 앵커(2026-06-17): kiwipiepy로 문장을 고정 분리 → 1모델 classify →
    # 문장별 explode. 교차검증(verify)과 동일 분석 방법·동일 프롬프트(clause_label/<버전>).
    # _label_sentences는 verify와 공유(deferred import — 순환 회피). split/chunk는 모듈
    # 레벨 import(아래)라 테스트에서 patch 가능.
    from .clause_label_verify import _label_sentences

    allowed_aspect = taxonomy.aspect_keys_set
    fallback_aspect = taxonomy.fallback_aspect

    def _process(item: tuple[int, str, str, str]) -> tuple[str, list[dict[str, Any]], Exception | None]:
        _, doc_id, _doc_title, doc_text = item
        # 취소 후 아직 시작 안 한 doc은 작업(LLM/kiwi) 없이 즉시 반환 → 큐 잔여가 순식간에 비워짐.
        if cancel_event.is_set():
            return doc_id, [], None
        try:
            sentences = split_anchor_sentences(doc_text)
            if not sentences:
                return doc_id, [], None
            # LLM 호출만 chunk로 나눈다(문장/sentence_index는 전역 보존).
            chunks = build_sentence_chunks(sentences, max_sentences=40, max_chars=12000, overlap=0)
            labels: dict[int, dict[str, Any]] = {}
            for start0, sub in chunks:
                # 중단(silverone 2026-06-29) — 긴 문서(chunk 多)가 in-flight여도 chunk
                # 사이에서 멈춘다. 외부 루프의 doc 단위 취소만으로는 진행 중 긴 doc을
                # 못 멈춰 체감이 느렸다(거기까지 labels는 보존).
                if cancel_event.is_set():
                    break
                local = _label_sentences(client, system_prompt, sub, max_tokens, allowed_aspect, fallback_aspect)
                for li, label in local.items():
                    gi = start0 + li  # start0(0-based chunk offset) + li(1-based) = 전역 1-based
                    if gi not in labels and 1 <= gi <= len(sentences):
                        labels[gi] = label
            rows_out: list[dict[str, Any]] = []
            for gi, sentence in enumerate(sentences, start=1):
                lab = labels.get(gi)
                if not lab or not lab["relevant"]:
                    continue
                for aspect in (lab["aspects"] or [fallback_aspect]):
                    rows_out.append(
                        {"doc_id": doc_id, "clause": sentence, "sentiment": lab["sentiment"], "aspect": aspect}
                    )
            return doc_id, rows_out, None
        except (LloaResponseParseError, OSError, ValueError) as exc:
            return doc_id, [], exc

    if target_docs:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(_process, item): item for item in target_docs}
            for future in as_completed(futures):
                # 취소 감지 시 즉시 멈춘다 — 남은 future 취소 + 루프 탈출. 부분 결과는
                # 저장 안 하므로(재실행=처음부터) 드레인하지 않는다. break해야 진행률도
                # 그 자리에 멈춘다(드레인하면 빈 doc로 완료수가 치솟아 98%로 보임).
                if cancel_event.is_set():
                    for pending in futures:
                        pending.cancel()
                    cancelled = True
                    break
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
                if progress_path and (completed_docs % 5 == 0 or completed_docs == target_count):
                    write_progress(
                        progress_path,
                        processed_rows=completed_docs,
                        total_rows=target_count,
                        started_at=started_at,
                        message="clause_label processing",
                    )

    # silverone 2026-06-08 — fail-loud: 시도한 doc 중 파싱 실패율이 임계 이상이면 build 중단.
    # (LLOA HTTP 에러는 _process에서 안 잡혀 이미 fail-loud — 여기는 LLOA가 200을 주되
    # 전부 파싱 불가한 경우 등 degraded 결과를 "완료"로 덮지 않기 위한 가드.)
    attempted = len(target_docs)
    max_failure_rate = config.dataset_build_max_failure_rate
    if attempted > 0 and parse_failures / attempted >= max_failure_rate:
        raise RuntimeError(
            "dataset_clause_label aborted: 파싱 실패율 "
            f"{parse_failures / attempted:.0%} (parse_failures={parse_failures}, "
            f"attempted={attempted}) >= 임계 {max_failure_rate:.0%}. LLOA 응답/서버 "
            "상태를 확인하고 재시도하세요 (DATASET_BUILD_MAX_FAILURE_RATE로 조정 가능)."
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

    _cancel.end(dataset_version_id)

    if progress_path:
        write_progress(
            progress_path,
            # 중단이면 처리분만(100% 아님). 정상 완료면 대상 전체.
            processed_rows=completed_docs if cancelled else target_count,
            total_rows=target_count,
            started_at=started_at,
            message="clause_label cancelled" if cancelled else "clause_label completed",
        )

    summary = {
        "input_artifact_ref": clean_artifact_ref,
        "input_row_count": total_rows,
        "processed_doc_count": processed_docs,
        "skipped_by_filter": skipped_by_filter,
        "skipped_empty": skipped_empty,
        "parse_failures": parse_failures,
        "cancelled": cancelled,
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
        "taxonomy_id": taxonomy.taxonomy_id,
        "taxonomy_hash": taxonomy.taxonomy_hash,
        # silverone 2026-05-28 — 실행 당시 적용된 subject variables snapshot.
        # doc_genuineness PR-α2와 동일한 패턴. subject metadata가 누락된 옛
        # dataset이면 festival default값이 기록된다.
        "applied": {
            "prompt_version": prompt_version,
            "subject_name": subject_config["subject_name"],
            "subject_aliases": list(subject_config["subject_aliases"]),
            "subject_type": subject_config["subject_type"],
            # 행사별 추가 슬롯 적용 내용 snapshot (감사 — extra_instructions는
            # 사실상 프롬프트 코드라 무엇이 적용됐는지 보존한다).
            "extra_instructions": subject_config.get("extra_instructions", ""),
            "extra_examples": subject_config.get("extra_examples", ""),
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
