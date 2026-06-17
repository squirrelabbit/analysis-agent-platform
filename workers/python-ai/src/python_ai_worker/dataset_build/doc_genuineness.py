from __future__ import annotations

"""dataset_doc_genuineness entry point — cleaned doc 단위 3-tier 진성 분류.

5/14~19 LLOA + claude-haiku PoC로 production-ready 검증된 doc-level genuineness
분류를 정식 통합. 5-step pipeline의 clean 직후 단계 (ADR-017 task_registry,
5/19 결정). cleaned doc 하나씩 LLOA 호출 → genuine_review / non_review / uncertain
3-tier 라벨 + reason 문장 생성.
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .. import runtime as rt
from ..config import load_config
from ..config_paths import resolve_config_dir
from ..clients.lloa import LloaClient, LloaConfig, LloaResponseParseError
from ..obs import get, skill_handler
from ..prompt_options import load_prompt_body
from ._chunking import build_sentence_chunks, split_anchor_sentences
from ._common import write_progress

LOGGER = get(__name__)

# 긴 문서 chunk aggregate (ADR-029) — cleaned_text가 max_input_chars(기본 20000)를
# 넘으면 truncate 대신 문장 window chunk로 나눠 각각 분류하고 "진성 hit 우선"으로
# aggregate한다. 짧은 문서는 기존 truncate 경로 유지(계약 보존).
_CHUNK_MAX_SENTENCES = 60
_CHUNK_MAX_CHARS = 12000
_CHUNK_OVERLAP_SENTENCES = 0

# silverone 2026-06-02 — prompt는 task-folder(config/prompts/doc_genuineness/)에서
# resolve. version은 payload(/prompt_options에서 고른 stem) > index.yaml default.
# artifact prompt_version은 resolve된 stem(예 "v1")을 그대로 기록한다.
_PROMPT_TASK = "doc_genuineness"
# silverone 2026-05-22 — prompt T/F/A 분류를 production schema에 매핑.
# T=genuine_review, F=non_review, A=uncertain.
# silverone 2026-06-16 — legacy mixed tier 완전 제거. mixed 출력은 거부된다.
_ALLOWED_TIERS = {"genuine_review", "non_review", "uncertain"}
# silverone 2026-05-28 (D2) — clause_label과 동일 concurrency default.
# `concurrency` payload key로 override 가능. festival 2121 docs 기준
# sequential ~25분 → ThreadPoolExecutor(8) ~3분 (clause_label 패턴 검증값).
_DEFAULT_CONCURRENCY = 8
# silverone 2026-06-01 (D2 후속) — env fallback. payload > env > default 8.
# LLOA upstream 보호용 cap. invalid / 0 / negative는 default로 fall-back.
_MAX_CONCURRENCY = 32
_DOC_GENUINENESS_CONCURRENCY_ENV = "LLOA_DOC_GENUINENESS_CONCURRENCY"

# silverone 2026-06-04 — LLOA 입력 안정화 (chunking 아님).
# 초장문 doc는 LLOA가 HTTP 400 (request too large / context_length)로 거부해
# build 전체를 죽일 수 있다. clean output은 그대로 두고, doc_genuineness LLOA
# 호출 직전에만 input_text를 char 기준 truncate한다. parent doc 1 row = 결과 1
# 유지 (row 복제 없음). chunking은 후속 설계(별도 doc_chunks artifact)로 분리.
# 우선순위: payload['max_input_chars'] > env > default 20000. invalid는 default.
_DEFAULT_MAX_INPUT_CHARS = 20_000
_DOC_GENUINENESS_MAX_INPUT_CHARS_ENV = "LLOA_DOC_GENUINENESS_MAX_INPUT_CHARS"
# error_body 로깅 시 truncate 길이 (PII / 로그 폭주 방지).
_ERROR_BODY_LOG_LIMIT = 256


def _coerce_positive_int(raw: Any) -> int | None:
    """positive int면 반환, 아니면 None. bool은 reject (int subclass 회피)."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return value


def _resolve_concurrency(payload: dict[str, Any]) -> int:
    """concurrency 우선순위: payload > env > default. cap = _MAX_CONCURRENCY.

    payload나 env에 invalid / 0 / negative가 들어와도 silent fallback —
    operator가 잘못된 값을 넣었더라도 build 자체는 진행되도록.
    """
    if value := _coerce_positive_int(payload.get("concurrency")):
        return min(_MAX_CONCURRENCY, value)
    if value := _coerce_positive_int(os.environ.get(_DOC_GENUINENESS_CONCURRENCY_ENV)):
        return min(_MAX_CONCURRENCY, value)
    return _DEFAULT_CONCURRENCY


def _resolve_max_input_chars(payload: dict[str, Any]) -> int:
    """LLOA 입력 truncate 길이: payload > env > default 20000.

    silverone 2026-06-04 — invalid / 0 / negative / non-int는 default fallback
    하되, *값이 있는데 invalid*면 obs warning을 남긴다 (운영 실수로 build가
    조용히 잘못된 길이로 도는 것보다 가시화가 낫다는 결정). 값 자체가 없으면
    (정상 미지정) warning 없이 다음 우선순위로.
    """
    payload_raw = payload.get("max_input_chars")
    if payload_raw is not None:
        if value := _coerce_positive_int(payload_raw):
            return value
        LOGGER.warning(
            "doc_genuineness.max_input_chars_invalid",
            source="payload",
            raw_value=str(payload_raw),
            fallback="env_or_default",
        )
    env_raw = os.environ.get(_DOC_GENUINENESS_MAX_INPUT_CHARS_ENV)
    if env_raw is not None and env_raw.strip():
        if value := _coerce_positive_int(env_raw):
            return value
        LOGGER.warning(
            "doc_genuineness.max_input_chars_invalid",
            source="env",
            raw_value=env_raw,
            fallback=_DEFAULT_MAX_INPUT_CHARS,
        )
    return _DEFAULT_MAX_INPUT_CHARS


def _truncate_text(text: str, limit: int) -> tuple[str, int, int, bool]:
    """char 기준 truncate. 반환: (used_text, original_length, used_length, truncated)."""
    original_length = len(text)
    if original_length <= limit:
        return text, original_length, original_length, False
    return text[:limit], original_length, limit, True


def _read_error_body(exc: BaseException) -> str:
    """HTTPError 등 file-like 예외에서 응답 body를 안전하게 추출 (truncate).

    LLOA 400 진단(어떤 doc이 왜 거부됐나)을 위해 body 앞부분만 남긴다.
    body가 없거나 read 실패해도 build를 방해하지 않게 ''로 fallback.
    """
    reader = getattr(exc, "read", None)
    if not callable(reader):
        return ""
    try:
        raw = reader()
    except Exception:  # noqa: BLE001 — 진단용 best-effort, 실패해도 무시
        return ""
    if isinstance(raw, (bytes, bytearray)):
        raw = bytes(raw).decode("utf-8", "replace")
    return str(raw)[:_ERROR_BODY_LOG_LIMIT]


def _find_prompt_path(name: str) -> Path | None:
    """tier 4 fallback: config/prompts/<name>.md. host repo + container 자동 탐지."""
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


def _load_prompt_template(payload: dict[str, Any]) -> tuple[str, str]:
    """request inline → tier 4 file resolver. tier 2/3(dataset/project)은 후속 PR.

    반환: (system_prompt_text, prompt_version_label).
    """
    inline = payload.get("doc_genuineness_prompt_content")
    if isinstance(inline, str) and inline.strip():
        version = str(payload.get("doc_genuineness_prompt_version") or "request_inline").strip()
        return inline, version

    # silverone 2026-06-02 — 카탈로그 빌드. 사용자가 /prompt_options에서 고른
    # version(stem, 예 "v1")을 payload['doc_genuineness_prompt_version']로 받아
    # 그 version 파일을 로드한다. 미지정이면 index.yaml default. unknown version은
    # load_prompt_body가 PromptOptionsError(ValueError) → worker 400으로 reject.
    # artifact의 prompt_version은 실제 resolve된 stem을 기록해 감사 가능하게 한다.
    requested = str(payload.get("doc_genuineness_prompt_version") or "").strip() or None
    body, stem = load_prompt_body(_PROMPT_TASK, requested)
    return body, stem


def _strip_front_matter(template: str) -> str:
    """YAML front-matter (---로 감싸진 블록) 제거 후 본문만 반환."""
    text = template.lstrip()
    if not text.startswith("---"):
        return template
    body = text[3:]
    end = body.find("\n---")
    if end < 0:
        return template
    return body[end + 4 :].lstrip("\n")


# silverone 2026-05-22 (PR-α2) — doc_genuineness subject 변수화.
# control plane이 ``dataset.metadata.doc_genuineness`` 를 payload["doc_genuineness"]
# 로 inject하면, Python이 이 dict로 prompt placeholder를 치환한 system prompt를
# LLOA에 보낸다. ``subject_name`` 누락 시 fail-loud (festival prompt fallback X).


def _extract_doc_genuineness_config(payload: dict[str, Any]) -> dict[str, Any]:
    """payload["doc_genuineness"]를 정규화된 config dict로 변환한다.

    필수: ``subject_name``. 누락 또는 공백이면 ``ValueError``.
    선택 (없으면 default):
      - ``subject_aliases``: [] (list[str])
      - ``recruitment_keywords``: [] (list[str])
      - ``subject_type``: "generic" (str, prompt에는 노출 안 됨, snapshot용)
    """
    raw = payload.get("doc_genuineness")
    if not isinstance(raw, dict):
        raise ValueError(
            "dataset_doc_genuineness requires payload['doc_genuineness'] object with subject_name"
        )
    subject_name = str(raw.get("subject_name") or "").strip()
    if not subject_name:
        raise ValueError(
            "dataset_doc_genuineness requires subject_name in payload['doc_genuineness'] (fail-loud — no festival fallback)"
        )
    aliases = [str(item).strip() for item in raw.get("subject_aliases") or [] if str(item).strip()]
    keywords = [str(item).strip() for item in raw.get("recruitment_keywords") or [] if str(item).strip()]
    subject_type = str(raw.get("subject_type") or "generic").strip() or "generic"
    return {
        "subject_name": subject_name,
        "subject_aliases": aliases,
        "recruitment_keywords": keywords,
        "subject_type": subject_type,
    }


def _render_quoted_list(values: list[str]) -> str:
    """``["a","b"]`` → ``"'a', 'b'"``. 빈 list는 빈 문자열."""
    if not values:
        return ""
    return ", ".join(f"'{v}'" for v in values)


_CONDITIONAL_BLOCK_PATTERN = re.compile(
    r"\{\{#if (?P<var>\w+)\}\}(?P<body>.*?)\{\{/if\}\}", re.DOTALL
)


def _render_prompt(template: str, config: dict[str, Any]) -> str:
    """placeholder + 조건부 블록 치환.

    문법:
      - ``{{var}}``: 단순 치환.
      - ``{{#if var}}...{{/if}}``: ``var``가 truthy면 본문만 남기고, falsy면
        블록 통째 제거. list는 비어 있지 않을 때만 truthy.

    silverone 2026-05-22 (PR-α2) — handlebars 전체 도입은 과함. doc_genuineness
    1 prompt가 쓰는 최소 문법만 직접 구현.
    """
    truthy = {
        "subject_name": bool(config["subject_name"]),
        "subject_aliases": bool(config["subject_aliases"]),
        "recruitment_keywords": bool(config["recruitment_keywords"]),
    }

    def repl_block(match: re.Match) -> str:
        var = match.group("var")
        return match.group("body") if truthy.get(var, False) else ""

    rendered = _CONDITIONAL_BLOCK_PATTERN.sub(repl_block, template)

    substitutions = {
        "subject_name": config["subject_name"],
        "subject_aliases": _render_quoted_list(config["subject_aliases"]),
        "recruitment_keywords": _render_quoted_list(config["recruitment_keywords"]),
    }
    for key, value in substitutions.items():
        rendered = rendered.replace("{{" + key + "}}", value)
    return rendered


def _classify_doc(
    client: LloaClient,
    *,
    system_prompt: str,
    doc_id: str,
    doc_text: str,
    max_tokens: int,
) -> dict[str, Any]:
    """단일 doc LLOA 호출 + 응답 파싱."""
    user_payload = json.dumps(
        {"doc_id": doc_id, "doc_text": doc_text},
        ensure_ascii=False,
    )
    response = client.create_json_response(
        system=system_prompt,
        user=user_payload,
        max_tokens=max_tokens,
    )
    body = response.body
    if not isinstance(body, dict):
        raise LloaResponseParseError(
            f"doc_genuineness expected JSON object, got {type(body).__name__}",
            raw_text=str(body),
            finish_reason=response.finish_reason,
        )
    tier = str(body.get("genuineness") or "").strip()
    if tier not in _ALLOWED_TIERS:
        raise LloaResponseParseError(
            f"doc_genuineness invalid tier: {tier!r} (expected one of {sorted(_ALLOWED_TIERS)})",
            raw_text=json.dumps(body, ensure_ascii=False),
            finish_reason=response.finish_reason,
        )
    reason = str(body.get("reason") or "").strip()
    return {
        "genuineness": tier,
        "reason": reason,
        "usage": response.usage,
    }


@skill_handler("python-ai")
def _chunk_aggregate_classify(
    client: LloaClient, *, system_prompt: str, doc_id: str, doc_text: str,
    max_tokens: int, max_sentences: int, max_chars: int, overlap: int,
) -> dict[str, Any]:
    """긴 문서를 문장 window chunk로 나눠 각 chunk를 분류하고 "진성 hit 우선"으로
    aggregate (ADR-029). chunk 실패는 그 chunk만 uncertain 취급(doc 안 죽임). genuine
    chunk의 sentence span을 genuine_spans로 기록. 단일 모드·verify 공통 코어."""
    sentences = split_anchor_sentences(doc_text)
    chunks = build_sentence_chunks(sentences, max_sentences=max_sentences, max_chars=max_chars, overlap=overlap)
    tiers: list[str] = []
    genuine_spans: list[dict[str, int]] = []
    chunk_failures = 0
    first_genuine_reason = ""
    prompt_toks = comp_toks = 0
    for ci, (start0, sub) in enumerate(chunks):
        try:
            r = _classify_doc(
                client, system_prompt=system_prompt, doc_id=f"{doc_id}#c{ci}",
                doc_text=" ".join(sub), max_tokens=max_tokens,
            )
            g = r["genuineness"]
            usage = r.get("usage") or {}
            prompt_toks += int(usage.get("prompt_tokens") or 0)
            comp_toks += int(usage.get("completion_tokens") or 0)
        except (LloaResponseParseError, OSError) as exc:
            LOGGER.warning(
                "doc_genuineness.chunk_failed", doc_id=doc_id, chunk_index=ci,
                error_category=type(exc).__name__, error_message=str(exc),
            )
            g = "uncertain"
            chunk_failures += 1
            r = None
        tiers.append(g)
        if g == "genuine_review":
            genuine_spans.append({"chunk_index": ci, "sentence_start": start0 + 1, "sentence_end": start0 + len(sub)})
            if not first_genuine_reason and r is not None:
                first_genuine_reason = str(r.get("reason") or "").strip()
    if "genuine_review" in tiers:
        final = "genuine_review"
        reason = first_genuine_reason or f"{len(chunks)} chunk 중 일부에서 실제 방문/체험 후기 확인."
    elif "uncertain" in tiers:
        final = "uncertain"
        reason = "긴 문서 chunk aggregate — 진성 chunk 없음, 불확실 chunk 존재."
    else:
        final = "non_review"
        reason = "긴 문서 chunk aggregate — 모든 chunk non_review."
    return {
        "genuineness": final, "reason": reason, "genuine_spans": genuine_spans,
        "chunk_count": len(chunks), "chunk_failures": chunk_failures,
        "all_failed": len(chunks) > 0 and chunk_failures == len(chunks),
        "usage": {"prompt_tokens": prompt_toks, "completion_tokens": comp_toks},
    }


def run_dataset_doc_genuineness(payload: dict[str, Any]) -> dict[str, Any]:
    """ADR-017 / 5/19 결정 — clean 직후 doc-level 3-tier 진성 분류.

    각 row의 cleaned_text를 LLOA에 한 호출씩 보내 genuine_review / non_review /
    uncertain 라벨을 받는다. 후속 clause_label이 이 라벨을 옵션 필터로
    사용해서 *모든 doc 처리* vs *genuine_review·uncertain만 처리*를 선택할 수
    있다 (5/19 결정 — default는 모든 doc 처리, 사용자가 명시 시만 필터).

    verify 모드(payload['verify']=true, ADR-026): 모델 2개로 교차 분류 + 불일치
    judge로 final_label을 정하는 경로로 위임한다. 옵션 플래그라 task는 동일.
    """
    if payload.get("verify"):
        from .doc_genuineness_verify import run_dataset_doc_genuineness_verify

        return run_dataset_doc_genuineness_verify(payload)

    dataset_version_id = str(payload.get("dataset_version_id") or "").strip()
    clean_artifact_ref = str(payload.get("clean_artifact_ref") or "").strip()
    output_path_raw = str(payload.get("output_path") or "").strip()
    progress_path = str(payload.get("progress_path") or "").strip()
    if not dataset_version_id or not clean_artifact_ref or not output_path_raw:
        raise ValueError(
            "dataset_doc_genuineness requires dataset_version_id, clean_artifact_ref, output_path"
        )

    output_path = Path(output_path_raw)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = time.monotonic()

    config = load_config()
    if not (config.lloa_api_key or "").strip():
        raise ValueError(
            "dataset_doc_genuineness requires LLOA API key — set LLOA_API_KEY / WISENUT_LLOA_API_KEY / WISENUT_LLOA_MAX_V1_2_1_API_KEY"
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
        # reasoning_effort=config.lloa_reasoning_effort,
        reasoning_effort='low',
        prepend_no_think=config.lloa_prepend_no_think,
    )
    client = LloaClient(lloa_config)

    template, prompt_version = _load_prompt_template(payload)
    doc_genuineness_config = _extract_doc_genuineness_config(payload)
    system_prompt = _render_prompt(template, doc_genuineness_config)
    # max-v1.2.1이 /no_think 무시하고 reasoning을 길게 내면 1024로는 content가
    # 잘려 빈 응답이 된다(ADR-026 진단). 여유를 둔다(단일 모델은 parse 실패를
    # uncertain으로 격리하지만 애초에 실패를 줄인다).
    max_tokens = int(payload.get("max_tokens") or 4096)

    rows = rt._iter_rows(clean_artifact_ref)
    total_rows = len(rows)
    if progress_path:
        write_progress(
            progress_path,
            processed_rows=0,
            total_rows=total_rows,
            started_at=started_at,
            message="doc_genuineness queued",
        )

    concurrency = _resolve_concurrency(payload)
    max_input_chars = _resolve_max_input_chars(payload)
    # ADR-029 — 긴 문서 chunk aggregate. **기본 ON**: cleaned_text > max_input_chars인
    # doc는 자동으로 chunk 경로(별도 플래그·Go 배선 불필요). 짧은 doc은 기존 단일 호출.
    # payload['chunking']=false로 명시 비활성화하면 옛 truncate 경로로 fallback.
    chunking_enabled = payload.get("chunking", True) is not False
    chunk_max_sentences = max(1, int(payload.get("max_chunk_sentences") or _CHUNK_MAX_SENTENCES))
    chunk_max_chars = max(1, int(payload.get("max_chunk_chars") or _CHUNK_MAX_CHARS))
    chunk_overlap = max(0, int(payload.get("overlap_sentences") if payload.get("overlap_sentences") is not None else _CHUNK_OVERLAP_SENTENCES))

    tier_counts: dict[str, int] = {tier: 0 for tier in _ALLOWED_TIERS}
    parse_failures = 0
    request_failures = 0
    truncated_docs = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0
    chunked_doc_count = 0
    total_chunk_count = 0
    chunk_failure_count = 0
    genuine_span_count = 0

    # silverone 2026-05-28 (D2) — clause_label 동일 3-패스 패턴:
    # (1) row scan으로 empty shortcut + LLOA target 분리,
    # (2) ThreadPoolExecutor로 LLOA 병렬 호출 + 결과 doc_id별 모음,
    # (3) 원본 row 순서로 jsonl write (sequential과 동일 ordering 보장).

    # 패스 1: classification 대상 분리.
    records_by_doc: dict[str, dict[str, Any]] = {}
    lloa_targets: list[tuple[int, str, str]] = []
    for index, row in enumerate(rows):
        doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
        cleaned_text = str(row.get("cleaned_text") or "").strip()
        if not cleaned_text:
            # empty doc은 non_review로 간주 + reason 명시. LLOA 호출 안 함.
            records_by_doc[doc_id] = {
                "doc_id": doc_id,
                "genuineness": "non_review",
                "reason": "본문이 비어 있어 first-person observation 없음.",
                "prompt_version": prompt_version,
                "source": "empty_text_shortcut",
                "original_length": 0,
                "used_length": 0,
                "truncated": False,
            }
            tier_counts["non_review"] += 1
            continue
        lloa_targets.append((index, doc_id, cleaned_text))

    # 패스 2: 병렬 LLOA 호출.
    # silverone 2026-06-04 — LLOA 호출 직전 truncate + per-doc 격리.
    # error_kind: None(성공) / "parse"(응답 파싱 실패) / "request"(HTTP 400·413·
    # timeout 등 호출 자체 실패). request 실패는 해당 doc만 uncertain으로 격리하고
    # build를 계속한다 (한 doc이 전체 build를 죽이지 않게).
    def _process_chunked(doc_id: str, doc_text: str) -> dict[str, Any]:
        """긴 문서 chunk aggregate (ADR-029) — 공통 코어 호출 + 단일모드 record 빌드."""
        agg = _chunk_aggregate_classify(
            client, system_prompt=system_prompt, doc_id=doc_id, doc_text=doc_text,
            max_tokens=max_tokens, max_sentences=chunk_max_sentences,
            max_chars=chunk_max_chars, overlap=chunk_overlap,
        )
        final = agg["genuineness"]
        record: dict[str, Any] = {
            "doc_id": doc_id,
            "genuineness": final,
            "reason": agg["reason"],
            "prompt_version": prompt_version,
            "source": "lloa_chunk_aggregate",
            "original_length": len(doc_text),
            "used_length": len(doc_text),  # chunk가 전체를 커버(truncate 아님)
            "truncated": False,
            "chunked": True,
            "chunk_count": agg["chunk_count"],
            "chunk_failure_count": agg["chunk_failures"],
            "genuine_spans": agg["genuine_spans"],
        }
        if agg["chunk_failures"] > 0:
            record["needs_review"] = True
        return {
            "doc_id": doc_id, "chunked": True, "record": record, "tier": final,
            "chunk_count": agg["chunk_count"], "chunk_failures": agg["chunk_failures"],
            "genuine_span_count": len(agg["genuine_spans"]), "all_failed": agg["all_failed"],
            "usage": agg["usage"],
        }

    def _process(item: tuple[int, str, str]) -> dict[str, Any]:
        _, target_doc_id, doc_text = item
        if chunking_enabled and len(doc_text) > max_input_chars:
            return _process_chunked(target_doc_id, doc_text)
        used_text, original_length, used_length, truncated = _truncate_text(
            doc_text, max_input_chars
        )
        outcome: dict[str, Any] = {
            "doc_id": target_doc_id,
            "original_length": original_length,
            "used_length": used_length,
            "truncated": truncated,
            "result": None,
            "error": None,
            "error_kind": None,
            "status_code": None,
            "error_body": "",
        }
        try:
            outcome["result"] = _classify_doc(
                client,
                system_prompt=system_prompt,
                doc_id=target_doc_id,
                doc_text=used_text,
                max_tokens=max_tokens,
            )
        except LloaResponseParseError as exc:
            outcome["error"] = exc
            outcome["error_kind"] = "parse"
        except OSError as exc:
            # urllib HTTPError(400/413/context_length) ⊂ URLError ⊂ OSError,
            # TimeoutError·ConnectionError도 OSError. 호출 자체 실패만 per-doc
            # 격리하고, logic 버그(KeyError 등)는 그대로 전파시켜 fail-loud 유지.
            outcome["error"] = exc
            outcome["error_kind"] = "request"
            outcome["status_code"] = getattr(exc, "code", None)
            outcome["error_body"] = _read_error_body(exc)
        return outcome

    completed = len(records_by_doc)  # empty shortcuts 이미 처리됨
    if lloa_targets:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(_process, item): item for item in lloa_targets}
            for future in as_completed(futures):
                outcome = future.result()
                doc_id = outcome["doc_id"]
                if outcome.get("chunked"):
                    chunked_doc_count += 1
                    total_chunk_count += outcome["chunk_count"]
                    chunk_failure_count += outcome["chunk_failures"]
                    genuine_span_count += outcome["genuine_span_count"]
                    if outcome["all_failed"]:
                        # doc 전체 chunk 실패 → request_failure로 카운트(대량 실패 abort 안전망).
                        request_failures += 1
                    records_by_doc[doc_id] = outcome["record"]
                    tier_counts[outcome["tier"]] += 1
                    usage = outcome["usage"]
                    total_prompt_tokens += int(usage.get("prompt_tokens") or 0)
                    total_completion_tokens += int(usage.get("completion_tokens") or 0)
                    completed += 1
                    if progress_path and (completed % 10 == 0 or completed == total_rows):
                        write_progress(
                            progress_path, processed_rows=completed, total_rows=total_rows,
                            started_at=started_at, message="doc_genuineness processing",
                        )
                    continue
                length_fields = {
                    "original_length": outcome["original_length"],
                    "used_length": outcome["used_length"],
                    "truncated": outcome["truncated"],
                }
                if outcome["truncated"]:
                    truncated_docs += 1
                    LOGGER.warning(
                        "doc_genuineness.truncated",
                        doc_id=doc_id,
                        original_length=outcome["original_length"],
                        used_length=outcome["used_length"],
                        max_input_chars=max_input_chars,
                    )
                kind = outcome["error_kind"]
                if kind == "parse":
                    exc = outcome["error"]
                    LOGGER.warning(
                        "doc_genuineness.parse_failed",
                        doc_id=doc_id,
                        error_category=type(exc).__name__,
                        error_message=str(exc),
                        finish_reason=getattr(exc, "finish_reason", ""),
                    )
                    parse_failures += 1
                    records_by_doc[doc_id] = {
                        "doc_id": doc_id,
                        "genuineness": "non_review",
                        "reason": f"fallback: LLOA 응답 파싱 실패 ({type(exc).__name__})",
                        "prompt_version": prompt_version,
                        "source": "lloa_parse_failure",
                        **length_fields,
                    }
                    tier_counts["non_review"] += 1
                elif kind == "request":
                    exc = outcome["error"]
                    LOGGER.warning(
                        "doc_genuineness.request_failed",
                        doc_id=doc_id,
                        error_category=type(exc).__name__,
                        error_message=str(exc),
                        status_code=outcome["status_code"],
                        error_body=outcome["error_body"],
                        original_length=outcome["original_length"],
                        used_length=outcome["used_length"],
                        truncated=outcome["truncated"],
                    )
                    request_failures += 1
                    records_by_doc[doc_id] = {
                        "doc_id": doc_id,
                        "genuineness": "uncertain",
                        "reason": (
                            f"fallback: LLOA 요청 실패 ({type(exc).__name__}) — "
                            "해당 doc 격리, build 계속"
                        ),
                        "prompt_version": prompt_version,
                        "source": "lloa_request_failure",
                        **length_fields,
                    }
                    tier_counts["uncertain"] += 1
                else:
                    result = outcome["result"]
                    records_by_doc[doc_id] = {
                        "doc_id": doc_id,
                        "genuineness": result["genuineness"],
                        "reason": result["reason"],
                        "prompt_version": prompt_version,
                        "source": "lloa",
                        **length_fields,
                    }
                    tier_counts[result["genuineness"]] += 1
                    usage = result.get("usage") or {}
                    total_prompt_tokens += int(usage.get("prompt_tokens") or 0)
                    total_completion_tokens += int(usage.get("completion_tokens") or 0)
                completed += 1
                if progress_path and (completed % 10 == 0 or completed == total_rows):
                    write_progress(
                        progress_path,
                        processed_rows=completed,
                        total_rows=total_rows,
                        started_at=started_at,
                        message="doc_genuineness processing",
                    )

    # silverone 2026-06-08 — fail-loud: LLOA 실패(요청+파싱)율이 임계 이상이면 build 중단.
    # per-doc 격리(uncertain fallback)는 소수 flaky doc 보호용 — LLOA 서버 다운으로
    # 대부분/전부 실패한 결과를 "완료"로 덮으면 운영자가 망가진 결과를 정상으로 오인한다.
    failure_count = request_failures + parse_failures
    max_failure_rate = config.dataset_build_max_failure_rate
    if total_rows > 0 and failure_count / total_rows >= max_failure_rate:
        raise RuntimeError(
            "dataset_doc_genuineness aborted: LLOA 실패율 "
            f"{failure_count / total_rows:.0%} (request_failures={request_failures}, "
            f"parse_failures={parse_failures}, total={total_rows}) >= 임계 "
            f"{max_failure_rate:.0%}. LLOA 서버 상태를 확인하고 재시도하세요 "
            "(DATASET_BUILD_MAX_FAILURE_RATE로 조정 가능)."
        )

    # 패스 3: 원본 row 순서로 jsonl write.
    processed = 0
    with output_path.open("w", encoding="utf-8") as dst:
        for index, row in enumerate(rows):
            doc_id = str(row.get("row_id") or f"{dataset_version_id}:row:{index}")
            record = records_by_doc.get(doc_id)
            if record is None:
                continue
            dst.write(json.dumps(record, ensure_ascii=False))
            dst.write("\n")
            processed += 1

    if progress_path:
        write_progress(
            progress_path,
            processed_rows=processed,
            total_rows=processed,
            started_at=started_at,
            message="doc_genuineness completed",
        )

    summary = {
        "input_artifact_ref": clean_artifact_ref,
        "input_row_count": total_rows,
        "processed_row_count": processed,
        "tier_counts": tier_counts,
        "parse_failures": parse_failures,
        "request_failures": request_failures,
        "truncated_docs": truncated_docs,
        "max_input_chars": max_input_chars,
        "chunking": {
            "enabled": chunking_enabled,
            "strategy": "sentence_window",
            "threshold_chars": max_input_chars,
            "max_chunk_sentences": chunk_max_sentences,
            "max_chunk_chars": chunk_max_chars,
            "overlap_sentences": chunk_overlap,
            "chunked_doc_count": chunked_doc_count,
            "chunk_count": total_chunk_count,
            "chunk_failure_count": chunk_failure_count,
            "genuine_span_count": genuine_span_count,
        },
        "prompt_version": prompt_version,
        "model": lloa_config.model,
        "concurrency": concurrency,
        "reasoning_effort": lloa_config.reasoning_effort,
        "total_prompt_tokens": total_prompt_tokens,
        "total_completion_tokens": total_completion_tokens,
        # silverone 2026-05-22 (PR-α2) — 실행 당시 적용된 subject variables snapshot.
        # dataset.metadata가 나중에 바뀌어도 "이 결과는 어떤 기준으로 만들어졌나"를
        # 추적할 수 있게 artifact summary에 남긴다. control plane이 이 값을
        # version.metadata["doc_genuineness_applied"]로 또 한 번 보존.
        "applied": {
            "prompt_version": prompt_version,
            "subject_name": doc_genuineness_config["subject_name"],
            "subject_aliases": list(doc_genuineness_config["subject_aliases"]),
            "recruitment_keywords": list(doc_genuineness_config["recruitment_keywords"]),
            "subject_type": doc_genuineness_config["subject_type"],
        },
    }
    return {
        "notes": [
            f"dataset_doc_genuineness — {processed} docs classified "
            f"(genuine_review={tier_counts['genuine_review']}, "
            f"non_review={tier_counts['non_review']}, "
            f"uncertain={tier_counts['uncertain']}, "
            f"parse_failures={parse_failures}, "
            f"request_failures={request_failures}, "
            f"truncated={truncated_docs})",
            f"prompt: {prompt_version}, model: {lloa_config.model}, "
            f"max_input_chars: {max_input_chars}",
        ],
        "artifact": rt._set_scope_fields(
            {
                "skill_name": "dataset_doc_genuineness",
                "dataset_version_id": dataset_version_id,
                "doc_genuineness_uri": str(output_path),
                "doc_genuineness_ref": str(output_path),
                "progress_ref": progress_path,
                "summary": summary,
            },
            declared_result_scope="full_dataset",
            runtime_result_scope="full_dataset",
        ),
    }
