from __future__ import annotations

"""dataset_doc_genuineness entry point — cleaned doc 단위 3-tier 진성 분류.

5/14~19 LLOA + claude-haiku PoC로 production-ready 검증된 doc-level genuineness
분류를 정식 통합. 5-step pipeline의 clean 직후 단계 (ADR-017 task_registry,
5/19 결정). cleaned doc 하나씩 LLOA 호출 → genuine_review / mixed / non_review
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
from ._common import write_progress

LOGGER = get(__name__)

# silverone 2026-06-02 — prompt는 task-folder(config/prompts/doc_genuineness/)에서
# resolve. version은 payload(/prompt_options에서 고른 stem) > index.yaml default.
# artifact prompt_version은 resolve된 stem(예 "v1")을 그대로 기록한다.
_PROMPT_TASK = "doc_genuineness"
# silverone 2026-05-22 — prompt T/F/A 분류를 production schema에 매핑.
# T=genuine_review, F=non_review, A=uncertain. mixed는 prompt에서 더는 생성
# 안 되지만 enum에는 보존 — 옛 호출자 / clause_label default filter 호환.
_ALLOWED_TIERS = {"genuine_review", "mixed", "non_review", "uncertain"}
# silverone 2026-05-28 (D2) — clause_label과 동일 concurrency default.
# `concurrency` payload key로 override 가능. festival 2121 docs 기준
# sequential ~25분 → ThreadPoolExecutor(8) ~3분 (clause_label 패턴 검증값).
_DEFAULT_CONCURRENCY = 8
# silverone 2026-06-01 (D2 후속) — env fallback. payload > env > default 8.
# LLOA upstream 보호용 cap. invalid / 0 / negative는 default로 fall-back.
_MAX_CONCURRENCY = 32
_DOC_GENUINENESS_CONCURRENCY_ENV = "LLOA_DOC_GENUINENESS_CONCURRENCY"


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
def run_dataset_doc_genuineness(payload: dict[str, Any]) -> dict[str, Any]:
    """ADR-017 / 5/19 결정 — clean 직후 doc-level 3-tier 진성 분류.

    각 row의 cleaned_text를 LLOA에 한 호출씩 보내 genuine_review / mixed /
    non_review 라벨을 받는다. 후속 clause_label이 이 라벨을 옵션 필터로
    사용해서 *모든 doc 처리* vs *genuine_review·mixed만 처리*를 선택할 수
    있다 (5/19 결정 — default는 모든 doc 처리, 사용자가 명시 시만 필터).
    """
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

    lloa_config = LloaConfig(
        api_key=config.lloa_api_key,
        api_url=config.lloa_api_url,
        model=config.lloa_model,
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
    max_tokens = int(payload.get("max_tokens") or 1024)

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

    tier_counts: dict[str, int] = {tier: 0 for tier in _ALLOWED_TIERS}
    parse_failures = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0

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
            }
            tier_counts["non_review"] += 1
            continue
        lloa_targets.append((index, doc_id, cleaned_text))

    # 패스 2: 병렬 LLOA 호출.
    def _process(item: tuple[int, str, str]) -> tuple[str, dict[str, Any] | None, Exception | None]:
        _, target_doc_id, doc_text = item
        try:
            result = _classify_doc(
                client,
                system_prompt=system_prompt,
                doc_id=target_doc_id,
                doc_text=doc_text,
                max_tokens=max_tokens,
            )
            return target_doc_id, result, None
        except LloaResponseParseError as exc:
            return target_doc_id, None, exc

    completed = len(records_by_doc)  # empty shortcuts 이미 처리됨
    if lloa_targets:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(_process, item): item for item in lloa_targets}
            for future in as_completed(futures):
                doc_id, result, exc = future.result()
                if exc is not None:
                    LOGGER.warning(
                        "doc_genuineness.parse_failed",
                        doc_id=doc_id,
                        error_category=type(exc).__name__,
                        error_message=str(exc),
                        finish_reason=exc.finish_reason,
                    )
                    parse_failures += 1
                    records_by_doc[doc_id] = {
                        "doc_id": doc_id,
                        "genuineness": "non_review",
                        "reason": f"fallback: LLOA 응답 파싱 실패 ({type(exc).__name__})",
                        "prompt_version": prompt_version,
                        "source": "lloa_parse_failure",
                    }
                    tier_counts["non_review"] += 1
                else:
                    records_by_doc[doc_id] = {
                        "doc_id": doc_id,
                        "genuineness": result["genuineness"],
                        "reason": result["reason"],
                        "prompt_version": prompt_version,
                        "source": "lloa",
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
            f"mixed={tier_counts['mixed']}, "
            f"non_review={tier_counts['non_review']}, "
            f"uncertain={tier_counts['uncertain']}, "
            f"parse_failures={parse_failures})",
            f"prompt: {prompt_version}, model: {lloa_config.model}",
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
