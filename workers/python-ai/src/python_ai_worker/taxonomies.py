from __future__ import annotations

"""Taxonomy config loader (Phase 1, 2026-05-27).

``config/taxonomies/*.json``을 single source로 삼아 aspect/sentiment 정의를
한 곳에서 관리하기 위한 기반. taxonomy_driven_config_2026-05-27.md §4 Phase 1.

본 모듈은 *정의 + 검증*만 제공한다. clause_label / planner schema / artifact
metadata와의 연동은 후속 Phase 2~4에서 진행한다 — 현재는 dead code (아무도
load_taxonomy를 호출하지 않음). 다음 PR에서 callsite를 추가하면 옛 hand-
sync 위치를 한 곳씩 taxonomy config로 교체한다.
"""

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config_paths import resolve_config_dir


# host(repo)에서는 ``<repo>/config/taxonomies``, container(/app)에서는
# ``/app/config/taxonomies``로 발견. PYTHON_AI_TAXONOMIES_DIR env override
# 가능 (prompts dir과 같은 패턴).
TAXONOMIES_DIR_ENV = "PYTHON_AI_TAXONOMIES_DIR"

_INDEX_FILENAME = "index.yaml"
# index.yaml이 없거나 default 필드가 비었을 때만 쓰는 비상 fallback(코드 리터럴).
# 정상 경로는 config/taxonomies/index.yaml의 ``default`` — 그걸 바꾸면 코드 변경 없이
# 전역 default taxonomy를 교체할 수 있다 (프롬프트 index.yaml과 동일 패턴).
_DEFAULT_TAXONOMY_FALLBACK = "festival-gunsan"


def default_taxonomies_dir() -> Path:
    return resolve_config_dir(TAXONOMIES_DIR_ENV, __file__, "taxonomies")


def _read_default_taxonomy_id() -> str:
    """``config/taxonomies/index.yaml``의 ``default`` taxonomy_id를 읽는다. 프롬프트
    index.yaml과 동일한 의존성 없는 단순 파서(``key: value`` 한 줄, ``#`` 주석 허용).
    파일/필드 누락은 비상 fallback(_DEFAULT_TAXONOMY_FALLBACK)으로 — worker boot가
    index.yaml 부재로 깨지지 않게 한다."""
    try:
        index_path = default_taxonomies_dir() / _INDEX_FILENAME
        for raw_line in index_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            if key.strip() == "default":
                resolved = value.strip().strip('"').strip("'")
                if resolved:
                    return resolved
                break
    except OSError:
        pass
    return _DEFAULT_TAXONOMY_FALLBACK


# 전역 default taxonomy_id — index.yaml에서 import 시점 1회 resolve. taxonomy endpoint
# 기본 id + clause_label/planner의 per-dataset 미지정 fallback으로 쓰인다.
DEFAULT_TAXONOMY_ID = _read_default_taxonomy_id()


class TaxonomyError(ValueError):
    """taxonomy config 검증 실패. 메시지에 구체적 위반 위치 포함을 권장."""


@dataclass(frozen=True)
class AspectSpec:
    key: str
    label: str
    description: str


@dataclass(frozen=True)
class Taxonomy:
    """taxonomy config의 in-memory 표현. JSON file을 load_taxonomy로 읽고 검증
    한 결과."""

    taxonomy_id: str
    domain: str
    aspects: tuple[AspectSpec, ...]
    sentiments: tuple[str, ...]
    fallback_aspect: str
    taxonomy_hash: str

    @property
    def aspect_keys(self) -> tuple[str, ...]:
        return tuple(a.key for a in self.aspects)

    @property
    def aspect_keys_set(self) -> frozenset[str]:
        return frozenset(a.key for a in self.aspects)

    @property
    def sentiments_set(self) -> frozenset[str]:
        return frozenset(self.sentiments)


def load_taxonomy(
    taxonomy_id: str,
    *,
    base_dir: Path | None = None,
) -> Taxonomy:
    """``<base_dir>/<taxonomy_id>.json``을 읽어 검증된 ``Taxonomy``를 돌려준다.

    base_dir default는 ``DEFAULT_TAXONOMIES_DIR``. file이 없거나 JSON parse
    실패 또는 검증 실패면 ``TaxonomyError``.
    """

    path = (base_dir or default_taxonomies_dir()) / f"{taxonomy_id}.json"
    if not path.exists():
        raise TaxonomyError(f"taxonomy config not found: {path}")
    raw_text = path.read_text(encoding="utf-8")
    return parse_taxonomy(raw_text, expected_id=taxonomy_id)


def parse_taxonomy(
    raw_text: str,
    *,
    expected_id: str | None = None,
) -> Taxonomy:
    """JSON 문자열을 검증된 ``Taxonomy``로 변환. 파일 I/O 없는 entry point."""

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise TaxonomyError(f"taxonomy JSON parse error: {exc}") from exc
    return _build_taxonomy(data, expected_id=expected_id)


def _build_taxonomy(data: Any, *, expected_id: str | None) -> Taxonomy:
    if not isinstance(data, dict):
        raise TaxonomyError("taxonomy must be a JSON object")

    required = ("taxonomy_id", "domain", "aspects", "sentiments", "fallback_aspect")
    missing = [k for k in required if k not in data]
    if missing:
        raise TaxonomyError(
            f"taxonomy missing required keys: {', '.join(missing)}"
        )

    taxonomy_id = str(data["taxonomy_id"]).strip()
    if not taxonomy_id:
        raise TaxonomyError("taxonomy.taxonomy_id must be non-empty")
    if expected_id is not None and taxonomy_id != expected_id:
        raise TaxonomyError(
            f"taxonomy_id mismatch: expected '{expected_id}', got '{taxonomy_id}'"
        )

    domain = str(data["domain"]).strip()
    if not domain:
        raise TaxonomyError("taxonomy.domain must be non-empty")

    aspects = _parse_aspects(data["aspects"])
    sentiments = _parse_sentiments(data["sentiments"])

    fallback = str(data["fallback_aspect"]).strip()
    aspect_keys = {a.key for a in aspects}
    if fallback not in aspect_keys:
        raise TaxonomyError(
            f"taxonomy.fallback_aspect '{fallback}' is not in aspect keys: "
            f"{sorted(aspect_keys)}"
        )

    # stable hash — canonical JSON dump (sort keys, no whitespace, UTF-8).
    canonical = json.dumps(
        data, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    taxonomy_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    return Taxonomy(
        taxonomy_id=taxonomy_id,
        domain=domain,
        aspects=tuple(aspects),
        sentiments=tuple(sentiments),
        fallback_aspect=fallback,
        taxonomy_hash=taxonomy_hash,
    )


def _parse_aspects(raw: Any) -> list[AspectSpec]:
    if not isinstance(raw, list) or not raw:
        raise TaxonomyError("taxonomy.aspects must be a non-empty list")
    seen: set[str] = set()
    out: list[AspectSpec] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise TaxonomyError(f"taxonomy.aspects[{idx}] must be an object")
        for field_name in ("key", "label", "description"):
            if field_name not in item:
                raise TaxonomyError(
                    f"taxonomy.aspects[{idx}] missing '{field_name}'"
                )
        key = str(item["key"]).strip()
        if not key:
            raise TaxonomyError(
                f"taxonomy.aspects[{idx}].key must be non-empty"
            )
        if key in seen:
            raise TaxonomyError(
                f"taxonomy.aspects[{idx}].key '{key}' duplicated"
            )
        seen.add(key)
        out.append(
            AspectSpec(
                key=key,
                label=str(item["label"]).strip(),
                description=str(item["description"]).strip(),
            )
        )
    return out


def _parse_sentiments(raw: Any) -> list[str]:
    if not isinstance(raw, list) or not raw:
        raise TaxonomyError("taxonomy.sentiments must be a non-empty list")
    seen: set[str] = set()
    out: list[str] = []
    for idx, item in enumerate(raw):
        text = str(item or "").strip()
        if not text:
            raise TaxonomyError(
                f"taxonomy.sentiments[{idx}] must be a non-empty string"
            )
        if text in seen:
            raise TaxonomyError(
                f"taxonomy.sentiments[{idx}] '{text}' duplicated"
            )
        seen.add(text)
        out.append(text)
    return out


class TaxonomyMismatchError(ValueError):
    """clause_label artifact taxonomy_id가 planner와 달라 fail-loud로 처리.

    Phase 3-B (silverone 2026-05-27). hash mismatch는 (label/description만 다를
    수 있어) warning으로 처리하고, *id mismatch*만 본 예외를 raise한다.
    """

    def __init__(
        self,
        *,
        artifact_taxonomy_id: str,
        planner_taxonomy_id: str,
    ) -> None:
        super().__init__(
            f"clause_label taxonomy_id '{artifact_taxonomy_id}' does not match "
            f"planner taxonomy_id '{planner_taxonomy_id}'"
        )
        self.artifact_taxonomy_id = artifact_taxonomy_id
        self.planner_taxonomy_id = planner_taxonomy_id


def check_taxonomy_compatibility(
    *,
    planner_taxonomy: Taxonomy,
    artifact_taxonomy_id: str | None,
    artifact_taxonomy_hash: str | None,
) -> dict[str, Any]:
    """analyze 시 clause_label artifact와 planner taxonomy의 정합성을 검사.

    Phase 3-B (silverone 2026-05-27). 결과 dict를 ``result.taxonomy_check``에
    inject해 운영자가 audit log로 추적할 수 있게 한다. 사용자 화면에는 직접
    노출하지 않는다 (display.warnings는 별도).

    분기 / ``status`` 값:
      - ``legacy_missing``: artifact_taxonomy_id가 비어 있음. 옛 artifact
        호환 — 정상 실행 허용. control plane이 wire에 metadata를 inject하기
        전까지는 대부분 이 분기로 떨어진다.
      - ``id_mismatch``: artifact_taxonomy_id ≠ planner.taxonomy_id —
        ``TaxonomyMismatchError`` raise (fail-loud). LLM이 옛 enum으로 plan을
        만들면 빈 결과/오류로 떨어지므로 사전 차단.
      - ``hash_mismatch``: id는 같지만 hash가 다름. label/description 변경
        등으로 같은 taxonomy가 미세하게 다르게 빌드된 경우. 실행 허용 +
        warning만.
      - ``ok``: 모두 일치.
    """

    aid = (artifact_taxonomy_id or "").strip() or None
    ahash = (artifact_taxonomy_hash or "").strip() or None

    if aid is None:
        status = "legacy_missing"
    elif aid != planner_taxonomy.taxonomy_id:
        raise TaxonomyMismatchError(
            artifact_taxonomy_id=aid,
            planner_taxonomy_id=planner_taxonomy.taxonomy_id,
        )
    elif ahash is not None and ahash != planner_taxonomy.taxonomy_hash:
        status = "hash_mismatch"
    else:
        status = "ok"

    return {
        "planner_taxonomy_id": planner_taxonomy.taxonomy_id,
        "artifact_taxonomy_id": aid,
        "planner_taxonomy_hash": planner_taxonomy.taxonomy_hash,
        "artifact_taxonomy_hash": ahash,
        "status": status,
    }


def taxonomy_payload(taxonomy: Taxonomy) -> dict[str, Any]:
    """Taxonomy를 API 노출용 JSON-serializable dict로 변환.

    taxonomy endpoint(``GET /taxonomy``)가 그대로 반환하는 wire shape
    (silverone 2026-06-04). aspects는 config 정의 순서(``taxonomy.aspects``)를
    유지해 프론트 표시 순서를 deterministic하게 둔다.
    """

    return {
        "taxonomy_id": taxonomy.taxonomy_id,
        "domain": taxonomy.domain,
        "aspects": [
            {"key": a.key, "label": a.label, "description": a.description}
            for a in taxonomy.aspects
        ],
        "sentiments": list(taxonomy.sentiments),
        "fallback_aspect": taxonomy.fallback_aspect,
        "taxonomy_hash": taxonomy.taxonomy_hash,
    }


def list_taxonomies() -> list[dict[str, Any]]:
    """config/taxonomies/*.json을 스캔해 사용 가능한 taxonomy 목록(요약)을 반환한다.
    선택 UI / 목록 endpoint용. 파싱 실패 파일은 건너뛴다(한 파일이 목록 전체를
    깨지 않게). taxonomy_id 사전순 정렬."""
    base = default_taxonomies_dir()
    out: list[dict[str, Any]] = []
    if not base.is_dir():
        return out
    for path in sorted(base.glob("*.json")):
        try:
            tx = load_taxonomy(path.stem, base_dir=base)
        except (TaxonomyError, OSError):
            continue
        out.append(
            {
                "taxonomy_id": tx.taxonomy_id,
                "domain": tx.domain,
                "aspect_count": len(tx.aspects),
                "taxonomy_hash": tx.taxonomy_hash,
                "is_default": tx.taxonomy_id == DEFAULT_TAXONOMY_ID,
            }
        )
    return out


def render_aspect_taxonomy_block(taxonomy: Taxonomy) -> str:
    """taxonomy의 aspect를 prompt에 inject 가능한 markdown table로 변환.

    Phase 2-B (2026-05-27) — clause_label prompt template ``{{ASPECT_TAXONOMY}}``
    placeholder 자리에 들어간다. 기존 hand-coded 9-aspect 표를 대체.

    형식:
        | Aspect | 설명 |
        |---|---|
        | <key> | <label> — <description> |

    deterministic — 같은 taxonomy면 같은 출력. taxonomy.aspects 순서를 그대로
    따른다 (taxonomy config JSON 정의 순서).
    """

    lines = ["| Aspect | 설명 |", "|---|---|"]
    for aspect in taxonomy.aspects:
        label = aspect.label.replace("|", "\\|")
        description = aspect.description.replace("|", "\\|")
        lines.append(f"| {aspect.key} | {label} — {description} |")
    return "\n".join(lines)


def render_sentiment_taxonomy_block(taxonomy: Taxonomy) -> str:
    """taxonomy의 sentiment를 prompt list block으로 변환.

    Phase 2-B (2026-05-27) — list만 노출. sentiment label별 *설명*은
    taxonomy config에 없으므로 본 helper는 enum list만 만든다 (prompt 본문의
    sentiment 라벨 설명은 별도 위치에 유지).
    """

    return "\n".join(f"- {s}" for s in taxonomy.sentiments)


__all__ = [
    "AspectSpec",
    "DEFAULT_TAXONOMY_ID",
    "TAXONOMIES_DIR_ENV",
    "Taxonomy",
    "TaxonomyError",
    "TaxonomyMismatchError",
    "check_taxonomy_compatibility",
    "default_taxonomies_dir",
    "list_taxonomies",
    "load_taxonomy",
    "parse_taxonomy",
    "render_aspect_taxonomy_block",
    "render_sentiment_taxonomy_block",
    "taxonomy_payload",
]
