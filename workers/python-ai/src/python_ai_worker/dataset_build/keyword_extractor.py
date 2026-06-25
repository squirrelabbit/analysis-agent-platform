from __future__ import annotations

"""clause 키워드 추출기 (silverone 2026-06-10).

분석팀이 준 ``keyword_extractor.py``(konlpy.Okt 기반)의 *의도*(절 단위 핵심 명사 +
festival 도메인 불용어)를 흡수하되, **제품 의존성은 Okt(JVM/openjdk)를 쓰지 않는다.**
worker는 이미 ``runtime.common._extract_noun_tokens``(Kiwi, JVM 불필요)로 명사를
추출하므로 그걸 재사용한다. Okt는 offline 비교 harness에서만 쓴다(제품 import 금지).

설계:
- ``KeywordExtractor`` 인터페이스(Protocol) — 나중에 Okt/사전/LLM 구현으로 교체 가능.
- 기본 구현 ``KiwiKeywordExtractor`` — Kiwi 명사 후보 + festival 불용어/중복/숫자/
  최소길이 후처리. Kiwi 미설치 환경(regex fallback)에서도 후처리는 동일하게 적용돼
  결과가 결정적이다(불용어/중복 제거는 이 모듈이 보장).
"""

import json
from pathlib import Path
from typing import Protocol, runtime_checkable

from ..obs import get
from ..runtime.common import _extract_noun_tokens

LOGGER = get(__name__)

# 도메인 불용어는 config asset로 외부화 (silverone 2026-06-25, Phase 2). 코드 상수
# FESTIVAL_STOPWORDS를 config/keyword_stopwords/<rule>.json으로 옮겼다 — noise_patterns/
# taxonomy 외부화와 동일 패턴(host repo + container /app 자동 탐지, Docker 동봉).
# 데이터셋 특정 지명(강릉/야행)은 여기서 제거했다 — block 규칙/추천 제외어로 처리.
# 언어 일반 불용어는 runtime.constants.STOPWORDS(_extract_noun_tokens가 이미 적용)에 있다.
DEFAULT_KEYWORD_STOPWORDS_RULE = "festival-v1"


def _find_keyword_stopwords_config_root() -> Path | None:
    """host repo와 container(/app) 모두 자동 탐지. parents loop으로 첫 매치.
    (clean.py:_find_noise_patterns_config_root 동일 패턴.)"""
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "config" / "keyword_stopwords"
        if candidate.is_dir():
            return candidate
    return None


def load_keyword_stopwords(rule_name: str = DEFAULT_KEYWORD_STOPWORDS_RULE) -> frozenset[str]:
    """config/keyword_stopwords/<rule>.json의 words를 불용어 집합으로 로드.

    config 누락/파싱 실패면 빈 집합 + obs warning (noise_patterns graceful 패턴).
    정상 경로(repo·Docker)에서는 항상 존재한다."""
    name = (str(rule_name).strip() or DEFAULT_KEYWORD_STOPWORDS_RULE)
    root = _find_keyword_stopwords_config_root()
    path = root / f"{name}.json" if root else None
    if not path or not path.exists():
        LOGGER.warning(
            "keyword_stopwords.config_missing",
            rule_name=name,
            config_path=str(path) if path else "",
        )
        return frozenset()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        words = data.get("words") if isinstance(data, dict) else None
        if isinstance(words, list):
            return frozenset(str(w).strip() for w in words if str(w).strip())
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.warning(
            "keyword_stopwords.config_load_failed",
            error_category=type(exc).__name__,
            error_message=str(exc),
            config_path=str(path),
        )
    return frozenset()


# extra_stopwords 미지정 시 config 기본 룰을 로드한다는 sentinel.
_UNSET_STOPWORDS = object()

# Kiwi POS 중 키워드로 쓸 태그 — 일반명사(NNG)·고유명사(NNP)만. 의존명사(NNB)·
# 대명사(NP: 여기/이곳)·수사(NR: 하나)는 구조적으로 제외해 노이즈를 줄인다.
KEYWORD_POS_PREFIXES: tuple[str, ...] = ("NNG", "NNP")

KIWI_EXTRACTOR_VERSION = "kiwi-noun-v2"


@runtime_checkable
class KeywordExtractor(Protocol):
    """절 텍스트 → 핵심 키워드 list. 구현 교체용 인터페이스."""

    version: str

    def extract(self, text: str) -> list[str]:
        ...


class KiwiKeywordExtractor:
    """Kiwi 명사 추출(runtime.common._extract_noun_tokens) 재사용 + 후처리.

    후처리(불용어/중복/숫자/최소길이)는 이 클래스가 보장한다 — Kiwi 미설치 시
    _extract_noun_tokens가 regex fallback(불용어 미적용)으로 떨어져도 결과가 동일하게
    필터되도록. 중복은 순서 유지로 제거한다.
    """

    version = KIWI_EXTRACTOR_VERSION

    def __init__(
        self,
        *,
        min_len: int = 2,
        extra_stopwords: frozenset[str] | set[str] | None = _UNSET_STOPWORDS,  # type: ignore[assignment]
        stopwords_rule_name: str | None = None,
        block_terms: frozenset[str] | set[str] | None = None,
        synonym_map: dict[str, str] | None = None,
        user_dictionary_path: str = "",
    ) -> None:
        self._min_len = max(1, int(min_len))
        # extra_stopwords 미지정 → config 룰 로드. 명시 시(테스트/특수 호출)는 그 집합만.
        if extra_stopwords is _UNSET_STOPWORDS:
            base = load_keyword_stopwords(stopwords_rule_name or DEFAULT_KEYWORD_STOPWORDS_RULE)
        else:
            base = {str(w).strip() for w in (extra_stopwords or ()) if str(w).strip()}
        # 운영자 사전 block 규칙은 도메인 불용어에 가산(silverone 2026-06-25, Phase 2 baked-in).
        block = {str(w).strip() for w in (block_terms or ()) if str(w).strip()}
        self._stopwords = set(base) | block
        # synonym(대표어 병합) — surface token을 canonical로 치환. 원본 불변(추출 결과만).
        self._synonym_map = {
            str(k).strip(): str(v).strip()
            for k, v in (synonym_map or {}).items()
            if str(k).strip() and str(v).strip()
        }
        self._user_dictionary_path = user_dictionary_path

    def extract(self, text: str) -> list[str]:
        if not text or not str(text).strip():
            return []
        tokens, _ = _extract_noun_tokens(
            text,
            stopwords=self._stopwords,
            user_dictionary_path=self._user_dictionary_path,
            min_token_length=self._min_len,
            allowed_pos_prefixes=list(KEYWORD_POS_PREFIXES),
        )
        seen: set[str] = set()
        keywords: list[str] = []
        for raw in tokens:
            token = str(raw or "").strip()
            if not token:
                continue
            if len(token) < self._min_len:
                continue
            if token in self._stopwords:
                continue
            if token.isdigit():
                continue
            # 대표어 병합 후 dedup(수제맥주·맥주 → 맥주 1건). 순서 유지.
            canon = self._synonym_map.get(token, token)
            if canon in seen:
                continue
            seen.add(canon)
            keywords.append(canon)
        return keywords


# 제품 기본 extractor. 호출부는 이 factory를 통해 받는다(엔진 교체 지점 단일화).
def default_keyword_extractor() -> KeywordExtractor:
    return KiwiKeywordExtractor()
