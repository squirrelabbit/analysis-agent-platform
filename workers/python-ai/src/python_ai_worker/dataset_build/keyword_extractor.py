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

from typing import Protocol, runtime_checkable

from ..runtime.common import _extract_noun_tokens

# 일반 불용어는 runtime.constants.STOPWORDS(_extract_noun_tokens가 이미 적용)에 있다.
# 여기서는 festival 분석에서만 의미 없는 *도메인* 단어를 추가한다 — 분석팀
# keyword_extractor.py에서 흡수. (config 파일화는 후속 — 우선 코드 상수로 시작)
FESTIVAL_STOPWORDS: frozenset[str] = frozenset(
    {
        # 일반 의존명사/부사
        "것", "수", "등", "및", "더", "때", "곳", "분", "말", "점", "중", "줄",
        "이번", "정말", "진짜", "너무", "조금", "약간", "많이", "다시", "함께",
        "오늘", "내일", "어제", "올해", "작년", "내년",
        "우리", "저희", "남편", "아이", "아내", "친구", "가족", "일행",
        # 축제 문서에서 반복되지만 분석 의미 없는 단어
        "후기", "방문", "축제", "행사", "강릉", "문화", "유산", "야행",
        "느낌", "생각", "기억", "사진", "영상", "블로그", "포스팅",
    }
)

KIWI_EXTRACTOR_VERSION = "kiwi-noun-v1"


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
        extra_stopwords: frozenset[str] | set[str] | None = FESTIVAL_STOPWORDS,
        user_dictionary_path: str = "",
    ) -> None:
        self._min_len = max(1, int(min_len))
        self._stopwords = {str(w).strip() for w in (extra_stopwords or ()) if str(w).strip()}
        self._user_dictionary_path = user_dictionary_path

    def extract(self, text: str) -> list[str]:
        if not text or not str(text).strip():
            return []
        tokens, _engine = _extract_noun_tokens(
            text,
            stopwords=self._stopwords,
            user_dictionary_path=self._user_dictionary_path,
            min_token_length=self._min_len,
            allowed_pos_prefixes=["N"],
        )
        seen: set[str] = set()
        keywords: list[str] = []
        for raw in tokens:
            token = str(raw or "").strip()
            if not token or token in seen:
                continue
            if len(token) < self._min_len:
                continue
            if token in self._stopwords:
                continue
            if token.isdigit():
                continue
            seen.add(token)
            keywords.append(token)
        return keywords


# 제품 기본 extractor. 호출부는 이 factory를 통해 받는다(엔진 교체 지점 단일화).
def default_keyword_extractor() -> KeywordExtractor:
    return KiwiKeywordExtractor()
