"""KiwiKeywordExtractor 후처리 잠금 (silverone 2026-06-10).

불용어/중복/숫자/최소길이 후처리가 extractor 자체로 보장됨을 검증한다.

silverone 2026-06-16 — kiwipiepy 설치 환경에서도 결과가 결정적이 되도록 복합명사
재결합(인접 명사 위치 기준 결합)을 추가했다. kiwipiepy(형태소: 푸드+트럭)와 regex
fallback(공백: 푸드트럭) 양쪽에서 ``푸드트럭``이 한 토큰으로 동일하게 나온다.
"""

from __future__ import annotations

import unittest

from python_ai_worker.dataset_build.keyword_extractor import (
    FESTIVAL_STOPWORDS,
    KIWI_EXTRACTOR_VERSION,
    KeywordExtractor,
    KiwiKeywordExtractor,
    default_keyword_extractor,
)


class KiwiKeywordExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ex = KiwiKeywordExtractor()

    def test_empty_and_whitespace_return_empty(self) -> None:
        self.assertEqual(self.ex.extract(""), [])
        self.assertEqual(self.ex.extract("   \n\t "), [])
        self.assertEqual(self.ex.extract(None), [])  # type: ignore[arg-type]

    def test_festival_stopwords_removed(self) -> None:
        # "축제", "방문"은 festival 도메인 불용어 → 결과에서 빠져야 한다.
        out = self.ex.extract("축제 방문 푸드트럭 가격")
        self.assertNotIn("축제", out)
        self.assertNotIn("방문", out)
        self.assertIn("푸드트럭", out)
        self.assertIn("가격", out)

    def test_dedup_preserves_order(self) -> None:
        out = self.ex.extract("가격 가격 푸드트럭 가격 음식")
        self.assertEqual(out, ["가격", "푸드트럭", "음식"])

    def test_min_len_drops_single_char(self) -> None:
        # 기본 min_len=2 — 1글자 토큰 제외.
        out = self.ex.extract("물 가격 차")
        self.assertNotIn("물", out)
        self.assertNotIn("차", out)
        self.assertIn("가격", out)

    def test_pure_digits_dropped(self) -> None:
        out = self.ex.extract("2024 가격 100 음식")
        self.assertNotIn("2024", out)
        self.assertNotIn("100", out)
        self.assertEqual(out, ["가격", "음식"])

    def test_min_len_configurable(self) -> None:
        # min_len=3 — 2글자 "가격"은 떨어지고 4글자 "푸드트럭"만 남는다.
        # (extractor 후처리의 min_len 필터를 토크나이저 무관하게 검증)
        ex3 = KiwiKeywordExtractor(min_len=3)
        out = ex3.extract("가격 푸드트럭")
        self.assertNotIn("가격", out)
        self.assertIn("푸드트럭", out)

    def test_extra_stopwords_override(self) -> None:
        ex = KiwiKeywordExtractor(extra_stopwords={"가격"})
        out = ex.extract("가격 푸드트럭")
        self.assertNotIn("가격", out)
        self.assertIn("푸드트럭", out)

    def test_version_and_interface(self) -> None:
        self.assertEqual(self.ex.version, KIWI_EXTRACTOR_VERSION)
        self.assertIsInstance(self.ex, KeywordExtractor)
        self.assertIsInstance(default_keyword_extractor(), KeywordExtractor)

    def test_festival_stopwords_nonempty(self) -> None:
        # Downloads 파일 흡수 확인 — 도메인 단어가 실제로 들어 있어야 한다.
        for w in ("축제", "강릉", "야행", "후기"):
            self.assertIn(w, FESTIVAL_STOPWORDS)

    def test_compound_rejoin_keeps_compounds_splits_on_space(self) -> None:
        # silverone 2026-06-16 — 복합명사 재결합. kiwipiepy가 푸드트럭→푸드+트럭,
        # 드론쇼→드론+쇼로 쪼개도 위치 인접이라 도로 합쳐 한 토큰. 반면 공백으로
        # 띄운 별개 명사는 합치지 않는다. (regex fallback 환경에서도 동일 결과)
        out = self.ex.extract("푸드트럭 드론쇼 야시장 음식")
        self.assertIn("푸드트럭", out)
        self.assertIn("드론쇼", out)
        self.assertNotIn("푸드", out)  # 쪼개진 조각이 남으면 안 됨
        self.assertNotIn("트럭", out)
        # 공백으로 분리된 명사는 결합되지 않는다.
        out2 = self.ex.extract("음식 가격")
        self.assertEqual(out2, ["음식", "가격"])


if __name__ == "__main__":
    unittest.main()
