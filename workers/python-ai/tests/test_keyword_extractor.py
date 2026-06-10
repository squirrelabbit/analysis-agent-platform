"""KiwiKeywordExtractor 후처리 잠금 (silverone 2026-06-10).

Kiwi 미설치 환경(regex fallback)에서도 불용어/중복/숫자/최소길이 후처리가
extractor 자체로 보장됨을 검증한다 — 따라서 결과가 결정적이다.
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


if __name__ == "__main__":
    unittest.main()
