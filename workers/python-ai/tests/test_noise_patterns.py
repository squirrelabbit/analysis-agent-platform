"""5/11 inline noise scrub 도입(`config/noise_patterns/festival-v1.json`) lock.

document_cluster 검증(2026-05-11 vault `검토-raw/document_cluster_검증_2026-05-11`)에서
sentence cluster 결과 25806 sentence의 35%가 `존재하지 않는 이미지/스티커입니다`
반복 cluster로 묶이는 silent regression 확인 후 도입. garbage_rules(row 차단)와
책임 분리 — noise_patterns는 row 차단 X, 문자열만 strip.

silent regression으로 다음 invariant가 깨지면 sentence cluster의 후기 식별률이
다시 떨어진다.
"""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


_CONFIG_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "noise_patterns" / "festival-v1.json"
)


class NoisePatternsConfigContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(_CONFIG_PATH.exists(), f"config missing: {_CONFIG_PATH}")
        self.data = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))

    def test_required_fields_present(self) -> None:
        self.assertEqual(self.data.get("version"), "festival-v1")
        self.assertIn("patterns", self.data)
        self.assertIsInstance(self.data["patterns"], list)

    def test_must_have_required_baseline_patterns(self) -> None:
        # 5/11 검증에서 35% noise cluster의 핵심 3 패턴 + v3 cluster 20에서
        # 추가 발견된 'NAVER Corp 위치 메타 fragment'. 이것이 빠지면 sentence
        # cluster 결과에 다시 noise cluster가 등장 → 후속 필터 깨짐.
        required = {
            "존재하지 않는 이미지입니다",
            "존재하지 않는 스티커입니다",
            "네트워크 오류가 발생했습니다",
            "NAVER Corp",
        }
        patterns_combined = " ".join(self.data["patterns"])
        for token in required:
            self.assertIn(
                token,
                patterns_combined,
                f"required noise pattern missing: {token}",
            )

    def test_all_patterns_compile_as_regex(self) -> None:
        for raw in self.data["patterns"]:
            try:
                re.compile(raw)
            except re.error as exc:
                self.fail(f"pattern compile failed: {raw} ({exc})")

    def test_no_block_terms_key_to_avoid_garbage_rules_confusion(self) -> None:
        # garbage_rules와 schema 분리 invariant. 같은 파일에 block_terms를
        # 두면 운영자가 row 차단 효과로 오해 → false positive 큼.
        self.assertNotIn("block_terms", self.data)


class NoiseScrubBehaviorTests(unittest.TestCase):
    """`_apply_noise_scrub`의 동작 잠금. import는 test 내부에서 — module의 다른
    의존성(asset_client 등)이 test_*_*.py side에서 lazy load되어야 의미 있음."""

    def setUp(self) -> None:
        from python_ai_worker.dataset_build import _apply_noise_scrub

        self.scrub = _apply_noise_scrub
        self.patterns = [
            re.compile(r"존재하지 않는 이미지입니다\.?"),
            re.compile(r"존재하지 않는 스티커입니다\.?"),
            re.compile(r"네트워크 오류가 발생했습니다\.?"),
            re.compile(r"50m NAVER Corp\.[^.!?]{0,300}"),
        ]

    def test_strip_image_placeholder_inside_sentence(self) -> None:
        # 5/11 sentence cluster 검증 sample 그대로 — cluster 13/14/20 fragment.
        text = "안목해변에 내려서 또 바다 구경하다가 존재하지 않는 이미지입니다."
        scrubbed, hits = self.scrub(text, self.patterns)
        self.assertEqual(scrubbed, "안목해변에 내려서 또 바다 구경하다가")
        self.assertEqual(hits.get(r"존재하지 않는 이미지입니다\.?"), 1)

    def test_strip_sticker_placeholder_keeps_surrounding_text(self) -> None:
        text = "넘나좋은것ㅎㅎㅎ존재하지 않는 스티커입니다."
        scrubbed, hits = self.scrub(text, self.patterns)
        self.assertEqual(scrubbed, "넘나좋은것ㅎㅎㅎ")
        self.assertEqual(hits.get(r"존재하지 않는 스티커입니다\.?"), 1)

    def test_strip_network_error_pure_noise(self) -> None:
        text = "네트워크 오류가 발생했습니다."
        scrubbed, hits = self.scrub(text, self.patterns)
        self.assertEqual(scrubbed, "")
        self.assertEqual(hits.get(r"네트워크 오류가 발생했습니다\.?"), 1)

    def test_multiple_occurrences_counted(self) -> None:
        text = "존재하지 않는 이미지입니다. 본문 일부 존재하지 않는 이미지입니다."
        scrubbed, hits = self.scrub(text, self.patterns)
        self.assertEqual(scrubbed, "본문 일부")
        self.assertEqual(hits.get(r"존재하지 않는 이미지입니다\.?"), 2)

    def test_no_patterns_returns_unchanged(self) -> None:
        text = "후기 본문 그대로"
        scrubbed, hits = self.scrub(text, [])
        self.assertEqual(scrubbed, text)
        self.assertEqual(hits, {})

    def test_no_match_returns_unchanged(self) -> None:
        text = "오늘 강릉문화재야행 잘 다녀왔어요"
        scrubbed, hits = self.scrub(text, self.patterns)
        self.assertEqual(scrubbed, text)
        self.assertEqual(hits, {})

    def test_strip_naver_location_meta_fragment(self) -> None:
        # v3 cluster 20 (213 sentence) "50m NAVER Corp.<상호명><주소>..." fragment.
        # 마침표/!/? 만나기 전까지 strip.
        text = "후기 본문 50m NAVER Corp.강릉대도호부관아강원특별자치도 강릉시 임영로131번길 6 임영관 끝."
        scrubbed, hits = self.scrub(text, self.patterns)
        self.assertEqual(scrubbed, "후기 본문 .")
        self.assertEqual(hits.get(r"50m NAVER Corp\.[^.!?]{0,300}"), 1)

    def test_naver_pattern_stops_at_exclamation(self) -> None:
        # NAVER 위치 fragment가 ! 만나면 거기서 cut. 운영자 후기 콘텐츠 보존.
        text = "여기 좋아요 50m NAVER Corp.차온강원특별자치도 강릉시 너무 좋네요!추가"
        scrubbed, hits = self.scrub(text, self.patterns)
        self.assertIn("여기 좋아요", scrubbed)
        self.assertIn("!추가", scrubbed)
        self.assertNotIn("NAVER Corp", scrubbed)


class NoisePatternsLoaderFallbackTests(unittest.TestCase):
    """tier 4 config fallback — `noise_patterns_content` inject 없으면
    `config/noise_patterns/festival-v1.json` 로드. 향후 tier 1~3 resolver
    합류해도 fallback 경로는 유지되어야 한다."""

    def test_load_default_tier4_config(self) -> None:
        from python_ai_worker.dataset_build import _load_noise_patterns

        compiled, raw = _load_noise_patterns({})
        self.assertGreaterEqual(len(compiled), 3)
        self.assertGreaterEqual(len(raw), 3)
        joined = " ".join(raw)
        self.assertIn("존재하지 않는 이미지입니다", joined)

    def test_injected_patterns_override_config(self) -> None:
        from python_ai_worker.dataset_build import _load_noise_patterns

        compiled, raw = _load_noise_patterns(
            {"noise_patterns_content": {"patterns": [r"테스트 패턴\.?"]}}
        )
        self.assertEqual(raw, [r"테스트 패턴\.?"])
        self.assertEqual(len(compiled), 1)


if __name__ == "__main__":
    unittest.main()
