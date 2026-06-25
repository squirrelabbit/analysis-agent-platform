"""dataset_clause_label subject placeholder 치환 잠금.

silverone 2026-05-28 — doc_genuineness PR-α2 패턴을 clause_label에도 이식
(metadata source는 dataset.metadata.doc_genuineness 공유, recruitment_keywords는
clause_label에 inject하지 않음). subject metadata가 없으면 festival default로
fallback해서 옛 dataset과의 호환을 유지한다. Examples는 festival calibration
그대로 보존된다.
"""

from __future__ import annotations

import unittest


class ClauseLabelSubjectRenderTests(unittest.TestCase):
    def test_subject_metadata_injected(self) -> None:
        from python_ai_worker.dataset_build.clause_label import (
            _extract_subject_config,
            _inject_taxonomy,
            _render_subject_prompt,
            _strip_front_matter,
        )
        from python_ai_worker.prompt_options import resolve_prompt_path

        prompt_path = resolve_prompt_path("clause_label")
        self.assertIsNotNone(prompt_path)
        template = _inject_taxonomy(_strip_front_matter(prompt_path.read_text(encoding="utf-8")))

        config = _extract_subject_config({
            "doc_genuineness": {
                "subject_type": "festival",
                "subject_name": "강릉 국가유산야행",
                "subject_aliases": ["문화유산야행", "문화재야행", "강릉야행"],
                # recruitment_keywords는 inject 안 되므로 본문 없음
                "recruitment_keywords": ["서포터즈"],
            }
        })
        rendered = _render_subject_prompt(template, config)

        # subject_name이 헤더/Rules에 자연스럽게 inject되고 placeholder는 남지 않음
        self.assertIn("'강릉 국가유산야행'", rendered)
        self.assertNotIn("{{subject_name}}", rendered)
        self.assertNotIn("{{#if", rendered)
        self.assertNotIn("{{/if}}", rendered)

        # aliases는 quoted list로 inject + alias 안내 문장이 살아 있어야 함
        self.assertIn("'문화유산야행', '문화재야행', '강릉야행'", rendered)
        self.assertIn("also referred to as", rendered)

        # 옛 hardcoded "festival reviews" / "축제와 관련된" 본문 헤더는 사라져야 함
        # (Examples 안의 "축제"는 calibration용으로 보존)
        self.assertNotIn("specializing in festival reviews", rendered)
        self.assertNotIn("clauses related to the festival", rendered)

        # Examples는 festival 기준 그대로 유지 (calibration 보존). 2026-06-17 문장
        # 형식 전환으로 예시 문장이 바뀌어 calibration 단어를 현 v3 예시 기준으로 갱신.
        self.assertIn("드론쇼", rendered)
        self.assertIn("스탬프", rendered)

        # aspect taxonomy 표는 그대로 inject됨
        self.assertIn("show_program", rendered)
        self.assertIn("ambiance_scenery", rendered)

    def test_missing_subject_falls_back_to_festival_default(self) -> None:
        from python_ai_worker.dataset_build.clause_label import (
            _extract_subject_config,
            _inject_taxonomy,
            _render_subject_prompt,
            _strip_front_matter,
        )
        from python_ai_worker.prompt_options import resolve_prompt_path

        prompt_path = resolve_prompt_path("clause_label")
        template = _inject_taxonomy(_strip_front_matter(prompt_path.read_text(encoding="utf-8")))

        # payload에 doc_genuineness 키 자체가 없는 옛 dataset 호환 경로
        config_no_payload = _extract_subject_config({})
        self.assertEqual(config_no_payload["subject_name"], "축제")
        self.assertEqual(config_no_payload["subject_aliases"], [])
        self.assertEqual(config_no_payload["subject_type"], "festival")

        rendered = _render_subject_prompt(template, config_no_payload)
        # festival default subject_name="축제"로 치환되고 placeholder 잔존 없음
        self.assertIn("'축제'", rendered)
        self.assertNotIn("{{subject_name}}", rendered)
        # subject_aliases가 빈 list이므로 alias 안내 블록은 통째 제거
        self.assertNotIn("also referred to as", rendered)

    def test_subject_name_blank_falls_back(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _extract_subject_config

        config = _extract_subject_config({"doc_genuineness": {"subject_name": "   "}})
        self.assertEqual(config["subject_name"], "축제")
        self.assertEqual(config["subject_type"], "festival")

    def test_extract_drops_blank_aliases(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _extract_subject_config

        config = _extract_subject_config({
            "doc_genuineness": {
                "subject_name": "전주 한옥마을 야간 투어",
                "subject_aliases": ["전주야행", "  ", "한옥야행"],
                "subject_type": "tour",
            }
        })
        self.assertEqual(config["subject_name"], "전주 한옥마을 야간 투어")
        self.assertEqual(config["subject_aliases"], ["전주야행", "한옥야행"])
        self.assertEqual(config["subject_type"], "tour")

    def test_subject_type_default_generic(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _extract_subject_config

        config = _extract_subject_config({"doc_genuineness": {"subject_name": "abc"}})
        self.assertEqual(config["subject_type"], "generic")
        self.assertEqual(config["subject_aliases"], [])


# silverone 2026-06-25 — 행사별 추가 슬롯(extra_instructions/extra_examples).
# clause_label은 payload['clause_label']에서만 읽는다(doc_genuineness.extra_*와 분리 —
# 출력 스키마가 달라 공용 금지). base 프롬프트 마커는 PR2에서 들어오므로 렌더러
# 동작은 인라인 템플릿으로 검증한다.
_EXTRA_SLOT_TEMPLATE = (
    "You are an analyst for '{{subject_name}}'.\n"
    "{{#if extra_instructions}}\n## 행사별 추가 지침\n{{extra_instructions}}\n{{/if}}\n"
    "{{#if extra_examples}}\n## 행사별 추가 예시\n{{extra_examples}}\n{{/if}}\n"
)


class ClauseLabelExtraSlotTests(unittest.TestCase):
    def test_extra_read_from_clause_label_key(self) -> None:
        from python_ai_worker.dataset_build.clause_label import (
            _extract_subject_config,
            _render_subject_prompt,
        )

        config = _extract_subject_config({
            "doc_genuineness": {"subject_name": "군산 맥주축제"},
            "clause_label": {
                "extra_instructions": "백년광장은 장소명으로 본다.",
                "extra_examples": ["문장A → neutral", "문장B → positive / food"],
            },
        })
        self.assertEqual(config["extra_instructions"], "백년광장은 장소명으로 본다.")
        self.assertIn("문장A", config["extra_examples"])
        self.assertIn("문장B", config["extra_examples"])

        rendered = _render_subject_prompt(_EXTRA_SLOT_TEMPLATE, config)
        self.assertIn("## 행사별 추가 지침", rendered)
        self.assertIn("백년광장은 장소명으로 본다.", rendered)
        self.assertIn("## 행사별 추가 예시", rendered)
        self.assertIn("문장A", rendered)
        self.assertNotIn("{{#if", rendered)
        self.assertNotIn("{{/if}}", rendered)
        self.assertNotIn("{{extra_instructions}}", rendered)

    def test_empty_extra_omits_sections(self) -> None:
        from python_ai_worker.dataset_build.clause_label import (
            _extract_subject_config,
            _render_subject_prompt,
        )

        config = _extract_subject_config({})  # clause_label 키 없음
        self.assertEqual(config["extra_instructions"], "")
        self.assertEqual(config["extra_examples"], "")

        rendered = _render_subject_prompt(_EXTRA_SLOT_TEMPLATE, config)
        self.assertNotIn("행사별 추가 지침", rendered)
        self.assertNotIn("행사별 추가 예시", rendered)
        self.assertNotIn("{{", rendered)

    def test_task_isolation_doc_genuineness_extra_not_read(self) -> None:
        """doc_genuineness.extra_*는 clause_label로 새면 안 된다(스키마 오염 방지)."""
        from python_ai_worker.dataset_build.clause_label import _extract_subject_config

        config = _extract_subject_config({
            "doc_genuineness": {
                "subject_name": "군산 맥주축제",
                "extra_instructions": "문서 진성 판정용 지침",
                "extra_examples": "문서 예시",
            },
            # clause_label 키 자체 없음 → clause_label extra는 빈값이어야 함
        })
        self.assertEqual(config["extra_instructions"], "")
        self.assertEqual(config["extra_examples"], "")


class PromptSlotHelperTests(unittest.TestCase):
    def test_extract_extra_slot_variants(self) -> None:
        from python_ai_worker.dataset_build._prompt_slots import extract_extra_slot

        # 문자열
        self.assertEqual(
            extract_extra_slot({"extra_instructions": " 규칙 ", "extra_examples": " 예시 "}),
            {"extra_instructions": "규칙", "extra_examples": "예시"},
        )
        # 리스트 → 번호 블록
        out = extract_extra_slot({"extra_examples": ["가", "", "나"]})
        self.assertIn("예시 1:\n가", out["extra_examples"])
        self.assertIn("예시 2:\n나", out["extra_examples"])
        self.assertEqual(out["extra_instructions"], "")
        # dict 아님/누락 → 빈값
        self.assertEqual(
            extract_extra_slot(None),
            {"extra_instructions": "", "extra_examples": ""},
        )


if __name__ == "__main__":
    unittest.main()
