from __future__ import annotations

"""taxonomy task + taxonomy_payload 직렬화 검증 (silverone 2026-06-04).

``GET /taxonomy`` proxy의 Python backend(`run_task("taxonomy", ...)`)와
:func:`taxonomy_payload` wire shape를 잠근다. Go control-plane은 이 응답을
그대로 프론트로 전달한다.
"""

import unittest

from python_ai_worker.task_router import capability_names, run_task
from python_ai_worker.taxonomies import (
    DEFAULT_TAXONOMY_ID,
    load_taxonomy,
    taxonomy_payload,
)


class TaxonomyPayloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.taxonomy = load_taxonomy(DEFAULT_TAXONOMY_ID)
        self.payload = taxonomy_payload(self.taxonomy)

    def test_top_level_keys(self) -> None:
        self.assertEqual(
            set(self.payload.keys()),
            {
                "taxonomy_id",
                "domain",
                "aspects",
                "sentiments",
                "fallback_aspect",
                "taxonomy_hash",
            },
        )

    def test_scalar_fields_match_taxonomy(self) -> None:
        self.assertEqual(self.payload["taxonomy_id"], self.taxonomy.taxonomy_id)
        self.assertEqual(self.payload["domain"], self.taxonomy.domain)
        self.assertEqual(self.payload["fallback_aspect"], self.taxonomy.fallback_aspect)
        self.assertEqual(self.payload["taxonomy_hash"], self.taxonomy.taxonomy_hash)
        self.assertEqual(self.payload["sentiments"], list(self.taxonomy.sentiments))

    def test_aspects_preserve_order_and_shape(self) -> None:
        # config 정의 순서 유지 + key/label/description 3-field.
        self.assertEqual(
            [a["key"] for a in self.payload["aspects"]],
            list(self.taxonomy.aspect_keys),
        )
        for aspect in self.payload["aspects"]:
            self.assertEqual(set(aspect.keys()), {"key", "label", "description"})
            self.assertTrue(aspect["label"])

    def test_fallback_aspect_is_a_known_key(self) -> None:
        keys = {a["key"] for a in self.payload["aspects"]}
        self.assertIn(self.payload["fallback_aspect"], keys)


class TaxonomyTaskDispatchTests(unittest.TestCase):
    def test_capability_listed(self) -> None:
        self.assertIn("taxonomy", capability_names())

    def test_default_id_when_omitted(self) -> None:
        out = run_task("taxonomy", {})
        self.assertEqual(out["taxonomy_id"], DEFAULT_TAXONOMY_ID)
        self.assertEqual(len(out["aspects"]), 9)

    def test_explicit_id_matches_default(self) -> None:
        out = run_task("taxonomy", {"taxonomy_id": DEFAULT_TAXONOMY_ID})
        self.assertEqual(out, run_task("taxonomy", {}))

    def test_blank_id_falls_back_to_default(self) -> None:
        out = run_task("taxonomy", {"taxonomy_id": "  "})
        self.assertEqual(out["taxonomy_id"], DEFAULT_TAXONOMY_ID)

    def test_unknown_id_raises_value_error(self) -> None:
        # TaxonomyError(ValueError) → main.py HTTP 400.
        with self.assertRaises(ValueError):
            run_task("taxonomy", {"taxonomy_id": "does-not-exist"})


if __name__ == "__main__":
    unittest.main()
