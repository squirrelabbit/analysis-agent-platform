from __future__ import annotations

import unittest

from python_ai_worker.skills._composition_views import (
    build_cluster_overview_view,
    build_issue_overview_view,
)


class CompositionViewTests(unittest.TestCase):
    def test_issue_overview_view_validates_composed_shape(self) -> None:
        view = build_issue_overview_view(
            {
                "ranked_issues": [
                    {
                        "rank": 1,
                        "label": "결제 오류",
                        "count": 12,
                        "representative_samples": [
                            {
                                "text": "결제 오류가 반복 발생했습니다",
                                "source_index": 0,
                            }
                        ],
                        "date_range": {
                            "start": None,
                            "end": None,
                        },
                    }
                ],
                "coverage": {
                    "documents_considered": 12,
                    "total_documents": 20,
                },
            },
            {
                "summary": "결제 오류가 가장 큰 이슈입니다.",
                "key_findings": ["결제 오류 VOC 비중이 높습니다."],
                "evidence": [
                    {
                        "rank": 1,
                        "source_index": 0,
                        "snippet": "결제 오류가 반복 발생했습니다",
                        "rationale": "대표 VOC입니다.",
                    }
                ],
                "selection_source": "document_sample",
                "quality_tier": "llm_dependent",
                "llm_output_parsed_strictly": True,
            },
        )

        self.assertEqual(view["view_name"], "issue_overview")
        self.assertEqual(view["ranked_issues"][0]["label"], "결제 오류")
        self.assertEqual(view["coverage"]["documents_considered"], 12)

    def test_cluster_overview_view_validates_composed_shape(self) -> None:
        view = build_cluster_overview_view(
            {
                "ranked_issues": [
                    {
                        "rank": 1,
                        "label": "결제/승인 오류",
                        "count": 8,
                        "representative_samples": [
                            {
                                "text": "결제 승인 오류가 다시 발생했습니다",
                                "source_index": 1,
                            }
                        ],
                        "date_range": {
                            "start": None,
                            "end": None,
                        },
                    }
                ],
                "coverage": {
                    "documents_considered": 8,
                    "total_documents": 20,
                },
            },
            {
                "clusters": [
                    {
                        "cluster_id": "cluster-1",
                        "label": "결제/승인 오류",
                        "candidate_labels": ["결제/승인 오류", "결제 오류"],
                    }
                ]
            },
            {
                "summary": "결제/승인 오류 군집이 가장 큽니다.",
                "key_findings": ["군집 1이 전체의 큰 비중을 차지합니다."],
                "evidence": [
                    {
                        "rank": 1,
                        "source_index": 1,
                        "snippet": "결제 승인 오류가 다시 발생했습니다",
                        "rationale": "대표 군집 근거입니다.",
                    }
                ],
                "selection_source": "cluster_membership",
                "quality_tier": "llm_dependent",
                "llm_output_parsed_strictly": True,
            },
        )

        self.assertEqual(view["view_name"], "cluster_overview")
        self.assertEqual(view["cluster_labels"][0]["cluster_id"], "cluster-1")
        self.assertEqual(view["selection_source"], "cluster_membership")


if __name__ == "__main__":
    unittest.main()
