"""clause_label verify mode 잠금 (ADR-028) — 문장 앵커 교차모델 + reconciliation + judge.

splitter는 결정론적으로 patch하고, LLOA는 model별 urlopen 주입으로 mock한다.
classify(번호 문장 plaintext)와 judge(candidate_1 포함 JSON)를 user payload로 구분한다.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


def _completion(content: str) -> dict:
    return {
        "choices": [{"message": {"content": content, "reasoning_content": ""}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


# silverone 2026-06-25 — 행사별 추가 슬롯의 verify 경로 동작 잠금.
# classify(clause_label)에는 슬롯 마커가 있어 반영되고, judge에는 마커가 없어
# 미반영이다(분쟁 재판정에 행사 고유 예시는 불필요). verify는 _extract_subject_config
# 를 재사용하므로 payload['clause_label']의 extra_*가 classify에 자동 반영된다.
class ClauseLabelVerifyExtraSlotTests(unittest.TestCase):
    def test_judge_prompt_has_no_extra_markers(self) -> None:
        from python_ai_worker.prompt_options import resolve_prompt_path

        judge_path = resolve_prompt_path("clause_label_verify_judge")
        self.assertIsNotNone(judge_path)
        judge_body = judge_path.read_text(encoding="utf-8")
        self.assertNotIn("extra_instructions", judge_body)
        self.assertNotIn("extra_examples", judge_body)

    def test_verify_reuses_clause_label_extra(self) -> None:
        from python_ai_worker.dataset_build.clause_label import _extract_subject_config

        config = _extract_subject_config({
            "clause_label": {"extra_instructions": "행사 고유 규칙"},
        })
        self.assertEqual(config["extra_instructions"], "행사 고유 규칙")


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


def _fake_config():
    from python_ai_worker.config import WorkerConfig

    return WorkerConfig(
        lloa_api_key="k",
        lloa_api_url="http://lloa.example/v1/chat/completions",
        lloa_model="model-a",
        lloa_max_tokens=2048,
        lloa_timeout_sec=30,
        lloa_reasoning_effort=None,
        lloa_prepend_no_think=True,
    )


# 3 문장 고정 fixture:
#  s1 합의(positive/food)            → agree, 1 row
#  s2 sentiment neutral↔positive + aspect subset → sentiment_auto + union
#  s3 aspect disjoint(food vs show) → judge
SENTENCES = ["맛집 음식이 최고였다", "야경 보며 푸드트럭도 들렀다", "공연인지 먹거리인지 애매"]

# model별 classify 출력 (index 1-based)
LABELS_A = [
    {"index": 1, "relevant": True, "sentiment": "positive", "aspects": ["food"]},
    {"index": 2, "relevant": True, "sentiment": "neutral", "aspects": ["food"]},
    {"index": 3, "relevant": True, "sentiment": "positive", "aspects": ["food"]},
]
LABELS_B = [
    {"index": 1, "relevant": True, "sentiment": "positive", "aspects": ["food"]},
    {"index": 2, "relevant": True, "sentiment": "positive", "aspects": ["food", "ambiance_scenery"]},
    {"index": 3, "relevant": True, "sentiment": "positive", "aspects": ["show_program"]},
]


class ClauseLabelVerifyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        with self.clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "본문(분할은 patch)"}, ensure_ascii=False) + "\n")

    def _run(self, *, judge_item: dict | None):
        from python_ai_worker.dataset_build import clause_label_verify as v

        def make_urlopen(model: str):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                user = json.loads(req.data.decode("utf-8"))["messages"][1]["content"]
                if "candidate_1" in user:  # judge 호출
                    return _Resp(_completion(json.dumps([judge_item] if judge_item else [], ensure_ascii=False)))
                labels = LABELS_A if model == "model-a" else LABELS_B
                return _Resp(_completion(json.dumps(labels, ensure_ascii=False)))

            return _fake

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=make_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1",
            "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out),
            "verify": True,
            "classify_models": ["model-a", "model-b"],
            "judge_model": "model-judge", "include_genuineness": [],
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init), \
             patch.object(v, "_split_anchor_sentences", return_value=SENTENCES):
            result = v.run_dataset_clause_label_verify(payload)
        rows = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines() if line.strip()]
        return result, rows

    def test_agree_union_sentiment_auto_and_judge(self) -> None:
        # judge가 s3을 food로 확정.
        result, rows = self._run(judge_item={
            "sentence_index": 3, "relevant": True, "sentiment": "positive",
            "aspects": ["food"], "chosen": "candidate_1", "reason": "음식 맥락",
        })
        by_sent: dict[str, list[dict]] = {}
        for r in rows:
            by_sent.setdefault(r["clause"], []).append(r)

        # s1 agree → 1 row food/positive
        s1 = by_sent[SENTENCES[0]]
        self.assertEqual(len(s1), 1)
        self.assertEqual(s1[0]["resolution"], "agree")
        self.assertEqual(s1[0]["aspect"], "food")
        self.assertEqual(s1[0]["sentiment"], "positive")
        self.assertFalse(s1[0]["needs_review"])

        # s2: sentiment neutral↔positive→positive, aspect {food}⊂{food,ambiance}→union → 2 rows
        s2 = by_sent[SENTENCES[1]]
        self.assertEqual({r["aspect"] for r in s2}, {"food", "ambiance_scenery"})
        self.assertTrue(all(r["sentiment"] == "positive" for r in s2))  # non-neutral 채택
        self.assertTrue(all(r["resolution"] == "union" for r in s2))

        # s3: disjoint → judge → food
        s3 = by_sent[SENTENCES[2]]
        self.assertEqual(len(s3), 1)
        self.assertEqual(s3[0]["resolution"], "judge")
        self.assertEqual(s3[0]["aspect"], "food")
        self.assertFalse(s3[0]["needs_review"])

        summary = result["artifact"]["summary"]
        self.assertEqual(summary["mode"], "verify")
        rc = summary["resolution_counts"]
        self.assertEqual(rc["agree"], 1)
        self.assertEqual(rc["union"], 1)
        self.assertEqual(rc["judge"], 1)

    def test_rows_carry_model_ab_and_judge_snapshots(self) -> None:
        # 풍부 검토 큐(ADR-028): 각 행에 model A/B snapshot + judge 사유 보존.
        _result, rows = self._run(judge_item={
            "sentence_index": 3, "relevant": True, "sentiment": "positive",
            "aspects": ["food"], "chosen": "candidate_1", "reason": "음식 맥락",
        })
        by_sent: dict[str, list[dict]] = {}
        for r in rows:
            by_sent.setdefault(r["clause"], []).append(r)

        # 합의(s1): model A/B snapshot 존재, judge는 미개입(None).
        s1 = by_sent[SENTENCES[0]][0]
        self.assertIsNotNone(s1["model_a_result"])
        self.assertIsNotNone(s1["model_b_result"])
        self.assertIn("sentiment", s1["model_a_result"])
        self.assertIn("aspects", s1["model_b_result"])
        self.assertIsNone(s1["judge_result"])

        # judge 해소(s3): judge_result에 사유 포함 + model A/B 스냅샷 보존.
        s3 = by_sent[SENTENCES[2]][0]
        self.assertIsNotNone(s3["judge_result"])
        self.assertEqual(s3["judge_result"]["reason"], "음식 맥락")
        self.assertIsNotNone(s3["model_a_result"])
        self.assertIsNotNone(s3["model_b_result"])

    def test_judge_invalid_aspect_marks_needs_review(self) -> None:
        # judge가 s3에 invalid aspect → fallback 없이 needs_review
        _result, rows = self._run(judge_item={
            "sentence_index": 3, "relevant": True, "sentiment": "positive",
            "aspects": ["not_a_real_aspect"], "chosen": "other", "reason": "x",
        })
        s3 = [r for r in rows if r["clause"] == SENTENCES[2]]
        self.assertTrue(all(r["needs_review"] for r in s3))
        self.assertTrue(all(r["resolution"] == "needs_review" for r in s3))

    def test_judge_missing_isolates_needs_review(self) -> None:
        # judge가 s3 결과를 안 줌 → needs_review + union aspects 보존
        _result, rows = self._run(judge_item=None)
        s3 = [r for r in rows if r["clause"] == SENTENCES[2]]
        self.assertTrue(s3)
        self.assertTrue(all(r["needs_review"] for r in s3))
        self.assertTrue(all(r["resolution"] == "needs_review" for r in s3))
        self.assertEqual({r["aspect"] for r in s3}, {"food", "show_program"})  # union 보존

    def test_classify_models_validation(self) -> None:
        from python_ai_worker.dataset_build import clause_label_verify as v

        with patch.object(v, "load_config", return_value=_fake_config()):
            with self.assertRaises(ValueError):
                v.run_dataset_clause_label_verify({
                    "dataset_version_id": "x", "clean_artifact_ref": str(self.clean),
                    "output_path": str(self.out), "verify": True,
                    "classify_models": ["only-one"],
                    "doc_genuineness": {"subject_name": "x"},
                })


class BuildSentenceChunksTests(unittest.TestCase):
    def test_overlap_zero_and_three(self) -> None:
        from python_ai_worker.dataset_build.clause_label_verify import build_sentence_chunks

        s = [f"s{i}" for i in range(100)]
        c0 = build_sentence_chunks(s, max_sentences=40, max_chars=99999, overlap=0)
        self.assertEqual([(st, len(sub)) for st, sub in c0], [(0, 40), (40, 40), (80, 20)])
        c3 = build_sentence_chunks(s, max_sentences=40, max_chars=99999, overlap=3)
        self.assertEqual([st for st, _ in c3], [0, 37, 74])

    def test_char_bound_and_oversized_single(self) -> None:
        from python_ai_worker.dataset_build.clause_label_verify import build_sentence_chunks

        # 문장당 5000자, max 12000 → 2문장씩.
        c = build_sentence_chunks(["x" * 5000] * 5, max_sentences=40, max_chars=12000, overlap=0)
        self.assertEqual([len(sub) for _, sub in c], [2, 2, 1])
        # 단일 문장이 max_chars 초과해도 1개는 넣는다(빈 chunk 방지).
        c2 = build_sentence_chunks(["y" * 50000], max_sentences=40, max_chars=12000, overlap=0)
        self.assertEqual([len(sub) for _, sub in c2], [1])


def _chunk_urlopen(model, *, fail_if=None):
    """chunk별 classify mock — 입력 문장 수만큼 동일 라벨(positive/food) 반환.
    fail_if(model, user)가 참인 chunk는 빈 content로 parse 실패시킨다."""
    def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
        user = json.loads(req.data.decode("utf-8"))["messages"][1]["content"]
        if "candidate_1" in user:
            return _Resp(_completion("[]"))
        if fail_if is not None and fail_if(model, user):
            return _Resp(_completion(""))  # 빈 content → LloaResponseParseError
        n = sum(1 for ln in user.splitlines() if ln.split(".")[0].strip().isdigit())
        arr = [{"index": i, "relevant": True, "sentiment": "positive", "aspects": ["food"]} for i in range(1, n + 1)]
        return _Resp(_completion(json.dumps(arr, ensure_ascii=False)))
    return _fake


class ClauseLabelVerifyChunkingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        with self.clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "본문"}, ensure_ascii=False) + "\n")

    def _run(self, sentences, *, fail_if=None, max_chunk_sentences=40):
        from python_ai_worker.dataset_build import clause_label_verify as v

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=_chunk_urlopen(config.model, fail_if=fail_if))

        payload = {
            "dataset_version_id": "ver1", "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out), "verify": True,
            "classify_models": ["model-a", "model-b"], "judge_model": "model-judge", "include_genuineness": [],
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1, "max_chunk_sentences": max_chunk_sentences,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init), \
             patch.object(v, "_split_anchor_sentences", return_value=sentences):
            result = v.run_dataset_clause_label_verify(payload)
        rows = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines() if line.strip()]
        return result, rows

    def test_long_doc_chunks_preserve_sentence_index(self) -> None:
        sentences = [f"sent{i:03d}" for i in range(1, 101)]  # 100문장 → 40/40/20 = 3 chunk
        result, rows = self._run(sentences)
        # 모든 문장 agree → 문장당 1 row(food) = 100 rows.
        self.assertEqual(len(rows), 100)
        self.assertEqual({r["sentence_index"] for r in rows}, set(range(1, 101)))
        self.assertEqual({r["chunk_index"] for r in rows}, {0, 1, 2})
        ch = result["artifact"]["summary"]["chunking"]
        self.assertEqual(ch["chunk_count"], 3)
        self.assertEqual(ch["chunked_doc_count"], 1)
        self.assertEqual(ch["chunk_failure_count"], 0)
        # chunk 경계: sent041은 chunk_index 1.
        by_idx = {r["sentence_index"]: r for r in rows}
        self.assertEqual(by_idx[40]["chunk_index"], 0)
        self.assertEqual(by_idx[41]["chunk_index"], 1)
        self.assertEqual(by_idx[81]["chunk_index"], 2)

    def test_chunk_classify_failure_isolated(self) -> None:
        sentences = [f"sent{i:03d}" for i in range(1, 101)]
        # model-a의 chunk1(sent041~080)만 실패 → 그 문장들은 b만 라벨 → judge로 라우팅
        # (2026-06-18). _chunk_urlopen의 judge mock은 []를 반환하므로 judge 미해소 →
        # needs_review로 격리된다(옛 partial_classify 아님).
        result, rows = self._run(sentences, fail_if=lambda model, user: model == "model-a" and "sent041" in user)
        by_idx = {r["sentence_index"]: r for r in rows}
        # 실패 chunk 문장: a 없음 → judge 라우팅 → judge 빈응답 → needs_review.
        self.assertTrue(by_idx[50]["needs_review"])
        self.assertEqual(by_idx[50]["resolution"], "needs_review")
        # 정상 chunk 문장: agree.
        self.assertFalse(by_idx[10]["needs_review"])
        self.assertEqual(by_idx[10]["resolution"], "agree")
        self.assertGreaterEqual(result["artifact"]["summary"]["chunking"]["chunk_failure_count"], 1)


class ClauseLabelVerifyGenuineSpansTests(unittest.TestCase):
    """genuine_spans 소비 + tier 필터 (ADR-029). 진성 구간만 처리, sentence_index 전역
    보존. non_review는 tier 필터로 skip."""

    SENTENCES = [f"sent{i:03d}" for i in range(1, 7)]  # 6문장

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.gen = Path(self.tmp.name) / "gen.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        with self.clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "x" * 200}, ensure_ascii=False) + "\n")

    def _write_gen(self, rec: dict) -> None:
        with self.gen.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"doc_id": "d1", **rec}, ensure_ascii=False) + "\n")

    def _run(self, *, include_genuineness):
        from python_ai_worker.dataset_build import clause_label_verify as v

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=_chunk_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1", "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out), "verify": True,
            "classify_models": ["model-a", "model-b"], "judge_model": "model-judge",
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1, "include_genuineness": include_genuineness,
            "doc_genuineness_ref": str(self.gen),
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init), \
             patch.object(v, "_split_anchor_sentences", return_value=self.SENTENCES):
            result = v.run_dataset_clause_label_verify(payload)
        rows = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines() if line.strip()]
        return result, rows

    def test_processes_only_genuine_spans(self) -> None:
        self._write_gen({"genuineness": "genuine_review",
                         "genuine_spans": [{"chunk_index": 1, "sentence_start": 3, "sentence_end": 4}]})
        _result, rows = self._run(include_genuineness=["genuine_review", "uncertain"])
        self.assertEqual({r["sentence_index"] for r in rows}, {3, 4})  # 전역 index 보존

    def test_no_spans_processes_full_doc(self) -> None:
        self._write_gen({"genuineness": "genuine_review"})  # spans 없음 → 전체 처리
        _result, rows = self._run(include_genuineness=["genuine_review", "uncertain"])
        self.assertEqual({r["sentence_index"] for r in rows}, set(range(1, 7)))

    def test_non_review_skipped_by_tier_filter(self) -> None:
        self._write_gen({"genuineness": "non_review"})
        result, rows = self._run(include_genuineness=["genuine_review", "uncertain"])
        self.assertEqual(rows, [])
        self.assertEqual(result["artifact"]["summary"]["processed_row_count"], 0)

    def test_progress_denominator_is_target_count(self) -> None:
        # 진행률 분모는 실제 LLM 처리 대상(targets) 기준이어야 한다. genuineness 필터로
        # skip된 doc은 분모에서 빠진다(전체 input이 분모면 화면 총계/ETA가 부풀려짐).
        # 2 doc(d1 genuine 처리, d2 non_review skip) → progress total_rows = 1(=대상),
        # processed_rows = 1. (input 2건은 summary.input_row_count로 따로 노출.)
        from python_ai_worker.dataset_build import clause_label_verify as v

        clean = Path(self.tmp.name) / "clean2.jsonl"
        gen = Path(self.tmp.name) / "gen2.jsonl"
        out = Path(self.tmp.name) / "out2.jsonl"
        with clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "x" * 200}, ensure_ascii=False) + "\n")
            f.write(json.dumps({"row_id": "d2", "cleaned_text": "y" * 200}, ensure_ascii=False) + "\n")
        with gen.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"doc_id": "d1", "genuineness": "genuine_review"}, ensure_ascii=False) + "\n")
            f.write(json.dumps({"doc_id": "d2", "genuineness": "non_review"}, ensure_ascii=False) + "\n")

        calls: list[dict] = []

        def _spy(path, *, processed_rows, total_rows, started_at, message, **kw):
            calls.append({"processed_rows": processed_rows, "total_rows": total_rows, "message": message})

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=_chunk_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1", "clean_artifact_ref": str(clean),
            "output_path": str(out), "progress_path": str(self.tmp.name) + "/prog.json",
            "verify": True, "classify_models": ["model-a", "model-b"], "judge_model": "model-judge",
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1, "include_genuineness": ["genuine_review", "uncertain"],
            "doc_genuineness_ref": str(gen),
        }
        with patch.object(v, "write_progress", _spy), \
             patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init), \
             patch.object(v, "_split_anchor_sentences", return_value=self.SENTENCES):
            v.run_dataset_clause_label_verify(payload)

        processing = [c for c in calls if "processing" in c["message"]]
        self.assertTrue(processing, "processing progress write 없음")
        last = processing[-1]
        # 분모 = 대상 1건(genuine), input 2건이 아니라. processed도 1.
        self.assertEqual(last["total_rows"], 1)
        self.assertEqual(last["processed_rows"], 1)
        # 완료 write도 대상 기준 100%.
        completed = [c for c in calls if "completed" in c["message"]]
        self.assertTrue(completed)
        self.assertEqual(completed[-1]["total_rows"], 1)
        self.assertEqual(completed[-1]["processed_rows"], 1)

    def test_genuine_spans_across_multiple_chunks(self) -> None:
        # genuine_spans × chunking 상호작용: span 41~90(50문장) + max_chunk_sentences 20
        # → 허용문장이 3 chunk(20/20/10)로 나뉘어도 전역 sentence_index 보존, chunk 경계
        # 매핑 정확. (기존 genuine_spans 테스트는 1 chunk만 다뤄 이 조합이 미검증이었음)
        from python_ai_worker.dataset_build import clause_label_verify as v

        sentences = [f"sent{i:03d}" for i in range(1, 101)]
        self._write_gen({"genuineness": "genuine_review",
                         "genuine_spans": [{"chunk_index": 0, "sentence_start": 41, "sentence_end": 90}]})

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=_chunk_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1", "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out), "verify": True,
            "classify_models": ["model-a", "model-b"], "judge_model": "model-judge",
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1, "include_genuineness": ["genuine_review", "uncertain"],
            "doc_genuineness_ref": str(self.gen), "max_chunk_sentences": 20,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init), \
             patch.object(v, "_split_anchor_sentences", return_value=sentences):
            v.run_dataset_clause_label_verify(payload)
        rows = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines() if line.strip()]
        # span 41~90만, 전역 index 보존.
        self.assertEqual({r["sentence_index"] for r in rows}, set(range(41, 91)))
        # 50 허용문장 → 3 chunk(20/20/10), 경계 매핑.
        by_idx = {r["sentence_index"]: r for r in rows}
        self.assertEqual(by_idx[41]["chunk_index"], 0)
        self.assertEqual(by_idx[60]["chunk_index"], 0)
        self.assertEqual(by_idx[61]["chunk_index"], 1)
        self.assertEqual(by_idx[81]["chunk_index"], 2)
        self.assertEqual({r["chunk_index"] for r in rows}, {0, 1, 2})


class ClauseLabelVerifyPartialJudgeTests(unittest.TestCase):
    """partial(한 모델 미분류) → judge 라우팅 (2026-06-18). 옛 partial_classify(단일
    라벨 무비판 채택 + 검토 큐 격리) 대신 judge가 권위 라벨을 낸다."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        with self.clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "본문"}, ensure_ascii=False) + "\n")

    def test_reconcile_partial_routes_to_judge(self) -> None:
        from python_ai_worker.dataset_build.clause_label_verify import _reconcile_sentence

        lbl = {"relevant": True, "sentiment": "positive", "aspects": ["food"]}
        # 한쪽만 라벨 → judge로 라우팅(옛 partial_classify/review 아님).
        self.assertEqual(_reconcile_sentence(lbl, None)["status"], "judge")
        self.assertEqual(_reconcile_sentence(None, lbl)["status"], "judge")
        # 둘 다 미분류 → judge에 보낼 후보 없음 → classify_missing(review) 유지.
        both = _reconcile_sentence(None, None)
        self.assertEqual(both["status"], "review")
        self.assertEqual(both["resolution"], "classify_missing")

    def test_partial_resolved_by_judge(self) -> None:
        from python_ai_worker.dataset_build import clause_label_verify as v

        sentences = ["맛집 음식이 최고였다", "공연이 멋졌다"]
        labels_a = [
            {"index": 1, "relevant": True, "sentiment": "positive", "aspects": ["food"]},
            {"index": 2, "relevant": True, "sentiment": "positive", "aspects": ["show_program"]},
        ]
        labels_b = [  # s2(index 2) 누락 → partial → judge
            {"index": 1, "relevant": True, "sentiment": "positive", "aspects": ["food"]},
        ]
        judge_item = {
            "sentence_index": 2, "relevant": True, "sentiment": "positive",
            "aspects": ["show_program"], "chosen": "candidate_1", "reason": "공연 호평",
        }

        def make_urlopen(model: str):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                user = json.loads(req.data.decode("utf-8"))["messages"][1]["content"]
                if "candidate_1" in user:  # judge 호출
                    return _Resp(_completion(json.dumps([judge_item], ensure_ascii=False)))
                arr = labels_a if model == "model-a" else labels_b
                return _Resp(_completion(json.dumps(arr, ensure_ascii=False)))

            return _fake

        original_init = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            original_init(self, config, urlopen=make_urlopen(config.model))

        payload = {
            "dataset_version_id": "ver1", "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out), "verify": True,
            "classify_models": ["model-a", "model-b"], "judge_model": "model-judge",
            "include_genuineness": [],
            "doc_genuineness": {"subject_type": "festival", "subject_name": "강릉 국가유산야행"},
            "concurrency": 1,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init), \
             patch.object(v, "_split_anchor_sentences", return_value=sentences):
            v.run_dataset_clause_label_verify(payload)
        rows = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines() if line.strip()]
        by_idx = {r["sentence_index"]: r for r in rows}

        # s1: 두 모델 합의.
        self.assertEqual(by_idx[1]["resolution"], "agree")
        # s2: model-b 미분류 → judge가 해소 → resolution=judge, 검토 불필요.
        self.assertEqual(by_idx[2]["resolution"], "judge")
        self.assertFalse(by_idx[2]["needs_review"])
        self.assertEqual(by_idx[2]["aspect"], "show_program")
        # 검토 큐 snapshot: model_b는 null(미분류), judge 결과는 존재.
        self.assertIsNone(by_idx[2]["model_b_result"])
        self.assertIsNotNone(by_idx[2]["judge_result"])


class ClauseLabelVerifyPrimaryAreaTests(unittest.TestCase):
    """ADR-030 Phase 1 — primary_area 병렬 관측. summary 통계 + row snapshot 잠금.
    primary_area는 reconcile에 안 쓰고(aspects/sentiment 판정 독립) 관측만 한다."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clean = Path(self.tmp.name) / "clean.jsonl"
        self.out = Path(self.tmp.name) / "out.jsonl"
        with self.clean.open("w", encoding="utf-8") as f:
            f.write(json.dumps({"row_id": "d1", "cleaned_text": "본문"}, ensure_ascii=False) + "\n")

    def test_primary_area_collected(self) -> None:
        from python_ai_worker.dataset_build import clause_label_verify as v

        sentences = ["맥주 맛있었다", "공연 좋았다"]
        # 두 모델 모두 primary_area emit. s1 일치(food), s2 aspect/sentiment는 합의지만
        # primary_area는 불일치(content vs overall) → reconcile은 agree, primary_area만 갈림.
        la = [
            {"index": 1, "relevant": True, "sentiment": "positive", "aspects": ["food"], "primary_area": "food_beverage"},
            {"index": 2, "relevant": True, "sentiment": "positive", "aspects": ["show_program"], "primary_area": "content_program"},
        ]
        lb = [
            {"index": 1, "relevant": True, "sentiment": "positive", "aspects": ["food"], "primary_area": "food_beverage"},
            {"index": 2, "relevant": True, "sentiment": "positive", "aspects": ["show_program"], "primary_area": "overall"},
        ]

        def make(model: str):
            def _fake(req, timeout=None):  # type: ignore[no-untyped-def]
                user = json.loads(req.data.decode("utf-8"))["messages"][1]["content"]
                if "candidate_1" in user:
                    return _Resp(_completion("[]"))
                return _Resp(_completion(json.dumps(la if model == "model-a" else lb, ensure_ascii=False)))
            return _fake

        orig = v.LloaClient.__init__

        def _init(self, config, *, urlopen=None):
            orig(self, config, urlopen=make(config.model))

        payload = {
            "dataset_version_id": "ver1", "clean_artifact_ref": str(self.clean),
            "output_path": str(self.out), "verify": True,
            "classify_models": ["model-a", "model-b"], "judge_model": "model-judge",
            "include_genuineness": [],
            "doc_genuineness": {"subject_type": "festival", "subject_name": "테스트축제"},
            "concurrency": 1,
        }
        with patch.object(v, "load_config", return_value=_fake_config()), \
             patch.object(v.LloaClient, "__init__", _init), \
             patch.object(v, "_split_anchor_sentences", return_value=sentences):
            result = v.run_dataset_clause_label_verify(payload)

        pa = result["artifact"]["summary"]["primary_area"]
        self.assertEqual(pa["both_count"], 2)
        self.assertEqual(pa["agree_count"], 1)         # s1만 일치
        self.assertEqual(pa["agreement_rate"], 0.5)
        self.assertIn("food_beverage", pa["distribution_model_a"])
        self.assertIn("content_program↔overall", pa["confusion_top"])

        # primary_area는 reconcile에 안 쓰임 — s2도 aspect/sentiment 합의라 resolution=agree.
        rows = [json.loads(l) for l in self.out.read_text(encoding="utf-8").splitlines() if l.strip()]
        self.assertTrue(all(r["resolution"] == "agree" for r in rows))
        # row snapshot에 primary_area 적재.
        s1 = next(r for r in rows if r["sentence_index"] == 1)
        self.assertEqual(s1["model_a_result"]["primary_area"], "food_beverage")
        self.assertEqual(s1["model_b_result"]["primary_area"], "food_beverage")


if __name__ == "__main__":
    unittest.main()
