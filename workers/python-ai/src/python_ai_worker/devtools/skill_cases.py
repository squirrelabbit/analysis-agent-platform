from __future__ import annotations

import copy
import csv
import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from ..task_router import run_task, task_handlers


@dataclass(frozen=True)
class SkillCase:
    skill_name: str
    description: str
    build: Callable[["SkillCaseContext"], dict[str, Any]]


class SkillCaseContext:
    def __init__(self, *, allow_llm: bool = False, keep_tempdir: bool = False) -> None:
        self.allow_llm = allow_llm
        self.keep_tempdir = keep_tempdir
        self._tempdir: tempfile.TemporaryDirectory[str] | None = None
        if keep_tempdir:
            self.temp_dir = Path(tempfile.mkdtemp(prefix="python-ai-skill-"))
        else:
            self._tempdir = tempfile.TemporaryDirectory(prefix="python-ai-skill-")
            self.temp_dir = Path(self._tempdir.name)
        self.history: list[dict[str, Any]] = []

    def close(self) -> None:
        if self.keep_tempdir:
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            return
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None

    def __enter__(self) -> "SkillCaseContext":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if not self.keep_tempdir:
            self.close()

    def write_csv(self, name: str, fieldnames: list[str], rows: list[dict[str, Any]]) -> Path:
        path = self.temp_dir / name
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return path

    def write_jsonl(self, name: str, rows: list[dict[str, Any]]) -> Path:
        path = self.temp_dir / name
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False))
                handle.write("\n")
        return path

    def write_parquet(self, name: str, rows: list[dict[str, Any]]) -> Path:
        path = self.temp_dir / name
        pq.write_table(pa.Table.from_pylist(rows), path)
        return path

    @contextmanager
    def llm_mode(self) -> Any:
        if self.allow_llm:
            yield
            return
        sentinel = object()
        previous = os.environ.get("ANTHROPIC_API_KEY", sentinel)
        os.environ["ANTHROPIC_API_KEY"] = ""
        try:
            yield
        finally:
            if previous is sentinel:
                os.environ.pop("ANTHROPIC_API_KEY", None)
            else:
                os.environ["ANTHROPIC_API_KEY"] = str(previous)

    def run(self, skill_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_payload = copy.deepcopy(payload)
        with self.llm_mode():
            result = run_task(skill_name, normalized_payload)
        self.history.append(
            {
                "skill_name": skill_name,
                "payload": copy.deepcopy(normalized_payload),
                "result": copy.deepcopy(result),
            }
        )
        return result


def available_skill_cases() -> dict[str, SkillCase]:
    return dict(SKILL_CASES)


def run_skill_case(skill_name: str, *, allow_llm: bool = False, keep_tempdir: bool = False) -> dict[str, Any]:
    case = available_skill_cases().get(skill_name)
    if case is None:
        raise ValueError(f"unknown skill case: {skill_name}")
    with SkillCaseContext(allow_llm=allow_llm, keep_tempdir=keep_tempdir) as ctx:
        final_result = case.build(ctx)
        response = {
            "skill_name": case.skill_name,
            "description": case.description,
            "steps": copy.deepcopy(ctx.history),
            "final_result": copy.deepcopy(final_result),
        }
        if keep_tempdir:
            response["temp_dir"] = str(ctx.temp_dir)
        return response


def _prior(*entries: tuple[str, dict[str, Any]]) -> dict[str, Any]:
    return {key: value["artifact"] for key, value in entries}


def _issue_rows() -> list[dict[str, Any]]:
    return [
        {"occurred_at": "2026-03-24", "channel": "app", "text": "결제 오류가 반복 발생했습니다"},
        {"occurred_at": "2026-03-24", "channel": "app", "text": "결제 승인 오류가 다시 발생했습니다"},
        {"occurred_at": "2026-03-25", "channel": "web", "text": "로그인이 자주 실패하고 오류가 보입니다"},
        {"occurred_at": "2026-03-26", "channel": "call", "text": "배송 문의가 계속 들어옵니다"},
        {"occurred_at": "2026-03-27", "channel": "web", "text": "환불 요청과 결제 문의가 계속 들어옵니다"},
        {"occurred_at": "2026-03-27", "channel": "app", "text": "로그인 인증 오류가 반복됩니다"},
    ]


def _comparison_rows() -> list[dict[str, Any]]:
    return [
        {"occurred_at": "2026-03-24", "text": "로그인 오류가 간헐적으로 발생합니다"},
        {"occurred_at": "2026-03-25", "text": "배송 문의가 증가했습니다"},
        {"occurred_at": "2026-03-26", "text": "결제 오류가 발생했습니다"},
        {"occurred_at": "2026-03-27", "text": "결제 오류가 다시 증가했습니다"},
        {"occurred_at": "2026-03-27", "text": "결제 승인 오류가 반복됩니다"},
        {"occurred_at": "2026-03-27", "text": "결제 실패 문의가 늘었습니다"},
    ]


def _cluster_rows() -> list[dict[str, Any]]:
    return [
        {"text": "결제 오류가 반복 발생했습니다"},
        {"text": "결제 승인 오류가 다시 발생했습니다"},
        {"text": "로그인이 계속 실패합니다"},
        {"text": "로그인 인증 오류가 반복됩니다"},
        {"text": "배송 문의가 계속 들어옵니다"},
        {"text": "결제 오류가 반복 발생했습니다!!"},
    ]


def _prepared_rows() -> list[dict[str, Any]]:
    return [
        {"normalized_text": "결제 오류가 반복 발생했습니다", "channel": "app"},
        {"normalized_text": "빠르게 해결되어 만족합니다", "channel": "app"},
        {"normalized_text": "문의 접수 후 확인 중입니다", "channel": "call"},
        {"normalized_text": "오류는 있었지만 대응이 빨라서 만족스럽습니다", "channel": "web"},
    ]


def _dataset_prepare_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv(
        "issues_raw.csv",
        ["channel", "text"],
        [
            {"channel": "app", "text": "결제 오류가 반복 발생했습니다!!!"},
            {"channel": "web", "text": "   "},
            {"channel": "call", "text": "로그인이 자주 실패하고 오류가 보입니다"},
        ],
    )
    return ctx.run(
        "dataset_prepare",
        {
            "dataset_version_id": "version-skill-case",
            "dataset_name": str(csv_path),
            "text_column": "text",
            "output_path": str(ctx.temp_dir / "issues_raw.prepared.parquet"),
        },
    )


def _sentiment_label_case(ctx: SkillCaseContext) -> dict[str, Any]:
    prepared_path = ctx.write_parquet("issues.prepared.parquet", _prepared_rows())
    return ctx.run(
        "sentiment_label",
        {
            "dataset_version_id": "version-skill-case",
            "dataset_name": str(prepared_path),
            "text_column": "normalized_text",
            "output_path": str(ctx.temp_dir / "issues.sentiment.parquet"),
        },
    )


def _embedding_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    return ctx.run(
        "embedding",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "output_path": str(ctx.temp_dir / "issues.embeddings.jsonl"),
        },
    )


def _planner_case(ctx: SkillCaseContext) -> dict[str, Any]:
    return ctx.run(
        "planner",
        {
            "dataset_name": "issues.csv",
            "data_type": "unstructured",
            "goal": "VOC 이슈를 요약해줘",
        },
    )


def _document_filter_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    return ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류",
            "sample_n": 3,
        },
    )


def _deduplicate_documents_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("duplicates.csv", ["text"], _cluster_rows())
    return ctx.run(
        "deduplicate_documents",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "sample_n": 3,
            "duplicate_threshold": 0.8,
        },
    )


def _keyword_frequency_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류",
            "sample_n": 3,
        },
    )
    return ctx.run(
        "keyword_frequency",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "top_n": 5,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )


def _time_bucket_count_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류 추세",
            "sample_n": 5,
        },
    )
    return ctx.run(
        "time_bucket_count",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "time_column": "occurred_at",
            "bucket": "day",
            "top_n": 3,
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )


def _meta_group_count_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "채널별 결제 오류",
            "sample_n": 5,
        },
    )
    return ctx.run(
        "meta_group_count",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "dimension_column": "channel",
            "top_n": 3,
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )


def _document_sample_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류",
            "sample_n": 5,
        },
    )
    return ctx.run(
        "document_sample",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류",
            "sample_n": 3,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )


def _dictionary_tagging_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 로그인 배송 이슈",
            "sample_n": 5,
        },
    )
    return ctx.run(
        "dictionary_tagging",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "top_n": 5,
            "sample_n": 3,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )


def _embedding_cluster_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues_cluster.csv", ["text"], _cluster_rows())
    dedup = ctx.run(
        "deduplicate_documents",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "sample_n": 3,
            "duplicate_threshold": 0.8,
        },
    )
    embedding = ctx.run(
        "embedding",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "output_path": str(ctx.temp_dir / "issues_cluster.embeddings.jsonl"),
        },
    )
    return ctx.run(
        "embedding_cluster",
        {
            "dataset_name": str(csv_path),
            "embedding_uri": embedding["artifact"]["embedding_uri"],
            "cluster_similarity_threshold": 0.2,
            "sample_n": 2,
            "top_n": 3,
            "prior_artifacts": _prior(("step:dedup", dedup)),
        },
    )


def _cluster_label_candidates_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues_cluster.csv", ["text"], _cluster_rows())
    embedding = ctx.run(
        "embedding",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "output_path": str(ctx.temp_dir / "issues_cluster.embeddings.jsonl"),
        },
    )
    clusters = ctx.run(
        "embedding_cluster",
        {
            "dataset_name": str(csv_path),
            "embedding_uri": embedding["artifact"]["embedding_uri"],
            "cluster_similarity_threshold": 0.2,
            "sample_n": 2,
            "top_n": 3,
        },
    )
    return ctx.run(
        "cluster_label_candidates",
        {
            "dataset_name": str(csv_path),
            "sample_n": 2,
            "top_n": 3,
            "prior_artifacts": _prior(("step:cluster", clusters)),
        },
    )


def _semantic_search_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    embedding = ctx.run(
        "embedding",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "output_path": str(ctx.temp_dir / "issues.embeddings.jsonl"),
        },
    )
    return ctx.run(
        "semantic_search",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류 관련 문서를 찾아줘",
            "sample_n": 3,
            "embedding_uri": embedding["artifact"]["embedding_uri"],
        },
    )


def _unstructured_issue_summary_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "VOC 이슈를 요약해줘",
            "sample_n": 5,
        },
    )
    keywords = ctx.run(
        "keyword_frequency",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "top_n": 10,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    samples = ctx.run(
        "document_sample",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "VOC 이슈를 요약해줘",
            "sample_n": 3,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    return ctx.run(
        "unstructured_issue_summary",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "VOC 이슈를 요약해줘",
            "sample_n": 3,
            "top_n": 10,
            "prior_artifacts": _prior(("step:keywords", keywords), ("step:samples", samples)),
        },
    )


def _issue_breakdown_summary_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues_breakdown.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "채널별 이슈를 분해해서 보여줘",
            "sample_n": 5,
        },
    )
    grouped = ctx.run(
        "meta_group_count",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "dimension_column": "channel",
            "top_n": 3,
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    ctx.run(
        "document_sample",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "채널별 이슈를 분해해서 보여줘",
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    return ctx.run(
        "issue_breakdown_summary",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "dimension_column": "channel",
            "top_n": 3,
            "sample_n": 2,
            "prior_artifacts": _prior(("step:grouped", grouped)),
        },
    )


def _issue_cluster_summary_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues_cluster.csv", ["text"], _cluster_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "주요 이슈 군집을 묶어서 보여줘",
            "sample_n": 6,
        },
    )
    dedup = ctx.run(
        "deduplicate_documents",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "sample_n": 3,
            "duplicate_threshold": 0.8,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    embedding = ctx.run(
        "embedding",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "output_path": str(ctx.temp_dir / "issues_cluster.embeddings.jsonl"),
        },
    )
    clusters = ctx.run(
        "embedding_cluster",
        {
            "dataset_name": str(csv_path),
            "embedding_uri": embedding["artifact"]["embedding_uri"],
            "cluster_similarity_threshold": 0.2,
            "sample_n": 2,
            "top_n": 3,
            "prior_artifacts": _prior(("step:dedup", dedup)),
        },
    )
    labels = ctx.run(
        "cluster_label_candidates",
        {
            "dataset_name": str(csv_path),
            "sample_n": 2,
            "top_n": 3,
            "prior_artifacts": _prior(("step:cluster", clusters)),
        },
    )
    return ctx.run(
        "issue_cluster_summary",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "sample_n": 2,
            "top_n": 3,
            "prior_artifacts": _prior(("step:cluster", clusters), ("step:labels", labels)),
        },
    )


def _issue_trend_summary_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues_trend.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "최근 결제 오류 추세를 보여줘",
            "sample_n": 5,
        },
    )
    buckets = ctx.run(
        "time_bucket_count",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "time_column": "occurred_at",
            "bucket": "day",
            "top_n": 3,
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    ctx.run(
        "document_sample",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "최근 결제 오류 추세를 보여줘",
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    return ctx.run(
        "issue_trend_summary",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "time_column": "occurred_at",
            "bucket": "day",
            "top_n": 3,
            "sample_n": 2,
            "prior_artifacts": _prior(("step:buckets", buckets)),
        },
    )


def _issue_period_compare_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues_compare.csv", ["occurred_at", "text"], _comparison_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "전일 대비 결제 오류가 얼마나 달라졌는지 비교해줘",
            "sample_n": 5,
        },
    )
    ctx.run(
        "document_sample",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "전일 대비 결제 오류가 얼마나 달라졌는지 비교해줘",
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    return ctx.run(
        "issue_period_compare",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "time_column": "occurred_at",
            "bucket": "day",
            "window_size": 1,
            "top_n": 3,
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )


def _issue_sentiment_summary_case(ctx: SkillCaseContext) -> dict[str, Any]:
    prepared_path = ctx.write_parquet("issues.prepared.parquet", _prepared_rows())
    sentiment = ctx.run(
        "sentiment_label",
        {
            "dataset_version_id": "version-skill-case",
            "dataset_name": str(prepared_path),
            "text_column": "normalized_text",
            "output_path": str(ctx.temp_dir / "issues.sentiment.parquet"),
        },
    )
    sentiment_dataset = sentiment["artifact"]["sentiment_uri"]
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(prepared_path),
            "text_column": "normalized_text",
            "query": "긍정 부정 감성 분포를 보여줘",
            "sample_n": 5,
        },
    )
    ctx.run(
        "document_sample",
        {
            "dataset_name": str(prepared_path),
            "text_column": "normalized_text",
            "query": "긍정 부정 감성 분포를 보여줘",
            "sample_n": 3,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    return ctx.run(
        "issue_sentiment_summary",
        {
            "dataset_name": sentiment_dataset,
            "prepared_dataset_name": str(prepared_path),
            "text_column": "normalized_text",
            "sentiment_column": "sentiment_label",
            "sample_n": 3,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )


def _issue_taxonomy_summary_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues_taxonomy.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "카테고리 태그 기준으로 이슈를 분류해줘",
            "sample_n": 5,
        },
    )
    tagging = ctx.run(
        "dictionary_tagging",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "top_n": 5,
            "sample_n": 3,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    return ctx.run(
        "issue_taxonomy_summary",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "top_n": 5,
            "sample_n": 3,
            "prior_artifacts": _prior(("step:tagging", tagging)),
        },
    )


def _issue_evidence_summary_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    filtered = ctx.run(
        "document_filter",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류 관련 근거를 보여줘",
            "sample_n": 5,
        },
    )
    samples = ctx.run(
        "document_sample",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류 관련 근거를 보여줘",
            "sample_n": 2,
            "prior_artifacts": _prior(("step:filter", filtered)),
        },
    )
    return ctx.run(
        "issue_evidence_summary",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류 관련 근거를 보여줘",
            "sample_n": 2,
            "prior_artifacts": _prior(("step:samples", samples)),
        },
    )


def _evidence_pack_case(ctx: SkillCaseContext) -> dict[str, Any]:
    csv_path = ctx.write_csv("issues.csv", ["occurred_at", "channel", "text"], _issue_rows())
    embedding = ctx.run(
        "embedding",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "output_path": str(ctx.temp_dir / "issues.embeddings.jsonl"),
        },
    )
    semantic = ctx.run(
        "semantic_search",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류 관련 근거를 보여줘",
            "sample_n": 2,
            "embedding_uri": embedding["artifact"]["embedding_uri"],
        },
    )
    return ctx.run(
        "evidence_pack",
        {
            "dataset_name": str(csv_path),
            "text_column": "text",
            "query": "결제 오류 관련 근거를 보여줘",
            "sample_n": 2,
            "prior_artifacts": _prior(("step:semantic", semantic)),
        },
    )


SKILL_CASES: dict[str, SkillCase] = {
    "planner": SkillCase("planner", "rule-based planner fallback case", _planner_case),
    "dataset_prepare": SkillCase("dataset_prepare", "prepare raw rows into normalized jsonl", _dataset_prepare_case),
    "sentiment_label": SkillCase("sentiment_label", "label prepared rows with fallback sentiment", _sentiment_label_case),
    "embedding": SkillCase("embedding", "build dense-or-token embedding sidecar", _embedding_case),
    "document_filter": SkillCase("document_filter", "lexical narrowing over issue rows", _document_filter_case),
    "deduplicate_documents": SkillCase("deduplicate_documents", "collapse duplicate or near-duplicate rows", _deduplicate_documents_case),
    "keyword_frequency": SkillCase("keyword_frequency", "count top terms from filtered rows", _keyword_frequency_case),
    "time_bucket_count": SkillCase("time_bucket_count", "bucket filtered rows by time", _time_bucket_count_case),
    "meta_group_count": SkillCase("meta_group_count", "group filtered rows by metadata dimension", _meta_group_count_case),
    "document_sample": SkillCase("document_sample", "select representative rows after filtering", _document_sample_case),
    "dictionary_tagging": SkillCase("dictionary_tagging", "apply deterministic taxonomy rules", _dictionary_tagging_case),
    "embedding_cluster": SkillCase("embedding_cluster", "cluster token-vector embeddings", _embedding_cluster_case),
    "cluster_label_candidates": SkillCase("cluster_label_candidates", "derive heuristic labels from clusters", _cluster_label_candidates_case),
    "semantic_search": SkillCase("semantic_search", "retrieve evidence candidates from embedding sidecar", _semantic_search_case),
    "unstructured_issue_summary": SkillCase("unstructured_issue_summary", "summarize generic issue rows with support artifacts", _unstructured_issue_summary_case),
    "issue_breakdown_summary": SkillCase("issue_breakdown_summary", "summarize grouped issues by channel", _issue_breakdown_summary_case),
    "issue_cluster_summary": SkillCase("issue_cluster_summary", "summarize dominant issue clusters", _issue_cluster_summary_case),
    "issue_trend_summary": SkillCase("issue_trend_summary", "summarize issue trends by time bucket", _issue_trend_summary_case),
    "issue_period_compare": SkillCase("issue_period_compare", "compare current vs previous periods", _issue_period_compare_case),
    "issue_sentiment_summary": SkillCase("issue_sentiment_summary", "summarize sentiment label distribution", _issue_sentiment_summary_case),
    "issue_taxonomy_summary": SkillCase("issue_taxonomy_summary", "summarize taxonomy-tagged issues", _issue_taxonomy_summary_case),
    "issue_evidence_summary": SkillCase("issue_evidence_summary", "build final evidence summary from sampled rows", _issue_evidence_summary_case),
    "evidence_pack": SkillCase("evidence_pack", "build reusable evidence bundle from semantic candidates", _evidence_pack_case),
}


def validate_skill_cases() -> None:
    handlers = set(task_handlers())
    cases = set(SKILL_CASES)
    if handlers != cases:
        missing = sorted(handlers - cases)
        extra = sorted(cases - handlers)
        message = {
            "missing_cases": missing,
            "extra_cases": extra,
        }
        raise ValueError(f"skill case registry mismatch: {json.dumps(message, ensure_ascii=False)}")


validate_skill_cases()
