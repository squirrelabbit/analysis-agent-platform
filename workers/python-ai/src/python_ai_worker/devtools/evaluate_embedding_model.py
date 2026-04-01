from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from typing import Any

from .. import runtime as rt
from .embedding_eval_cases import CLUSTER_EVAL_CASES, SEARCH_EVAL_CASES


_generate_dense_embeddings = rt._generate_dense_embeddings
_generate_query_embedding = rt._generate_query_embedding
_cluster_embedding_records = rt._cluster_embedding_records
_tokenize = rt._tokenize


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate a local embedding model against fixed search/cluster fixtures.")
    parser.add_argument("--model", default=rt.DEFAULT_LOCAL_EMBEDDING_MODEL, help="Embedding model name to evaluate.")
    parser.add_argument("--format", choices=("json", "markdown"), default="json", help="Output format.")
    return parser


def evaluate_embedding_model(model: str) -> dict[str, Any]:
    search_reports = [_evaluate_search_case(case, model=model) for case in SEARCH_EVAL_CASES]
    cluster_reports = [_evaluate_cluster_case(case, model=model) for case in CLUSTER_EVAL_CASES]
    return {
        "model": model,
        "summary": {
            "search_case_count": len(search_reports),
            "search_top1_pass_count": sum(1 for report in search_reports if report["top1_pass"]),
            "search_topk_pass_count": sum(1 for report in search_reports if report["topk_pass"]),
            "cluster_case_count": len(cluster_reports),
            "cluster_dense_hybrid_pass_count": sum(1 for report in cluster_reports if report["dense_hybrid_pass"]),
            "cluster_dense_only_pass_count": sum(1 for report in cluster_reports if report["dense_only_pass"]),
        },
        "search_reports": search_reports,
        "cluster_reports": cluster_reports,
    }


def _evaluate_search_case(case: Any, *, model: str) -> dict[str, Any]:
    expected = list(case.expected_top_indices)
    dense_result = _generate_dense_embeddings(list(case.documents), model=model)
    query_vector = _generate_query_embedding(case.query, model=model)
    if dense_result is None or not query_vector:
        return {
            "case_id": case.case_id,
            "description": case.description,
            "status": "embedding_unavailable",
            "expected_top_indices": expected,
            "ranked_indices": [],
            "top1_pass": False,
            "topk_pass": False,
        }

    scored = []
    for index, vector in enumerate(dense_result["embeddings"]):
        normalized = _normalize_dense_vector(vector)
        score = sum(float(query_vector[pos]) * normalized[pos] for pos in range(min(len(query_vector), len(normalized))))
        scored.append({"index": index, "score": round(score, 6), "text": case.documents[index]})
    scored.sort(key=lambda item: (-float(item["score"]), int(item["index"])))

    ranked_indices = [int(item["index"]) for item in scored]
    top1_pass = bool(ranked_indices and expected and ranked_indices[0] == expected[0])
    topk_pass = all(index in ranked_indices[: len(expected)] for index in expected)
    return {
        "case_id": case.case_id,
        "description": case.description,
        "status": "ok",
        "expected_top_indices": expected,
        "ranked_indices": ranked_indices,
        "top1_pass": top1_pass,
        "topk_pass": topk_pass,
        "matches": scored[: min(5, len(scored))],
    }


def _evaluate_cluster_case(case: Any, *, model: str) -> dict[str, Any]:
    expected = sorted(tuple(sorted(cluster)) for cluster in case.expected_clusters)
    dense_result = _generate_dense_embeddings(list(case.documents), model=model)
    if dense_result is None:
        return {
            "case_id": case.case_id,
            "description": case.description,
            "status": "embedding_unavailable",
            "expected_clusters": expected,
            "dense_hybrid_clusters": [],
            "dense_only_clusters": [],
            "dense_hybrid_pass": False,
            "dense_only_pass": False,
        }

    records = []
    for index, text in enumerate(case.documents):
        token_counts = Counter(_tokenize(text))
        records.append(
            {
                "source_index": index,
                "row_id": f"{case.case_id}:row:{index}",
                "chunk_id": f"{case.case_id}:row:{index}:chunk:0",
                "text": text,
                "token_counts": dict(token_counts),
                "embedding": list(dense_result["embeddings"][index]),
            }
        )

    dense_hybrid_clusters = _cluster_embedding_records(
        records,
        case.similarity_threshold,
        sample_n=2,
        top_n=3,
        similarity_mode="dense-hybrid",
    )
    dense_only_clusters = _cluster_embedding_records(
        records,
        case.similarity_threshold,
        sample_n=2,
        top_n=3,
        similarity_mode="dense-only",
    )
    dense_hybrid_actual = sorted(tuple(sorted(cluster["member_source_indices"])) for cluster in dense_hybrid_clusters)
    dense_only_actual = sorted(tuple(sorted(cluster["member_source_indices"])) for cluster in dense_only_clusters)
    return {
        "case_id": case.case_id,
        "description": case.description,
        "status": "ok",
        "expected_clusters": expected,
        "dense_hybrid_clusters": dense_hybrid_actual,
        "dense_only_clusters": dense_only_actual,
        "dense_hybrid_pass": dense_hybrid_actual == expected,
        "dense_only_pass": dense_only_actual == expected,
    }


def _normalize_dense_vector(values: Any) -> list[float]:
    vector: list[float] = []
    for item in list(values or []):
        try:
            vector.append(float(item))
        except (TypeError, ValueError):
            return []
    norm = math.sqrt(sum(component * component for component in vector))
    if norm <= 0:
        return []
    return [component / norm for component in vector]


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# Embedding Eval Report",
        "",
        f"- model: `{report['model']}`",
        f"- search top1 pass: `{report['summary']['search_top1_pass_count']}/{report['summary']['search_case_count']}`",
        f"- search topk pass: `{report['summary']['search_topk_pass_count']}/{report['summary']['search_case_count']}`",
        f"- cluster dense-hybrid pass: `{report['summary']['cluster_dense_hybrid_pass_count']}/{report['summary']['cluster_case_count']}`",
        f"- cluster dense-only pass: `{report['summary']['cluster_dense_only_pass_count']}/{report['summary']['cluster_case_count']}`",
        "",
        "## Search",
    ]
    for item in report["search_reports"]:
        lines.append(f"- `{item['case_id']}` top1={item['top1_pass']} topk={item['topk_pass']} ranked={item.get('ranked_indices', [])}")
    lines.extend(["", "## Cluster"])
    for item in report["cluster_reports"]:
        lines.append(
            f"- `{item['case_id']}` hybrid={item['dense_hybrid_pass']} dense_only={item['dense_only_pass']} "
            f"expected={item.get('expected_clusters', [])} hybrid_clusters={item.get('dense_hybrid_clusters', [])}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = evaluate_embedding_model(args.model)
    if args.format == "markdown":
        print(render_markdown(report))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
