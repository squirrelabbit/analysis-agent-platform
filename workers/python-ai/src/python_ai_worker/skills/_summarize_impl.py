from __future__ import annotations

"""Private summarize-layer skill implementations."""

from collections import Counter
from typing import Any

from .. import runtime as rt

def _cluster_membership_ref(*artifacts: dict[str, Any] | None) -> str:
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        value = str(artifact.get("cluster_membership_ref") or "").strip()
        if value:
            return value
    return ""


def _cluster_execution_meta(*artifacts: dict[str, Any] | None) -> dict[str, Any]:
    meta = {
        "cluster_execution_mode": "",
        "cluster_materialization_scope": "",
        "cluster_materialized_ref_used": False,
        "cluster_fallback_reason": "",
    }
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        execution_mode = str(artifact.get("cluster_execution_mode") or "").strip()
        if execution_mode and not meta["cluster_execution_mode"]:
            meta["cluster_execution_mode"] = execution_mode
        scope = str(artifact.get("cluster_materialization_scope") or "").strip()
        if scope and not meta["cluster_materialization_scope"]:
            meta["cluster_materialization_scope"] = scope
        if bool(artifact.get("cluster_materialized_ref_used")):
            meta["cluster_materialized_ref_used"] = True
        fallback_reason = str(artifact.get("cluster_fallback_reason") or "").strip()
        if fallback_reason and not meta["cluster_fallback_reason"]:
            meta["cluster_fallback_reason"] = fallback_reason
    return meta


def _cluster_samples_from_membership(cluster_membership_ref: str, cluster_id: str, sample_n: int) -> list[dict[str, Any]]:
    rows = rt._load_cluster_membership_rows(cluster_membership_ref, cluster_id, limit=max(0, sample_n))
    samples: list[dict[str, Any]] = []
    for row in rows:
        text = str(row.get("text") or "").strip()
        if not text:
            continue
        samples.append(
            {
                "source_index": int(row.get("source_index") or 0),
                "row_id": str(row.get("row_id") or "").strip(),
                "chunk_id": str(row.get("chunk_id") or "").strip(),
                "chunk_index": int(row.get("chunk_index") or 0),
                "text": text[:240],
            }
        )
        if len(samples) >= sample_n:
            break
    return samples


def run_issue_trend_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_trend_task_payload(payload)
    prior = rt._find_prior_artifact(payload.get("prior_artifacts"), "time_bucket_count")
    if prior:
        return {
            "notes": [
                "issue_trend_summary reused time_bucket_count artifact",
                f"dataset source: {normalized['dataset_name']}",
            ],
            "artifact": rt._copy_artifact_fields(prior, "issue_trend_summary", normalized["step"].get("step_id")),
        }

    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = rt._build_time_bucket_artifact(normalized, selected_rows)
    artifact["skill_name"] = "issue_trend_summary"
    return {
        "notes": [
            f"python-ai built {normalized['bucket']} trend",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_issue_breakdown_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_breakdown_task_payload(payload)
    prior = rt._find_prior_artifact(payload.get("prior_artifacts"), "meta_group_count")
    if prior:
        return {
            "notes": [
                "issue_breakdown_summary reused meta_group_count artifact",
                f"dataset source: {normalized['dataset_name']}",
            ],
            "artifact": rt._copy_artifact_fields(prior, "issue_breakdown_summary", normalized["step"].get("step_id")),
        }

    selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
    artifact = rt._build_meta_group_artifact(normalized, selected_rows)
    artifact["skill_name"] = "issue_breakdown_summary"
    return {
        "notes": [
            f"python-ai grouped rows by {normalized['dimension_column']}",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": artifact,
    }


def run_issue_cluster_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    labeled_clusters = rt._find_prior_artifact(payload.get("prior_artifacts"), "cluster_label_candidates")
    embedded_clusters = rt._find_prior_artifact(payload.get("prior_artifacts"), "embedding_cluster")
    if labeled_clusters is None and embedded_clusters is None:
        raise ValueError(
            "issue_cluster_summary requires cluster_label_candidates or embedding_cluster prior artifact"
        )
    cluster_membership_ref = _cluster_membership_ref(labeled_clusters, embedded_clusters)
    cluster_execution_meta = _cluster_execution_meta(labeled_clusters, embedded_clusters)

    clusters: list[dict[str, Any]] = []
    if labeled_clusters:
        for item in list(labeled_clusters.get("clusters") or []):
            if isinstance(item, dict):
                clusters.append(dict(item))
    elif embedded_clusters:
        for item in list(embedded_clusters.get("clusters") or []):
            if not isinstance(item, dict):
                continue
            top_terms = list(item.get("top_terms") or [])
            labels = rt._cluster_candidate_labels(top_terms)
            clusters.append(
                {
                    "cluster_id": item.get("cluster_id"),
                    "document_count": int(item.get("document_count") or 0),
                    "label": labels[0] if labels else "기타 이슈",
                    "candidate_labels": labels,
                    "top_terms": top_terms[: normalized["top_n"]],
                    "samples": list(item.get("sample_documents") or [])[: normalized["sample_n"]],
                    "rationale": rt._cluster_label_rationale(top_terms),
                }
            )
    total_documents = sum(int(item.get("document_count") or 0) for item in clusters)
    ranked_clusters = []
    for rank, cluster in enumerate(
        sorted(clusters, key=lambda item: (-int(item.get("document_count") or 0), str(item.get("label") or ""))),
        start=1,
    ):
        count = int(cluster.get("document_count") or 0)
        cluster_id = str(cluster.get("cluster_id") or "").strip()
        membership_samples = _cluster_samples_from_membership(cluster_membership_ref, cluster_id, normalized["sample_n"])
        samples = membership_samples or list(cluster.get("samples") or [])[: normalized["sample_n"]]
        ranked_clusters.append(
            {
                "rank": rank,
                "cluster_id": cluster_id,
                "label": cluster.get("label") or "기타 이슈",
                "document_count": count,
                "ratio_pct": round((count / total_documents) * 100, 2) if total_documents > 0 else 0.0,
                "candidate_labels": list(cluster.get("candidate_labels") or []),
                "top_terms": list(cluster.get("top_terms") or [])[: normalized["top_n"]],
                "samples": samples,
                "rationale": cluster.get("rationale") or "",
            }
        )

    summary = {
        "cluster_count": len(ranked_clusters),
        "clustered_document_count": total_documents,
    }
    if ranked_clusters:
        summary["dominant_cluster_label"] = ranked_clusters[0]["label"]
        summary["dominant_cluster_count"] = ranked_clusters[0]["document_count"]

    return {
        "notes": [
            f"issue_cluster_summary summarized {len(ranked_clusters)} clusters",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": {
            "skill_name": "issue_cluster_summary",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "cluster_membership_ref": cluster_membership_ref,
            "cluster_execution_mode": cluster_execution_meta["cluster_execution_mode"],
            "cluster_materialization_scope": cluster_execution_meta["cluster_materialization_scope"],
            "cluster_materialized_ref_used": cluster_execution_meta["cluster_materialized_ref_used"],
            "cluster_fallback_reason": cluster_execution_meta["cluster_fallback_reason"],
            "summary": summary,
            "clusters": ranked_clusters,
        },
    }


def run_issue_period_compare(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_compare_task_payload(payload)
    bucket_documents: dict[str, list[str]] = {}
    skipped_rows = 0

    for item in rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts")):
        row = item["row"]
        text = item["text"]
        raw_timestamp = str(row.get(normalized["time_column"]) or "").strip()
        if not text or not raw_timestamp:
            skipped_rows += 1
            continue
        parsed_at = rt._parse_timestamp(raw_timestamp)
        if parsed_at is None:
            skipped_rows += 1
            continue
        bucket_label = rt._bucket_label(parsed_at, normalized["bucket"])
        bucket_documents.setdefault(bucket_label, []).append(text[:240])

    bucket_labels = sorted(bucket_documents)
    current_buckets, previous_buckets = rt._resolve_compare_periods(bucket_labels, normalized)
    current_documents = rt._collect_bucket_documents(bucket_documents, current_buckets)
    previous_documents = rt._collect_bucket_documents(bucket_documents, previous_buckets)
    current_terms = Counter()
    previous_terms = Counter()
    for document in current_documents:
        current_terms.update(rt._tokenize(document))
    for document in previous_documents:
        previous_terms.update(rt._tokenize(document))

    current_count = len(current_documents)
    previous_count = len(previous_documents)
    count_delta = current_count - previous_count
    count_delta_ratio_pct = None
    if previous_count > 0:
        count_delta_ratio_pct = round((count_delta / previous_count) * 100, 2)

    notes = [
        f"python-ai compared {normalized['window_size']} {normalized['bucket']} bucket(s)",
        f"dataset source: {normalized['dataset_name']}",
    ]
    if skipped_rows > 0:
        notes.append(f"skipped_rows={skipped_rows}")
    if not current_buckets:
        notes.append("current period could not be resolved")

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "issue_period_compare",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "time_column": normalized["time_column"],
            "bucket": normalized["bucket"],
            "window_size": normalized["window_size"],
            "summary": {
                "current_count": current_count,
                "previous_count": previous_count,
                "count_delta": count_delta,
                "count_delta_ratio_pct": count_delta_ratio_pct,
                "current_period_start": rt._period_start(current_buckets),
                "current_period_end": rt._period_end(current_buckets),
                "previous_period_start": rt._period_start(previous_buckets),
                "previous_period_end": rt._period_end(previous_buckets),
            },
            "periods": {
                "current": rt._build_period_payload(current_buckets, current_documents, current_terms, normalized["top_n"], normalized["sample_n"]),
                "previous": rt._build_period_payload(previous_buckets, previous_documents, previous_terms, normalized["top_n"], normalized["sample_n"]),
            },
            "top_term_deltas": rt._build_term_deltas(current_terms, previous_terms, normalized["top_n"]),
        },
    }


def _prepared_text_lookup(dataset_name: str, text_column: str, prior_artifacts: Any) -> tuple[dict[str, str], dict[int, str]]:
    by_row_id: dict[str, str] = {}
    by_source_index: dict[int, str] = {}
    if not dataset_name:
        return by_row_id, by_source_index
    for item in rt._selected_text_rows(dataset_name, text_column, prior_artifacts):
        text = item["text"]
        if not text:
            continue
        row_id = str(item.get("row_id") or "").strip()
        if row_id:
            by_row_id[row_id] = text
        by_source_index[int(item["source_index"])] = text
    return by_row_id, by_source_index


def run_issue_sentiment_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_sentiment_summary_payload(payload)
    label_counts: Counter[str] = Counter()
    label_samples: dict[str, list[str]] = {}
    unlabeled_rows = 0
    prepared_by_row_id, prepared_by_source_index = _prepared_text_lookup(
        normalized["prepared_dataset_name"],
        normalized["text_column"],
        payload.get("prior_artifacts"),
    )

    for item in rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts")):
        row = item["row"]
        text = item["text"]
        if not text and normalized["prepared_dataset_name"]:
            row_id = str(row.get(normalized["row_id_column"]) or row.get("row_id") or "").strip()
            if row_id:
                text = prepared_by_row_id.get(row_id, "")
            if not text:
                source_index_value = row.get(normalized["source_row_index_column"])
                try:
                    source_index = int(source_index_value)
                except (TypeError, ValueError):
                    source_index = int(item["source_index"])
                text = prepared_by_source_index.get(source_index, "")
        label = str(row.get(normalized["sentiment_column"]) or "").strip().lower()
        if not text or label not in rt.SENTIMENT_LABELS:
            unlabeled_rows += 1
            continue
        label_counts.update([label])
        label_samples.setdefault(label, [])
        if len(label_samples[label]) < normalized["sample_n"]:
            label_samples[label].append(text[:240])

    total_labeled = sum(label_counts.values())
    ranked_labels = sorted(label_counts.items(), key=lambda item: (-item[1], item[0]))
    breakdown = []
    for rank, (label, count) in enumerate(ranked_labels, start=1):
        ratio_pct = round((count / total_labeled) * 100, 2) if total_labeled > 0 else 0.0
        breakdown.append(
            {
                "rank": rank,
                "sentiment_label": label,
                "count": count,
                "ratio_pct": ratio_pct,
                "samples": label_samples.get(label, []),
            }
        )

    notes = [
        f"python-ai summarized sentiment labels across {total_labeled} rows",
        f"dataset source: {normalized['dataset_name']}",
    ]
    if normalized["prepared_dataset_name"]:
        notes.append(f"prepared dataset source: {normalized['prepared_dataset_name']}")
    if unlabeled_rows > 0:
        notes.append(f"unlabeled_rows={unlabeled_rows}")
    if not breakdown:
        notes.append("no labeled rows found")

    summary = {
        "document_count": total_labeled,
        "sentiment_column": normalized["sentiment_column"],
        "label_count": len(label_counts),
    }
    if breakdown:
        summary["dominant_label"] = breakdown[0]["sentiment_label"]
        summary["dominant_label_count"] = breakdown[0]["count"]
    for label in ("positive", "negative", "neutral", "mixed", "unknown"):
        count = label_counts.get(label, 0)
        summary[f"{label}_count"] = count
        summary[f"{label}_ratio_pct"] = round((count / total_labeled) * 100, 2) if total_labeled > 0 else 0.0

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "issue_sentiment_summary",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "prepared_dataset_name": normalized["prepared_dataset_name"],
            "text_column": normalized["text_column"],
            "sentiment_column": normalized["sentiment_column"],
            "summary": summary,
            "breakdown": breakdown,
        },
    }


def run_issue_taxonomy_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_dictionary_tagging_payload(payload)
    prior = rt._find_prior_artifact(payload.get("prior_artifacts"), "dictionary_tagging")
    if prior:
        breakdown = list(prior.get("taxonomy_breakdown") or [])
        summary = dict(prior.get("summary") or {})
    else:
        selected_rows = rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts"))
        tagging_artifact = rt._build_dictionary_tagging_artifact(normalized, selected_rows)
        breakdown = list(tagging_artifact.get("taxonomy_breakdown") or [])
        summary = dict(tagging_artifact.get("summary") or {})

    if breakdown:
        summary["dominant_taxonomy"] = breakdown[0]["taxonomy_id"]
        summary["dominant_taxonomy_label"] = breakdown[0]["label"]
        summary["dominant_taxonomy_count"] = breakdown[0]["count"]

    return {
        "notes": [
            f"issue_taxonomy_summary summarized {len(breakdown)} taxonomy groups",
            f"dataset source: {normalized['dataset_name']}",
        ],
        "artifact": {
            "skill_name": "issue_taxonomy_summary",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "summary": summary,
            "taxonomy_breakdown": breakdown,
        },
    }


def run_issue_evidence_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return _run_evidence_summary(payload, "issue_evidence_summary")


def run_evidence_pack(payload: dict[str, Any]) -> dict[str, Any]:
    return _run_evidence_summary(payload, "evidence_pack")


def _run_evidence_summary(payload: dict[str, Any], artifact_skill_name: str) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    selected, selection_source = rt._select_evidence_candidates(payload, normalized)
    analysis_context = rt._analysis_context_entries(payload.get("prior_artifacts"))
    client = rt._anthropic_client()
    if client and client.is_enabled():
        try:
            return rt._run_evidence_pack_with_llm(
                client,
                normalized,
                selected,
                selection_source,
                artifact_skill_name,
                analysis_context,
            )
        except Exception as exc:
            fallback = rt._run_evidence_pack_fallback(
                normalized,
                selected,
                selection_source,
                artifact_skill_name,
                analysis_context,
            )
            fallback["notes"].append(f"anthropic evidence fallback: {exc}")
            return fallback
    return rt._run_evidence_pack_fallback(
        normalized,
        selected,
        selection_source,
        artifact_skill_name,
        analysis_context,
    )

def run_unstructured_issue_summary(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = rt._normalize_text_task_payload(payload)
    keyword_artifact = rt._find_prior_artifact(payload.get("prior_artifacts"), "keyword_frequency")
    sample_artifact = rt._find_prior_artifact(payload.get("prior_artifacts"), "document_sample")
    if keyword_artifact or sample_artifact:
        summary = {
            "document_count": int((((keyword_artifact or {}).get("summary") or {}).get("document_count") or 0)),
            "unique_terms": int((((keyword_artifact or {}).get("summary") or {}).get("unique_terms") or 0)),
            "total_terms": int((((keyword_artifact or {}).get("summary") or {}).get("total_terms") or 0)),
        }
        top_terms = list((keyword_artifact or {}).get("top_terms") or [])
        samples = []
        for item in list((sample_artifact or {}).get("samples") or []):
            samples.append(
                {
                    "rank": int(item.get("rank") or 0),
                    "text": str(item.get("text") or "")[:240],
                }
            )
        return {
            "notes": [
                "unstructured_issue_summary reused support skill artifacts",
                f"dataset source: {normalized['dataset_name']}",
            ],
            "artifact": {
                "skill_name": "unstructured_issue_summary",
                "step_id": normalized["step"].get("step_id"),
                "dataset_name": normalized["dataset_name"],
                "summary": summary,
                "top_terms": top_terms,
                "samples": samples,
            },
        }

    documents = [item["text"] for item in rt._selected_text_rows(normalized["dataset_name"], normalized["text_column"], payload.get("prior_artifacts")) if item["text"]]
    tokens = Counter()
    samples: list[dict[str, Any]] = []
    total_terms = 0
    for index, document in enumerate(documents):
        row_tokens = rt._tokenize(document)
        total_terms += len(row_tokens)
        tokens.update(row_tokens)
        if index < normalized["sample_n"]:
            samples.append({"rank": index + 1, "text": document[:240]})

    top_terms = [
        {"term": term, "count": count}
        for term, count in tokens.most_common(normalized["top_n"])
    ]
    notes = [
        f"python-ai analyzed {len(documents)} documents",
        f"dataset source: {normalized['dataset_name']}",
    ]
    if not documents:
        notes.append("no non-empty documents found")

    return {
        "notes": notes,
        "artifact": {
            "skill_name": "unstructured_issue_summary",
            "step_id": normalized["step"].get("step_id"),
            "dataset_name": normalized["dataset_name"],
            "summary": {
                "document_count": len(documents),
                "unique_terms": len(tokens),
                "total_terms": total_terms,
            },
            "top_terms": top_terms,
            "samples": samples,
        },
    }
