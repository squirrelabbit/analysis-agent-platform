package service

import (
	"fmt"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

func clusterPlanReady(plan domain.SkillPlan, version domain.DatasetVersion) bool {
	request, ok := domain.ClusterMaterializationRequestForPlan(plan)
	if !ok || request == nil {
		return true
	}
	return domain.ClusterRequestMatchesMetadata(*request, version.Metadata)
}

func clusterStepReady(step domain.SkillPlanStep, version domain.DatasetVersion) bool {
	if step.SkillName != "embedding_cluster" {
		return true
	}
	request := domain.ClusterBuildRequestFromStep(step)
	return domain.ClusterRequestMatchesMetadata(request, version.Metadata)
}

func invalidateClusterArtifacts(version *domain.DatasetVersion, reason string) {
	if version == nil {
		return
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	currentStatus := strings.TrimSpace(fmt.Sprintf("%v", version.Metadata["cluster_status"]))
	if currentStatus == "" || currentStatus == "not_requested" {
		version.Metadata["cluster_status"] = "not_requested"
		delete(version.Metadata, "cluster_stale_reason")
	} else {
		version.Metadata["cluster_status"] = "stale"
		version.Metadata["cluster_stale_reason"] = reason
	}
	delete(version.Metadata, "cluster_ref")
	delete(version.Metadata, "cluster_format")
	delete(version.Metadata, "cluster_summary_ref")
	delete(version.Metadata, "cluster_summary_format")
	delete(version.Metadata, "cluster_membership_ref")
	delete(version.Metadata, "cluster_membership_format")
	delete(version.Metadata, "cluster_notes")
	delete(version.Metadata, "cluster_summary")
	delete(version.Metadata, "cluster_algorithm")
	delete(version.Metadata, "cluster_source_embedding_ref")
	delete(version.Metadata, "cluster_error")
	delete(version.Metadata, "cluster_similarity_threshold")
	delete(version.Metadata, "cluster_top_n")
	delete(version.Metadata, "cluster_sample_n")
	delete(version.Metadata, "cluster_params_hash")
	delete(version.Metadata, "clustered_at")
}

func invalidateEmbeddingArtifacts(version *domain.DatasetVersion, reason string) {
	if version == nil {
		return
	}
	if version.EmbeddingStatus != "" && version.EmbeddingStatus != "not_requested" {
		version.EmbeddingStatus = "stale"
	} else if version.EmbeddingStatus == "" {
		version.EmbeddingStatus = "not_requested"
	}
	version.EmbeddingURI = nil
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	if version.EmbeddingStatus == "stale" {
		version.Metadata["embedding_stale_reason"] = reason
	} else {
		delete(version.Metadata, "embedding_stale_reason")
	}
	delete(version.Metadata, "embedding_ref")
	delete(version.Metadata, "embedding_format")
	delete(version.Metadata, "embedding_index_ref")
	delete(version.Metadata, "embedding_index_source_ref")
	delete(version.Metadata, "embedding_index_source_format")
	delete(version.Metadata, "embedding_notes")
	delete(version.Metadata, "embedding_usage")
	delete(version.Metadata, "embedding_provider")
	delete(version.Metadata, "embedding_representation")
	delete(version.Metadata, "embedding_vector_dim")
	delete(version.Metadata, "chunk_ref")
	delete(version.Metadata, "chunk_format")
	delete(version.Metadata, "chunk_id_column")
	delete(version.Metadata, "chunk_index_column")
	delete(version.Metadata, "chunk_text_column")
	delete(version.Metadata, "chunking_strategy")
	delete(version.Metadata, "chunk_count")
	delete(version.Metadata, "document_count")
	delete(version.Metadata, "source_row_count")
	delete(version.Metadata, "embedding_error")
	invalidateClusterArtifacts(version, reason)
}

func invalidateSentimentArtifacts(version *domain.DatasetVersion, reason string) {
	if version == nil {
		return
	}
	if version.SentimentStatus != "" && version.SentimentStatus != "not_requested" {
		version.SentimentStatus = "stale"
	} else if version.SentimentStatus == "" {
		version.SentimentStatus = "not_requested"
	}
	version.SentimentURI = nil
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	if version.SentimentStatus == "stale" {
		version.Metadata["sentiment_stale_reason"] = reason
	} else {
		delete(version.Metadata, "sentiment_stale_reason")
	}
	delete(version.Metadata, "sentiment_ref")
	delete(version.Metadata, "sentiment_format")
	delete(version.Metadata, "sentiment_notes")
	delete(version.Metadata, "sentiment_summary")
	delete(version.Metadata, "sentiment_usage")
	delete(version.Metadata, "sentiment_error")
}

func invalidateDownstreamArtifactsForPrepare(version *domain.DatasetVersion, reason string) {
	invalidateSentimentArtifacts(version, reason)
	invalidateEmbeddingArtifacts(version, reason)
}

func invalidatePrepareArtifacts(version *domain.DatasetVersion, reason string) {
	if version == nil {
		return
	}
	if version.PrepareStatus != "" && version.PrepareStatus != "not_requested" {
		version.PrepareStatus = "stale"
	} else if version.PrepareStatus == "" {
		version.PrepareStatus = "not_requested"
	}
	version.PrepareURI = nil
	version.PreparedAt = nil
	version.PreparePromptVer = nil
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	if version.PrepareStatus == "stale" {
		version.Metadata["prepare_stale_reason"] = reason
	} else {
		delete(version.Metadata, "prepare_stale_reason")
	}
	delete(version.Metadata, "prepared_ref")
	delete(version.Metadata, "prepared_format")
	delete(version.Metadata, "prepare_format")
	delete(version.Metadata, "prepare_notes")
	delete(version.Metadata, "prepare_summary")
	delete(version.Metadata, "prepare_usage")
	delete(version.Metadata, "prepare_error")
	delete(version.Metadata, "prepare_progress_ref")
	delete(version.Metadata, "prepare_max_rows")
	delete(version.Metadata, "prepared_text_column")
	clearLLMFallbackMetadata(version.Metadata, "prepare")
	invalidateDownstreamArtifactsForPrepare(version, reason)
}

func invalidateDownstreamArtifactsForClean(version *domain.DatasetVersion, reason string) {
	invalidatePrepareArtifacts(version, reason)
}
