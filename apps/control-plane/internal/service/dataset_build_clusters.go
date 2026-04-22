package service

import (
	"context"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

func (s *DatasetService) BuildClusters(projectID, datasetID, datasetVersionID string, input domain.DatasetClusterBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "cluster build requires unstructured or mixed dataset version"}
	}
	if !embeddingBuildReady(version) {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "embeddings must be ready before cluster build"}
	}

	force := input.Force != nil && *input.Force
	normalizedRequest := domain.NormalizeClusterBuildRequest(input)
	if domain.ClusterRequestMatchesMetadata(normalizedRequest, version.Metadata) && !force {
		return version, nil
	}

	embeddingIndexSourceRef := strings.TrimSpace(metadataString(version.Metadata, "embedding_index_source_ref", ""))
	if input.EmbeddingIndexSourceRef != nil && strings.TrimSpace(*input.EmbeddingIndexSourceRef) != "" {
		embeddingIndexSourceRef = strings.TrimSpace(*input.EmbeddingIndexSourceRef)
	}
	if embeddingIndexSourceRef == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "embedding index source ref is required for cluster build"}
	}

	chunkRef := strings.TrimSpace(metadataString(version.Metadata, "chunk_ref", ""))
	if input.ChunkRef != nil && strings.TrimSpace(*input.ChunkRef) != "" {
		chunkRef = strings.TrimSpace(*input.ChunkRef)
	}
	if chunkRef == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "chunk_ref is required for cluster build"}
	}

	outputPath := s.deriveClusterURI(version)
	if input.OutputPath != nil && strings.TrimSpace(*input.OutputPath) != "" {
		outputPath = strings.TrimSpace(*input.OutputPath)
	}
	membershipOutputPath := deriveClusterMembershipURI(outputPath)
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := ensureParentDir(membershipOutputPath); err != nil {
		return domain.DatasetVersion{}, err
	}

	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["cluster_status"] = "building"
	version.Metadata["cluster_ref"] = outputPath
	version.Metadata["cluster_format"] = "json"
	version.Metadata["cluster_summary_ref"] = outputPath
	version.Metadata["cluster_summary_format"] = "json"
	version.Metadata["cluster_membership_ref"] = membershipOutputPath
	version.Metadata["cluster_membership_format"] = "parquet"
	version.Metadata["cluster_source_embedding_ref"] = embeddingIndexSourceRef
	version.Metadata["cluster_similarity_threshold"] = *normalizedRequest.SimilarityThreshold
	version.Metadata["cluster_top_n"] = *normalizedRequest.TopN
	version.Metadata["cluster_sample_n"] = *normalizedRequest.SampleN
	version.Metadata["cluster_params_hash"] = domain.ClusterRequestHash(normalizedRequest)
	delete(version.Metadata, "cluster_error")
	delete(version.Metadata, "cluster_stale_reason")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id":           version.DatasetVersionID,
		"dataset_name":                 datasetSourceForUnstructured(version),
		"embedding_index_source_ref":   embeddingIndexSourceRef,
		"chunk_ref":                    chunkRef,
		"output_path":                  outputPath,
		"cluster_similarity_threshold": *normalizedRequest.SimilarityThreshold,
		"top_n":                        *normalizedRequest.TopN,
		"sample_n":                     *normalizedRequest.SampleN,
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/dataset_cluster_build", payload)
	if err != nil {
		version.Metadata["cluster_status"] = "failed"
		version.Metadata["cluster_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	version.ReadyAt = &now
	clusterRef := artifactString(response.Artifact, "cluster_ref")
	if clusterRef == "" {
		clusterRef = outputPath
	}
	clusterSummaryRef := artifactString(response.Artifact, "cluster_summary_ref")
	if clusterSummaryRef == "" {
		clusterSummaryRef = clusterRef
	}
	clusterSummaryFormat := artifactString(response.Artifact, "cluster_summary_format")
	if clusterSummaryFormat == "" {
		clusterSummaryFormat = artifactString(response.Artifact, "cluster_format")
	}
	if clusterSummaryFormat == "" {
		clusterSummaryFormat = "json"
	}
	clusterMembershipRef := artifactString(response.Artifact, "cluster_membership_ref")
	if clusterMembershipRef == "" {
		clusterMembershipRef = membershipOutputPath
	}
	clusterMembershipFormat := artifactString(response.Artifact, "cluster_membership_format")
	if clusterMembershipFormat == "" {
		clusterMembershipFormat = "parquet"
	}
	clusterMetadata := map[string]any{
		"cluster_status":               "ready",
		"cluster_ref":                  clusterRef,
		"cluster_format":               clusterSummaryFormat,
		"cluster_summary_ref":          clusterSummaryRef,
		"cluster_summary_format":       clusterSummaryFormat,
		"cluster_membership_ref":       clusterMembershipRef,
		"cluster_membership_format":    clusterMembershipFormat,
		"cluster_notes":                response.Notes,
		"cluster_algorithm":            artifactString(response.Artifact, "cluster_algorithm"),
		"cluster_source_embedding_ref": embeddingIndexSourceRef,
		"cluster_similarity_threshold": *normalizedRequest.SimilarityThreshold,
		"cluster_top_n":                *normalizedRequest.TopN,
		"cluster_sample_n":             *normalizedRequest.SampleN,
		"cluster_params_hash":          domain.ClusterRequestHash(normalizedRequest),
		"clustered_at":                 now,
	}
	if summary, ok := response.Artifact["summary"].(map[string]any); ok {
		clusterMetadata["cluster_summary"] = summary
	}
	version.Metadata = mergeStringAny(version.Metadata, clusterMetadata)
	delete(version.Metadata, "cluster_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}
