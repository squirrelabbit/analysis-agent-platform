package service

import (
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

func TestInvalidateDownstreamArtifactsForPrepareMarksReadyArtifactsStale(t *testing.T) {
	version := domain.DatasetVersion{
		SentimentStatus: "ready",
		SentimentURI:    stringPtr("sentiment.parquet"),
		EmbeddingStatus: "ready",
		EmbeddingURI:    stringPtr("embeddings.jsonl"),
		Metadata: map[string]any{
			"cluster_status":             "ready",
			"cluster_ref":                "clusters.json",
			"cluster_summary_ref":        "clusters.json",
			"cluster_membership_ref":     "clusters.memberships.parquet",
			"embedding_index_source_ref": "embeddings.index.parquet",
			"chunk_ref":                  "chunks.parquet",
		},
	}

	invalidateDownstreamArtifactsForPrepare(&version, "prepare output changed")

	if version.SentimentStatus != "stale" {
		t.Fatalf("expected stale sentiment status, got %s", version.SentimentStatus)
	}
	if version.EmbeddingStatus != "stale" {
		t.Fatalf("expected stale embedding status, got %s", version.EmbeddingStatus)
	}
	if got := metadataString(version.Metadata, "cluster_status", ""); got != "stale" {
		t.Fatalf("expected stale cluster status, got %s", got)
	}
	if metadataString(version.Metadata, "cluster_ref", "") != "" {
		t.Fatalf("expected cleared cluster ref: %+v", version.Metadata)
	}
	if metadataString(version.Metadata, "cluster_summary_ref", "") != "" {
		t.Fatalf("expected cleared cluster summary ref: %+v", version.Metadata)
	}
	if metadataString(version.Metadata, "cluster_membership_ref", "") != "" {
		t.Fatalf("expected cleared cluster membership ref: %+v", version.Metadata)
	}
	if metadataString(version.Metadata, "embedding_index_source_ref", "") != "" {
		t.Fatalf("expected cleared embedding index ref: %+v", version.Metadata)
	}
}

func TestClusterStepReadyRespectsStoredClusterParams(t *testing.T) {
	version := domain.DatasetVersion{
		Metadata: map[string]any{
			"cluster_status":               "ready",
			"cluster_ref":                  "clusters.json",
			"cluster_similarity_threshold": 0.3,
			"cluster_top_n":                10,
			"cluster_sample_n":             3,
		},
	}
	defaultStep := domain.SkillPlanStep{SkillName: "embedding_cluster", Inputs: map[string]any{}}
	customStep := domain.SkillPlanStep{
		SkillName: "embedding_cluster",
		Inputs: map[string]any{
			"cluster_similarity_threshold": 0.2,
		},
	}

	if !clusterStepReady(defaultStep, version) {
		t.Fatalf("expected default cluster step to match stored params")
	}
	if clusterStepReady(customStep, version) {
		t.Fatalf("expected custom cluster step to reject mismatched stored params")
	}
}
