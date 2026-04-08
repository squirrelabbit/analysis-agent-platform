package domain

import "testing"

func TestClusterMaterializationRequestForPlanReturnsConsistentRequest(t *testing.T) {
	plan := SkillPlan{
		Steps: []SkillPlanStep{
			{
				SkillName: "embedding_cluster",
				Inputs: map[string]any{
					"cluster_similarity_threshold": 0.2,
					"top_n":                        4,
					"sample_n":                     2,
				},
			},
			{
				SkillName: "issue_cluster_summary",
			},
		},
	}

	request, ok := ClusterMaterializationRequestForPlan(plan)
	if !ok || request == nil {
		t.Fatalf("expected materialized cluster request")
	}
	if request.SimilarityThreshold == nil || *request.SimilarityThreshold != 0.2 {
		t.Fatalf("unexpected threshold: %+v", request.SimilarityThreshold)
	}
	if request.TopN == nil || *request.TopN != 4 {
		t.Fatalf("unexpected top_n: %+v", request.TopN)
	}
	if request.SampleN == nil || *request.SampleN != 2 {
		t.Fatalf("unexpected sample_n: %+v", request.SampleN)
	}
}

func TestClusterMaterializationRequestForPlanRejectsMixedClusterParams(t *testing.T) {
	plan := SkillPlan{
		Steps: []SkillPlanStep{
			{
				SkillName: "embedding_cluster",
				Inputs: map[string]any{
					"cluster_similarity_threshold": 0.2,
				},
			},
			{
				SkillName: "embedding_cluster",
				Inputs: map[string]any{
					"cluster_similarity_threshold": 0.4,
				},
			},
		},
	}

	request, ok := ClusterMaterializationRequestForPlan(plan)
	if ok || request != nil {
		t.Fatalf("expected mixed cluster params to skip materialized request: %+v", request)
	}
}

func TestClusterRequestMatchesMetadataUsesDefaultsWhenParamsMissing(t *testing.T) {
	request := DatasetClusterBuildRequest{}
	metadata := map[string]any{
		"cluster_status": "ready",
		"cluster_ref":    "clusters.json",
	}

	if !ClusterRequestMatchesMetadata(request, metadata) {
		t.Fatalf("expected default cluster request to match ready metadata without explicit params")
	}
}

func TestClusterMaterializationRequestForPlanSkipsSubsetPipeline(t *testing.T) {
	plan := SkillPlan{
		Steps: []SkillPlanStep{
			{
				SkillName: "document_filter",
				Inputs: map[string]any{
					"query": "결제",
				},
			},
			{
				SkillName: "embedding_cluster",
				Inputs: map[string]any{
					"cluster_similarity_threshold": 0.2,
				},
			},
		},
	}

	request, ok := ClusterMaterializationRequestForPlan(plan)
	if ok || request != nil {
		t.Fatalf("expected subset pipeline to skip materialized request, got ok=%v request=%+v", ok, request)
	}
}
