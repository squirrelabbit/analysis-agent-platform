package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func TestBuildClustersStoresSummaryAndMembershipRefs(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-1", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-1",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	version := domain.DatasetVersion{
		DatasetVersionID: "version-cluster",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		PrepareStatus:    "ready",
		PrepareURI:       datasetStringPtr("/tmp/issues.prepared.parquet"),
		EmbeddingStatus:  "ready",
		Metadata: map[string]any{
			"embedding_index_source_ref": "/tmp/issues.embeddings.index.parquet",
			"chunk_ref":                  "/tmp/issues.embeddings.chunks.parquet",
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("unexpected decode error: %v", err)
		}
		if payload["output_path"] == "" {
			t.Fatalf("expected output_path in cluster build payload")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"cluster_ref":               "/tmp/issues.clusters.summary.json",
				"cluster_format":            "json",
				"cluster_summary_ref":       "/tmp/issues.clusters.summary.json",
				"cluster_summary_format":    "json",
				"cluster_membership_ref":    "/tmp/issues.clusters.memberships.parquet",
				"cluster_membership_format": "parquet",
				"cluster_algorithm":         "dense-hybrid-v1",
				"summary": map[string]any{
					"cluster_count":                3,
					"cluster_similarity_threshold": 0.2,
					"top_n":                        3,
					"sample_n":                     2,
					"cluster_membership_row_count": 6,
				},
			},
		})
	}))
	defer server.Close()
	service.pythonAIWorkerURL = server.URL

	result, err := service.BuildClusters(project.ProjectID, dataset.DatasetID, version.DatasetVersionID, domain.DatasetClusterBuildRequest{
		SimilarityThreshold: datasetFloat64Ptr(0.2),
		TopN:                datasetIntPtr(3),
		SampleN:             datasetIntPtr(2),
	})
	if err != nil {
		t.Fatalf("unexpected build clusters error: %v", err)
	}

	if got := metadataString(result.Metadata, "cluster_ref", ""); got != "/tmp/issues.clusters.summary.json" {
		t.Fatalf("unexpected cluster ref: %s", got)
	}
	if got := metadataString(result.Metadata, "cluster_summary_ref", ""); got != "/tmp/issues.clusters.summary.json" {
		t.Fatalf("unexpected cluster summary ref: %s", got)
	}
	if got := metadataString(result.Metadata, "cluster_membership_ref", ""); got != "/tmp/issues.clusters.memberships.parquet" {
		t.Fatalf("unexpected cluster membership ref: %s", got)
	}
	if got := metadataString(result.Metadata, "cluster_membership_format", ""); got != "parquet" {
		t.Fatalf("unexpected cluster membership format: %s", got)
	}
	if got := metadataString(result.Metadata, "cluster_status", ""); got != "ready" {
		t.Fatalf("unexpected cluster status: %s", got)
	}
}

func TestGetClusterMembersLoadsSummaryAndMembership(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewDatasetService(repository, "", t.TempDir(), t.TempDir())

	project := domain.Project{ProjectID: "project-cluster-members", Name: "test", CreatedAt: time.Now().UTC()}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}
	dataset := domain.Dataset{
		DatasetID: "dataset-cluster-members",
		ProjectID: project.ProjectID,
		Name:      "issues",
		DataType:  "unstructured",
		CreatedAt: time.Now().UTC(),
	}
	if err := repository.SaveDataset(dataset); err != nil {
		t.Fatalf("unexpected save dataset error: %v", err)
	}

	summaryPath := filepath.Join(t.TempDir(), "clusters.json")
	membershipPath := filepath.Join(t.TempDir(), "clusters.memberships.parquet")
	if err := os.WriteFile(summaryPath, []byte(`{
		"clusters":[
			{"cluster_id":"cluster-01","document_count":3,"top_terms":[{"term":"결제","count":3}]},
			{"cluster_id":"cluster-02","document_count":1,"top_terms":[{"term":"배송","count":1}]}
		]
	}`), 0o644); err != nil {
		t.Fatalf("unexpected summary write error: %v", err)
	}
	writeClusterMembershipParquet(t, membershipPath, []map[string]any{
		{
			"cluster_id":             "cluster-01",
			"cluster_rank":           1,
			"cluster_document_count": 3,
			"source_index":           0,
			"row_id":                 "row-0",
			"chunk_id":               "row-0:chunk:0",
			"chunk_index":            0,
			"text":                   "결제 오류가 반복 발생했습니다",
			"is_sample":              true,
		},
		{
			"cluster_id":             "cluster-01",
			"cluster_rank":           1,
			"cluster_document_count": 3,
			"source_index":           1,
			"row_id":                 "row-1",
			"chunk_id":               "row-1:chunk:0",
			"chunk_index":            0,
			"text":                   "결제 승인 오류가 다시 발생했습니다",
			"is_sample":              true,
		},
		{
			"cluster_id":             "cluster-01",
			"cluster_rank":           1,
			"cluster_document_count": 3,
			"source_index":           5,
			"row_id":                 "row-5",
			"chunk_id":               "row-5:chunk:0",
			"chunk_index":            0,
			"text":                   "결제 오류 문의가 접수됐습니다",
			"is_sample":              false,
		},
	})

	version := domain.DatasetVersion{
		DatasetVersionID: "version-cluster-members",
		DatasetID:        dataset.DatasetID,
		ProjectID:        project.ProjectID,
		StorageURI:       "/tmp/issues.csv",
		DataType:         "unstructured",
		Metadata: map[string]any{
			"cluster_status":            "ready",
			"cluster_ref":               summaryPath,
			"cluster_summary_ref":       summaryPath,
			"cluster_membership_ref":    membershipPath,
			"cluster_membership_format": "parquet",
		},
		PrepareStatus:   "ready",
		SentimentStatus: "not_requested",
		EmbeddingStatus: "ready",
		CreatedAt:       time.Now().UTC(),
	}
	if err := repository.SaveDatasetVersion(version); err != nil {
		t.Fatalf("unexpected save dataset version error: %v", err)
	}

	limit := 2
	samplesOnly := true
	response, err := service.GetClusterMembers(
		project.ProjectID,
		dataset.DatasetID,
		version.DatasetVersionID,
		"cluster-01",
		domain.DatasetClusterMembersQuery{
			Limit:       &limit,
			SamplesOnly: &samplesOnly,
		},
	)
	if err != nil {
		t.Fatalf("unexpected get cluster members error: %v", err)
	}
	if response.TotalCount != 3 {
		t.Fatalf("unexpected total_count: %d", response.TotalCount)
	}
	if response.SampleCount != 2 {
		t.Fatalf("unexpected sample_count: %d", response.SampleCount)
	}
	if len(response.Items) != 2 {
		t.Fatalf("unexpected item count: %d", len(response.Items))
	}
	if !response.Items[0].IsSample || !response.Items[1].IsSample {
		t.Fatalf("expected samples_only response, got %+v", response.Items)
	}
	if got := stringValue(response.Cluster["cluster_id"]); got != "cluster-01" {
		t.Fatalf("unexpected cluster summary payload: %+v", response.Cluster)
	}
	if response.ClusterMembershipRef != membershipPath {
		t.Fatalf("unexpected cluster membership ref: %s", response.ClusterMembershipRef)
	}
}
