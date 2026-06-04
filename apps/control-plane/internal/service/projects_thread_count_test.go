package service

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// silverone 2026-06-01 — Project.AnalysisThreadCount 잠금. 사이드바 채팅
// count가 project 단위 합산이고, dataset 단위 thread API와 분리됨을 보장.
// store level + service level 두 layer 모두 잠금.

func TestCountAnalysisThreadsByProjectEmpty(t *testing.T) {
	memStore := store.NewMemoryStore()
	count, err := memStore.CountAnalysisThreadsByProject("p-empty")
	if err != nil {
		t.Fatalf("CountAnalysisThreadsByProject: %v", err)
	}
	if count != 0 {
		t.Errorf("empty project must return 0, got %d", count)
	}
}

func TestCountAnalysisThreadsByProjectSumsAcrossDatasets(t *testing.T) {
	memStore := store.NewMemoryStore()
	now := time.Now().UTC()
	// p1 — 3 thread across 2 datasets
	for i, threadID := range []struct{ tid, dsid string }{
		{"t1", "d1"}, {"t2", "d1"}, {"t3", "d2"},
	} {
		_ = i
		if err := memStore.SaveAnalysisThread(domain.AnalysisThread{
			ThreadID: threadID.tid, ProjectID: "p1", DatasetID: threadID.dsid,
			DatasetVersionID: "v1", CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("SaveAnalysisThread: %v", err)
		}
	}
	// p2 — 1 thread (다른 project 제외 검증)
	if err := memStore.SaveAnalysisThread(domain.AnalysisThread{
		ThreadID: "t4", ProjectID: "p2", DatasetID: "d3",
		DatasetVersionID: "v1", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("SaveAnalysisThread p2: %v", err)
	}

	c1, err := memStore.CountAnalysisThreadsByProject("p1")
	if err != nil {
		t.Fatalf("count p1: %v", err)
	}
	if c1 != 3 {
		t.Errorf("p1 thread count: want 3, got %d", c1)
	}
	c2, err := memStore.CountAnalysisThreadsByProject("p2")
	if err != nil {
		t.Fatalf("count p2: %v", err)
	}
	if c2 != 1 {
		t.Errorf("p2 thread count: want 1, got %d", c2)
	}
	c3, err := memStore.CountAnalysisThreadsByProject("p-nonexistent")
	if err != nil {
		t.Fatalf("count p-nonexistent: %v", err)
	}
	if c3 != 0 {
		t.Errorf("nonexistent project: want 0, got %d", c3)
	}
}

// service level integration — ProjectService.GetProject 응답에 채워지는지.
func TestProjectServiceGetProjectIncludesAnalysisThreadCount(t *testing.T) {
	memStore := store.NewMemoryStore()
	svc := NewProjectService(memStore, "", "")

	now := time.Now().UTC()
	if err := memStore.SaveProject(domain.Project{
		ProjectID: "p1", Name: "test", CreatedAt: now,
	}); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	// 2 threads
	for _, tid := range []string{"t1", "t2"} {
		if err := memStore.SaveAnalysisThread(domain.AnalysisThread{
			ThreadID: tid, ProjectID: "p1", DatasetID: "d1",
			DatasetVersionID: "v1", CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("SaveAnalysisThread %s: %v", tid, err)
		}
	}

	proj, err := svc.GetProject("p1")
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if proj.AnalysisThreadCount != 2 {
		t.Errorf("AnalysisThreadCount: want 2, got %d", proj.AnalysisThreadCount)
	}
	// 기존 count 회귀 없음 잠금
	if proj.PromptCount != 0 {
		t.Errorf("PromptCount: want 0 (no prompts), got %d", proj.PromptCount)
	}
	if proj.DatasetVersionCount != 0 {
		t.Errorf("DatasetVersionCount: want 0 (no datasets), got %d", proj.DatasetVersionCount)
	}
}

func TestProjectServiceCountsDatasetsSeparatelyFromVersions(t *testing.T) {
	memStore := store.NewMemoryStore()
	svc := NewProjectService(memStore, "", "")
	now := time.Now().UTC()
	if err := memStore.SaveProject(domain.Project{
		ProjectID: "p-counts", Name: "counts", CreatedAt: now,
	}); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	for _, datasetID := range []string{"d1", "d2"} {
		if err := memStore.SaveDataset(domain.Dataset{
			ProjectID: "p-counts", DatasetID: datasetID, Name: datasetID, DataType: "unstructured", CreatedAt: now,
		}); err != nil {
			t.Fatalf("SaveDataset %s: %v", datasetID, err)
		}
	}
	for _, version := range []domain.DatasetVersion{
		{ProjectID: "p-counts", DatasetID: "d1", DatasetVersionID: "v1", StorageURI: "v1.csv", CreatedAt: now},
		{ProjectID: "p-counts", DatasetID: "d1", DatasetVersionID: "v2", StorageURI: "v2.csv", CreatedAt: now},
		{ProjectID: "p-counts", DatasetID: "d2", DatasetVersionID: "v3", StorageURI: "v3.csv", CreatedAt: now},
	} {
		if err := memStore.SaveDatasetVersion(version); err != nil {
			t.Fatalf("SaveDatasetVersion %s: %v", version.DatasetVersionID, err)
		}
	}

	proj, err := svc.GetProject("p-counts")
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if proj.DatasetCount != 2 {
		t.Errorf("DatasetCount: want 2, got %d", proj.DatasetCount)
	}
	if proj.DatasetVersionCount != 3 {
		t.Errorf("DatasetVersionCount: want 3, got %d", proj.DatasetVersionCount)
	}
}

func TestProjectServiceListProjectsIncludesAnalysisThreadCount(t *testing.T) {
	memStore := store.NewMemoryStore()
	svc := NewProjectService(memStore, "", "")

	now := time.Now().UTC()
	for _, pid := range []string{"p1", "p2"} {
		if err := memStore.SaveProject(domain.Project{
			ProjectID: pid, Name: pid, CreatedAt: now,
		}); err != nil {
			t.Fatalf("SaveProject %s: %v", pid, err)
		}
	}
	// p1: 1 thread / p2: 0 thread
	if err := memStore.SaveAnalysisThread(domain.AnalysisThread{
		ThreadID: "t1", ProjectID: "p1", DatasetID: "d1",
		DatasetVersionID: "v1", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("SaveAnalysisThread: %v", err)
	}

	resp, err := svc.ListProjects()
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	counts := map[string]int{}
	for _, p := range resp.Items {
		counts[p.ProjectID] = p.AnalysisThreadCount
	}
	if counts["p1"] != 1 {
		t.Errorf("p1: want 1, got %d", counts["p1"])
	}
	if counts["p2"] != 0 {
		t.Errorf("p2: want 0 (no threads), got %d", counts["p2"])
	}
}

func TestProjectServiceGetProjectEmptyThreadCountIsZero(t *testing.T) {
	memStore := store.NewMemoryStore()
	svc := NewProjectService(memStore, "", "")

	if err := memStore.SaveProject(domain.Project{
		ProjectID: "p-empty", Name: "empty", CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	proj, err := svc.GetProject("p-empty")
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if proj.AnalysisThreadCount != 0 {
		t.Errorf("empty project: AnalysisThreadCount must be 0, got %d", proj.AnalysisThreadCount)
	}
}
