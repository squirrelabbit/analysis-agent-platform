package service

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/workflows"
)

// silverone 2026-05-21 δ-2: 옛 helpers 중 dataset_build/analyze_v2 test가 실제로
// 쓰는 minimal 셋만 남긴다. analysis/embedding 흐름 helper는 같이 삭제됨.

type fakeDatasetBuildStarter struct {
	startCalls []workflows.StartDatasetBuildInput
}

func (s *fakeDatasetBuildStarter) StartDatasetBuildWorkflow(input workflows.StartDatasetBuildInput) (string, error) {
	s.startCalls = append(s.startCalls, input)
	return "dataset-build-" + input.JobID, nil
}

func (s *fakeDatasetBuildStarter) EngineName() string {
	return "temporal"
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func datasetStringPtr(value string) *string {
	return &value
}

func datasetBoolPtr(value bool) *bool {
	return &value
}

func waitForDatasetBuildJobStatus(t *testing.T, service *DatasetService, projectID, jobID, expected string) domain.DatasetBuildJob {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := service.GetDatasetBuildJob(projectID, jobID)
		if err == nil && job.Status == expected {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dataset build job %s did not reach status %s", jobID, expected)
	return domain.DatasetBuildJob{}
}

func waitForDatasetVersionCleanReady(t *testing.T, service *DatasetService, projectID, datasetID, versionID string) domain.DatasetVersion {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		version, err := service.GetDatasetVersion(projectID, datasetID, versionID)
		if err == nil && version.CleanStatus == "ready" {
			return version
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dataset version %s did not reach clean ready", versionID)
	return domain.DatasetVersion{}
}
