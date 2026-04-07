package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

const (
	datasetBuildTypePrepare   = "prepare"
	datasetBuildTypeSentiment = "sentiment"
	datasetBuildTypeEmbedding = "embedding"
	datasetBuildTypeCluster   = "cluster"
)

func (s *DatasetService) CreatePrepareJob(projectID, datasetID, datasetVersionID string, input domain.DatasetPrepareRequest, triggeredBy string) (domain.DatasetBuildJob, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if active, err := s.findActiveDatasetBuildJob(projectID, version.DatasetVersionID, datasetBuildTypePrepare); err != nil {
		return domain.DatasetBuildJob{}, err
	} else if active != nil {
		return *active, nil
	}

	job := domain.DatasetBuildJob{
		JobID:            id.New(),
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: datasetVersionID,
		BuildType:        datasetBuildTypePrepare,
		Status:           "queued",
		Request:          requestToMap(input),
		TriggeredBy:      normalizeTriggeredBy(triggeredBy),
		CreatedAt:        time.Now().UTC(),
	}
	version.PrepareStatus = "queued"
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.dispatchDatasetBuildJob(job, func() error {
		_, err := s.BuildPrepare(projectID, datasetID, datasetVersionID, input)
		return err
	}); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return job, nil
}

func (s *DatasetService) CreateSentimentJob(projectID, datasetID, datasetVersionID string, input domain.DatasetSentimentBuildRequest, triggeredBy string) (domain.DatasetBuildJob, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if active, err := s.findActiveDatasetBuildJob(projectID, version.DatasetVersionID, datasetBuildTypeSentiment); err != nil {
		return domain.DatasetBuildJob{}, err
	} else if active != nil {
		return *active, nil
	}

	job := domain.DatasetBuildJob{
		JobID:            id.New(),
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: datasetVersionID,
		BuildType:        datasetBuildTypeSentiment,
		Status:           "queued",
		Request:          requestToMap(input),
		TriggeredBy:      normalizeTriggeredBy(triggeredBy),
		CreatedAt:        time.Now().UTC(),
	}
	version.SentimentStatus = "queued"
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.dispatchDatasetBuildJob(job, func() error {
		_, err := s.BuildSentiment(projectID, datasetID, datasetVersionID, input)
		return err
	}); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return job, nil
}

func (s *DatasetService) CreateEmbeddingJob(projectID, datasetID, datasetVersionID string, input domain.DatasetEmbeddingBuildRequest, triggeredBy string) (domain.DatasetBuildJob, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if active, err := s.findActiveDatasetBuildJob(projectID, version.DatasetVersionID, datasetBuildTypeEmbedding); err != nil {
		return domain.DatasetBuildJob{}, err
	} else if active != nil {
		return *active, nil
	}

	job := domain.DatasetBuildJob{
		JobID:            id.New(),
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: datasetVersionID,
		BuildType:        datasetBuildTypeEmbedding,
		Status:           "queued",
		Request:          requestToMap(input),
		TriggeredBy:      normalizeTriggeredBy(triggeredBy),
		CreatedAt:        time.Now().UTC(),
	}
	version.EmbeddingStatus = "queued"
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.dispatchDatasetBuildJob(job, func() error {
		_, err := s.BuildEmbeddings(projectID, datasetID, datasetVersionID, input)
		return err
	}); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return job, nil
}

func (s *DatasetService) CreateClusterJob(projectID, datasetID, datasetVersionID string, input domain.DatasetClusterBuildRequest, triggeredBy string) (domain.DatasetBuildJob, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if active, err := s.findActiveDatasetBuildJob(projectID, version.DatasetVersionID, datasetBuildTypeCluster); err != nil {
		return domain.DatasetBuildJob{}, err
	} else if active != nil {
		return *active, nil
	}

	job := domain.DatasetBuildJob{
		JobID:            id.New(),
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: datasetVersionID,
		BuildType:        datasetBuildTypeCluster,
		Status:           "queued",
		Request:          requestToMap(input),
		TriggeredBy:      normalizeTriggeredBy(triggeredBy),
		CreatedAt:        time.Now().UTC(),
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["cluster_status"] = "queued"
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.dispatchDatasetBuildJob(job, func() error {
		_, err := s.BuildClusters(projectID, datasetID, datasetVersionID, input)
		return err
	}); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return job, nil
}

func (s *DatasetService) GetDatasetBuildJob(projectID, jobID string) (domain.DatasetBuildJob, error) {
	job, err := s.store.GetDatasetBuildJob(projectID, jobID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.DatasetBuildJob{}, ErrNotFound{Resource: "dataset build job"}
		}
		return domain.DatasetBuildJob{}, err
	}
	return withBuildJobDiagnostics(job), nil
}

func (s *DatasetService) ListDatasetBuildJobs(projectID, datasetID, datasetVersionID string) (domain.DatasetBuildJobListResponse, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJobListResponse{}, err
	}
	items, err := s.store.ListDatasetBuildJobs(projectID, version.DatasetVersionID)
	if err != nil {
		return domain.DatasetBuildJobListResponse{}, err
	}
	for index := range items {
		items[index] = withBuildJobDiagnostics(items[index])
	}
	return domain.DatasetBuildJobListResponse{Items: items}, nil
}

func (s *DatasetService) findActiveDatasetBuildJob(projectID, datasetVersionID, buildType string) (*domain.DatasetBuildJob, error) {
	items, err := s.store.ListDatasetBuildJobs(projectID, datasetVersionID)
	if err != nil {
		return nil, err
	}
	for _, job := range items {
		if job.BuildType != buildType {
			continue
		}
		if job.Status == "queued" || job.Status == "running" {
			jobCopy := job
			return &jobCopy, nil
		}
	}
	return nil, nil
}

func (s *DatasetService) runDatasetBuildJob(job domain.DatasetBuildJob, runner func() error) {
	startedAt := time.Now().UTC()
	job.Status = "running"
	job.StartedAt = &startedAt
	job.ErrorMessage = nil
	_ = s.store.SaveDatasetBuildJob(job)

	var runErr error
	defer func() {
		if recovered := recover(); recovered != nil {
			runErr = fmt.Errorf("dataset build job panic: %v", recovered)
		}
		completedAt := time.Now().UTC()
		job.CompletedAt = &completedAt
		if runErr != nil {
			job.Status = "failed"
			message := runErr.Error()
			job.ErrorMessage = &message
		} else {
			job.Status = "completed"
			job.ErrorMessage = nil
		}
		_ = s.store.SaveDatasetBuildJob(job)
	}()

	runErr = runner()
}

func (s *DatasetService) dispatchDatasetBuildJob(job domain.DatasetBuildJob, fallbackRunner func() error) error {
	if s.buildJobStarter != nil && s.buildJobStarter.EngineName() == "temporal" {
		workflowID, err := s.buildJobStarter.StartDatasetBuildWorkflow(workflows.StartDatasetBuildInput{
			JobID:            job.JobID,
			ProjectID:        job.ProjectID,
			DatasetID:        job.DatasetID,
			DatasetVersionID: job.DatasetVersionID,
			BuildType:        job.BuildType,
		})
		if err == nil {
			if strings.TrimSpace(workflowID) != "" {
				job.WorkflowID = &workflowID
			}
			job.ErrorMessage = nil
			job.LastErrorType = nil
			if saveErr := s.store.SaveDatasetBuildJob(job); saveErr != nil {
				return saveErr
			}
			return nil
		}
		completedAt := time.Now().UTC()
		job.Status = "failed"
		job.CompletedAt = &completedAt
		message := fmt.Sprintf("failed to start dataset build workflow: %v", err)
		job.ErrorMessage = &message
		errorType := "workflow_start_failed"
		job.LastErrorType = &errorType
		if saveErr := s.store.SaveDatasetBuildJob(job); saveErr != nil {
			return saveErr
		}
		return err
	}
	go s.runDatasetBuildJob(job, fallbackRunner)
	return nil
}

func requestToMap(payload any) map[string]any {
	raw, err := json.Marshal(payload)
	if err != nil {
		return map[string]any{}
	}
	decoded := make(map[string]any)
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return map[string]any{}
	}
	for key, value := range decoded {
		if value == nil {
			delete(decoded, key)
		}
	}
	return decoded
}

func normalizeTriggeredBy(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "system"
	}
	return trimmed
}

func withBuildJobDiagnostics(job domain.DatasetBuildJob) domain.DatasetBuildJob {
	retryCount := job.Attempt - 1
	if retryCount < 0 {
		retryCount = 0
	}
	job.Diagnostics = &domain.BuildJobDiagnostics{
		RetryCount:            retryCount,
		LastErrorType:         cloneStringPointer(job.LastErrorType),
		LastErrorMessage:      cloneStringPointer(job.ErrorMessage),
		WorkflowID:            cloneStringPointer(job.WorkflowID),
		WorkflowRunID:         cloneStringPointer(job.WorkflowRunID),
		ResumedExecutionCount: job.ResumedExecutionCount,
	}
	return job
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	return &trimmed
}
