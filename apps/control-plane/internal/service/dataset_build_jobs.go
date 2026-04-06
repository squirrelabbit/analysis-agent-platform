package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

const (
	datasetBuildTypePrepare   = "prepare"
	datasetBuildTypeSentiment = "sentiment"
	datasetBuildTypeEmbedding = "embedding"
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
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	go s.runDatasetBuildJob(job, func() error {
		_, err := s.BuildPrepare(projectID, datasetID, datasetVersionID, input)
		return err
	})
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
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	go s.runDatasetBuildJob(job, func() error {
		_, err := s.BuildSentiment(projectID, datasetID, datasetVersionID, input)
		return err
	})
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
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	go s.runDatasetBuildJob(job, func() error {
		_, err := s.BuildEmbeddings(projectID, datasetID, datasetVersionID, input)
		return err
	})
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
	return job, nil
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
