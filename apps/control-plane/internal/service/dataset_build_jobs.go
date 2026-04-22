package service

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

const (
	datasetBuildTypeClean     = "clean"
	datasetBuildTypePrepare   = "prepare"
	datasetBuildTypeSentiment = "sentiment"
	datasetBuildTypeEmbedding = "embedding"
	datasetBuildTypeCluster   = "cluster"
)

func (s *DatasetService) CreateCleanJob(projectID, datasetID, datasetVersionID string, input domain.DatasetCleanRequest, triggeredBy string) (domain.DatasetBuildJob, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetBuildJob{}, ErrInvalidArgument{Message: "dataset clean requires unstructured or mixed dataset version"}
	}
	if len(resolveDatasetBuildTextSelection(version.Metadata, input.TextColumns).Columns) == 0 {
		return domain.DatasetBuildJob{}, ErrInvalidArgument{Message: "text_columns is required for dataset clean"}
	}
	if active, err := s.findActiveDatasetBuildJob(projectID, version.DatasetVersionID, datasetBuildTypeClean); err != nil {
		return domain.DatasetBuildJob{}, err
	} else if active != nil {
		return *active, nil
	}

	job := domain.DatasetBuildJob{
		JobID:            id.New(),
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: datasetVersionID,
		BuildType:        datasetBuildTypeClean,
		Status:           "queued",
		Request:          requestToMap(input),
		TriggeredBy:      normalizeTriggeredBy(triggeredBy),
		CreatedAt:        time.Now().UTC(),
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["clean_status"] = "queued"
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if err := s.dispatchDatasetBuildJob(job, func() error {
		_, err := s.BuildClean(projectID, datasetID, datasetVersionID, input)
		return err
	}); err != nil {
		return domain.DatasetBuildJob{}, err
	}
	return job, nil
}

func (s *DatasetService) CreatePrepareJob(projectID, datasetID, datasetVersionID string, input domain.DatasetPrepareRequest, triggeredBy string) (domain.DatasetBuildJob, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetBuildJob{}, err
	}
	if status := cleanStatus(version); status == "queued" || status == "cleaning" || status == "failed" || status == "stale" {
		return domain.DatasetBuildJob{}, ErrInvalidArgument{Message: "dataset clean must be ready before prepare"}
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
	normalizedRequest := domain.NormalizeClusterBuildRequest(input)
	version.Metadata["cluster_status"] = "queued"
	version.Metadata["cluster_similarity_threshold"] = *normalizedRequest.SimilarityThreshold
	version.Metadata["cluster_top_n"] = *normalizedRequest.TopN
	version.Metadata["cluster_sample_n"] = *normalizedRequest.SampleN
	version.Metadata["cluster_params_hash"] = domain.ClusterRequestHash(normalizedRequest)
	delete(version.Metadata, "cluster_stale_reason")
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
	if version, versionErr := s.store.GetDatasetVersion(projectID, job.DatasetVersionID); versionErr == nil {
		return withBuildJobDiagnosticsForVersion(job, version), nil
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
		items[index] = withBuildJobDiagnosticsForVersion(items[index], version)
	}
	return domain.DatasetBuildJobListResponse{Items: items}, nil
}

func (s *DatasetService) latestDatasetVersionBuildJobStatuses(projectID string, version domain.DatasetVersion) ([]domain.DatasetVersionBuildJobStatus, error) {
	items, err := s.store.ListDatasetBuildJobs(projectID, version.DatasetVersionID)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	latestByType := latestDatasetBuildJobsByType(items)
	orderedTypes := []string{
		datasetBuildTypeClean,
		datasetBuildTypePrepare,
		datasetBuildTypeSentiment,
		datasetBuildTypeEmbedding,
		datasetBuildTypeCluster,
	}
	result := make([]domain.DatasetVersionBuildJobStatus, 0, len(latestByType))
	for _, buildType := range orderedTypes {
		job, ok := latestByType[buildType]
		if !ok {
			continue
		}
		result = append(result, datasetVersionBuildJobStatus(withBuildJobDiagnosticsForVersion(job, version)))
	}
	return result, nil
}

func datasetVersionBuildJobStatus(job domain.DatasetBuildJob) domain.DatasetVersionBuildJobStatus {
	return domain.DatasetVersionBuildJobStatus{
		JobID:        job.JobID,
		BuildType:    job.BuildType,
		Status:       job.Status,
		TriggeredBy:  job.TriggeredBy,
		Attempt:      job.Attempt,
		CreatedAt:    job.CreatedAt,
		StartedAt:    job.StartedAt,
		CompletedAt:  job.CompletedAt,
		ErrorMessage: job.ErrorMessage,
		Diagnostics:  job.Diagnostics,
	}
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

func withBuildJobDiagnosticsForVersion(job domain.DatasetBuildJob, version domain.DatasetVersion) domain.DatasetBuildJob {
	job = withBuildJobDiagnostics(job)
	enrichBuildJobDiagnosticsFromVersion(&job, version)
	return job
}

func enrichBuildJobDiagnosticsFromVersion(job *domain.DatasetBuildJob, version domain.DatasetVersion) {
	prefix := buildJobMetadataPrefix(job.BuildType)
	if prefix == "" || version.Metadata == nil {
		return
	}
	if job.Diagnostics == nil {
		job.Diagnostics = &domain.BuildJobDiagnostics{}
	}
	if progress := loadBuildJobProgress(version.Metadata, prefix); progress != nil {
		job.Diagnostics.Progress = progress
	}
	if !metadataBool(version.Metadata, prefix+"_llm_fallback") {
		return
	}
	job.Diagnostics.LLMFallback = true
	if count, ok := anyToInt(version.Metadata[prefix+"_llm_fallback_count"]); ok {
		job.Diagnostics.LLMFallbackCount = count
	}
	if reason := metadataString(version.Metadata, prefix+"_llm_fallback_reason", ""); reason != "" {
		job.Diagnostics.LLMFallbackReason = stringPointer(reason)
	}
	if provider := metadataString(version.Metadata, prefix+"_llm_provider", ""); provider != "" {
		job.Diagnostics.LLMProvider = stringPointer(provider)
	}
	if model := metadataString(version.Metadata, prefix+"_llm_model", ""); model != "" {
		job.Diagnostics.LLMModel = stringPointer(model)
	}
	if warning := metadataString(version.Metadata, prefix+"_warning", ""); warning != "" {
		job.Diagnostics.Warnings = append(job.Diagnostics.Warnings, warning)
	}
}

type buildJobProgressFile struct {
	Percent        float64  `json:"percent"`
	ProcessedRows  int      `json:"processed_rows"`
	TotalRows      int      `json:"total_rows"`
	ElapsedSeconds float64  `json:"elapsed_seconds"`
	ETASeconds     *float64 `json:"eta_seconds"`
	Message        string   `json:"message"`
	UpdatedAt      string   `json:"updated_at"`
}

func loadBuildJobProgress(metadata map[string]any, prefix string) *domain.BuildJobProgress {
	progressRef := strings.TrimSpace(metadataString(metadata, prefix+"_progress_ref", ""))
	if progressRef == "" {
		return nil
	}
	raw, err := os.ReadFile(progressRef)
	if err != nil {
		return nil
	}
	var decoded buildJobProgressFile
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil
	}
	progress := &domain.BuildJobProgress{
		Percent:        decoded.Percent,
		ProcessedRows:  decoded.ProcessedRows,
		TotalRows:      decoded.TotalRows,
		ElapsedSeconds: decoded.ElapsedSeconds,
		ETASeconds:     decoded.ETASeconds,
		Message:        strings.TrimSpace(decoded.Message),
	}
	if parsedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(decoded.UpdatedAt)); err == nil {
		progress.UpdatedAt = &parsedAt
	}
	return progress
}

func buildJobMetadataPrefix(buildType string) string {
	switch strings.TrimSpace(buildType) {
	case datasetBuildTypeClean:
		return "clean"
	case datasetBuildTypePrepare:
		return "prepare"
	case datasetBuildTypeSentiment:
		return "sentiment"
	default:
		return ""
	}
}

func metadataBool(metadata map[string]any, key string) bool {
	if metadata == nil {
		return false
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	return &trimmed
}
